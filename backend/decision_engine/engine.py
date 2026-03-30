from __future__ import annotations

import logging
import time
import uuid

from backend.config import BackendConfig
from backend.decision_engine.context_builder import DecisionContextBuilder
from backend.domain.models import AuditLogRecord
from backend.safety.validator import ActionProposal
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from integrations.llama.client import LlamaDecisionClient
from shared.contracts.messages import AlertEvent, DecisionOrigin, LlmDecisionResponse, TelemetryMessage


log = logging.getLogger("backend.decision")


class DecisionEngine:
    def __init__(self, config: BackendConfig, store: StateStore, telemetry_history: TelemetryHistoryStore, llama_client: LlamaDecisionClient) -> None:
        self._config = config
        self._store = store
        self._telemetry_history = telemetry_history
        self._llama = llama_client
        self._context_builder = DecisionContextBuilder(config, store, telemetry_history)

    def evaluate_telemetry(self, message: TelemetryMessage) -> ActionProposal | None:
        zone_state = self._store.get_zone_state(message.zone_id)

        deterministic = self._deterministic_rule(message, zone_state)
        if deterministic is not None:
            return deterministic

        llm_request = self._context_builder.build(message)
        response = self._llama.recommend(llm_request)
        self._record_llama_audit(message, response)
        if response is None or response.decision == "no_action":
            return None
        return self._proposal_from_llm(message, response)

    def _proposal_from_llm(self, message: TelemetryMessage, response: LlmDecisionResponse) -> ActionProposal | None:
        mapping = {
            "open_valve": ("irrigation_valve", "OPEN"),
            "close_valve": ("irrigation_valve", "CLOSE"),
            "dose_solution": ("nutrient_doser", "START"),
        }
        actuator_action = mapping.get(response.decision)
        if actuator_action is None:
            log.error("unsupported llama decision=%s zone_id=%s", response.decision, response.zone_id)
            return None
        actuator, action = actuator_action
        response_zone = self._store.get_zone_state(response.zone_id)
        target_device_id = str(response_zone.get("device_id") or message.device_id)
        return ActionProposal(
            trace_id=message.trace_id,
            device_id=target_device_id,
            zone_id=response.zone_id,
            actuator=actuator,
            action=action,
            duration_sec=int(response.requested_duration_sec or 0),
            origin=DecisionOrigin.LLAMA,
            reason=response.rationale,
            requested_at_ms=message.ts_ms,
            metadata={
                "confidence": response.confidence,
                "decision": response.decision,
                "dose_ml": int(response.dose_ml or 0),
            },
        )

    def _record_llama_audit(self, message: TelemetryMessage, response: LlmDecisionResponse | None) -> None:
        created_at_ms = int(time.time() * 1000)
        if response is None:
            self._store.append_audit_log(
                AuditLogRecord(
                    audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                    trace_id=message.trace_id,
                    action_type="LLAMA_FALLBACK",
                    message="llama recommendation unavailable",
                    created_at_ms=created_at_ms,
                    device_id=message.device_id,
                    zone_id=message.zone_id,
                    payload={
                        "decision": "fallback",
                        "reason": "llama returned no recommendation",
                    },
                )
            )
            return

        payload = response.model_dump()
        payload["reason"] = response.rationale
        action_type = "LLAMA_NO_ACTION" if response.decision == "no_action" else "LLAMA_DECISION"
        self._store.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=message.trace_id,
                action_type=action_type,
                message=response.rationale,
                created_at_ms=created_at_ms,
                device_id=message.device_id,
                zone_id=response.zone_id,
                payload=payload,
            )
        )

    def _deterministic_rule(self, message: TelemetryMessage, zone_state: dict) -> ActionProposal | None:
        telemetry = message.sensors
        water_level = telemetry.get("water_level")
        if isinstance(water_level, (int, float)) and float(water_level) <= 10.0:
            self._record_alert(message, "critical", "water_level", "Low water level detected in the feed line")
            if zone_state.get("device_state", {}).get("valve_open"):
                return ActionProposal(
                    message.trace_id,
                    message.device_id,
                    message.zone_id,
                    "irrigation_valve",
                    "CLOSE",
                    0,
                    DecisionOrigin.DETERMINISTIC,
                    "water level critically low",
                    message.ts_ms,
                )
            return None
        return None

    def _record_alert(self, message: TelemetryMessage, severity: str, category: str, text: str) -> None:
        self._store.record_alarm(
            AlertEvent(
                alert_id=f"alert-{uuid.uuid4().hex[:12]}",
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                severity=severity,
                category=category,
                message=text,
                created_at_ms=message.ts_ms,
                details={"message_id": message.message_id},
            )
        )
