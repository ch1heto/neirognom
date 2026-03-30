from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any

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
        self._critical_thresholds = self._load_critical_thresholds()

    def _load_critical_thresholds(self) -> dict[str, Any]:
        path = Path(__file__).resolve().parents[2] / "knowledge_base" / "alerts" / "critical_thresholds.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("critical thresholds load failed path=%s error=%s", path, exc)
            return {}
        return payload if isinstance(payload, dict) else {}

    def _threshold_value(self, sensor_name: str, primary_key: str, fallback_key: str, default: float | None) -> float | None:
        sensors = self._critical_thresholds.get("sensors") or {}
        sensor_thresholds = sensors.get(sensor_name)
        if not isinstance(sensor_thresholds, dict):
            thresholds = self._critical_thresholds.get("thresholds") or {}
            sensor_thresholds = thresholds.get(sensor_name)
        if not isinstance(sensor_thresholds, dict):
            return default
        value = sensor_thresholds.get(primary_key)
        if value is None:
            value = sensor_thresholds.get(fallback_key)
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

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
        proposal: ActionProposal | None = None

        water_level = telemetry.get("water_level")
        water_level_min_critical = self._threshold_value("water_level", "min_critical", "critical_low", 10.0)
        if isinstance(water_level, (int, float)) and water_level_min_critical is not None and float(water_level) <= water_level_min_critical:
            self._record_alert(message, "critical", "water_level", f"Water level critically low: {float(water_level):.2f} <= {water_level_min_critical:.2f}")
            if zone_state.get("device_state", {}).get("valve_open"):
                proposal = ActionProposal(
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

        ph = telemetry.get("ph")
        ph_min_critical = self._threshold_value("ph", "min_critical", "critical_low", None)
        ph_max_critical = self._threshold_value("ph", "max_critical", "critical_high", None)
        if isinstance(ph, (int, float)):
            ph_value = float(ph)
            if ph_min_critical is not None and ph_value <= ph_min_critical:
                self._record_alert(message, "critical", "ph", f"pH critically low: {ph_value:.2f} <= {ph_min_critical:.2f}")
                if zone_state.get("device_state", {}).get("valve_open") and proposal is None:
                    proposal = ActionProposal(
                        message.trace_id,
                        message.device_id,
                        message.zone_id,
                        "irrigation_valve",
                        "CLOSE",
                        0,
                        DecisionOrigin.DETERMINISTIC,
                        "pH critically low",
                        message.ts_ms,
                    )
            elif ph_max_critical is not None and ph_value >= ph_max_critical:
                self._record_alert(message, "critical", "ph", f"pH critically high: {ph_value:.2f} >= {ph_max_critical:.2f}")
                if zone_state.get("device_state", {}).get("valve_open") and proposal is None:
                    proposal = ActionProposal(
                        message.trace_id,
                        message.device_id,
                        message.zone_id,
                        "irrigation_valve",
                        "CLOSE",
                        0,
                        DecisionOrigin.DETERMINISTIC,
                        "pH critically high",
                        message.ts_ms,
                    )

        ec = telemetry.get("ec")
        ec_min_critical = self._threshold_value("ec", "min_critical", "critical_low", None)
        ec_max_critical = self._threshold_value("ec", "max_critical", "critical_high", None)
        if isinstance(ec, (int, float)):
            ec_value = float(ec)
            if ec_min_critical is not None and ec_value <= ec_min_critical:
                self._record_alert(message, "critical", "ec", f"EC critically low: {ec_value:.2f} <= {ec_min_critical:.2f}")
                if zone_state.get("device_state", {}).get("valve_open") and proposal is None:
                    proposal = ActionProposal(
                        message.trace_id,
                        message.device_id,
                        message.zone_id,
                        "irrigation_valve",
                        "CLOSE",
                        0,
                        DecisionOrigin.DETERMINISTIC,
                        "EC critically low",
                        message.ts_ms,
                    )
            elif ec_max_critical is not None and ec_value >= ec_max_critical:
                self._record_alert(message, "critical", "ec", f"EC critically high: {ec_value:.2f} >= {ec_max_critical:.2f}")
                if zone_state.get("device_state", {}).get("valve_open") and proposal is None:
                    proposal = ActionProposal(
                        message.trace_id,
                        message.device_id,
                        message.zone_id,
                        "irrigation_valve",
                        "CLOSE",
                        0,
                        DecisionOrigin.DETERMINISTIC,
                        "EC critically high",
                        message.ts_ms,
                    )

        return proposal

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
