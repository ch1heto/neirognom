from __future__ import annotations

import logging
import uuid
from typing import Any

from backend.config import BackendConfig
from backend.safety.validator import ActionProposal
from backend.state.store import StateStore
from integrations.llama.client import LlamaDecisionClient
from shared.contracts.messages import AlertEvent, DecisionOrigin, LlmDecisionRequest, TelemetryMessage


log = logging.getLogger("backend.decision")


class DecisionEngine:
    def __init__(self, config: BackendConfig, store: StateStore, llama_client: LlamaDecisionClient) -> None:
        self._config = config
        self._store = store
        self._llama = llama_client

    def evaluate_telemetry(self, message: TelemetryMessage) -> ActionProposal | None:
        telemetry = message.sensors
        zone_state = self._store.get_zone_state(message.zone_id)

        deterministic = self._deterministic_rule(message, zone_state, telemetry)
        if deterministic is not None:
            return deterministic

        llm_request = LlmDecisionRequest(
            trace_id=message.trace_id,
            device_id=message.device_id,
            zone_id=message.zone_id,
            current_state=self._store.get_current_state(),
            telemetry_window=self._store.get_sensor_history(
                message.device_id,
                sensor="soil_moisture",
                start_ms=max(0, message.ts_ms - 3600_000),
                end_ms=message.ts_ms,
                limit=50,
            ),
            zone_limits={k: v for k, v in zone_state.items() if k not in {"telemetry", "device_state"}},
            global_limits=self._store.get_current_state().get("global", {}),
        )
        response = self._llama.recommend(llm_request)
        if response is None or response.decision == "no_action":
            return None

        actuator = response.actuator or self._default_actuator(response.decision)
        action = response.action or self._default_action(response.decision)
        duration_sec = int(response.duration_sec or 0)
        log.info(
            "llama recommendation trace_id=%s zone_id=%s decision=%s actuator=%s duration_sec=%s confidence=%.2f",
            message.trace_id,
            response.zone_id,
            response.decision,
            actuator,
            duration_sec,
            response.confidence,
        )
        return ActionProposal(
            trace_id=message.trace_id,
            device_id=message.device_id,
            zone_id=response.zone_id,
            actuator=actuator,
            action=action,
            duration_sec=duration_sec,
            origin=DecisionOrigin.LLAMA,
            reason=response.reason,
            requested_at_ms=message.ts_ms,
            metadata={"confidence": response.confidence, "decision": response.decision},
        )

    def _deterministic_rule(
        self,
        message: TelemetryMessage,
        zone_state: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> ActionProposal | None:
        if telemetry.get("leak") is True:
            self._record_alert(message, "emergency", "leak", "Leak detected; emergency stop requested")
            return ActionProposal(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                actuator="master_pump",
                action="OFF",
                duration_sec=0,
                origin=DecisionOrigin.DETERMINISTIC,
                reason="leak detected",
                requested_at_ms=message.ts_ms,
            )

        if telemetry.get("overflow") is True:
            self._record_alert(message, "critical", "overflow", "Overflow detected; close irrigation valve")
            return ActionProposal(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                actuator="irrigation_valve",
                action="CLOSE",
                duration_sec=0,
                origin=DecisionOrigin.DETERMINISTIC,
                reason="overflow detected",
                requested_at_ms=message.ts_ms,
            )

        tank_level = telemetry.get("tank_level")
        if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
            self._record_alert(message, "critical", "tank_level", "Water source unavailable; irrigation blocked")
            return None

        valve_open_ms = telemetry.get("valve_open_ms")
        max_duration_ms = int(zone_state.get("max_duration_per_run_sec", 0)) * 1000
        if isinstance(valve_open_ms, (int, float)) and max_duration_ms > 0 and float(valve_open_ms) > max_duration_ms:
            self._record_alert(message, "warning", "valve_timeout", "Valve open too long; forcing close")
            return ActionProposal(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                actuator="irrigation_valve",
                action="CLOSE",
                duration_sec=0,
                origin=DecisionOrigin.DETERMINISTIC,
                reason="valve already open too long",
                requested_at_ms=message.ts_ms,
            )

        temperature = telemetry.get("temperature")
        if isinstance(temperature, (int, float)) and float(temperature) >= 30.0:
            return ActionProposal(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                actuator="vent_fan",
                action="ON",
                duration_sec=30,
                origin=DecisionOrigin.DETERMINISTIC,
                reason="temperature too high",
                requested_at_ms=message.ts_ms,
            )

        soil_moisture = telemetry.get("soil_moisture")
        if isinstance(soil_moisture, (int, float)) and float(soil_moisture) >= 35.0:
            return None

        return None

    def _record_alert(self, message: TelemetryMessage, severity: str, category: str, text: str) -> None:
        self._store.record_alert(
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

    @staticmethod
    def _default_actuator(decision: str) -> str:
        return {
            "water_zone": "irrigation_valve",
            "stop_zone": "irrigation_valve",
            "ventilate_zone": "vent_fan",
            "block_zone": "master_pump",
        }.get(decision, "irrigation_valve")

    @staticmethod
    def _default_action(decision: str) -> str:
        return {
            "water_zone": "OPEN",
            "stop_zone": "CLOSE",
            "ventilate_zone": "ON",
            "block_zone": "OFF",
        }.get(decision, "OPEN")
