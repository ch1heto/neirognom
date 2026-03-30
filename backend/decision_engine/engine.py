from __future__ import annotations

import logging
import uuid

from backend.config import BackendConfig
from backend.decision_engine.context_builder import DecisionContextBuilder
from backend.safety.validator import ActionProposal
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from integrations.llama.client import LlamaDecisionClient
from shared.contracts.messages import AlertEvent, DecisionOrigin, TelemetryMessage


log = logging.getLogger("backend.decision")


class DecisionEngine:
    def __init__(self, config: BackendConfig, store: StateStore, telemetry_history: TelemetryHistoryStore, llama_client: LlamaDecisionClient) -> None:
        self._config = config
        self._store = store
        self._telemetry_history = telemetry_history
        self._llama = llama_client
        self._context_builder = DecisionContextBuilder(config, store, telemetry_history)

    def evaluate_telemetry(self, message: TelemetryMessage) -> ActionProposal | None:
        telemetry = message.sensors
        zone_state = self._store.get_zone_state(message.zone_id)

        deterministic = self._deterministic_rule(message, zone_state)
        if deterministic is not None:
            return deterministic

        llm_request = self._context_builder.build(message)
        response = self._llama.recommend(llm_request)
        if response is None or response.decision == "no_action":
            return None

        if response.decision == "water_zone":
            return ActionProposal(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=response.zone_id,
                actuator="irrigation_sequence",
                action="START",
                duration_sec=int(response.duration_sec or 0),
                origin=DecisionOrigin.LLAMA,
                reason=response.reason,
                requested_at_ms=message.ts_ms,
                metadata={"confidence": response.confidence, "decision": response.decision},
            )

        actuator = response.actuator or self._default_actuator(response.decision)
        action = response.action or self._default_action(response.decision)
        return ActionProposal(
            trace_id=message.trace_id,
            device_id=message.device_id,
            zone_id=response.zone_id,
            actuator=actuator,
            action=action,
            duration_sec=int(response.duration_sec or 0),
            origin=DecisionOrigin.LLAMA,
            reason=response.reason,
            requested_at_ms=message.ts_ms,
            metadata={"confidence": response.confidence, "decision": response.decision},
        )

    def _deterministic_rule(self, message: TelemetryMessage, zone_state: dict) -> ActionProposal | None:
        telemetry = message.sensors
        if telemetry.get("leak") is True:
            self._record_alert(message, "emergency", "leak", "Leak detected; abort irrigation and stop pump")
            return ActionProposal(message.trace_id, message.device_id, message.zone_id, "master_pump", "OFF", 0, DecisionOrigin.DETERMINISTIC, "leak detected", message.ts_ms)
        if telemetry.get("overflow") is True:
            self._record_alert(message, "critical", "overflow", "Overflow detected; close zone valve")
            return ActionProposal(message.trace_id, message.device_id, message.zone_id, "irrigation_valve", "CLOSE", 0, DecisionOrigin.DETERMINISTIC, "overflow detected", message.ts_ms)
        tank_level = telemetry.get("tank_level")
        if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
            self._record_alert(message, "critical", "tank_level", "Water source unavailable")
            return None
        temperature = telemetry.get("temperature")
        if isinstance(temperature, (int, float)) and float(temperature) >= 30.0:
            return ActionProposal(message.trace_id, message.device_id, message.zone_id, "vent_fan", "ON", 30, DecisionOrigin.DETERMINISTIC, "temperature too high", message.ts_ms)
        soil_moisture = telemetry.get("soil_moisture")
        if isinstance(soil_moisture, (int, float)) and float(soil_moisture) < 20.0 and not zone_state.get("blocked"):
            return ActionProposal(message.trace_id, message.device_id, message.zone_id, "irrigation_sequence", "START", 10, DecisionOrigin.DETERMINISTIC, "soil moisture below threshold", message.ts_ms)
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

    @staticmethod
    def _default_actuator(decision: str) -> str:
        return {"stop_zone": "irrigation_valve", "ventilate_zone": "vent_fan", "block_zone": "master_pump"}.get(decision, "irrigation_valve")

    @staticmethod
    def _default_action(decision: str) -> str:
        return {"stop_zone": "CLOSE", "ventilate_zone": "ON", "block_zone": "OFF"}.get(decision, "OPEN")
