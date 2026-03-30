from __future__ import annotations

from typing import Any

import paho.mqtt.client as mqtt

from backend.config import BackendConfig
from backend.execution.orchestrator import IrrigationOrchestrator
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.store import StateStore
from shared.contracts.messages import CommandAck, CommandResult, TelemetryMessage


class CommandDispatcher:
    def __init__(self, mqtt_client: mqtt.Client, config: BackendConfig, store: StateStore, safety_validator: SafetyValidator) -> None:
        self._orchestrator = IrrigationOrchestrator(mqtt_client, config, store, safety_validator)

    def dispatch_proposal(self, proposal: ActionProposal) -> dict[str, Any]:
        return self._orchestrator.start(proposal)

    def observe_telemetry(self, message: TelemetryMessage) -> None:
        payload = message.model_dump()
        payload["ts_ms"] = message.ts_ms
        self._orchestrator.observe_telemetry(payload)

    def handle_ack(self, ack: CommandAck) -> dict[str, Any] | None:
        return self._orchestrator.handle_ack(ack)

    def handle_result(self, result: CommandResult) -> dict[str, Any] | None:
        return self._orchestrator.handle_result(result)

    def handle_device_offline(self, device_id: str, zone_id: str | None, reason: str, trace_id: str) -> None:
        self._orchestrator.handle_device_offline(device_id, zone_id, reason, trace_id)

    def recover_active_executions(self) -> None:
        self._orchestrator.recover_active_executions()

    def sweep(self) -> None:
        self._orchestrator.sweep()
