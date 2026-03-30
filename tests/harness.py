from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

import paho.mqtt.client as mqtt

from backend.api.tools import BackendToolService
from backend.config import load_backend_config
from backend.decision_engine.engine import DecisionEngine
from backend.dispatcher.service import CommandDispatcher
from backend.ingestion.service import IngestionService
from backend.operator.service import OperatorControlService
from backend.security.monitor import SecurityMonitor
from backend.safety.validator import SafetyValidator
from backend.state.influx import MemoryTelemetryHistoryStore
from backend.state.store import MemoryStateStore
from integrations.openclaw_mcp.server import OpenClawMcpServer
from integrations.openclaw_mcp.tools import OpenClawMcpAdapter
from mqtt.topics import command_ack_topic, command_result_topic, state_topic, telemetry_topic
from shared.contracts.messages import LlmDecisionResponse


class DummyPublishResult:
    def __init__(self, rc: int = mqtt.MQTT_ERR_SUCCESS) -> None:
        self.rc = rc


class DummyMqttClient:
    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    def publish(self, topic: str, payload: str, qos: int = 1):
        self.published.append({"topic": topic, "payload": json.loads(payload), "qos": qos})
        return DummyPublishResult()


class StubLlamaClient:
    def __init__(self) -> None:
        self.response: LlmDecisionResponse | dict[str, Any] | None = None
        self.calls: list[Any] = []

    def recommend(self, request):
        self.calls.append(request)
        if self.response is None or isinstance(self.response, LlmDecisionResponse):
            return self.response
        try:
            return LlmDecisionResponse.model_validate(self.response)
        except Exception:
            return None


class BackendTestHarness:
    def __init__(self, *, command_ttl_sec: int | None = None, max_simultaneous_zones: int | None = None, mcp_action_token: str = "test-mcp-token") -> None:
        config = load_backend_config()
        global_safety = config.global_safety
        openclaw_mcp = config.openclaw_mcp
        if command_ttl_sec is not None:
            global_safety = replace(global_safety, command_ttl_sec=command_ttl_sec)
        if max_simultaneous_zones is not None:
            global_safety = replace(global_safety, max_simultaneous_zones=max_simultaneous_zones)
        openclaw_mcp = replace(openclaw_mcp, action_auth_token=mcp_action_token, require_action_token=True)
        self.config = replace(config, global_safety=global_safety, openclaw_mcp=openclaw_mcp)

        self.store = MemoryStateStore()
        self.store.initialize(self.config.zone_configs(), self.config.global_safety)
        self.mqtt = DummyMqttClient()
        self.telemetry_history = MemoryTelemetryHistoryStore()
        self.security_monitor = SecurityMonitor(self.config, self.store, self.telemetry_history)
        self.validator = SafetyValidator(self.config)
        self.llama = StubLlamaClient()
        self.decision_engine = DecisionEngine(self.config, self.store, self.telemetry_history, self.llama)
        self.dispatcher = CommandDispatcher(self.mqtt, self.config, self.store, self.validator)
        self.ingestion = IngestionService(self.store, self.telemetry_history, self.decision_engine, self.dispatcher, self.security_monitor)
        self.tools = BackendToolService(self.store, self.telemetry_history, self.dispatcher)
        self.operator = OperatorControlService(self.config, self.store, self.telemetry_history, self.tools)
        self.mcp_adapter = OpenClawMcpAdapter(self.operator, self.config.openclaw_mcp)
        self.mcp_server = OpenClawMcpServer(
            "127.0.0.1",
            0,
            self.mcp_adapter,
            server_name=self.config.openclaw_mcp.server_name,
            server_version=self.config.openclaw_mcp.server_version,
            lazy_bind=True,
        )

    def seed_device_state(self, payload: dict[str, Any]) -> None:
        self.ingest_state(payload)

    def ingest_telemetry(self, payload: dict[str, Any]) -> None:
        self.ingestion.ingest(telemetry_topic(str(payload["device_id"])), json.dumps(payload).encode("utf-8"))

    def ingest_state(self, payload: dict[str, Any]) -> None:
        self.ingestion.ingest(state_topic(str(payload["device_id"])), json.dumps(payload).encode("utf-8"))

    def ingest_ack(self, payload: dict[str, Any]) -> None:
        self.ingestion.ingest(command_ack_topic(str(payload["device_id"])), json.dumps(payload).encode("utf-8"))

    def ingest_result(self, payload: dict[str, Any]) -> None:
        self.ingestion.ingest(command_result_topic(str(payload["device_id"])), json.dumps(payload).encode("utf-8"))

    def execute_manual_action(self, action: dict[str, Any]) -> dict[str, Any]:
        return self.tools.execute_manual_action(action)

    def last_command_message(self) -> dict[str, Any]:
        if not self.mqtt.published:
            raise AssertionError("No MQTT messages were published")
        return self.mqtt.published[-1]["payload"]

    def command_status(self, command_id: str) -> dict[str, Any] | None:
        return self.store.get_command_status(command_id)

    def latest_command_status(self) -> dict[str, Any]:
        payload = self.last_command_message()
        status = self.command_status(str(payload["command_id"]))
        if status is None:
            raise AssertionError(f"Missing command status for {payload['command_id']}")
        return status
