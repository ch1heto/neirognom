from __future__ import annotations

import logging
import time

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from backend.api.tools import BackendToolService
from backend.config import load_backend_config
from backend.decision_engine.engine import DecisionEngine
from backend.dispatcher.service import CommandDispatcher
from backend.ingestion.service import IngestionService
from backend.logging_utils import configure_logging
from backend.operator.service import OperatorControlService
from backend.operator.web import OperatorUiServer
from backend.security.monitor import SecurityMonitor
from backend.safety.validator import SafetyValidator
from backend.state.influx import build_telemetry_history_store
from backend.state.store import build_state_store
from integrations.llama.client import LlamaDecisionClient
from integrations.openclaw_mcp.server import OpenClawMcpServer
from integrations.openclaw_mcp.tools import OpenClawMcpAdapter
from mqtt.topics import ingestion_subscription_topics


log = logging.getLogger("backend.runtime")


class BackendRuntime:
    def __init__(self) -> None:
        load_dotenv()
        self.config = load_backend_config()
        configure_logging()

        self.state_store = build_state_store(self.config.sqlite, self.config.state_store_backend)
        self.state_store.initialize(self.config.zone_configs(), self.config.global_safety)
        self.telemetry_history = build_telemetry_history_store(self.config.influx, self.config.telemetry_history_backend)

        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.config.mqtt.client_id)
        if self.config.mqtt.username:
            self.mqtt.username_pw_set(self.config.mqtt.username, self.config.mqtt.password)

        self.safety = SafetyValidator(self.config)
        self.security_monitor = SecurityMonitor(self.config, self.state_store, self.telemetry_history)
        self.llama = LlamaDecisionClient(self.config.llama)
        self.decision_engine = DecisionEngine(self.config, self.state_store, self.telemetry_history, self.llama)
        self.dispatcher = CommandDispatcher(self.mqtt, self.config, self.state_store, self.safety)
        self.ingestion = IngestionService(self.state_store, self.telemetry_history, self.decision_engine, self.dispatcher, self.security_monitor, self.config.ingestion)
        self.backend_tools = BackendToolService(self.state_store, self.telemetry_history, self.dispatcher)
        self.operator_service = OperatorControlService(self.config, self.state_store, self.telemetry_history, self.backend_tools)
        self.openclaw_adapter = OpenClawMcpAdapter(self.operator_service, self.config.openclaw_mcp)
        self.operator_ui = (
            OperatorUiServer(self.config.operator_ui.host, self.config.operator_ui.port, self.operator_service)
            if self.config.operator_ui.enabled
            else None
        )
        self.openclaw_mcp_server = (
            OpenClawMcpServer(
                self.config.openclaw_mcp.host,
                self.config.openclaw_mcp.port,
                self.openclaw_adapter,
                server_name=self.config.openclaw_mcp.server_name,
                server_version=self.config.openclaw_mcp.server_version,
            )
            if self.config.openclaw_mcp.enabled
            else None
        )

        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        reason_text = str(reason_code)
        try:
            reason_code_value = int(reason_code)
        except (TypeError, ValueError):
            reason_code_value = 0 if str(reason_code).lower() in {"success", "0"} else 1
        if reason_code_value != 0:
            self.security_monitor.auth_failed(reason_text)
            log.error("backend mqtt connect rejected reason_code=%s", reason_code)
            return
        subscriptions = ingestion_subscription_topics(self.config.mqtt.qos_default)
        for topic, qos in subscriptions:
            client.subscribe(topic, qos=qos)
        self.security_monitor.broker_connected()
        self.dispatcher.recover_active_executions()
        log.info("backend mqtt connected host=%s port=%s reason_code=%s", self.config.mqtt.host, self.config.mqtt.port, reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self.security_monitor.broker_disconnected(str(reason_code))
        log.warning("backend mqtt disconnected reason_code=%s", reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        self.ingestion.ingest(msg.topic, msg.payload)

    def run(self) -> None:
        log.info(
            "backend startup mqtt=%s:%d sqlite=%s influx_enabled=%s llama=%s operator_ui=%s openclaw_mcp=%s zones=%s",
            self.config.mqtt.host,
            self.config.mqtt.port,
            self.config.sqlite.path,
            self.config.influx.enabled,
            self.config.llama.api_url,
            self.operator_ui.url if self.operator_ui else "disabled",
            self.openclaw_mcp_server.url if self.openclaw_mcp_server else "disabled",
            ",".join(self.config.zone_ids),
        )
        if self.operator_ui is not None:
            self.operator_ui.start()
        if self.openclaw_mcp_server is not None:
            self.openclaw_mcp_server.start()
        self.mqtt.connect(self.config.mqtt.host, self.config.mqtt.port, keepalive=60)
        self.mqtt.loop_start()
        try:
            while True:
                self.security_monitor.sweep()
                self.dispatcher.sweep()
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("backend shutdown requested")
        finally:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
            if self.openclaw_mcp_server is not None:
                self.openclaw_mcp_server.stop()
            if self.operator_ui is not None:
                self.operator_ui.stop()
