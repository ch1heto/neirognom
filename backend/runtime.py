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
from backend.safety.validator import SafetyValidator
from backend.state.store import build_state_store
from integrations.llama.client import LlamaDecisionClient
from integrations.openclaw_mcp.tools import OpenClawMcpAdapter


log = logging.getLogger("backend.runtime")


class BackendRuntime:
    def __init__(self) -> None:
        load_dotenv()
        self.config = load_backend_config()
        configure_logging()

        self.store = build_state_store(self.config.database)
        self.store.initialize(self.config.zone_configs(), self.config.global_safety)

        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.config.mqtt.client_id)
        if self.config.mqtt.username:
            self.mqtt.username_pw_set(self.config.mqtt.username, self.config.mqtt.password)

        self.safety = SafetyValidator(self.config)
        self.llama = LlamaDecisionClient(self.config.llama)
        self.decision_engine = DecisionEngine(self.config, self.store, self.llama)
        self.dispatcher = CommandDispatcher(self.mqtt, self.config, self.store, self.safety)
        self.ingestion = IngestionService(self.store, self.decision_engine, self.dispatcher)
        self.backend_tools = BackendToolService(self.store, self.dispatcher)
        self.openclaw_adapter = OpenClawMcpAdapter(self.backend_tools)

        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        subscriptions = [
            ("greenhouse/device/+/telemetry/raw", self.config.mqtt.qos_default),
            ("greenhouse/device/+/state", self.config.mqtt.qos_default),
            ("greenhouse/device/+/cmd/ack", self.config.mqtt.qos_default),
            ("greenhouse/device/+/cmd/result", self.config.mqtt.qos_default),
            ("greenhouse/device/+/event/error", self.config.mqtt.qos_default),
        ]
        for topic, qos in subscriptions:
            client.subscribe(topic, qos=qos)
        log.info("backend mqtt connected host=%s port=%s reason_code=%s", self.config.mqtt.host, self.config.mqtt.port, reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        log.warning("backend mqtt disconnected reason_code=%s", reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        self.ingestion.ingest(msg.topic, msg.payload)

    def run(self) -> None:
        log.info(
            "backend startup mqtt=%s:%d db_backend=%s llama=%s openclaw_operator_enabled=%s zones=%s",
            self.config.mqtt.host,
            self.config.mqtt.port,
            self.config.database.backend,
            self.config.llama.api_url,
            self.config.openclaw.enabled,
            ",".join(self.config.zone_ids),
        )
        self.mqtt.connect(self.config.mqtt.host, self.config.mqtt.port, keepalive=60)
        self.mqtt.loop_start()
        try:
            while True:
                self.dispatcher.sweep_expired()
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("backend shutdown requested")
        finally:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
