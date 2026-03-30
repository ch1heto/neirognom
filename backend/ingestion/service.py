from __future__ import annotations

import json
import logging
import time
import uuid

from pydantic import ValidationError

from backend.decision_engine.engine import DecisionEngine
from backend.dispatcher.service import CommandDispatcher
from backend.security.monitor import SecurityMonitor
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from mqtt.topics import CMD_ACK_SUFFIX, CMD_RESULT_SUFFIX, ERROR_SUFFIX, STATE_SUFFIX, TELEMETRY_RAW_SUFFIX, parse_topic
from shared.contracts.messages import CommandAck, CommandResult, DeviceStateMessage, TelemetryMessage


log = logging.getLogger("backend.ingestion")


class IngestionService:
    def __init__(self, state_store: StateStore, telemetry_history: TelemetryHistoryStore, decision_engine: DecisionEngine, dispatcher: CommandDispatcher, security_monitor: SecurityMonitor) -> None:
        self._state_store = state_store
        self._telemetry_history = telemetry_history
        self._decision_engine = decision_engine
        self._dispatcher = dispatcher
        self._security_monitor = security_monitor

    def ingest(self, topic: str, payload_bytes: bytes) -> None:
        parsed = parse_topic(topic)
        if parsed is None:
            log.warning("validation fail invalid_topic=%s", topic)
            return
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            log.error("validation fail topic=%s error=%s", topic, exc)
            return
        if not isinstance(payload, dict):
            log.error("validation fail topic=%s error=payload_must_be_object", topic)
            return

        payload.setdefault("device_id", parsed.device_id)
        payload.setdefault("trace_id", payload.get("message_id") or f"trace-{uuid.uuid4().hex[:12]}")

        try:
            if parsed.channel == TELEMETRY_RAW_SUFFIX:
                payload.setdefault("ts_ms", int(time.time() * 1000))
                message = TelemetryMessage.model_validate(payload)
                if self._state_store.seen_message(message.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": message.message_id, "device_id": message.device_id, "zone_id": message.zone_id, "trace_id": message.trace_id})
                    return
                self._state_store.write_telemetry_snapshot(message)
                self._telemetry_history.write_telemetry(message)
                self._security_monitor.process_telemetry(message)
                self._dispatcher.observe_telemetry(message)
                proposal = self._decision_engine.evaluate_telemetry(message)
                if proposal is not None:
                    self._dispatcher.dispatch_proposal(proposal)
                log.info("ingest receive telemetry device_id=%s zone_id=%s trace_id=%s", message.device_id, message.zone_id, message.trace_id)
                return

            if parsed.channel == STATE_SUFFIX:
                payload.setdefault("ts_ms", int(time.time() * 1000))
                message = DeviceStateMessage.model_validate(payload)
                if self._state_store.seen_message(message.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": message.message_id, "device_id": message.device_id, "zone_id": message.zone_id, "trace_id": message.trace_id})
                    return
                self._state_store.write_device_state(message)
                self._security_monitor.device_seen(message.device_id, message.zone_id, message.trace_id)
                log.info("state update device_id=%s zone_id=%s trace_id=%s", message.device_id, message.zone_id, message.trace_id)
                return

            if parsed.channel == CMD_ACK_SUFFIX:
                ack = CommandAck.model_validate(payload)
                if self._state_store.seen_message(ack.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": ack.message_id, "device_id": ack.device_id, "zone_id": ack.zone_id, "trace_id": ack.trace_id, "command_id": ack.command_id})
                    return
                self._dispatcher.handle_ack(ack)
                log.info("ack receive command_id=%s device_id=%s zone_id=%s", ack.command_id, ack.device_id, ack.zone_id)
                return

            if parsed.channel == CMD_RESULT_SUFFIX:
                result = CommandResult.model_validate(payload)
                if self._state_store.seen_message(result.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": result.message_id, "device_id": result.device_id, "zone_id": result.zone_id, "trace_id": result.trace_id, "command_id": result.command_id})
                    return
                self._dispatcher.handle_result(result)
                log.info("result receive command_id=%s device_id=%s zone_id=%s", result.command_id, result.device_id, result.zone_id)
                return
        except ValidationError as exc:
            self._state_store.note_incident("validation_fail", {"topic": topic, "error": str(exc), "payload": payload})
            log.error("validation fail topic=%s error=%s", topic, exc)
            return

        if parsed.channel == ERROR_SUFFIX:
            self._state_store.note_incident("device_error", payload)
            log.error("device error device_id=%s payload=%s", parsed.device_id, json.dumps(payload, ensure_ascii=False))
            return

        log.warning("ingest receive unhandled_channel=%s topic=%s", parsed.channel, topic)
