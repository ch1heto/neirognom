from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass

from pydantic import ValidationError

from backend.config import IngestionPolicyConfig
from backend.decision_engine.engine import DecisionEngine
from backend.dispatcher.service import CommandDispatcher
from backend.security.monitor import SecurityMonitor
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from mqtt.topics import ACK_SUFFIX, EVENTS_SUFFIX, PRESENCE_SUFFIX, RESULT_SUFFIX, STATE_SUFFIX, TELEMETRY_SUFFIX, parse_topic
from shared.contracts.messages import CommandAck, CommandResult, DeviceConnectivity, DeviceStateMessage, EventEnvelope, PresenceMessage, TelemetryMessage


log = logging.getLogger("backend.ingestion")


@dataclass(frozen=True)
class TelemetryDisposition:
    kind: str
    reason: str | None = None
    write_history: bool = True
    update_current_state: bool = True
    trigger_decision: bool = True


class IngestionService:
    def __init__(
        self,
        state_store: StateStore,
        telemetry_history: TelemetryHistoryStore,
        decision_engine: DecisionEngine,
        dispatcher: CommandDispatcher,
        security_monitor: SecurityMonitor,
        ingestion_policy: IngestionPolicyConfig | None = None,
    ) -> None:
        self._state_store = state_store
        self._telemetry_history = telemetry_history
        self._decision_engine = decision_engine
        self._dispatcher = dispatcher
        self._security_monitor = security_monitor
        self._policy = ingestion_policy or IngestionPolicyConfig()

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
        payload.setdefault("zone_id", parsed.zone_id)
        payload.setdefault("correlation_id", payload.get("trace_id") or payload.get("message_id") or f"trace-{uuid.uuid4().hex[:12]}")

        try:
            if parsed.channel == TELEMETRY_SUFFIX:
                payload.setdefault("timestamp", int(time.time() * 1000))
                message = TelemetryMessage.model_validate(payload)
                if self._state_store.seen_message(message.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": message.message_id, "device_id": message.device_id, "zone_id": message.zone_id, "trace_id": message.trace_id})
                    return
                disposition = self._classify_telemetry(message)
                self._handle_telemetry(topic, message, disposition)
                return

            if parsed.channel == STATE_SUFFIX:
                payload.setdefault("timestamp", int(time.time() * 1000))
                message = DeviceStateMessage.model_validate(payload)
                if self._state_store.seen_message(message.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": message.message_id, "device_id": message.device_id, "zone_id": message.zone_id, "trace_id": message.trace_id})
                    return
                self._state_store.write_device_state(message)
                self._handle_device_connectivity(message.device_id, message.zone_id, message.connectivity, message.trace_id)
                log.info("state update device_id=%s zone_id=%s trace_id=%s", message.device_id, message.zone_id, message.trace_id)
                return

            if parsed.channel == PRESENCE_SUFFIX:
                payload.setdefault("timestamp", int(time.time() * 1000))
                message = PresenceMessage.model_validate(payload)
                if self._state_store.seen_message(message.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": message.message_id, "device_id": message.device_id, "zone_id": message.zone_id, "trace_id": message.trace_id})
                    return
                self._state_store.write_device_state(
                    DeviceStateMessage(
                        message_id=message.message_id,
                        correlation_id=message.correlation_id,
                        device_id=message.device_id,
                        zone_id=message.zone_id,
                        timestamp=message.timestamp,
                        connectivity=message.connectivity,
                        state={},
                        message_counter=message.message_counter,
                        status=message.status,
                        meta=message.meta,
                    )
                )
                self._handle_device_connectivity(message.device_id, message.zone_id, message.connectivity, message.trace_id)
                log.info("presence update device_id=%s zone_id=%s trace_id=%s", message.device_id, message.zone_id, message.trace_id)
                return

            if parsed.channel == ACK_SUFFIX:
                ack = CommandAck.model_validate(payload)
                if self._state_store.seen_message(ack.message_id):
                    self._state_store.note_incident("replay_suspected", {"topic": topic, "message_id": ack.message_id, "device_id": ack.device_id, "zone_id": ack.zone_id, "trace_id": ack.trace_id, "command_id": ack.command_id})
                    return
                self._dispatcher.handle_ack(ack)
                log.info("ack receive command_id=%s device_id=%s zone_id=%s", ack.command_id, ack.device_id, ack.zone_id)
                return

            if parsed.channel == RESULT_SUFFIX:
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

        if parsed.channel == EVENTS_SUFFIX:
            try:
                payload.setdefault("event_id", payload.get("message_id") or f"event-{uuid.uuid4().hex[:12]}")
                payload.setdefault("timestamp", int(time.time() * 1000))
                payload.setdefault("source", "device" if parsed.scope == "device" else "system")
                payload.setdefault("category", "device_event" if parsed.scope == "device" else "system_event")
                payload.setdefault("severity", "warning")
                payload.setdefault("message", str(payload.get("message") or payload.get("error_message") or payload.get("error_code") or "event"))
                event = EventEnvelope.model_validate(payload)
            except ValidationError as exc:
                self._state_store.note_incident("validation_fail", {"topic": topic, "error": str(exc), "payload": payload})
                log.error("validation fail topic=%s error=%s", topic, exc)
                return
            self._state_store.note_incident("system_event", event.model_dump())
            log.error("event receive scope=%s device_id=%s payload=%s", parsed.scope, parsed.device_id, json.dumps(event.model_dump(), ensure_ascii=False))
            return

        log.warning("ingest receive unhandled_channel=%s topic=%s", parsed.channel, topic)

    def _classify_telemetry(self, message: TelemetryMessage) -> TelemetryDisposition:
        skew_ms = max(0, self._policy.allowed_clock_skew_sec) * 1000
        current_device = self._state_store.get_device_state(message.device_id)
        current_ts = int(current_device.get("last_telemetry_ms") or 0)
        current_counter = current_device.get("last_telemetry_counter")

        if current_ts and message.ts_ms + skew_ms < current_ts:
            return TelemetryDisposition(
                kind="out_of_order",
                reason="out_of_order_timestamp",
                write_history=self._should_keep_stale_history(),
                update_current_state=False,
                trigger_decision=False,
            )
        if current_counter is not None and message.message_counter + max(0, self._policy.allowed_counter_reorder_window) < int(current_counter):
            return self._stale_disposition("stale_message_counter")
        if current_counter is not None and message.message_counter < int(current_counter):
            return TelemetryDisposition(
                kind="out_of_order",
                reason="out_of_order_counter",
                write_history=self._should_keep_stale_history(),
                update_current_state=False,
                trigger_decision=False,
            )
        return TelemetryDisposition(kind="current")

    def _stale_disposition(self, reason: str) -> TelemetryDisposition:
        keep_history = self._should_keep_stale_history()
        return TelemetryDisposition(
            kind="stale",
            reason=reason,
            write_history=keep_history,
            update_current_state=False,
            trigger_decision=False,
        )

    def _should_keep_stale_history(self) -> bool:
        return self._policy.stale_message_policy == "history_only"

    def _handle_telemetry(self, topic: str, message: TelemetryMessage, disposition: TelemetryDisposition) -> None:
        if disposition.write_history:
            self._telemetry_history.write_telemetry(message)
        if disposition.kind != "current":
            self._state_store.note_incident(
                disposition.kind,
                {
                    "topic": topic,
                    "message_id": message.message_id,
                    "device_id": message.device_id,
                    "zone_id": message.zone_id,
                    "trace_id": message.trace_id,
                    "timestamp": message.ts_ms,
                    "message_counter": message.message_counter,
                    "reason": disposition.reason,
                    "history_written": disposition.write_history,
                },
            )
            log.warning(
                "telemetry %s device_id=%s zone_id=%s trace_id=%s reason=%s history_written=%s",
                disposition.kind,
                message.device_id,
                message.zone_id,
                message.trace_id,
                disposition.reason,
                disposition.write_history,
            )
            return

        self._state_store.write_telemetry_snapshot(message)
        self._security_monitor.process_telemetry(message)
        self._dispatcher.observe_telemetry(message)
        if not self._automation_enabled():
            log.info("automation paused; skipping automated decision zone_id=%s trace_id=%s", message.zone_id, message.trace_id)
            return
        proposal = self._decision_engine.evaluate_telemetry(message)
        if proposal is not None:
            self._dispatcher.dispatch_proposal(proposal)
        log.info("ingest receive telemetry device_id=%s zone_id=%s trace_id=%s", message.device_id, message.zone_id, message.trace_id)

    def _automation_enabled(self) -> bool:
        flags = self._state_store.get_automation_flags()
        return bool((flags.get("automation_enabled") or {}).get("enabled", True))

    def _handle_device_connectivity(self, device_id: str, zone_id: str, connectivity: DeviceConnectivity, trace_id: str) -> None:
        if connectivity in {DeviceConnectivity.OFFLINE, DeviceConnectivity.SAFE_MODE}:
            reason = "device_safe_mode" if connectivity == DeviceConnectivity.SAFE_MODE else "device_offline"
            self._state_store.set_device_fault(device_id, reason, zone_id)
            self._security_monitor.device_offline(device_id, zone_id, trace_id, reason=reason)
            self._dispatcher.handle_device_offline(device_id, zone_id, reason, trace_id)
            return
        self._state_store.clear_device_fault(device_id)
        self._security_monitor.device_seen(device_id, zone_id, trace_id)

