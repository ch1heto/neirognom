from __future__ import annotations

import json
import logging
import time
from typing import Any

import paho.mqtt.client as mqtt

from backend.config import BackendConfig
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.store import StateStore
from mqtt.topics import command_execute_topic
from shared.contracts.messages import CommandAck, CommandLifecycle, CommandRequest, CommandResult


log = logging.getLogger("backend.dispatcher")


class CommandDispatcher:
    def __init__(
        self,
        mqtt_client: mqtt.Client,
        config: BackendConfig,
        store: StateStore,
        safety_validator: SafetyValidator,
    ) -> None:
        self._mqtt = mqtt_client
        self._config = config
        self._store = store
        self._safety = safety_validator

    def dispatch_proposal(self, proposal: ActionProposal) -> dict[str, Any]:
        safety = self._safety.validate(self._store, proposal)
        if not safety.allowed:
            payload = {
                "trace_id": proposal.trace_id,
                "ts_ms": proposal.requested_at_ms,
                "reasons": safety.reasons,
            }
            command = self._safety.build_command(proposal)
            self._store.create_command(command, CommandLifecycle.REJECTED_BY_SAFETY)
            self._store.transition_command(command.command_id, CommandLifecycle.REJECTED_BY_SAFETY, payload)
            log.warning(
                "safety rejection trace_id=%s zone_id=%s actuator=%s reasons=%s",
                proposal.trace_id,
                proposal.zone_id,
                proposal.actuator,
                ",".join(safety.reasons),
            )
            return {"status": "rejected_by_safety", "command_id": command.command_id, "reasons": safety.reasons}

        command = self._safety.build_command(proposal)
        self._store.create_command(command, CommandLifecycle.QUEUED)
        topic = command_execute_topic(command.device_id)
        payload = json.dumps(command.model_dump(), ensure_ascii=False)
        result = self._mqtt.publish(topic, payload, qos=self._config.mqtt.qos_default)
        lifecycle = CommandLifecycle.SENT if result.rc == mqtt.MQTT_ERR_SUCCESS else CommandLifecycle.FAILED
        self._store.transition_command(
            command.command_id,
            lifecycle,
            {"ts_ms": int(time.time() * 1000), "mqtt_rc": result.rc, "topic": topic},
        )
        log.info(
            "command publish trace_id=%s command_id=%s device_id=%s zone_id=%s topic=%s rc=%s",
            command.trace_id,
            command.command_id,
            command.device_id,
            command.zone_id,
            topic,
            result.rc,
        )
        return {"status": lifecycle.value, "command_id": command.command_id}

    def handle_ack(self, ack: CommandAck) -> dict[str, Any] | None:
        lifecycle = {
            "queued": CommandLifecycle.QUEUED,
            "received": CommandLifecycle.ACKED,
            "acked": CommandLifecycle.ACKED,
            "running": CommandLifecycle.RUNNING,
            "failed": CommandLifecycle.FAILED,
            "expired": CommandLifecycle.EXPIRED,
        }[ack.state]
        return self._store.transition_command(
            ack.command_id,
            lifecycle,
            {"ts_ms": ack.acked_at_ms, "details": ack.details, "message_id": ack.message_id},
        )

    def handle_result(self, result: CommandResult) -> dict[str, Any] | None:
        lifecycle = {
            "completed": CommandLifecycle.COMPLETED,
            "failed": CommandLifecycle.FAILED,
            "expired": CommandLifecycle.EXPIRED,
        }[result.final_state]
        return self._store.transition_command(
            result.command_id,
            lifecycle,
            {"ts_ms": result.result_at_ms, "metrics": result.metrics, "details": result.details, "message_id": result.message_id},
        )

    def sweep_expired(self) -> None:
        now_ms = int(time.time() * 1000)
        current_state = self._store.get_current_state()
        for command in current_state.get("commands", {}).values():
            if command.get("lifecycle") in {CommandLifecycle.COMPLETED.value, CommandLifecycle.FAILED.value, CommandLifecycle.EXPIRED.value, CommandLifecycle.REJECTED_BY_SAFETY.value}:
                continue
            if now_ms > int(command.get("expires_at_ms") or 0):
                self._store.transition_command(
                    command["command_id"],
                    CommandLifecycle.EXPIRED,
                    {"ts_ms": now_ms, "details": "command TTL exceeded"},
                )
