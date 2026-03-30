from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import paho.mqtt.client as mqtt

from backend.config import BackendConfig
from backend.domain.models import AuditLogRecord, CommandExecutionRecord, ExecutionPhase, SafetyLockRecord
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.store import StateStore
from mqtt.topics import command_execute_topic
from shared.contracts.messages import ActuatorCommandMessage, CommandAck, CommandLifecycle, CommandResult, SafetyConstraints


log = logging.getLogger("backend.execution")


class IrrigationOrchestrator:
    def __init__(self, mqtt_client: mqtt.Client, config: BackendConfig, store: StateStore, safety: SafetyValidator) -> None:
        self._mqtt = mqtt_client
        self._config = config
        self._store = store
        self._safety = safety

    def start(self, proposal: ActionProposal) -> dict[str, Any]:
        safety = self._safety.validate(self._store, proposal)
        command = self._safety.build_command_record(proposal)
        stored_command, created = self._store.create_or_get_command(command)
        if not created:
            return {"status": stored_command["lifecycle"], "command_id": stored_command["command_id"], "idempotent": True}

        if not safety.allowed:
            self._store.update_command(command.command_id, lifecycle=CommandLifecycle.ABORTED, last_error=";".join(safety.reasons))
            self._audit(command.trace_id, "COMMAND_REJECTED", "command rejected by safety", command.command_id, zone_id=command.zone_id, device_id=command.device_id, payload={"reasons": safety.reasons})
            return {"status": CommandLifecycle.ABORTED.value, "command_id": command.command_id, "reasons": safety.reasons}

        execution = CommandExecutionRecord(
            execution_id=f"exec-{uuid.uuid4().hex[:12]}",
            command_id=command.command_id,
            device_id=command.device_id,
            zone_id=command.zone_id,
            lifecycle=CommandLifecycle.PLANNED,
            phase=ExecutionPhase.RESERVE_ZONE if command.command_type == "IRRIGATE_ZONE" else ExecutionPhase.VALIDATE_REQUEST,
            step_index=0,
            active_step=None,
            started_at_ms=command.requested_at_ms,
            updated_at_ms=command.requested_at_ms,
            expires_at_ms=command.expires_at_ms,
            phase_deadline_ms=None,
            monitor_until_ms=None,
            target_duration_ms=int(command.requested_payload.get("duration_sec") or 0) * 1000,
            target_volume_ml=command.requested_payload.get("metadata", {}).get("target_volume_ml"),
            delivered_volume_ml=0.0,
            flow_confirmed=False,
            reserved_lock_id=None,
            last_error=None,
            result_payload={},
            metadata={"aborting": False},
        )
        self._store.create_execution(execution)
        self._store.update_command(command.command_id, current_execution_id=execution.execution_id)
        self._audit(command.trace_id, "COMMAND_PLANNED", "command planned", command.command_id, execution.execution_id, command.device_id, command.zone_id, {"proposal": command.requested_payload})

        if command.command_type == "IRRIGATE_ZONE":
            self._reserve_zone(command.command_id, execution.execution_id, command.zone_id, command.expires_at_ms)
            return self._publish_step(command.command_id, execution.execution_id, "open_valve", "irrigation_valve", "OPEN", 0, {})

        payload = command.requested_payload
        return self._publish_step(
            command.command_id,
            execution.execution_id,
            "actuator_action",
            str(payload["actuator"]),
            str(payload["action"]),
            int(command.requested_payload.get("duration_sec") or 0) * 1000,
            dict(payload.get("metadata", {})),
        )

    def observe_telemetry(self, telemetry: dict[str, Any]) -> None:
        now_ms = int(telemetry["ts_ms"])
        zone_id = telemetry["zone_id"]
        for execution in self._store.list_active_executions():
            if execution["zone_id"] != zone_id:
                continue
            if execution["phase"] == ExecutionPhase.CONFIRM_FLOW.value:
                flow_rate = float(telemetry["sensors"].get("flow_rate_ml_per_min") or 0.0)
                zone = self._store.get_zone_state(zone_id)
                if flow_rate >= float(zone.get("min_flow_ml_per_min") or 0.0):
                    self._store.update_execution(
                        execution["execution_id"],
                        lifecycle=CommandLifecycle.EXECUTING,
                        phase=ExecutionPhase.MONITOR_RUN,
                        flow_confirmed=True,
                        monitor_until_ms=now_ms + int(execution.get("target_duration_ms") or 0),
                        phase_deadline_ms=None,
                        updated_at_ms=now_ms,
                    )
                    self._store.update_command(execution["command_id"], lifecycle=CommandLifecycle.EXECUTING)
            if execution["phase"] == ExecutionPhase.MONITOR_RUN.value:
                self._accumulate_flow(execution, telemetry)
                if telemetry["sensors"].get("leak") is True or telemetry["sensors"].get("overflow") is True:
                    self._begin_abort(execution["command_id"], execution["execution_id"], "telemetry_anomaly")

    def handle_ack(self, ack: CommandAck) -> dict[str, Any] | None:
        command = self._store.get_command(ack.command_id)
        if command is None:
            self._reject_protocol_message("ACK", ack.trace_id, ack.command_id, ack.device_id, ack.zone_id, "unknown_command")
            return None
        execution_id = ack.execution_id or command.get("current_execution_id")
        if not execution_id:
            self._reject_protocol_message("ACK", ack.trace_id, ack.command_id, ack.device_id, ack.zone_id, "missing_execution_binding")
            return None
        execution = self._store.get_execution(execution_id)
        if execution is None:
            self._reject_protocol_message("ACK", ack.trace_id, ack.command_id, ack.device_id, ack.zone_id, "unknown_execution")
            return None
        if not self._validate_command_binding(command, execution, ack.device_id, ack.zone_id, ack.execution_id, ack.step):
            self._reject_protocol_message("ACK", ack.trace_id, ack.command_id, ack.device_id, ack.zone_id, "binding_mismatch")
            return None
        lifecycle = {
            "received": CommandLifecycle.ACKED,
            "acked": CommandLifecycle.ACKED,
            "running": CommandLifecycle.EXECUTING,
            "failed": CommandLifecycle.FAILED,
            "expired": CommandLifecycle.EXPIRED,
            "rejected": CommandLifecycle.ABORTED,
        }[ack.status]
        if not self._is_valid_transition(current_lifecycle=execution["lifecycle"], next_lifecycle=lifecycle, message_kind="ACK"):
            self._reject_protocol_message("ACK", ack.trace_id, ack.command_id, ack.device_id, ack.zone_id, f"malformed_transition:{execution['lifecycle']}->{lifecycle.value}")
            return None
        self._store.update_command(ack.command_id, lifecycle=lifecycle)
        updated = self._store.update_execution(
            execution_id,
            lifecycle=lifecycle,
            updated_at_ms=ack.local_timestamp_ms,
            result_payload_update={"last_ack": ack.model_dump(), "observed_state": ack.observed_state},
        )
        self._audit(ack.trace_id, "COMMAND_ACK", "device acknowledged command", ack.command_id, execution_id, ack.device_id, ack.zone_id, {"status": ack.status, "error_code": ack.error_code, "error_message": ack.error_message})
        return updated

    def handle_result(self, result: CommandResult) -> dict[str, Any] | None:
        command = self._store.get_command(result.command_id)
        if command is None:
            self._reject_protocol_message("RESULT", result.trace_id, result.command_id, result.device_id, result.zone_id, "unknown_command")
            return None
        execution_id = result.execution_id or command.get("current_execution_id")
        if not execution_id:
            self._reject_protocol_message("RESULT", result.trace_id, result.command_id, result.device_id, result.zone_id, "missing_execution_binding")
            return None
        execution = self._store.get_execution(execution_id)
        if execution is None:
            self._reject_protocol_message("RESULT", result.trace_id, result.command_id, result.device_id, result.zone_id, "unknown_execution")
            return None
        if not self._validate_command_binding(command, execution, result.device_id, result.zone_id, result.execution_id, result.step):
            self._reject_protocol_message("RESULT", result.trace_id, result.command_id, result.device_id, result.zone_id, "binding_mismatch")
            return None
        next_lifecycle = {
            "completed": CommandLifecycle.COMPLETED,
            "failed": CommandLifecycle.FAILED,
            "expired": CommandLifecycle.EXPIRED,
            "aborted": CommandLifecycle.ABORTED,
        }[result.status]
        if not self._is_valid_transition(current_lifecycle=execution["lifecycle"], next_lifecycle=next_lifecycle, message_kind="RESULT"):
            self._reject_protocol_message("RESULT", result.trace_id, result.command_id, result.device_id, result.zone_id, f"malformed_transition:{execution['lifecycle']}->{next_lifecycle.value}")
            return None
        self._audit(result.trace_id, "COMMAND_RESULT", "device returned result", result.command_id, execution_id, result.device_id, result.zone_id, {"step": execution.get("active_step"), "status": result.status, "metrics": result.metrics, "error_code": result.error_code})
        if result.status in {"failed", "expired", "aborted"}:
            terminal = CommandLifecycle.EXPIRED if result.status == "expired" else CommandLifecycle.ABORTED
            self._begin_abort(result.command_id, execution_id, result.error_message or result.status, terminal_lifecycle=terminal)
            return self._store.get_execution(execution_id)

        if execution.get("metadata", {}).get("aborting"):
            return self._advance_abort(result, execution)
        return self._advance_success(result, execution)

    def recover_active_executions(self) -> None:
        for execution in self._store.list_recoverable_executions():
            command = self._store.get_command(execution["command_id"])
            if command is None:
                continue
            device_state = self._store.get_device_state(execution["device_id"]).get("state", {})
            if int(command["expires_at_ms"]) <= int(time.time() * 1000):
                self._expire(command["command_id"], execution["execution_id"])
                continue
            if device_state.get("pump_on") or device_state.get("valve_open") or execution.get("phase") != ExecutionPhase.FINISHED.value:
                self._begin_abort(command["command_id"], execution["execution_id"], "backend_restart_recovery")

    def sweep(self) -> None:
        now_ms = int(time.time() * 1000)
        for command in self._store.list_active_commands():
            if now_ms >= int(command["expires_at_ms"]):
                self._expire(command["command_id"], command.get("current_execution_id"))
        for execution in self._store.list_active_executions():
            if self._has_fail_safe_lock(execution["device_id"], execution["zone_id"]):
                self._begin_abort(execution["command_id"], execution["execution_id"], "active_safety_lock")
                continue
            phase = execution["phase"]
            if execution.get("metadata", {}).get("aborting") and phase == ExecutionPhase.VERIFY_SAFE_STOP.value and int(execution.get("phase_deadline_ms") or 0) <= now_ms:
                self._finalize_abort(execution["command_id"], execution["execution_id"], "safe stop verified")
                continue
            if phase == ExecutionPhase.WAIT_SETTLE_DELAY.value and int(execution.get("phase_deadline_ms") or 0) <= now_ms:
                self._publish_step(execution["command_id"], execution["execution_id"], "start_pump", "master_pump", "ON", self._config.global_safety.master_pump_timeout_sec * 1000, {})
            elif phase == ExecutionPhase.CONFIRM_FLOW.value and int(execution.get("phase_deadline_ms") or 0) <= now_ms:
                self._begin_abort(execution["command_id"], execution["execution_id"], "flow_not_confirmed")
            elif phase == ExecutionPhase.MONITOR_RUN.value and int(execution.get("monitor_until_ms") or 0) <= now_ms:
                self._publish_step(execution["command_id"], execution["execution_id"], "stop_pump", "master_pump", "OFF", 0, {})
            elif phase == ExecutionPhase.VERIFY_SAFE_STOP.value and int(execution.get("phase_deadline_ms") or 0) <= now_ms:
                self._complete_if_safe(execution["command_id"], execution["execution_id"])

    def _advance_success(self, result: CommandResult, execution: dict[str, Any]) -> dict[str, Any] | None:
        command_id = execution["command_id"]
        execution_id = execution["execution_id"]
        step = execution.get("active_step")
        zone = self._store.get_zone_state(execution["zone_id"])
        if step == "actuator_action":
            self._store.update_execution(execution_id, lifecycle=CommandLifecycle.COMPLETED, phase=ExecutionPhase.FINISHED, updated_at_ms=result.local_timestamp_ms, result_payload_update={"completed_at_ms": result.local_timestamp_ms, "details": result.error_message, "observed_state": result.observed_state})
            self._store.update_command(command_id, lifecycle=CommandLifecycle.COMPLETED)
            return self._store.get_execution(execution_id)
        if step == "open_valve":
            return self._store.update_execution(execution_id, lifecycle=CommandLifecycle.EXECUTING, phase=ExecutionPhase.WAIT_SETTLE_DELAY, updated_at_ms=result.local_timestamp_ms, phase_deadline_ms=result.local_timestamp_ms + int(zone.get("settle_delay_ms") or 0), result_payload_update={"open_valve_at_ms": result.local_timestamp_ms, "observed_state": result.observed_state})
        if step == "start_pump":
            return self._store.update_execution(execution_id, lifecycle=CommandLifecycle.EXECUTING, phase=ExecutionPhase.CONFIRM_FLOW, updated_at_ms=result.local_timestamp_ms, phase_deadline_ms=result.local_timestamp_ms + int(zone.get("flow_confirm_timeout_ms") or 0), result_payload_update={"start_pump_at_ms": result.local_timestamp_ms, "observed_state": result.observed_state})
        if step == "stop_pump":
            return self._publish_step(command_id, execution_id, "close_valve", "irrigation_valve", "CLOSE", 0, {})
        if step == "close_valve":
            self._store.update_execution(execution_id, lifecycle=CommandLifecycle.EXECUTING, phase=ExecutionPhase.VERIFY_SAFE_STOP, updated_at_ms=result.local_timestamp_ms, phase_deadline_ms=result.local_timestamp_ms + 1000, result_payload_update={"close_valve_at_ms": result.local_timestamp_ms, "observed_state": result.observed_state})
            return self._store.get_execution(execution_id)
        return self._store.get_execution(execution_id)

    def _advance_abort(self, result: CommandResult, execution: dict[str, Any]) -> dict[str, Any] | None:
        step = execution.get("active_step")
        if step == "stop_pump":
            return self._publish_step(execution["command_id"], execution["execution_id"], "close_valve", "irrigation_valve", "CLOSE", 0, {})
        if step == "close_valve":
            return self._finalize_abort(execution["command_id"], execution["execution_id"], result.error_message or "aborted")
        return self._store.get_execution(execution["execution_id"])

    def _publish_step(self, command_id: str, execution_id: str, step: str, actuator: str, action: str, max_duration_ms: int, parameters: dict[str, Any]) -> dict[str, Any]:
        command = self._store.get_command(command_id)
        execution = self._store.get_execution(execution_id)
        if command is None or execution is None:
            return {"status": "missing_command"}
        message = ActuatorCommandMessage(
            message_id=f"msg-{uuid.uuid4().hex[:12]}",
            trace_id=command["trace_id"],
            command_id=command_id,
            execution_id=execution_id,
            device_id=command["device_id"],
            zone_id=command["zone_id"],
            actuator=actuator,
            action=action,
            step=step,
            issued_at_ms=int(time.time() * 1000),
            expires_at_ms=int(command["expires_at_ms"]),
            nonce=str(command.get("metadata", {}).get("nonce") or f"nonce-{uuid.uuid4().hex[:16]}"),
            max_duration_ms=max_duration_ms,
            safety_constraints=SafetyConstraints(
                local_hard_max_duration_ms=max_duration_ms or int(self._config.global_safety.master_pump_timeout_sec * 1000),
                allowed_runtime_window_ms=max(0, int(command["expires_at_ms"]) - int(time.time() * 1000)),
            ),
            parameters=parameters,
        )
        result = self._mqtt.publish(command_execute_topic(command["device_id"]), json.dumps(message.model_dump(), ensure_ascii=False), qos=self._config.mqtt.qos_default)
        lifecycle = CommandLifecycle.DISPATCHED if result.rc == mqtt.MQTT_ERR_SUCCESS else CommandLifecycle.FAILED
        if execution.get("metadata", {}).get("aborting"):
            lifecycle = CommandLifecycle(execution.get("metadata", {}).get("terminal_lifecycle") or command["lifecycle"])
        phase = self._phase_for_step(step)
        self._store.update_command(command_id, lifecycle=lifecycle, requested_payload_update={"last_step": step})
        self._store.update_execution(execution_id, lifecycle=lifecycle, phase=phase, active_step=step, step_index=int(execution.get("step_index") or 0) + 1, updated_at_ms=message.issued_at_ms, result_payload_update={"last_publish_at_ms": message.issued_at_ms, "mqtt_rc": result.rc})
        self._audit(command["trace_id"], "COMMAND_PUBLISH", "published actuator step", command_id, execution_id, command["device_id"], command["zone_id"], {"step": step, "actuator": actuator, "action": action, "mqtt_rc": result.rc})
        return {"status": lifecycle.value, "command_id": command_id, "execution_id": execution_id, "step": step}

    def _phase_for_step(self, step: str) -> ExecutionPhase:
        return {
            "open_valve": ExecutionPhase.OPEN_VALVE,
            "start_pump": ExecutionPhase.START_PUMP,
            "stop_pump": ExecutionPhase.STOP_PUMP,
            "close_valve": ExecutionPhase.CLOSE_VALVE,
        }.get(step, ExecutionPhase.VALIDATE_REQUEST)

    def _reserve_zone(self, command_id: str, execution_id: str, zone_id: str, expires_at_ms: int) -> None:
        lock = SafetyLockRecord(
            lock_id=f"lock-{uuid.uuid4().hex[:12]}",
            scope="zone",
            scope_id=zone_id,
            kind="execution_reservation",
            reason=f"reserved for {command_id}",
            active=True,
            created_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
            owner=execution_id,
            payload={"command_id": command_id},
        )
        self._store.create_safety_lock(lock)
        self._store.update_execution(execution_id, reserved_lock_id=lock.lock_id, updated_at_ms=lock.created_at_ms)
        self._audit(self._store.get_command(command_id)["trace_id"], "ZONE_RESERVED", "zone reserved", command_id, execution_id, zone_id=zone_id, payload={"lock_id": lock.lock_id})

    def _begin_abort(
        self,
        command_id: str,
        execution_id: str | None,
        reason: str,
        terminal_lifecycle: CommandLifecycle = CommandLifecycle.ABORTED,
    ) -> None:
        if not execution_id:
            return
        execution = self._store.get_execution(execution_id)
        command = self._store.get_command(command_id)
        if execution is None or command is None:
            return
        if execution.get("metadata", {}).get("aborting"):
            return
        self._store.update_command(command_id, lifecycle=terminal_lifecycle, last_error=reason)
        self._store.update_execution(
            execution_id,
            lifecycle=terminal_lifecycle,
            metadata_update={"aborting": True, "terminal_lifecycle": terminal_lifecycle.value},
            last_error=reason,
            updated_at_ms=int(time.time() * 1000),
        )
        self._audit(command["trace_id"], "COMMAND_ABORTING", "aborting command", command_id, execution_id, command["device_id"], command["zone_id"], {"reason": reason})
        self._publish_step(command_id, execution_id, "stop_pump", "master_pump", "OFF", 0, {"abort_reason": reason})

    def _finalize_abort(self, command_id: str, execution_id: str, reason: str) -> dict[str, Any] | None:
        command = self._store.get_command(command_id)
        execution = self._store.get_execution(execution_id)
        if command is None or execution is None:
            return None
        terminal = execution.get("metadata", {}).get("terminal_lifecycle") or CommandLifecycle.ABORTED.value
        if execution.get("reserved_lock_id"):
            self._store.release_safety_lock(execution["reserved_lock_id"])
        self._store.update_execution(execution_id, lifecycle=terminal, phase=ExecutionPhase.FINISHED, updated_at_ms=int(time.time() * 1000), result_payload_update={"completed_at_ms": int(time.time() * 1000), "details": reason})
        self._store.update_command(command_id, lifecycle=terminal, last_error=reason)
        self._store.mark_zone_error(command["zone_id"], int(time.time() * 1000))
        self._audit(command["trace_id"], "COMMAND_ABORTED", "command aborted", command_id, execution_id, command["device_id"], command["zone_id"], {"reason": reason})
        return self._store.get_execution(execution_id)

    def _complete_if_safe(self, command_id: str, execution_id: str) -> None:
        command = self._store.get_command(command_id)
        execution = self._store.get_execution(execution_id)
        if command is None or execution is None:
            return
        device_state = self._store.get_device_state(command["device_id"]).get("state", {})
        zone = self._store.get_zone_state(command["zone_id"])
        flow_rate = float(zone.get("telemetry", {}).get("flow_rate_ml_per_min") or 0.0)
        if device_state.get("pump_on") or device_state.get("valve_open") or flow_rate > 0:
            self._begin_abort(command_id, execution_id, "unsafe_stop_detected")
            return
        if execution.get("reserved_lock_id"):
            self._store.release_safety_lock(execution["reserved_lock_id"])
        now_ms = int(time.time() * 1000)
        self._store.update_execution(execution_id, lifecycle=CommandLifecycle.COMPLETED, phase=ExecutionPhase.FINISHED, updated_at_ms=now_ms, result_payload_update={"completed_at_ms": now_ms, "delivered_volume_ml": execution.get("delivered_volume_ml") or 0.0})
        self._store.update_command(command_id, lifecycle=CommandLifecycle.COMPLETED)
        self._store.mark_zone_watering(command["zone_id"], now_ms)
        self._audit(command["trace_id"], "COMMAND_COMPLETED", "command completed safely", command_id, execution_id, command["device_id"], command["zone_id"], {"delivered_volume_ml": execution.get("delivered_volume_ml") or 0.0})

    def _expire(self, command_id: str, execution_id: str | None) -> None:
        if not execution_id:
            self._store.update_command(command_id, lifecycle=CommandLifecycle.EXPIRED, last_error="command TTL exceeded")
            return
        self._store.update_command(command_id, lifecycle=CommandLifecycle.EXPIRED, last_error="command TTL exceeded")
        self._store.update_execution(execution_id, lifecycle=CommandLifecycle.EXPIRED, metadata_update={"terminal_lifecycle": CommandLifecycle.EXPIRED.value}, last_error="command TTL exceeded", updated_at_ms=int(time.time() * 1000))
        self._begin_abort(command_id, execution_id, "command TTL exceeded", terminal_lifecycle=CommandLifecycle.EXPIRED)

    def _accumulate_flow(self, execution: dict[str, Any], telemetry: dict[str, Any]) -> None:
        flow_rate = float(telemetry["sensors"].get("flow_rate_ml_per_min") or 0.0)
        previous_ts = execution.get("metadata", {}).get("last_flow_ts_ms")
        if previous_ts:
            delta_min = max(0.0, (int(telemetry["ts_ms"]) - int(previous_ts)) / 60000.0)
            delivered = float(execution.get("delivered_volume_ml") or 0.0) + flow_rate * delta_min
        else:
            delivered = float(execution.get("delivered_volume_ml") or 0.0)
        self._store.update_execution(execution["execution_id"], delivered_volume_ml=delivered, metadata_update={"last_flow_ts_ms": telemetry["ts_ms"]}, updated_at_ms=int(telemetry["ts_ms"]))

    def _validate_command_binding(
        self,
        command: dict[str, Any],
        execution: dict[str, Any],
        device_id: str,
        zone_id: str,
        execution_id: str | None,
        step: str | None,
    ) -> bool:
        if command["device_id"] != device_id or execution["device_id"] != device_id:
            return False
        if command["zone_id"] != zone_id or execution["zone_id"] != zone_id:
            return False
        if execution_id and execution["execution_id"] != execution_id:
            return False
        if step and execution.get("active_step") and execution["active_step"] != step:
            return False
        return True

    def _is_valid_transition(self, current_lifecycle: str, next_lifecycle: CommandLifecycle, message_kind: str) -> bool:
        allowed: dict[str, set[str]] = {
            CommandLifecycle.PLANNED.value: {CommandLifecycle.DISPATCHED.value, CommandLifecycle.ACKED.value, CommandLifecycle.ABORTED.value, CommandLifecycle.EXPIRED.value},
            CommandLifecycle.DISPATCHED.value: {CommandLifecycle.ACKED.value, CommandLifecycle.EXECUTING.value, CommandLifecycle.FAILED.value, CommandLifecycle.EXPIRED.value, CommandLifecycle.ABORTED.value, CommandLifecycle.COMPLETED.value},
            CommandLifecycle.ACKED.value: {CommandLifecycle.EXECUTING.value, CommandLifecycle.FAILED.value, CommandLifecycle.EXPIRED.value, CommandLifecycle.ABORTED.value, CommandLifecycle.COMPLETED.value},
            CommandLifecycle.EXECUTING.value: {CommandLifecycle.COMPLETED.value, CommandLifecycle.FAILED.value, CommandLifecycle.EXPIRED.value, CommandLifecycle.ABORTED.value},
            CommandLifecycle.COMPLETED.value: set(),
            CommandLifecycle.FAILED.value: set(),
            CommandLifecycle.EXPIRED.value: set(),
            CommandLifecycle.ABORTED.value: set(),
        }
        if message_kind == "ACK" and next_lifecycle == CommandLifecycle.COMPLETED:
            return False
        return next_lifecycle.value in allowed.get(current_lifecycle, set())

    def _has_fail_safe_lock(self, device_id: str, zone_id: str) -> bool:
        critical_kinds = {
            "stale_heartbeat",
            "mqtt_disconnected",
            "mqtt_auth_failure",
            "empty_tank",
            "leak_suspicion",
            "flow_anomaly",
            "pressure_anomaly",
            "tank_depletion_trend",
        }
        for lock in self._store.get_active_safety_locks("global"):
            if lock["kind"] in critical_kinds:
                return True
        for lock in self._store.get_active_safety_locks("device", device_id):
            if lock["kind"] in critical_kinds or lock["kind"].startswith("sensor_"):
                return True
        for lock in self._store.get_active_safety_locks("zone", zone_id):
            if lock["kind"] in critical_kinds:
                return True
        return False

    def _reject_protocol_message(self, kind: str, trace_id: str, command_id: str, device_id: str, zone_id: str, reason: str) -> None:
        self._store.note_incident(
            "protocol_reject",
            {
                "trace_id": trace_id,
                "command_id": command_id,
                "device_id": device_id,
                "zone_id": zone_id,
                "kind": kind,
                "reason": reason,
            },
        )
        self._audit(trace_id, f"{kind}_REJECTED", f"{kind} rejected: {reason}", command_id=command_id, device_id=device_id, zone_id=zone_id, payload={"reason": reason})

    def _audit(self, trace_id: str, action_type: str, message: str, command_id: str | None = None, execution_id: str | None = None, device_id: str | None = None, zone_id: str | None = None, payload: dict[str, Any] | None = None) -> None:
        self._store.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=trace_id,
                action_type=action_type,
                message=message,
                created_at_ms=int(time.time() * 1000),
                command_id=command_id,
                execution_id=execution_id,
                device_id=device_id,
                zone_id=zone_id,
                payload=payload or {},
            )
        )
