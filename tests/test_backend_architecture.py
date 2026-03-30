import json
import os
import time
import unittest
from unittest import mock

import paho.mqtt.client as mqtt

from backend.config import load_backend_config
from backend.dispatcher.service import CommandDispatcher
from backend.domain.models import ExecutionState, ManualLeaseRecord
from backend.security.monitor import SecurityMonitor
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.influx import MemoryTelemetryHistoryStore
from backend.state.store import MemoryStateStore
from shared.contracts.messages import CommandAck, CommandLifecycle, CommandResult, DecisionOrigin, DeviceConnectivity, DeviceStateMessage, TelemetryMessage


class DummyPublishResult:
    def __init__(self, rc: int = mqtt.MQTT_ERR_SUCCESS) -> None:
        self.rc = rc


class DummyMqttClient:
    def __init__(self) -> None:
        self.published: list[dict] = []

    def publish(self, topic: str, payload: str, qos: int = 1):
        self.published.append({"topic": topic, "payload": json.loads(payload), "qos": qos})
        return DummyPublishResult()


class BackendArchitectureTests(unittest.TestCase):
    def _build_stack(self):
        config = load_backend_config()
        store = MemoryStateStore()
        store.initialize(config.zone_configs(), config.global_safety)
        store.write_device_state(
            DeviceStateMessage(
                message_id="msg-state-default",
                trace_id="trace-state-default",
                device_id="esp32-1",
                zone_id="tray_1",
                ts_ms=int(time.time() * 1000),
                connectivity=DeviceConnectivity.ONLINE,
                state={"pump_on": False, "valve_open": False},
            )
        )
        mqtt_client = DummyMqttClient()
        validator = SafetyValidator(config)
        dispatcher = CommandDispatcher(mqtt_client, config, store, validator)
        telemetry_history = MemoryTelemetryHistoryStore()
        security_monitor = SecurityMonitor(config, store, telemetry_history)
        return config, store, mqtt_client, validator, dispatcher, telemetry_history, security_monitor

    def test_config_uses_sqlite_and_influx_boundaries(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "STATE_STORE_BACKEND": "sqlite",
                "SQLITE_PATH": "control.db",
                "TELEMETRY_HISTORY_BACKEND": "influx",
                "INFLUX_ENABLED": "1",
                "INFLUX_URL": "http://127.0.0.1:8086",
                "INFLUX_ORG": "greenhouse",
                "INFLUX_BUCKET": "telemetry",
                "INFLUX_TOKEN": "token",
            },
            clear=False,
        ):
            config = load_backend_config()

        self.assertEqual(config.state_store_backend, "sqlite")
        self.assertEqual(config.sqlite.path, "control.db")
        self.assertEqual(config.telemetry_history_backend, "influx")
        self.assertTrue(config.influx.enabled)
        self.assertEqual(config.influx.bucket, "telemetry")

    def test_safety_validator_blocks_leak_cooldown_and_manual_lease(self) -> None:
        _, store, _, validator, _, _, _ = self._build_stack()
        zone = store.get_zone_state("tray_1")
        zone["telemetry"] = {"leak": True}
        zone["last_watering_at_ms"] = 10_000
        store._zones["tray_1"] = zone  # test-only seed
        store.create_manual_lease(
            ManualLeaseRecord(
                lease_id="lease-12345678",
                zone_id="tray_1",
                holder="operator",
                created_at_ms=10_000,
                expires_at_ms=9_999_999_999_999,
                payload={},
            )
        )

        decision = validator.validate(
            store,
            ActionProposal(
                trace_id="trace-12345678",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=10,
                origin=DecisionOrigin.DETERMINISTIC,
                reason="automatic watering",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("leak_detected", decision.reasons)
        self.assertIn("zone_cooldown_active", decision.reasons)
        self.assertIn("manual_lease_active", decision.reasons)

    def test_dispatcher_creates_idempotent_irrigation_execution(self) -> None:
        _, store, mqtt_client, _, dispatcher, _, _ = self._build_stack()
        proposal = ActionProposal(
            trace_id="trace-12345678",
            device_id="esp32-1",
            zone_id="tray_1",
            actuator="irrigation_sequence",
            action="START",
            duration_sec=5,
            origin=DecisionOrigin.OPERATOR,
            reason="manual watering",
            requested_at_ms=100_000,
            command_id="cmd-12345678",
        )

        first = dispatcher.dispatch_proposal(proposal)
        second = dispatcher.dispatch_proposal(proposal)
        status = store.get_command_status("cmd-12345678")

        self.assertEqual(first["status"], CommandLifecycle.DISPATCHED.value)
        self.assertTrue(second["idempotent"])
        self.assertEqual(len(mqtt_client.published), 1)
        self.assertEqual(mqtt_client.published[0]["payload"]["step"], "open_valve")
        self.assertIn("nonce", mqtt_client.published[0]["payload"])
        self.assertIn("safety_caps", mqtt_client.published[0]["payload"])
        self.assertEqual(mqtt_client.published[0]["payload"]["target_device_id"], "esp32-1")
        self.assertEqual(mqtt_client.published[0]["payload"]["target_zone_id"], "tray_1")
        self.assertEqual(status["lifecycle"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(status["execution"]["phase"], "OPEN_VALVE")
        self.assertEqual(store.get_zone_state("tray_1")["reserved_by_execution"], status["execution"]["execution_id"])

    def test_expired_command_transitions_to_expired_and_safe_stop(self) -> None:
        _, store, mqtt_client, _, dispatcher, _, _ = self._build_stack()
        dispatcher.dispatch_proposal(
            ActionProposal(
                trace_id="trace-12345679",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="expire me",
                requested_at_ms=0,
                command_id="cmd-expired1",
            )
        )

        dispatcher.sweep()
        status = store.get_command_status("cmd-expired1")

        self.assertEqual(status["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertEqual(status["execution"]["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertGreaterEqual(len(mqtt_client.published), 2)
        self.assertEqual(mqtt_client.published[-1]["payload"]["step"], "stop_pump")

    def test_recovery_aborts_active_execution_when_device_state_is_unsafe(self) -> None:
        _, store, mqtt_client, _, dispatcher, _, _ = self._build_stack()
        requested_at_ms = int(time.time() * 1000)
        dispatcher.dispatch_proposal(
            ActionProposal(
                trace_id="trace-12345680",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="recover me",
                requested_at_ms=requested_at_ms,
                command_id="cmd-recover1",
            )
        )
        store.write_device_state(
            DeviceStateMessage(
                message_id="msg-12345678",
                trace_id="trace-12345680",
                device_id="esp32-1",
                zone_id="tray_1",
                ts_ms=requested_at_ms + 1000,
                connectivity=DeviceConnectivity.ONLINE,
                state={"pump_on": True, "valve_open": True},
            )
        )

        dispatcher.recover_active_executions()
        status = store.get_command_status("cmd-recover1")

        self.assertEqual(status["lifecycle"], CommandLifecycle.ABORTED.value)
        self.assertGreaterEqual(len(mqtt_client.published), 2)
        self.assertEqual(mqtt_client.published[-1]["payload"]["step"], "stop_pump")

    def test_ack_rejects_unknown_command_and_mismatched_device(self) -> None:
        _, store, mqtt_client, _, dispatcher, _, _ = self._build_stack()
        proposal = ActionProposal(
            trace_id="trace-ackcheck1",
            device_id="esp32-1",
            zone_id="tray_1",
            actuator="irrigation_sequence",
            action="START",
            duration_sec=5,
            origin=DecisionOrigin.OPERATOR,
            reason="manual watering",
            requested_at_ms=100_000,
            command_id="cmd-ackcheck",
        )
        dispatcher.dispatch_proposal(proposal)
        status_before = store.get_command_status("cmd-ackcheck")

        unknown = dispatcher.handle_ack(
            CommandAck(
                message_id="msg-ack-unknown",
                trace_id="trace-ack-unknown",
                command_id="cmd-missing",
                device_id="esp32-1",
                zone_id="tray_1",
                status="acked",
                local_timestamp_ms=100_100,
                observed_state={},
            )
        )
        mismatched = dispatcher.handle_ack(
            CommandAck(
                message_id="msg-ack-baddev",
                trace_id="trace-ack-baddev",
                command_id="cmd-ackcheck",
                execution_id=status_before["execution"]["execution_id"],
                step="open_valve",
                device_id="esp32-other",
                zone_id="tray_1",
                status="acked",
                local_timestamp_ms=100_100,
                observed_state={},
            )
        )

        status_after = store.get_command_status("cmd-ackcheck")
        self.assertIsNone(unknown)
        self.assertIsNone(mismatched)
        self.assertEqual(status_after["lifecycle"], status_before["lifecycle"])
        self.assertGreaterEqual(len(store._audit_logs), 3)

    def test_result_rejects_malformed_transition(self) -> None:
        _, store, _, _, dispatcher, _, _ = self._build_stack()
        dispatcher.dispatch_proposal(
            ActionProposal(
                trace_id="trace-badresult",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="manual watering",
                requested_at_ms=100_000,
                command_id="cmd-badresult",
            )
        )
        status_before = store.get_command_status("cmd-badresult")
        rejected = dispatcher.handle_result(
            CommandResult(
                message_id="msg-result-1",
                trace_id="trace-badresult",
                command_id="cmd-badresult",
                execution_id=status_before["execution"]["execution_id"],
                step="stop_pump",
                device_id="esp32-1",
                zone_id="tray_1",
                status="completed",
                local_timestamp_ms=100_100,
                observed_state={},
                metrics={},
            )
        )
        status_after = store.get_command_status("cmd-badresult")
        self.assertIsNone(rejected)
        self.assertEqual(status_after["lifecycle"], CommandLifecycle.DISPATCHED.value)

    def test_duplicate_ack_is_ignored_by_orchestrator(self) -> None:
        _, store, _, _, dispatcher, _, _ = self._build_stack()
        dispatcher.dispatch_proposal(
            ActionProposal(
                trace_id="trace-dup-ack",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="manual watering",
                requested_at_ms=100_000,
                command_id="cmd-dup-ack",
            )
        )
        status_before = store.get_command_status("cmd-dup-ack")
        first = dispatcher.handle_ack(
            CommandAck(
                message_id="msg-ack-dup-1",
                trace_id="trace-dup-ack",
                command_id="cmd-dup-ack",
                execution_id=status_before["execution"]["execution_id"],
                step="open_valve",
                device_id="esp32-1",
                zone_id="tray_1",
                status="acked",
                local_timestamp_ms=100_100,
                observed_state={},
            )
        )
        duplicate = dispatcher.handle_ack(
            CommandAck(
                message_id="msg-ack-dup-2",
                trace_id="trace-dup-ack",
                command_id="cmd-dup-ack",
                execution_id=status_before["execution"]["execution_id"],
                step="open_valve",
                device_id="esp32-1",
                zone_id="tray_1",
                status="acked",
                local_timestamp_ms=100_101,
                observed_state={},
            )
        )

        status_after = store.get_command_status("cmd-dup-ack")
        self.assertIsNotNone(first)
        self.assertIsNotNone(duplicate)
        self.assertEqual(status_after["execution"]["execution_state"], ExecutionState.ACKED.value)
        self.assertTrue(any(entry["action_type"] == "COMMAND_ACK_DUPLICATE" for entry in store._audit_logs))

    def test_stale_result_is_rejected_after_step_advance(self) -> None:
        _, store, _, _, dispatcher, _, _ = self._build_stack()
        with mock.patch("backend.execution.orchestrator.time.time", return_value=100.0):
            dispatcher.dispatch_proposal(
                ActionProposal(
                    trace_id="trace-stale-result",
                    device_id="esp32-1",
                    zone_id="tray_1",
                    actuator="irrigation_sequence",
                    action="START",
                    duration_sec=5,
                    origin=DecisionOrigin.OPERATOR,
                    reason="manual watering",
                    requested_at_ms=100_000,
                    command_id="cmd-stale-result",
                )
            )
        status_before = store.get_command_status("cmd-stale-result")
        dispatcher.handle_result(
            CommandResult(
                message_id="msg-result-open-1",
                trace_id="trace-stale-result",
                command_id="cmd-stale-result",
                execution_id=status_before["execution"]["execution_id"],
                step="open_valve",
                device_id="esp32-1",
                zone_id="tray_1",
                status="completed",
                local_timestamp_ms=100_100,
                observed_state={},
                metrics={},
            )
        )
        with mock.patch("backend.execution.orchestrator.time.time", return_value=102.0):
            dispatcher.sweep()

        stale = dispatcher.handle_result(
            CommandResult(
                message_id="msg-result-open-stale",
                trace_id="trace-stale-result",
                command_id="cmd-stale-result",
                execution_id=status_before["execution"]["execution_id"],
                step="open_valve",
                device_id="esp32-1",
                zone_id="tray_1",
                status="completed",
                local_timestamp_ms=102_100,
                observed_state={},
                metrics={},
            )
        )

        status_after = store.get_command_status("cmd-stale-result")
        self.assertIsNone(stale)
        self.assertEqual(status_after["execution"]["active_step"], "start_pump")

    def test_security_monitor_sets_stale_heartbeat_lock(self) -> None:
        _, store, _, _, _, telemetry_history, security_monitor = self._build_stack()
        stale_at = int(time.time() * 1000) - 200_000
        store.write_device_state(
            DeviceStateMessage(
                message_id="msg-state-stale",
                trace_id="trace-state-stale",
                device_id="esp32-1",
                zone_id="tray_1",
                ts_ms=stale_at,
                connectivity=DeviceConnectivity.ONLINE,
                state={"pump_on": False, "valve_open": False},
            )
        )
        security_monitor.sweep(now_ms=stale_at + 200_000)
        locks = store.get_active_safety_locks("device", "esp32-1")
        alarms = store.get_active_alarms()
        self.assertTrue(any(lock["kind"] == "stale_heartbeat" for lock in locks))
        self.assertTrue(any(alarm["category"] == "stale_heartbeat" for alarm in alarms))

    def test_stale_heartbeat_blocks_new_commands_without_marking_device_offline(self) -> None:
        _, store, _, validator, dispatcher, telemetry_history, security_monitor = self._build_stack()
        stale_at = int(time.time() * 1000) - 200_000
        store.write_device_state(
            DeviceStateMessage(
                message_id="msg-state-stale-block",
                trace_id="trace-state-stale-block",
                device_id="esp32-1",
                zone_id="tray_1",
                ts_ms=stale_at,
                connectivity=DeviceConnectivity.ONLINE,
                state={"pump_on": False, "valve_open": False},
            )
        )
        security_monitor.sweep(now_ms=stale_at + 200_000)

        decision = validator.validate(
            store,
            ActionProposal(
                trace_id="trace-stale-block",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="stale heartbeat test",
                requested_at_ms=stale_at + 200_100,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("device_lock:stale_heartbeat", decision.reasons)
        self.assertEqual(store.get_device_state("esp32-1")["connectivity"], DeviceConnectivity.ONLINE.value)
        self.assertIsNone(store.get_device_state("esp32-1").get("fault_reason"))

    def test_security_monitor_flags_flow_anomaly_from_history(self) -> None:
        _, store, _, _, _, telemetry_history, security_monitor = self._build_stack()
        message = TelemetryMessage.model_validate(
            {
                "message_id": "msg-flow-anomaly",
                "trace_id": "trace-flow-anomaly",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "ts_ms": 1000,
                "local_ts_ms": 1000,
                "sensors": {"flow_rate_ml_per_min": 120.0, "tank_level": 90.0},
            }
        )
        telemetry_history.write_telemetry(message)
        security_monitor.process_telemetry(message)
        self.assertTrue(any(lock["kind"] == "flow_anomaly" for lock in store.get_active_safety_locks("zone", "tray_1")))

    def test_telemetry_contract_requires_zone_and_device(self) -> None:
        message = TelemetryMessage.model_validate(
            {
                "message_id": "msg-12345678",
                "trace_id": "trace-12345678",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "ts_ms": 1000,
                "local_ts_ms": 1000,
                "sensors": {"soil_moisture": 12.4, "temperature": 29.3},
            }
        )
        self.assertEqual(message.device_id, "esp32-1")
        self.assertEqual(message.zone_id, "tray_1")


if __name__ == "__main__":
    unittest.main()
