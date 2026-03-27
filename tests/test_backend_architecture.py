import json
import os
import time
import unittest
from unittest import mock

import paho.mqtt.client as mqtt

from backend.config import load_backend_config
from backend.dispatcher.service import CommandDispatcher
from backend.domain.models import ManualLeaseRecord
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.store import MemoryStateStore
from shared.contracts.messages import CommandLifecycle, DecisionOrigin, DeviceConnectivity, DeviceStateMessage, TelemetryMessage


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
        mqtt_client = DummyMqttClient()
        validator = SafetyValidator(config)
        dispatcher = CommandDispatcher(mqtt_client, config, store, validator)
        return config, store, mqtt_client, validator, dispatcher

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
        _, store, _, validator, _ = self._build_stack()
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
        _, store, mqtt_client, _, dispatcher = self._build_stack()
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
        self.assertEqual(status["lifecycle"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(status["execution"]["phase"], "OPEN_VALVE")
        self.assertEqual(store.get_zone_state("tray_1")["reserved_by_execution"], status["execution"]["execution_id"])

    def test_expired_command_transitions_to_expired_and_safe_stop(self) -> None:
        _, store, mqtt_client, _, dispatcher = self._build_stack()
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
        _, store, mqtt_client, _, dispatcher = self._build_stack()
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
