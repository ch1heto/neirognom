from __future__ import annotations

import unittest

from tests.fixtures import device_state_payload, telemetry_payload
from tests.harness import BackendTestHarness


class OperatorUiServiceTests(unittest.TestCase):
    def test_submit_manual_command_uses_backend_pipeline_and_operator_metadata(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        result = harness.operator.submit_manual_command(
            {
                "operator_id": "alice",
                "operator_name": "Alice",
                "zone_id": "tray_1",
                "device_id": "esp32-1",
                "ui_action": "test_watering",
                "duration_sec": 9,
            }
        )

        self.assertEqual(result["status"], "DISPATCHED")
        self.assertEqual(harness.mqtt.published[-1]["payload"]["step"], "open_valve")
        command = harness.command_status(result["command_id"])
        assert command is not None
        self.assertEqual(command["metadata"]["operator_id"], "alice")
        self.assertEqual(command["metadata"]["operator_name"], "Alice")
        self.assertEqual(command["metadata"]["submitted_via"], "operator_ui")

    def test_submit_manual_command_rejects_offline_device(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))

        result = harness.operator.submit_manual_command(
            {
                "operator_id": "alice",
                "operator_name": "Alice",
                "zone_id": "tray_1",
                "device_id": "esp32-1",
                "ui_action": "test_watering",
                "duration_sec": 8,
            }
        )

        self.assertEqual(result["status"], "ABORTED")
        self.assertIn("device_offline", result["reasons"])

    def test_list_devices_zones_exposes_locks_and_action_guards(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-operator-locks-0001",
                trace_id="trace-operator-locks-0001",
                sensors={"soil_moisture": 26.0, "temperature": 23.0, "tank_level": 0.0},
            )
        )

        snapshot = harness.operator.list_devices_zones()
        zone = next(item for item in snapshot["zones"] if item["zone_id"] == "tray_1")
        safety_locks = zone["safety_locks"]

        self.assertTrue(any(lock["kind"] == "empty_tank" for lock in safety_locks))
        self.assertFalse(zone["actions"]["test_watering"]["enabled"])
        self.assertTrue(any("water_source_unavailable" in reason or "global:empty_tank" in reason for reason in zone["actions"]["test_watering"]["reasons"]))

    def test_emergency_stop_creates_lock_and_safe_stop_commands(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        result = harness.operator.emergency_stop({"operator_id": "ops", "operator_name": "Ops"})

        locks = harness.store.get_active_safety_locks("global")
        self.assertEqual(result["status"], "emergency_stop_active")
        self.assertTrue(any(lock["kind"] == "manual_emergency_stop" for lock in locks))
        self.assertFalse(harness.store.get_automation_flags()["automation_enabled"]["enabled"])
        self.assertTrue(any(item["payload"]["action"] == "CLOSE" for item in harness.mqtt.published))
        self.assertTrue(any(item["payload"]["action"] == "OFF" for item in harness.mqtt.published))


if __name__ == "__main__":
    unittest.main()
