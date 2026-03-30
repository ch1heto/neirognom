from __future__ import annotations

import unittest

from shared.contracts.messages import CommandLifecycle
from tests.fixtures import device_state_payload
from tests.harness import BackendTestHarness


class BackendOperatorFlowTests(unittest.TestCase):
    def test_operator_ui_manual_command_uses_manual_ttl_and_operator_source(self) -> None:
        harness = BackendTestHarness(manual_command_ttl_sec=9)
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-ttl-0001",
                "command_id": "cmd-manual-ttl-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_valve",
                "action": "OPEN",
                "duration_sec": 3,
                "requested_at_ms": 100_000,
                "metadata": {"submitted_via": "operator_ui", "command_source": "operator"},
            }
        )

        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(harness.last_command_message()["ttl_sec"], 9)
        self.assertEqual(harness.last_command_message()["source"], "operator")
        status = harness.command_status("cmd-manual-ttl-0001")
        assert status is not None
        self.assertEqual(status["requested_by"], "operator")
        self.assertEqual(status["metadata"]["command_source"], "operator")

    def test_manual_operator_open_valve_uses_same_dispatch_pipeline(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-open-0001",
                "command_id": "cmd-manual-open-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_valve",
                "action": "OPEN",
                "duration_sec": 4,
                "reason": "operator open valve",
                "requested_at_ms": 100_000,
                "metadata": {"submitted_via": "operator_ui", "command_source": "operator"},
            }
        )

        status = harness.command_status("cmd-manual-open-0001")
        assert status is not None
        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(harness.mqtt.published[-1]["payload"]["actuator"], "irrigation_valve")
        self.assertEqual(harness.mqtt.published[-1]["payload"]["action"], "OPEN")
        self.assertEqual(status["requested_by"], "operator")
        self.assertEqual(status["lifecycle"], CommandLifecycle.DISPATCHED.value)

    def test_manual_operator_dose_solution_uses_doser_actuator(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-dose-0001",
                "command_id": "cmd-manual-dose-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "nutrient_doser",
                "action": "START",
                "duration_sec": 5,
                "reason": "operator dose solution",
                "requested_at_ms": 100_000,
                "metadata": {"submitted_via": "operator_ui", "command_source": "operator"},
            }
        )

        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(harness.mqtt.published[-1]["payload"]["actuator"], "nutrient_doser")
        status = harness.command_status("cmd-manual-dose-0001")
        assert status is not None
        self.assertEqual(status["requested_payload"]["actuator"], "nutrient_doser")

    def test_manual_operator_action_rejects_offline_device(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-offline-0001",
                "command_id": "cmd-manual-offline-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_valve",
                "action": "OPEN",
                "duration_sec": 5,
                "reason": "operator open valve",
                "requested_at_ms": 100_000,
            }
        )

        self.assertEqual(response["status"], CommandLifecycle.ABORTED.value)
        self.assertIn("device_offline", response["reasons"])
        self.assertEqual(len(harness.mqtt.published), 0)


if __name__ == "__main__":
    unittest.main()
