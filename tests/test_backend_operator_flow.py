from __future__ import annotations

import unittest
from unittest import mock

from shared.contracts.messages import CommandLifecycle
from tests.fixtures import device_state_payload, result_payload, telemetry_payload
from tests.harness import BackendTestHarness


class BackendOperatorFlowTests(unittest.TestCase):
    def test_manual_operator_action_uses_same_dispatch_pipeline(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-flow-0001",
                "command_id": "cmd-manual-flow-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 1,
                "reason": "operator watering",
                "requested_at_ms": 100_000,
            }
        )
        execution_id = response["execution_id"]
        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)

        harness.ingest_result(result_payload(command_id="cmd-manual-flow-0001", execution_id=execution_id, step="open_valve", trace_id="trace-manual-flow-0001", local_timestamp_ms=100_100))
        with mock.patch("backend.execution.orchestrator.time.time", return_value=102.0):
            harness.dispatcher.sweep()
        harness.ingest_result(result_payload(command_id="cmd-manual-flow-0001", execution_id=execution_id, step="start_pump", trace_id="trace-manual-flow-0001", message_id="msg-result-manual-0002", local_timestamp_ms=102_100))
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-manual-flow-0001",
                trace_id="trace-manual-flow-0001",
                ts_ms=102_200,
                sensors={"soil_moisture": 23.0, "temperature": 22.0, "tank_level": 83.0, "flow_rate_ml_per_min": 110.0},
            )
        )
        with mock.patch("backend.execution.orchestrator.time.time", return_value=104.5):
            harness.dispatcher.sweep()
        harness.ingest_result(result_payload(command_id="cmd-manual-flow-0001", execution_id=execution_id, step="stop_pump", trace_id="trace-manual-flow-0001", message_id="msg-result-manual-0003", local_timestamp_ms=104_600))
        harness.ingest_result(result_payload(command_id="cmd-manual-flow-0001", execution_id=execution_id, step="close_valve", trace_id="trace-manual-flow-0001", message_id="msg-result-manual-0004", local_timestamp_ms=104_700))
        harness.ingest_state(
            device_state_payload(
                message_id="msg-state-manual-0002",
                trace_id="trace-manual-flow-0001",
                ts_ms=104_800,
                state={"pump_on": False, "valve_open": False},
            )
        )
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-manual-flow-0002",
                trace_id="trace-manual-flow-0001",
                ts_ms=104_850,
                sensors={"soil_moisture": 23.5, "temperature": 22.0, "tank_level": 82.0, "flow_rate_ml_per_min": 0.0},
            )
        )
        with mock.patch("backend.execution.orchestrator.time.time", return_value=106.0):
            harness.dispatcher.sweep()

        status = harness.command_status("cmd-manual-flow-0001")
        assert status is not None
        self.assertEqual(status["requested_by"], "operator")
        self.assertEqual(status["lifecycle"], CommandLifecycle.COMPLETED.value)
        self.assertEqual(status["execution"]["phase"], "FINISHED")

    def test_manual_operator_action_rejects_offline_device(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-manual-offline-0001",
                "command_id": "cmd-manual-offline-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "reason": "operator watering",
                "requested_at_ms": 100_000,
            }
        )

        self.assertEqual(response["status"], CommandLifecycle.ABORTED.value)
        self.assertIn("device_offline", response["reasons"])
        self.assertEqual(len(harness.mqtt.published), 0)

    def test_valve_mutual_exclusion_blocks_second_zone(self) -> None:
        harness = BackendTestHarness(max_simultaneous_zones=1)
        harness.seed_device_state(device_state_payload(device_id="esp32-1", zone_id="tray_1"))
        harness.seed_device_state(device_state_payload(message_id="msg-state-other-0001", trace_id="trace-state-other-0001", device_id="esp32-2", zone_id="tray_2"))

        first = harness.execute_manual_action(
            {
                "trace_id": "trace-mutual-0001",
                "command_id": "cmd-mutual-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )
        second = harness.execute_manual_action(
            {
                "trace_id": "trace-mutual-0002",
                "command_id": "cmd-mutual-0002",
                "device_id": "esp32-2",
                "zone_id": "tray_2",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_010,
            }
        )

        self.assertEqual(first["status"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(second["status"], CommandLifecycle.ABORTED.value)
        self.assertIn("max_simultaneous_zones_reached", second["reasons"])


if __name__ == "__main__":
    unittest.main()
