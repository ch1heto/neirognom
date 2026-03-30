from __future__ import annotations

import unittest

from shared.contracts.messages import CommandLifecycle
from tests.fixtures import device_state_payload, llama_invalid_response, llama_water_response, telemetry_payload
from tests.harness import BackendTestHarness


class BackendAiFlowTests(unittest.TestCase):
    def test_llama_recommendation_dispatches_through_backend_pipeline(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = llama_water_response(zone_id="tray_1", duration_sec=12)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-flow-0001",
                trace_id="trace-ai-flow-0001",
                sensors={"soil_moisture": 24.0, "temperature": 23.0, "tank_level": 90.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 1)
        command = harness.latest_command_status()
        self.assertEqual(command["requested_by"], "llama")
        self.assertEqual(command["requested_payload"]["duration_sec"], 12)
        self.assertEqual(command["lifecycle"], CommandLifecycle.DISPATCHED.value)

    def test_invalid_llama_output_falls_back_to_no_dispatch(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = llama_invalid_response()

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-invalid-0001",
                trace_id="trace-ai-invalid-0001",
                sensors={"soil_moisture": 24.0, "temperature": 23.0, "tank_level": 90.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 0)
        self.assertEqual(harness.store.list_active_commands(), [])


if __name__ == "__main__":
    unittest.main()
