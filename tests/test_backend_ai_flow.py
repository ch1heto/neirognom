from __future__ import annotations

import unittest

from backend.domain.models import CommandType
from shared.contracts.messages import AlertEvent, CommandLifecycle
from tests.fixtures import device_state_payload, llama_invalid_json_response, llama_invalid_response, llama_water_response, telemetry_payload
from tests.harness import BackendTestHarness


class BackendAiFlowTests(unittest.TestCase):
    def test_llama_recommendation_dispatches_through_backend_pipeline_with_enriched_context(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.execute_manual_action(
            {
                "trace_id": "trace-ai-prior-command",
                "command_id": "cmd-ai-prior-command",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "vent_fan",
                "action": "ON",
                "duration_sec": 5,
                "requested_at_ms": 500,
            }
        )
        harness.store.record_alarm(
            AlertEvent(
                alert_id="alert-ai-context-0001",
                trace_id="trace-ai-context-0001",
                device_id="esp32-1",
                zone_id="tray_1",
                severity="warning",
                category="context_test",
                message="context alarm",
                created_at_ms=900,
                details={},
            )
        )
        harness.llama.response = llama_water_response(zone_id="tray_1", duration_sec=12)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-flow-0001",
                trace_id="trace-ai-flow-0001",
                sensors={"soil_moisture": 24.0, "temperature": 23.0, "tank_level": 90.0, "flow_rate_ml_per_min": 0.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        request = harness.llama.calls[0]
        self.assertEqual(request.zone_state["zone_id"], "tray_1")
        self.assertEqual(request.device_state["device_id"], "esp32-1")
        self.assertIn("automation_enabled", request.automation_flags)
        self.assertGreaterEqual(len(request.recent_zone_commands), 1)
        self.assertGreaterEqual(len(request.recent_device_commands), 1)
        self.assertIn("soil_moisture", request.telemetry_windows)
        self.assertIn("temperature", request.telemetry_windows)
        self.assertIn("tank_level", request.telemetry_windows)
        self.assertIn("flow_rate_ml_per_min", request.telemetry_windows)
        self.assertTrue(any(item.decision == "water_zone" for item in request.allowed_actions))
        self.assertGreaterEqual(len(request.active_alarms), 1)
        self.assertGreaterEqual(len(harness.mqtt.published), 2)
        command = harness.latest_command_status()
        self.assertEqual(command["requested_by"], "llama")
        self.assertEqual(command["requested_payload"]["duration_sec"], 12)
        self.assertEqual(command["lifecycle"], CommandLifecycle.DISPATCHED.value)

    def test_deterministic_path_still_bypasses_llama(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = llama_water_response(zone_id="tray_1", duration_sec=12)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-deterministic-0001",
                trace_id="trace-ai-deterministic-0001",
                sensors={"soil_moisture": 12.0, "temperature": 23.0, "tank_level": 90.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 0)
        self.assertEqual(len(harness.mqtt.published), 1)
        command = harness.latest_command_status()
        self.assertEqual(command["requested_by"], "deterministic")

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

    def test_invalid_llama_json_falls_back_to_no_dispatch(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = llama_invalid_json_response()

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-invalid-json-0001",
                trace_id="trace-ai-invalid-json-0001",
                sensors={"soil_moisture": 24.0, "temperature": 23.0, "tank_level": 90.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 0)
        self.assertEqual(harness.store.list_active_commands(), [])

    def test_llama_proposal_still_goes_through_safety_and_can_be_rejected(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))
        harness.llama.response = llama_water_response(zone_id="tray_1", duration_sec=12)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-offline-0001",
                trace_id="trace-ai-offline-0001",
                sensors={"soil_moisture": 24.0, "temperature": 23.0, "tank_level": 90.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 0)
        commands = list(harness.store.get_current_state().get("commands", {}).values())
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["command_type"], CommandType.IRRIGATE_ZONE.value)
        self.assertEqual(commands[0]["requested_by"], "llama")
        self.assertEqual(commands[0]["lifecycle"], CommandLifecycle.ABORTED.value)
        self.assertEqual(commands[0]["last_error"], "device_offline")


if __name__ == "__main__":
    unittest.main()
