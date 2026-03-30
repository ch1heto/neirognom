from __future__ import annotations

import json
import unittest
from unittest import mock

from backend.config import LlamaConfig
from backend.domain.models import CommandType
from integrations.llama.client import LlamaDecisionClient
from shared.contracts.messages import AlertEvent, CommandLifecycle, LlmDecisionRequest, TelemetryMessage
from tests.fixtures import (
    device_state_payload,
    llama_dose_response,
    llama_invalid_extra_fields_response,
    llama_invalid_json_response,
    llama_invalid_response,
    llama_missing_required_fields_response,
    llama_no_action_response,
    telemetry_payload,
)
from tests.harness import BackendTestHarness


class BackendAiFlowTests(unittest.TestCase):
    class _MockResponse:
        def __init__(self, body: dict) -> None:
            self._body = body

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._body

    def test_llama_recommendation_dispatches_through_backend_pipeline_with_enriched_context(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.execute_manual_action(
            {
                "trace_id": "trace-ai-prior-command",
                "command_id": "cmd-ai-prior-command",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "nutrient_doser",
                "action": "START",
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
        harness.llama.response = llama_dose_response(zone_id="tray_1", duration_sec=6, dose_ml=35)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-flow-0001",
                trace_id="trace-ai-flow-0001",
                sensors={"ph": 5.2, "ec": 0.9, "water_level": 72.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        request = harness.llama.calls[0]
        self.assertEqual(request.zone_state["zone_id"], "tray_1")
        self.assertEqual(request.device_state["device_id"], "esp32-1")
        self.assertIn("automation_enabled", request.automation_flags)
        self.assertGreaterEqual(len(request.recent_zone_commands), 1)
        self.assertGreaterEqual(len(request.recent_device_commands), 1)
        self.assertIn("ph", request.telemetry_windows)
        self.assertIn("ec", request.telemetry_windows)
        self.assertIn("water_level", request.telemetry_windows)
        self.assertTrue(any(item.decision == "dose_solution" for item in request.allowed_actions))
        self.assertGreaterEqual(len(request.active_alarms), 1)
        self.assertGreaterEqual(len(harness.mqtt.published), 1)
        command = harness.latest_command_status()
        self.assertEqual(command["requested_by"], "llama")
        self.assertEqual(command["requested_payload"]["actuator"], "nutrient_doser")
        self.assertEqual(command["requested_payload"]["duration_sec"], 6)
        self.assertEqual(command["requested_payload"]["metadata"]["dose_ml"], 35)
        self.assertEqual(command["lifecycle"], CommandLifecycle.DISPATCHED.value)

    def test_deterministic_path_still_bypasses_llama(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(state={"valve_open": True, "doser_active": False, "pump_on": False}))
        harness.llama.response = llama_dose_response(zone_id="tray_1", duration_sec=6)

        proposal = harness.decision_engine.evaluate_telemetry(
            TelemetryMessage.model_validate(
                telemetry_payload(
                    message_id="msg-ai-deterministic-0001",
                    trace_id="trace-ai-deterministic-0001",
                    sensors={"ph": 6.1, "ec": 1.7, "water_level": 5.0},
                )
            )
        )

        self.assertIsNotNone(proposal)
        assert proposal is not None
        self.assertEqual(proposal.actuator, "irrigation_valve")
        self.assertEqual(proposal.action, "CLOSE")
        self.assertEqual(len(harness.llama.calls), 0)

    def test_invalid_llama_output_falls_back_to_no_dispatch(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = llama_invalid_response()

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-invalid-0001",
                trace_id="trace-ai-invalid-0001",
                sensors={"ph": 5.3, "ec": 0.95, "water_level": 72.0},
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
                sensors={"ph": 5.3, "ec": 0.95, "water_level": 72.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 0)
        self.assertEqual(harness.store.list_active_commands(), [])

    def test_llama_client_accepts_valid_no_action_schema(self) -> None:
        client = LlamaDecisionClient(LlamaConfig(api_url="http://127.0.0.1:11434/v1/chat/completions", model="llama3.1", timeout_sec=5, api_key=""))
        request = LlmDecisionRequest(trace_id="trace-llama-no-action", device_id="esp32-1", zone_id="tray_1")
        body = {"choices": [{"message": {"content": json.dumps(llama_no_action_response())}}]}

        with mock.patch("integrations.llama.client.requests.post", return_value=self._MockResponse(body)) as post_mock:
            response = client.recommend(request)

        self.assertIsNotNone(response)
        assert response is not None
        self.assertEqual(response.decision, "no_action")
        self.assertEqual(response.zone_id, "tray_1")
        prompt = post_mock.call_args.kwargs["json"]["messages"][0]["content"]
        self.assertIn("Required fields are: decision, zone_id, rationale, confidence.", prompt)
        self.assertIn("Do not include fields like actuator, action, duration_sec", prompt)

    def test_llama_client_accepts_valid_dose_schema(self) -> None:
        client = LlamaDecisionClient(LlamaConfig(api_url="http://127.0.0.1:11434/v1/chat/completions", model="llama3.1", timeout_sec=5, api_key=""))
        request = LlmDecisionRequest(trace_id="trace-llama-dose", device_id="esp32-1", zone_id="tray_1")
        body = {"choices": [{"message": {"content": json.dumps(llama_dose_response(zone_id="tray_1", duration_sec=7, dose_ml=40, confidence=0.81))}}]}

        with mock.patch("integrations.llama.client.requests.post", return_value=self._MockResponse(body)):
            response = client.recommend(request)

        self.assertIsNotNone(response)
        assert response is not None
        self.assertEqual(response.decision, "dose_solution")
        self.assertEqual(response.requested_duration_sec, 7)
        self.assertEqual(response.dose_ml, 40)

    def test_llama_client_rejects_extra_fields(self) -> None:
        client = LlamaDecisionClient(LlamaConfig(api_url="http://127.0.0.1:11434/v1/chat/completions", model="llama3.1", timeout_sec=5, api_key=""))
        request = LlmDecisionRequest(trace_id="trace-llama-extra", device_id="esp32-1", zone_id="tray_1")
        body = {"choices": [{"message": {"content": json.dumps(llama_invalid_extra_fields_response())}}]}

        with mock.patch("integrations.llama.client.requests.post", return_value=self._MockResponse(body)):
            response = client.recommend(request)

        self.assertIsNone(response)

    def test_llama_client_rejects_missing_required_fields(self) -> None:
        client = LlamaDecisionClient(LlamaConfig(api_url="http://127.0.0.1:11434/v1/chat/completions", model="llama3.1", timeout_sec=5, api_key=""))
        request = LlmDecisionRequest(trace_id="trace-llama-missing", device_id="esp32-1", zone_id="tray_1")
        body = {"choices": [{"message": {"content": json.dumps(llama_missing_required_fields_response())}}]}

        with mock.patch("integrations.llama.client.requests.post", return_value=self._MockResponse(body)):
            response = client.recommend(request)

        self.assertIsNone(response)

    def test_llama_proposal_still_goes_through_safety_and_can_be_rejected(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))
        harness.llama.response = llama_dose_response(zone_id="tray_1", duration_sec=6)

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-ai-offline-0001",
                trace_id="trace-ai-offline-0001",
                sensors={"ph": 5.2, "ec": 0.9, "water_level": 72.0},
            )
        )

        self.assertEqual(len(harness.llama.calls), 1)
        self.assertEqual(len(harness.mqtt.published), 0)
        commands = list(harness.store.get_current_state().get("commands", {}).values())
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["command_type"], CommandType.SET_ACTUATOR.value)
        self.assertEqual(commands[0]["requested_by"], "llama")
        self.assertEqual(commands[0]["lifecycle"], CommandLifecycle.ABORTED.value)
        self.assertEqual(commands[0]["last_error"], "device_offline")


if __name__ == "__main__":
    unittest.main()
