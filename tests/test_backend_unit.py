from __future__ import annotations

import unittest

from backend.safety.validator import ActionProposal
from mqtt.topics import CMD_ACK_SUFFIX, TELEMETRY_RAW_SUFFIX, parse_topic
from shared.contracts.messages import DecisionOrigin
from tests.fixtures import device_state_payload, telemetry_payload
from tests.harness import BackendTestHarness


class BackendUnitTests(unittest.TestCase):
    def test_topic_routing_parses_known_channels(self) -> None:
        parsed = parse_topic("greenhouse/device/esp32-1/telemetry/raw")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.device_id, "esp32-1")
        self.assertEqual(parsed.channel, TELEMETRY_RAW_SUFFIX)

        ack = parse_topic("greenhouse/device/esp32-1/cmd/ack")
        self.assertIsNotNone(ack)
        assert ack is not None
        self.assertEqual(ack.channel, CMD_ACK_SUFFIX)

        self.assertIsNone(parse_topic("greenhouse/device/esp32-1/unknown"))
        self.assertIsNone(parse_topic("invalid/topic"))

    def test_command_record_normalizes_action_and_binds_nonce(self) -> None:
        harness = BackendTestHarness(command_ttl_sec=15)
        record = harness.validator.build_command_record(
            ActionProposal(
                trace_id="trace-unit-normalize",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="start",
                duration_sec=7,
                origin=DecisionOrigin.OPERATOR,
                reason="manual test",
                requested_at_ms=10_000,
                command_id="cmd-unit-normalize",
            )
        )

        self.assertEqual(record.command_id, "cmd-unit-normalize")
        self.assertEqual(record.requested_payload["action"], "START")
        self.assertEqual(record.expires_at_ms, 25_000)
        self.assertEqual(record.metadata["device_binding"], "esp32-1")
        self.assertEqual(record.metadata["zone_binding"], "tray_1")
        self.assertEqual(record.metadata["replay_window_ms"], 15_000)
        self.assertTrue(str(record.metadata["nonce"]).startswith("nonce-"))

    def test_safety_validator_rejects_offline_device(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(connectivity="offline"))

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-offline-check",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=10,
                origin=DecisionOrigin.OPERATOR,
                reason="manual watering",
                requested_at_ms=12_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("device_offline", decision.reasons)

    def test_decision_engine_prefers_deterministic_policy_before_llama(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.llama.response = {
            "decision": "water_zone",
            "zone_id": "tray_1",
            "duration_sec": 20,
            "reason": "llama fallback",
            "confidence": 0.75,
        }

        from shared.contracts.messages import TelemetryMessage

        proposal = harness.decision_engine.evaluate_telemetry(
            TelemetryMessage.model_validate(
                telemetry_payload(
                    message_id="msg-unit-temp-0001",
                    trace_id="trace-unit-temp-0001",
                    sensors={"soil_moisture": 42.0, "temperature": 31.5, "tank_level": 90.0},
                )
            )
        )

        self.assertIsNotNone(proposal)
        assert proposal is not None
        self.assertEqual(proposal.actuator, "vent_fan")
        self.assertEqual(proposal.action, "ON")
        self.assertEqual(len(harness.llama.calls), 0)


if __name__ == "__main__":
    unittest.main()
