from __future__ import annotations

import unittest

from backend.safety.validator import ActionProposal
from mqtt.topics import ACK_SUFFIX, EVENTS_SUFFIX, PRESENCE_SUFFIX, TELEMETRY_SUFFIX, parse_topic, presence_topic, system_events_topic, zone_telemetry_topic
from shared.contracts.messages import CommandAck, DecisionOrigin, TelemetryMessage
from tests.fixtures import device_state_payload, telemetry_payload
from tests.harness import BackendTestHarness


class BackendUnitTests(unittest.TestCase):
    def test_topic_routing_parses_known_channels(self) -> None:
        parsed = parse_topic("greenhouse/device/esp32-1/telemetry")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.device_id, "esp32-1")
        self.assertEqual(parsed.channel, TELEMETRY_SUFFIX)
        self.assertFalse(parsed.is_legacy)

        legacy = parse_topic("greenhouse/device/esp32-1/telemetry/raw")
        self.assertIsNotNone(legacy)
        assert legacy is not None
        self.assertEqual(legacy.channel, TELEMETRY_SUFFIX)
        self.assertTrue(legacy.is_legacy)
        self.assertEqual(legacy.canonical_topic, "greenhouse/device/esp32-1/telemetry")

        ack = parse_topic("greenhouse/device/esp32-1/ack")
        self.assertIsNotNone(ack)
        assert ack is not None
        self.assertEqual(ack.channel, ACK_SUFFIX)

        zone = parse_topic(zone_telemetry_topic("tray_1"))
        self.assertIsNotNone(zone)
        assert zone is not None
        self.assertEqual(zone.scope, "zone")
        self.assertEqual(zone.zone_id, "tray_1")

        presence = parse_topic(presence_topic("esp32-1"))
        self.assertIsNotNone(presence)
        assert presence is not None
        self.assertEqual(presence.channel, PRESENCE_SUFFIX)

        system = parse_topic(system_events_topic())
        self.assertIsNotNone(system)
        assert system is not None
        self.assertEqual(system.channel, EVENTS_SUFFIX)

        self.assertIsNone(parse_topic("greenhouse/device/esp32-1/unknown"))
        self.assertIsNone(parse_topic("invalid/topic"))

    def test_contract_models_accept_canonical_and_legacy_fields(self) -> None:
        telemetry = TelemetryMessage.model_validate(
            {
                "message_id": "msg-telemetry-canonical-0001",
                "correlation_id": "corr-telemetry-canonical-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "timestamp": 1_000,
                "message_counter": 7,
                "sensors": {"soil_moisture": 21.5},
                "status": {"online": True},
                "meta": {"firmware": "1.0.0"},
            }
        )
        self.assertEqual(telemetry.ts_ms, 1_000)
        self.assertEqual(telemetry.trace_id, "corr-telemetry-canonical-0001")
        self.assertEqual(telemetry.message_counter, 7)

        ack = CommandAck.model_validate(
            {
                "message_id": "msg-ack-legacy-0001",
                "trace_id": "trace-ack-legacy-0001",
                "command_id": "cmd-ack-legacy-0001",
                "execution_id": "exec-ack-legacy1",
                "step": "open_valve",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "status": "acked",
                "local_timestamp_ms": 1_100,
                "observed_state": {},
            }
        )
        self.assertEqual(ack.correlation_id, "trace-ack-legacy-0001")
        self.assertEqual(ack.local_timestamp, 1_100)
        self.assertEqual(ack.status_code, "acked")

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
