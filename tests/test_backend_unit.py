from __future__ import annotations

import unittest
from dataclasses import replace

from backend.domain.models import AutomationFlagRecord, ManualLeaseRecord, SafetyLockRecord
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

    def test_safety_validator_blocks_mutually_exclusive_zone_conflict(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.configure_zone("tray_1", mutually_exclusive_zones=["tray_2"])
        harness.configure_zone("tray_2", device_id="esp32-2")
        harness.seed_device_state(device_state_payload(message_id="msg-state-zone2-0001", trace_id="trace-state-zone2-0001", device_id="esp32-2", zone_id="tray_2"))
        harness.execute_manual_action(
            {
                "trace_id": "trace-exclusive-active",
                "command_id": "cmd-exclusive-active",
                "device_id": "esp32-2",
                "zone_id": "tray_2",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 10_000,
            }
        )

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-exclusive-check",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="conflict check",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("mutually_exclusive_zone_active:tray_2", decision.reasons)

    def test_safety_validator_blocks_shared_line_conflict(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.configure_zone("tray_1", line_id="line_a", shared_line_restricted=True)
        harness.configure_zone("tray_2", device_id="esp32-2", line_id="line_a", shared_line_restricted=True)
        harness.seed_device_state(device_state_payload(message_id="msg-state-zone2-line-0001", trace_id="trace-state-zone2-line-0001", device_id="esp32-2", zone_id="tray_2"))
        harness.execute_manual_action(
            {
                "trace_id": "trace-line-active",
                "command_id": "cmd-line-active",
                "device_id": "esp32-2",
                "zone_id": "tray_2",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 10_000,
            }
        )

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-line-check",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="line check",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("shared_line_conflict:line_a:tray_2", decision.reasons)

    def test_safety_validator_blocks_when_max_active_lines_reached(self) -> None:
        harness = BackendTestHarness(max_active_lines=1)
        harness.seed_device_state(device_state_payload())
        harness.configure_zone("tray_1", line_id="line_a")
        harness.configure_zone("tray_2", device_id="esp32-2", line_id="line_b")
        harness.seed_device_state(device_state_payload(message_id="msg-state-zone2-active-lines-0001", trace_id="trace-state-zone2-active-lines-0001", device_id="esp32-2", zone_id="tray_2"))
        harness.execute_manual_action(
            {
                "trace_id": "trace-lines-active",
                "command_id": "cmd-lines-active",
                "device_id": "esp32-2",
                "zone_id": "tray_2",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 10_000,
            }
        )

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-lines-check",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="line limit",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("max_active_lines_reached:2:1", decision.reasons)

    def test_duration_is_clamped_to_zone_limit_in_command_record(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.configure_zone("tray_1", max_open_duration_sec=4)
        harness.validator._zone_configs["tray_1"] = replace(harness.validator._zone_configs["tray_1"], max_open_duration_sec=4)

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-duration-clamp",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=12,
                origin=DecisionOrigin.OPERATOR,
                reason="duration clamp",
                requested_at_ms=10_000,
            ),
        )
        record = harness.validator.build_command_record(
            ActionProposal(
                trace_id="trace-duration-clamp",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=12,
                origin=DecisionOrigin.OPERATOR,
                reason="duration clamp",
                requested_at_ms=10_000,
            )
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.max_duration_ms, 4_000)
        self.assertEqual(record.requested_payload["duration_sec"], 4)
        self.assertEqual(record.metadata["duration_clamped_from_sec"], 12)

    def test_manual_lock_and_emergency_stop_are_explicit_rejection_reasons(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.store.set_automation_flag(
            AutomationFlagRecord(
                flag_name="maintenance_mode",
                enabled=True,
                updated_at_ms=10_000,
                payload={},
            )
        )
        harness.store.create_manual_lease(
            ManualLeaseRecord(
                lease_id="lease-manual-lock-0001",
                zone_id="tray_1",
                holder="operator-a",
                created_at_ms=10_000,
                expires_at_ms=30_000,
                payload={},
            )
        )
        harness.store.create_safety_lock(
            SafetyLockRecord(
                lock_id="lock-zone-manual-0001",
                scope="zone",
                scope_id="tray_1",
                kind="manual_lock",
                reason="operator lock",
                created_at_ms=10_000,
                owner="operator-a",
                payload={},
            )
        )
        harness.store._global_state["emergency_stop"] = True

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-lock-check",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_sequence",
                action="START",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="manual lock check",
                requested_at_ms=11_000,
                metadata={"operator_id": "operator-b"},
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("global_emergency_stop", decision.reasons)
        self.assertIn("manual_lock_active:operator-a", decision.reasons)
        self.assertIn("maintenance_mode_active", decision.reasons)

    def test_faulted_contour_blocks_new_irrigation(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.configure_zone("tray_1", pump_id="pump_main", line_id="line_a")
        harness.store._executions["exec-faulted-open-0001"] = {
            "execution_id": "exec-faulted-open-0001",
            "command_id": "cmd-faulted-open-0001",
            "device_id": "esp32-1",
            "zone_id": "tray_1",
            "lifecycle": "ABORTED",
            "phase": "CLOSE_VALVE",
            "active_step": "close_valve",
            "updated_at_ms": 10_000,
            "metadata": {},
            "result_payload": {},
        }
        harness.configure_zone("tray_1", device_state={"valve_open": True})

        decision = harness.validator.validate(
            harness.store,
            ActionProposal(
                trace_id="trace-faulted-contour",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="master_pump",
                action="ON",
                duration_sec=5,
                origin=DecisionOrigin.OPERATOR,
                reason="faulted contour check",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertTrue(any(reason.startswith("safe_stop_pending:") or reason.startswith("faulted_contour_open:") for reason in decision.reasons))

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
