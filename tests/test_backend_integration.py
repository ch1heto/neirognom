from __future__ import annotations

import json
import unittest
from unittest import mock

from backend.domain.models import ExecutionState
from mqtt.topics import telemetry_topic
from shared.contracts.messages import CommandLifecycle
from tests.fixtures import ack_payload, device_state_payload, presence_payload, result_payload, telemetry_payload
from tests.harness import BackendTestHarness


class BackendIntegrationTests(unittest.TestCase):
    def test_ingestion_updates_state_and_dispatches_deterministic_command(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload(state={"valve_open": True, "doser_active": False, "pump_on": False}))

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-integration-telemetry-0001",
                trace_id="trace-integration-telemetry-0001",
                sensors={"ph": 6.1, "ec": 1.7, "water_level": 5.0},
            )
        )

        zone = harness.store.get_zone_state("tray_1")
        self.assertEqual(zone["telemetry"]["water_level"], 5.0)
        self.assertEqual(len(harness.mqtt.published), 1)
        self.assertEqual(harness.mqtt.published[0]["topic"], "greenhouse/device/esp32-1/cmd")
        self.assertEqual(harness.mqtt.published[0]["payload"]["action"], "CLOSE")

    def test_duplicate_mqtt_message_is_ignored_and_audited(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        payload = telemetry_payload(
            message_id="msg-duplicate-0001",
            trace_id="trace-duplicate-0001",
            sensors={"ph": 6.0, "ec": 1.6, "water_level": 88.0},
        )

        harness.ingest_telemetry(payload)
        published_after_first = len(harness.mqtt.published)
        harness.ingest_telemetry(payload)

        self.assertEqual(len(harness.mqtt.published), published_after_first)
        incidents = [entry for entry in harness.store._audit_logs if entry["action_type"] == "INCIDENT"]
        self.assertTrue(any(entry["message"] == "replay_suspected" for entry in incidents))

    def test_old_timestamp_telemetry_goes_to_history_only_and_does_not_rollback_current_state(self) -> None:
        harness = BackendTestHarness(allowed_clock_skew_sec=1, stale_message_policy="history_only")
        harness.seed_device_state(device_state_payload())

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-current-telemetry-0001",
                trace_id="trace-current-telemetry-0001",
                ts_ms=10_000,
                message_counter=10,
                sensors={"ph": 6.1, "ec": 1.7, "water_level": 85.0},
            )
        )
        published_after_current = len(harness.mqtt.published)
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-old-telemetry-0001",
                trace_id="trace-old-telemetry-0001",
                ts_ms=5_000,
                message_counter=11,
                sensors={"ph": 5.3, "ec": 0.9, "water_level": 70.0},
            )
        )

        zone = harness.store.get_zone_state("tray_1")
        history = harness.telemetry_history.get_sensor_history("esp32-1", "ph", 0, 20_000)
        self.assertEqual(zone["telemetry"]["ph"], 6.1)
        self.assertEqual(len(harness.mqtt.published), published_after_current)
        self.assertTrue(any(item["message_id"] == "msg-old-telemetry-0001" for item in history))
        self.assertTrue(any(entry["message"] == "out_of_order" for entry in harness.store._audit_logs))

    def test_lower_message_counter_outside_reorder_window_is_stale(self) -> None:
        harness = BackendTestHarness(allowed_counter_reorder_window=2, stale_message_policy="history_only")
        harness.seed_device_state(device_state_payload())

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-counter-current-0001",
                trace_id="trace-counter-current-0001",
                ts_ms=10_000,
                message_counter=10,
                sensors={"ph": 6.1, "ec": 1.7, "water_level": 85.0},
            )
        )
        published_after_current = len(harness.mqtt.published)
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-counter-stale-0001",
                trace_id="trace-counter-stale-0001",
                ts_ms=10_100,
                message_counter=6,
                sensors={"ph": 5.2, "ec": 0.8, "water_level": 60.0},
            )
        )

        zone = harness.store.get_zone_state("tray_1")
        self.assertEqual(zone["telemetry"]["ph"], 6.1)
        self.assertEqual(len(harness.mqtt.published), published_after_current)
        self.assertTrue(any(entry["payload"].get("reason") == "stale_message_counter" for entry in harness.store._audit_logs if entry["action_type"] == "INCIDENT"))

    def test_slightly_out_of_order_counter_within_window_keeps_history_without_decision_trigger(self) -> None:
        harness = BackendTestHarness(allowed_counter_reorder_window=3, stale_message_policy="history_only")
        harness.seed_device_state(device_state_payload())

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-counter-base-0001",
                trace_id="trace-counter-base-0001",
                ts_ms=10_000,
                message_counter=10,
                sensors={"ph": 6.1, "ec": 1.7, "water_level": 85.0},
            )
        )
        published_after_base = len(harness.mqtt.published)
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-counter-reorder-0001",
                trace_id="trace-counter-reorder-0001",
                ts_ms=9_900,
                message_counter=9,
                sensors={"ph": 5.4, "ec": 0.95, "water_level": 80.0},
            )
        )

        zone = harness.store.get_zone_state("tray_1")
        history = harness.telemetry_history.get_sensor_history("esp32-1", "ph", 0, 20_000)
        self.assertEqual(zone["telemetry"]["ph"], 6.1)
        self.assertEqual(len(harness.mqtt.published), published_after_base)
        self.assertTrue(any(item["message_id"] == "msg-counter-reorder-0001" for item in history))
        self.assertTrue(any(entry["payload"].get("reason") == "out_of_order_counter" for entry in harness.store._audit_logs if entry["action_type"] == "INCIDENT"))

    def test_invalid_telemetry_payload_missing_required_fields_is_rejected(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        harness.ingestion.ingest(
            telemetry_topic("esp32-1"),
            json.dumps(
                {
                    "message_id": "msg-invalid-telemetry-0001",
                    "trace_id": "trace-invalid-telemetry-0001",
                    "device_id": "esp32-1",
                    "zone_id": "tray_1",
                    "ts_ms": 10_000,
                    "message_counter": 1,
                }
            ).encode("utf-8"),
        )

        self.assertEqual(len(harness.telemetry_history._history), 0)
        self.assertEqual(len(harness.mqtt.published), 0)
        self.assertTrue(any(entry["message"] == "validation_fail" for entry in harness.store._audit_logs if entry["action_type"] == "INCIDENT"))

    def test_active_execution_faults_deterministically_on_offline_presence(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-offline-active-0001",
                "command_id": "cmd-offline-active-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )
        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(len(harness.mqtt.published), 1)

        harness.ingest_presence(
            presence_payload(
                message_id="msg-presence-offline-0001",
                trace_id="trace-offline-active-0001",
                connectivity="offline",
                ts_ms=100_500,
            )
        )

        status = harness.command_status("cmd-offline-active-0001")
        device = harness.store.get_device_state("esp32-1")
        zone = harness.store.get_zone_state("tray_1")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.FAILED.value)
        self.assertEqual(status["execution"]["execution_state"], ExecutionState.FAULTED.value)
        self.assertEqual(status["execution"]["last_error"], "device_offline")
        self.assertEqual(device["connectivity"], "offline")
        self.assertEqual(device["fault_reason"], "device_offline")
        self.assertIsNone(zone["reserved_by_execution"])
        self.assertEqual(len(harness.mqtt.published), 1)
        self.assertTrue(any(lock["kind"] == "unsafe_execution_contour" for lock in harness.store.get_active_safety_locks("zone", "tray_1")))
        self.assertTrue(any(lock["kind"] == "device_offline" for lock in harness.store.get_active_safety_locks("device", "esp32-1")))

    def test_offline_device_blocks_new_irrigation_command_after_presence_event(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.ingest_presence(
            presence_payload(
                message_id="msg-presence-offline-block-0001",
                trace_id="trace-offline-block-0001",
                connectivity="offline",
                ts_ms=2_000,
            )
        )

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-offline-block-command-0001",
                "command_id": "cmd-offline-block-command-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 3_000,
            }
        )

        self.assertEqual(response["status"], CommandLifecycle.ABORTED.value)
        self.assertIn("device_offline", response["reasons"])
        self.assertEqual(len(harness.mqtt.published), 0)

    def test_repeated_offline_presence_is_idempotent_for_execution_faulting(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.execute_manual_action(
            {
                "trace_id": "trace-repeat-offline-0001",
                "command_id": "cmd-repeat-offline-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )

        offline_payload = presence_payload(
            message_id="msg-presence-repeat-offline-0001",
            trace_id="trace-repeat-offline-0001",
            connectivity="offline",
            ts_ms=100_500,
        )
        harness.ingest_presence(offline_payload)
        first_status = harness.command_status("cmd-repeat-offline-0001")
        duplicate = dict(offline_payload)
        duplicate["message_id"] = "msg-presence-repeat-offline-0002"
        duplicate["ts_ms"] = 100_600
        duplicate["timestamp"] = 100_600
        harness.ingest_presence(duplicate)
        second_status = harness.command_status("cmd-repeat-offline-0001")

        assert first_status is not None and second_status is not None
        self.assertEqual(first_status["execution"]["updated_at_ms"], second_status["execution"]["updated_at_ms"])
        self.assertEqual(first_status["execution"]["last_error"], second_status["execution"]["last_error"])
        self.assertEqual(len([entry for entry in harness.store._audit_logs if entry["action_type"] == "COMMAND_DEVICE_OFFLINE"]), 1)

    def test_online_presence_clears_offline_fault_fields_and_restores_connectivity(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.ingest_presence(
            presence_payload(
                message_id="msg-presence-offline-online-0001",
                trace_id="trace-offline-online-0001",
                connectivity="offline",
                ts_ms=2_000,
            )
        )
        harness.ingest_presence(
            presence_payload(
                message_id="msg-presence-online-0001",
                trace_id="trace-offline-online-0002",
                connectivity="online",
                ts_ms=3_000,
            )
        )

        device = harness.store.get_device_state("esp32-1")
        self.assertEqual(device["connectivity"], "online")
        self.assertIsNone(device["fault_reason"])
        self.assertFalse(any(lock["kind"] == "device_offline" for lock in harness.store.get_active_safety_locks("device", "esp32-1")))

    def test_stale_command_expires_when_no_ack_or_result_arrives(self) -> None:
        harness = BackendTestHarness(command_ttl_sec=1)
        harness.seed_device_state(device_state_payload())

        response = harness.execute_manual_action(
            {
                "trace_id": "trace-stale-command",
                "command_id": "cmd-stale-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 0,
            }
        )
        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)

        harness.dispatcher.sweep()
        status = harness.command_status("cmd-stale-0001")

        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertEqual(status["execution"]["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertEqual(harness.mqtt.published[-1]["payload"]["step"], "stop_pump")

    def test_timeout_without_ack_marks_execution_timed_out_and_releases_zone(self) -> None:
        harness = BackendTestHarness(command_ttl_sec=60)
        harness.seed_device_state(device_state_payload())

        with mock.patch("backend.execution.orchestrator.time.time", return_value=100.0):
            response = harness.execute_manual_action(
                {
                    "trace_id": "trace-timeout-no-ack",
                    "command_id": "cmd-timeout-no-ack-0001",
                    "device_id": "esp32-1",
                    "zone_id": "tray_1",
                    "actuator": "irrigation_sequence",
                    "action": "START",
                    "duration_sec": 5,
                    "requested_at_ms": 100_000,
                }
            )

        self.assertEqual(response["status"], CommandLifecycle.DISPATCHED.value)
        with mock.patch("backend.execution.orchestrator.time.time", return_value=106.0):
            harness.dispatcher.sweep()
        with mock.patch("backend.execution.orchestrator.time.time", return_value=112.0):
            harness.dispatcher.sweep()

        status = harness.command_status("cmd-timeout-no-ack-0001")
        zone = harness.store.get_zone_state("tray_1")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertEqual(status["execution"]["execution_state"], ExecutionState.TIMED_OUT.value)
        self.assertIsNone(zone["reserved_by_execution"])
        self.assertTrue(any(lock["kind"] == "unsafe_execution_contour" for lock in harness.store.get_active_safety_locks("zone", "tray_1")))

    def test_max_duration_enforcement_stops_pump_after_monitor_window(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-max-duration",
                "command_id": "cmd-max-duration-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 1,
                "requested_at_ms": 100_000,
            }
        )
        execution_id = response["execution_id"]

        harness.ingest_result(result_payload(command_id="cmd-max-duration-0001", execution_id=execution_id, step="open_valve", trace_id="trace-max-duration", local_timestamp_ms=100_100))
        with mock.patch("backend.execution.orchestrator.time.time", return_value=102.0):
            harness.dispatcher.sweep()
        harness.ingest_result(result_payload(command_id="cmd-max-duration-0001", execution_id=execution_id, step="start_pump", trace_id="trace-max-duration", message_id="msg-result-0002", local_timestamp_ms=102_100))
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-flow-confirm-0001",
                trace_id="trace-max-duration",
                ts_ms=102_200,
                sensors={"soil_moisture": 24.0, "temperature": 22.0, "tank_level": 84.0, "flow_rate_ml_per_min": 120.0},
            )
        )

        with mock.patch("backend.execution.orchestrator.time.time", return_value=104.5):
            harness.dispatcher.sweep()

        status = harness.command_status("cmd-max-duration-0001")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.DISPATCHED.value)
        self.assertEqual(status["execution"]["phase"], "STOP_PUMP")
        self.assertEqual(status["execution"]["active_step"], "stop_pump")
        self.assertEqual(harness.mqtt.published[-1]["payload"]["step"], "stop_pump")

    def test_missing_flow_confirmation_triggers_dry_run_abort(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-dry-run",
                "command_id": "cmd-dry-run-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )
        execution_id = response["execution_id"]

        harness.ingest_result(result_payload(command_id="cmd-dry-run-0001", execution_id=execution_id, step="open_valve", trace_id="trace-dry-run", local_timestamp_ms=100_100))
        with mock.patch("backend.execution.orchestrator.time.time", return_value=102.0):
            harness.dispatcher.sweep()
        harness.ingest_result(result_payload(command_id="cmd-dry-run-0001", execution_id=execution_id, step="start_pump", trace_id="trace-dry-run", message_id="msg-result-0003", local_timestamp_ms=102_100))

        with mock.patch("backend.execution.orchestrator.time.time", return_value=108.0):
            harness.dispatcher.sweep()

        status = harness.command_status("cmd-dry-run-0001")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.ABORTED.value)
        self.assertEqual(status["execution"]["active_step"], "stop_pump")
        self.assertEqual(status["execution"]["last_error"], "flow_not_confirmed")

    def test_command_ack_updates_execution_status(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-ack-flow",
                "command_id": "cmd-ack-flow-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )

        harness.ingest_ack(
            ack_payload(
                command_id="cmd-ack-flow-0001",
                execution_id=response["execution_id"],
                step="open_valve",
                trace_id="trace-ack-flow",
                status="acked",
            )
        )
        status = harness.command_status("cmd-ack-flow-0001")

        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.ACKED.value)
        self.assertEqual(status["execution"]["lifecycle"], CommandLifecycle.ACKED.value)
        self.assertEqual(status["execution"]["execution_state"], ExecutionState.ACKED.value)

    def test_duplicate_ack_is_ignored_without_regressing_state(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.execute_manual_action(
            {
                "trace_id": "trace-duplicate-ack",
                "command_id": "cmd-duplicate-ack-0001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "actuator": "irrigation_sequence",
                "action": "START",
                "duration_sec": 5,
                "requested_at_ms": 100_000,
            }
        )

        payload = ack_payload(
            command_id="cmd-duplicate-ack-0001",
            execution_id=response["execution_id"],
            step="open_valve",
            trace_id="trace-duplicate-ack",
            status="acked",
        )
        harness.ingest_ack(payload)
        duplicate = dict(payload)
        duplicate["message_id"] = "msg-ack-duplicate-0002"
        harness.ingest_ack(duplicate)

        status = harness.command_status("cmd-duplicate-ack-0001")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.ACKED.value)
        self.assertEqual(status["execution"]["execution_state"], ExecutionState.ACKED.value)
        self.assertTrue(any(entry["action_type"] == "COMMAND_ACK_DUPLICATE" for entry in harness.store._audit_logs))

    def test_timeout_after_ack_finalizes_fault_path_and_releases_zone(self) -> None:
        harness = BackendTestHarness(command_ttl_sec=60)
        harness.seed_device_state(device_state_payload())

        with mock.patch("backend.execution.orchestrator.time.time", return_value=100.0):
            response = harness.execute_manual_action(
                {
                    "trace_id": "trace-timeout-after-ack",
                    "command_id": "cmd-timeout-after-ack-0001",
                    "device_id": "esp32-1",
                    "zone_id": "tray_1",
                    "actuator": "irrigation_sequence",
                    "action": "START",
                    "duration_sec": 5,
                    "requested_at_ms": 100_000,
                }
            )
        harness.ingest_ack(
            ack_payload(
                command_id="cmd-timeout-after-ack-0001",
                execution_id=response["execution_id"],
                step="open_valve",
                trace_id="trace-timeout-after-ack",
                status="acked",
                local_timestamp_ms=100_100,
            )
        )
        with mock.patch("backend.execution.orchestrator.time.time", return_value=112.0):
            harness.dispatcher.sweep()
        with mock.patch("backend.execution.orchestrator.time.time", return_value=118.0):
            harness.dispatcher.sweep()

        status = harness.command_status("cmd-timeout-after-ack-0001")
        zone = harness.store.get_zone_state("tray_1")
        assert status is not None
        self.assertEqual(status["lifecycle"], CommandLifecycle.EXPIRED.value)
        self.assertEqual(status["execution"]["execution_state"], ExecutionState.TIMED_OUT.value)
        self.assertIn(status["execution"]["last_error"], {"result_timeout_during_safe_stop", "ack_timeout_during_safe_stop"})
        self.assertIsNone(zone["reserved_by_execution"])
        self.assertEqual(harness.mqtt.published[-1]["payload"]["step"], "stop_pump")


if __name__ == "__main__":
    unittest.main()
