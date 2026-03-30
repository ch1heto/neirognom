from __future__ import annotations

import unittest
from unittest import mock

from backend.domain.models import ExecutionState
from shared.contracts.messages import CommandLifecycle
from tests.fixtures import ack_payload, device_state_payload, result_payload, telemetry_payload
from tests.harness import BackendTestHarness


class BackendIntegrationTests(unittest.TestCase):
    def test_ingestion_updates_state_and_dispatches_deterministic_command(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-integration-telemetry-0001",
                trace_id="trace-integration-telemetry-0001",
                sensors={"soil_moisture": 12.0, "temperature": 23.0, "tank_level": 85.0},
            )
        )

        zone = harness.store.get_zone_state("tray_1")
        self.assertEqual(zone["telemetry"]["soil_moisture"], 12.0)
        self.assertEqual(len(harness.mqtt.published), 1)
        self.assertEqual(harness.mqtt.published[0]["topic"], "greenhouse/device/esp32-1/cmd")
        self.assertEqual(harness.mqtt.published[0]["payload"]["step"], "open_valve")

    def test_duplicate_mqtt_message_is_ignored_and_audited(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        payload = telemetry_payload(
            message_id="msg-duplicate-0001",
            trace_id="trace-duplicate-0001",
            sensors={"soil_moisture": 14.0, "temperature": 22.0, "tank_level": 88.0},
        )

        harness.ingest_telemetry(payload)
        harness.ingest_telemetry(payload)

        self.assertEqual(len(harness.mqtt.published), 1)
        incidents = [entry for entry in harness.store._audit_logs if entry["action_type"] == "INCIDENT"]
        self.assertTrue(any(entry["message"] == "replay_suspected" for entry in incidents))

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
