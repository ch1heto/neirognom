from __future__ import annotations

import unittest
from pydantic import ValidationError

from mqtt.topics import (
    ACK_SUFFIX,
    CMD_SUFFIX,
    EVENTS_SUFFIX,
    PRESENCE_SUFFIX,
    RESULT_SUFFIX,
    STATE_SUFFIX,
    TELEMETRY_SUFFIX,
    command_ack_topic,
    command_result_topic,
    command_topic,
    ingestion_subscription_topics,
    parse_topic,
    presence_topic,
    state_topic,
    system_events_topic,
    telemetry_topic,
    zone_telemetry_topic,
)
from shared.contracts.messages import ActuatorCommandMessage, CommandAck, CommandResult, EventEnvelope, PresenceMessage, TelemetryMessage


class TransportContractTests(unittest.TestCase):
    def test_topic_builders_use_canonical_routes(self) -> None:
        self.assertEqual(telemetry_topic("esp32-1"), "greenhouse/device/esp32-1/telemetry")
        self.assertEqual(zone_telemetry_topic("tray_1"), "greenhouse/zone/tray_1/telemetry")
        self.assertEqual(state_topic("esp32-1"), "greenhouse/device/esp32-1/state")
        self.assertEqual(command_topic("esp32-1"), "greenhouse/device/esp32-1/cmd")
        self.assertEqual(command_ack_topic("esp32-1"), "greenhouse/device/esp32-1/ack")
        self.assertEqual(command_result_topic("esp32-1"), "greenhouse/device/esp32-1/result")
        self.assertEqual(presence_topic("esp32-1"), "greenhouse/device/esp32-1/presence")
        self.assertEqual(system_events_topic(), "greenhouse/system/events")

    def test_subscription_set_covers_canonical_and_legacy_aliases(self) -> None:
        subscriptions = dict(ingestion_subscription_topics(1))
        self.assertIn("greenhouse/device/+/telemetry", subscriptions)
        self.assertIn("greenhouse/zone/+/telemetry", subscriptions)
        self.assertIn("greenhouse/device/+/presence", subscriptions)
        self.assertIn("greenhouse/system/events", subscriptions)
        self.assertIn("greenhouse/device/+/telemetry/raw", subscriptions)
        self.assertIn("greenhouse/device/+/cmd/ack", subscriptions)
        self.assertIn("greenhouse/device/+/cmd/result", subscriptions)

    def test_parse_topic_maps_legacy_aliases_to_canonical_channels(self) -> None:
        cases = {
            "greenhouse/device/esp32-1/telemetry": TELEMETRY_SUFFIX,
            "greenhouse/device/esp32-1/state": STATE_SUFFIX,
            "greenhouse/device/esp32-1/cmd": CMD_SUFFIX,
            "greenhouse/device/esp32-1/ack": ACK_SUFFIX,
            "greenhouse/device/esp32-1/result": RESULT_SUFFIX,
            "greenhouse/device/esp32-1/presence": PRESENCE_SUFFIX,
            "greenhouse/system/events": EVENTS_SUFFIX,
            "greenhouse/device/esp32-1/telemetry/raw": TELEMETRY_SUFFIX,
            "greenhouse/device/esp32-1/cmd/execute": CMD_SUFFIX,
            "greenhouse/device/esp32-1/cmd/ack": ACK_SUFFIX,
            "greenhouse/device/esp32-1/cmd/result": RESULT_SUFFIX,
            "greenhouse/device/esp32-1/event/error": EVENTS_SUFFIX,
        }
        for topic, channel in cases.items():
            with self.subTest(topic=topic):
                parsed = parse_topic(topic)
                self.assertIsNotNone(parsed)
                assert parsed is not None
                self.assertEqual(parsed.channel, channel)

    def test_telemetry_model_accepts_legacy_and_canonical_envelopes(self) -> None:
        canonical = TelemetryMessage.model_validate(
            {
                "message_id": "msg-telemetry-1001",
                "correlation_id": "corr-telemetry-1001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "timestamp": 1000,
                "message_counter": 11,
                "sensors": {"ph": 6.1, "ec": 1.7, "water_level": 18.0},
                "status": {"online": True},
                "meta": {"fw": "1.2.3"},
            }
        )
        legacy = TelemetryMessage.model_validate(
            {
                "message_id": "msg-telemetry-1002",
                "trace_id": "trace-telemetry-1002",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "ts_ms": 1002,
                "local_ts_ms": 1003,
                "sensors": {"ph": 6.0, "ec": 1.8, "water_level": 17.5},
                "metadata": {"message_counter": 12},
            }
        )
        self.assertEqual(canonical.message_counter, 11)
        self.assertEqual(legacy.timestamp, 1002)
        self.assertEqual(legacy.local_timestamp, 1003)
        self.assertEqual(legacy.correlation_id, "trace-telemetry-1002")

    def test_command_and_result_models_expose_machine_readable_fields(self) -> None:
        command = ActuatorCommandMessage.model_validate(
            {
                "message_id": "msg-command-1001",
                "command_id": "cmd-command-1001",
                "correlation_id": "corr-command-1001",
                "source": "backend",
                "target_device_id": "esp32-1",
                "target_zone_id": "tray_1",
                "action": "OPEN",
                "duration_sec": 5,
                "ttl_sec": 15,
                "created_at": 1000,
                "safety_caps": {
                    "local_hard_max_duration_ms": 5000,
                    "allowed_runtime_window_ms": 15000,
                },
                "execution_id": "exec-command-1001",
                "actuator": "irrigation_valve",
                "step": "open_valve",
                "nonce": "nonce-command-1001",
            }
        )
        result = CommandResult.model_validate(
            {
                "message_id": "msg-result-1001",
                "correlation_id": "corr-result-1001",
                "command_id": "cmd-command-1001",
                "execution_id": "exec-command-1001",
                "step": "open_valve",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "status": "completed",
                "status_code": "completed",
                "local_timestamp": 1010,
                "observed_state": {"valve_open": True},
            }
        )
        self.assertEqual(command.expires_at_ms, 16_000)
        self.assertEqual(command.max_duration_ms, 5_000)
        self.assertEqual(result.status_code, "completed")
        self.assertEqual(result.local_timestamp_ms, 1010)

    def test_activating_command_requires_positive_ttl(self) -> None:
        with self.assertRaises(ValidationError):
            ActuatorCommandMessage.model_validate(
                {
                    "message_id": "msg-command-ttl-0001",
                    "command_id": "cmd-command-ttl-0001",
                    "correlation_id": "corr-command-ttl-0001",
                    "source": "backend",
                    "target_device_id": "esp32-1",
                    "target_zone_id": "tray_1",
                    "action": "OPEN",
                    "duration_sec": 0,
                    "ttl_sec": 0,
                    "created_at": 1000,
                    "safety_caps": {
                        "local_hard_max_duration_ms": 5000,
                        "allowed_runtime_window_ms": 5000,
                    },
                    "execution_id": "exec-command-ttl-0001",
                    "actuator": "irrigation_valve",
                    "step": "open_valve",
                    "nonce": "nonce-command-ttl-0001",
                }
            )

    def test_non_activating_command_allows_zero_ttl(self) -> None:
        command = ActuatorCommandMessage.model_validate(
            {
                "message_id": "msg-command-stop-0001",
                "command_id": "cmd-command-stop-0001",
                "correlation_id": "corr-command-stop-0001",
                "source": "backend",
                "target_device_id": "esp32-1",
                "target_zone_id": "tray_1",
                "action": "STOP",
                    "duration_sec": 0,
                    "ttl_sec": 0,
                    "created_at": 1000,
                    "safety_caps": {
                        "local_hard_max_duration_ms": 0,
                        "allowed_runtime_window_ms": 0,
                    },
                    "execution_id": "exec-command-stop-0001",
                    "actuator": "nutrient_doser",
                    "step": "stop_doser",
                    "nonce": "nonce-command-stop-0001",
                }
            )
        self.assertEqual(command.ttl_sec, 0)

    def test_presence_model_accepts_canonical_and_safe_mode_values(self) -> None:
        presence = PresenceMessage.model_validate(
            {
                "message_id": "msg-presence-1001",
                "correlation_id": "corr-presence-1001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "timestamp": 2000,
                "message_counter": 4,
                "connectivity": "safe_mode",
                "status": {"reason": "wifi_lost"},
                "meta": {"lwt": True},
            }
        )
        self.assertEqual(presence.connectivity.value, "safe_mode")
        self.assertEqual(presence.status["reason"], "wifi_lost")

    def test_ack_and_result_require_status_code(self) -> None:
        ack = CommandAck.model_validate(
            {
                "message_id": "msg-ack-1001",
                "correlation_id": "corr-ack-1001",
                "command_id": "cmd-ack-1001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "status": "failed",
                "local_timestamp": 2010,
                "observed_state": {},
                "error_code": "stale_command",
                "error_message": "command expired locally",
            }
        )
        result = CommandResult.model_validate(
            {
                "message_id": "msg-result-1002",
                "correlation_id": "corr-result-1002",
                "command_id": "cmd-ack-1001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "status": "failed",
                "local_timestamp": 2020,
                "observed_state": {},
                "metrics": {},
                "error_code": "execution_timeout",
                "error_message": "pump local timeout",
            }
        )
        self.assertEqual(ack.status_code, "stale_command")
        self.assertEqual(result.status_code, "execution_timeout")

    def test_event_envelope_accepts_legacy_error_payload(self) -> None:
        event = EventEnvelope.model_validate(
            {
                "message_id": "msg-event-1001",
                "trace_id": "trace-event-1001",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "severity": "warning",
                "category": "device_error",
                "error_code": "sensor_fault",
                "error_message": "sensor timeout",
                "details": {"sensor": "flow"},
                "created_at_ms": 1234,
                "message": "sensor timeout",
            }
        )
        self.assertEqual(event.event_id, "msg-event-1001")
        self.assertEqual(event.correlation_id, "trace-event-1001")
        self.assertEqual(event.payload["sensor"], "flow")


if __name__ == "__main__":
    unittest.main()
