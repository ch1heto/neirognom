from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


MQTT_ROOT = "greenhouse"
DEVICE_PREFIX = f"{MQTT_ROOT}/device"
ZONE_PREFIX = f"{MQTT_ROOT}/zone"
SYSTEM_EVENTS_TOPIC = f"{MQTT_ROOT}/system/events"

TELEMETRY_SUFFIX = "telemetry"
STATE_SUFFIX = "state"
CMD_SUFFIX = "cmd"
ACK_SUFFIX = "ack"
RESULT_SUFFIX = "result"
EVENTS_SUFFIX = "events"
PRESENCE_SUFFIX = "presence"

LEGACY_TELEMETRY_RAW_SUFFIX = "telemetry/raw"
LEGACY_CMD_EXECUTE_SUFFIX = "cmd/execute"
LEGACY_CMD_ACK_SUFFIX = "cmd/ack"
LEGACY_CMD_RESULT_SUFFIX = "cmd/result"
LEGACY_ERROR_SUFFIX = "event/error"


@dataclass(frozen=True)
class ParsedTopic:
    scope: Literal["device", "zone", "system"]
    channel: str
    canonical_topic: str
    raw_topic: str
    device_id: str | None = None
    zone_id: str | None = None
    is_legacy: bool = False


def _device_topic(device_id: str, suffix: str) -> str:
    return f"{DEVICE_PREFIX}/{device_id}/{suffix}"


def _zone_topic(zone_id: str, suffix: str) -> str:
    return f"{ZONE_PREFIX}/{zone_id}/{suffix}"


def telemetry_topic(device_id: str) -> str:
    return _device_topic(device_id, TELEMETRY_SUFFIX)


def zone_telemetry_topic(zone_id: str) -> str:
    return _zone_topic(zone_id, TELEMETRY_SUFFIX)


def state_topic(device_id: str) -> str:
    return _device_topic(device_id, STATE_SUFFIX)


def command_topic(device_id: str) -> str:
    return _device_topic(device_id, CMD_SUFFIX)


def command_execute_topic(device_id: str) -> str:
    return command_topic(device_id)


def command_ack_topic(device_id: str) -> str:
    return _device_topic(device_id, ACK_SUFFIX)


def command_result_topic(device_id: str) -> str:
    return _device_topic(device_id, RESULT_SUFFIX)


def presence_topic(device_id: str) -> str:
    return _device_topic(device_id, PRESENCE_SUFFIX)


def system_events_topic() -> str:
    return SYSTEM_EVENTS_TOPIC


def ingestion_subscription_topics(qos: int) -> list[tuple[str, int]]:
    return [
        (f"{DEVICE_PREFIX}/+/{TELEMETRY_SUFFIX}", qos),
        (f"{ZONE_PREFIX}/+/{TELEMETRY_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{STATE_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{ACK_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{RESULT_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{PRESENCE_SUFFIX}", qos),
        (SYSTEM_EVENTS_TOPIC, qos),
        (f"{DEVICE_PREFIX}/+/{LEGACY_TELEMETRY_RAW_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{LEGACY_CMD_ACK_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{LEGACY_CMD_RESULT_SUFFIX}", qos),
        (f"{DEVICE_PREFIX}/+/{LEGACY_ERROR_SUFFIX}", qos),
    ]


def parse_topic(topic: str) -> ParsedTopic | None:
    parts = topic.strip("/").split("/")
    if len(parts) == 3 and parts[0] == MQTT_ROOT and parts[1] == "system" and parts[2] == EVENTS_SUFFIX:
        return ParsedTopic(
            scope="system",
            channel=EVENTS_SUFFIX,
            canonical_topic=SYSTEM_EVENTS_TOPIC,
            raw_topic=topic,
        )

    if len(parts) >= 4 and parts[0] == MQTT_ROOT and parts[1] == "device":
        device_id = parts[2]
        tail = "/".join(parts[3:])
        canonical = {
            TELEMETRY_SUFFIX: telemetry_topic(device_id),
            STATE_SUFFIX: state_topic(device_id),
            CMD_SUFFIX: command_topic(device_id),
            ACK_SUFFIX: command_ack_topic(device_id),
            RESULT_SUFFIX: command_result_topic(device_id),
            PRESENCE_SUFFIX: presence_topic(device_id),
            LEGACY_TELEMETRY_RAW_SUFFIX: telemetry_topic(device_id),
            LEGACY_CMD_EXECUTE_SUFFIX: command_topic(device_id),
            LEGACY_CMD_ACK_SUFFIX: command_ack_topic(device_id),
            LEGACY_CMD_RESULT_SUFFIX: command_result_topic(device_id),
            LEGACY_ERROR_SUFFIX: _device_topic(device_id, EVENTS_SUFFIX),
        }.get(tail)
        if canonical is None:
            return None
        channel = {
            LEGACY_TELEMETRY_RAW_SUFFIX: TELEMETRY_SUFFIX,
            LEGACY_CMD_EXECUTE_SUFFIX: CMD_SUFFIX,
            LEGACY_CMD_ACK_SUFFIX: ACK_SUFFIX,
            LEGACY_CMD_RESULT_SUFFIX: RESULT_SUFFIX,
            LEGACY_ERROR_SUFFIX: EVENTS_SUFFIX,
        }.get(tail, tail)
        return ParsedTopic(
            scope="device",
            channel=channel,
            canonical_topic=canonical,
            raw_topic=topic,
            device_id=device_id,
            is_legacy=tail
            in {
                LEGACY_TELEMETRY_RAW_SUFFIX,
                LEGACY_CMD_EXECUTE_SUFFIX,
                LEGACY_CMD_ACK_SUFFIX,
                LEGACY_CMD_RESULT_SUFFIX,
                LEGACY_ERROR_SUFFIX,
            },
        )

    if len(parts) == 4 and parts[0] == MQTT_ROOT and parts[1] == "zone" and parts[3] == TELEMETRY_SUFFIX:
        zone_id = parts[2]
        return ParsedTopic(
            scope="zone",
            channel=TELEMETRY_SUFFIX,
            canonical_topic=zone_telemetry_topic(zone_id),
            raw_topic=topic,
            zone_id=zone_id,
        )

    return None
