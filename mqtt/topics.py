from __future__ import annotations

from dataclasses import dataclass


MQTT_TOPIC_PREFIX = "greenhouse/device"
TELEMETRY_RAW_SUFFIX = "telemetry/raw"
STATE_SUFFIX = "state"
CMD_EXECUTE_SUFFIX = "cmd/execute"
CMD_ACK_SUFFIX = "cmd/ack"
CMD_RESULT_SUFFIX = "cmd/result"
ERROR_SUFFIX = "event/error"


@dataclass(frozen=True)
class ParsedTopic:
    device_id: str
    channel: str


def build_topic(device_id: str, suffix: str) -> str:
    return f"{MQTT_TOPIC_PREFIX}/{device_id}/{suffix}"


def telemetry_topic(device_id: str) -> str:
    return build_topic(device_id, TELEMETRY_RAW_SUFFIX)


def state_topic(device_id: str) -> str:
    return build_topic(device_id, STATE_SUFFIX)


def command_execute_topic(device_id: str) -> str:
    return build_topic(device_id, CMD_EXECUTE_SUFFIX)


def command_ack_topic(device_id: str) -> str:
    return build_topic(device_id, CMD_ACK_SUFFIX)


def command_result_topic(device_id: str) -> str:
    return build_topic(device_id, CMD_RESULT_SUFFIX)


def error_topic(device_id: str) -> str:
    return build_topic(device_id, ERROR_SUFFIX)


def parse_topic(topic: str) -> ParsedTopic | None:
    parts = topic.strip("/").split("/")
    if len(parts) < 4 or parts[0] != "greenhouse" or parts[1] != "device":
        return None

    device_id = parts[2]
    tail = "/".join(parts[3:])
    if tail in {
        TELEMETRY_RAW_SUFFIX,
        STATE_SUFFIX,
        CMD_EXECUTE_SUFFIX,
        CMD_ACK_SUFFIX,
        CMD_RESULT_SUFFIX,
        ERROR_SUFFIX,
    }:
        return ParsedTopic(device_id=device_id, channel=tail)
    return None
