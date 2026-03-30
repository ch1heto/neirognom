from __future__ import annotations

from typing import Any


def telemetry_payload(
    *,
    message_id: str = "msg-telemetry-0001",
    trace_id: str = "trace-telemetry-0001",
    device_id: str = "esp32-1",
    zone_id: str = "tray_1",
    ts_ms: int = 1_000,
    local_ts_ms: int | None = None,
    message_counter: int = 1,
    sensors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "correlation_id": trace_id,
        "device_id": device_id,
        "zone_id": zone_id,
        "timestamp": ts_ms,
        "local_timestamp": local_ts_ms if local_ts_ms is not None else ts_ms,
        "message_counter": message_counter,
        "sensors": sensors or {"ph": 6.1, "ec": 1.7, "water_level": 68.0},
        "status": {},
        "meta": {},
    }


def device_state_payload(
    *,
    message_id: str = "msg-state-0001",
    trace_id: str = "trace-state-0001",
    device_id: str = "esp32-1",
    zone_id: str = "tray_1",
    ts_ms: int = 1_000,
    message_counter: int = 1,
    connectivity: str = "online",
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "correlation_id": trace_id,
        "device_id": device_id,
        "zone_id": zone_id,
        "timestamp": ts_ms,
        "message_counter": message_counter,
        "connectivity": connectivity,
        "state": state or {"valve_open": False, "doser_active": False, "pump_on": False},
        "status": {},
        "meta": {},
    }


def presence_payload(
    *,
    message_id: str = "msg-presence-0001",
    trace_id: str = "trace-presence-0001",
    device_id: str = "esp32-1",
    zone_id: str = "tray_1",
    ts_ms: int = 1_000,
    message_counter: int = 1,
    connectivity: str = "online",
    status: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "correlation_id": trace_id,
        "device_id": device_id,
        "zone_id": zone_id,
        "timestamp": ts_ms,
        "message_counter": message_counter,
        "connectivity": connectivity,
        "status": status or {},
        "meta": meta or {},
    }


def llama_dose_response(*, zone_id: str = "tray_1", duration_sec: int = 6, dose_ml: int = 30, confidence: float = 0.87) -> dict[str, Any]:
    return {
        "decision": "dose_solution",
        "zone_id": zone_id,
        "requested_duration_sec": duration_sec,
        "dose_ml": dose_ml,
        "rationale": "ec and ph are outside the target band for this tray",
        "confidence": confidence,
    }


def llama_water_response(*, zone_id: str = "tray_1", duration_sec: int = 6, confidence: float = 0.87) -> dict[str, Any]:
    return llama_dose_response(zone_id=zone_id, duration_sec=duration_sec, confidence=confidence)


def llama_no_action_response(*, zone_id: str = "tray_1", confidence: float = 0.92) -> dict[str, Any]:
    return {
        "decision": "no_action",
        "zone_id": zone_id,
        "requested_duration_sec": 0,
        "dose_ml": 0,
        "rationale": "ph, ec, and water level are within target range",
        "confidence": confidence,
    }


def llama_invalid_response() -> dict[str, Any]:
    return {
        "decision": "dose_solution",
        "zone": "tray_1",
    }


def llama_invalid_extra_fields_response(*, zone_id: str = "tray_1") -> dict[str, Any]:
    return {
        "decision": "dose_solution",
        "zone_id": zone_id,
        "requested_duration_sec": 10,
        "dose_ml": 30,
        "rationale": "ph is below target",
        "confidence": 0.8,
        "max_duration_sec": 10,
        "description": "extra field not allowed",
    }


def llama_missing_required_fields_response() -> dict[str, Any]:
    return {
        "decision": "dose_solution",
        "requested_duration_sec": 10,
        "rationale": "missing zone_id and confidence",
    }


def llama_invalid_json_response() -> str:
    return '{"decision":"dose_solution","zone_id":"tray_1",'


def ack_payload(
    *,
    command_id: str,
    execution_id: str,
    step: str,
    message_id: str = "msg-ack-0001",
    trace_id: str = "trace-ack-0001",
    device_id: str = "esp32-1",
    zone_id: str = "tray_1",
    status: str = "acked",
    local_timestamp_ms: int = 1_100,
    observed_state: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_message: str = "",
    status_code: str = "",
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "correlation_id": trace_id,
        "command_id": command_id,
        "execution_id": execution_id,
        "step": step,
        "device_id": device_id,
        "zone_id": zone_id,
        "status": status,
        "local_timestamp": local_timestamp_ms,
        "observed_state": observed_state or {},
        "source": "device",
        "error_code": error_code,
        "error_message": error_message,
        "status_code": status_code or (error_code or status),
    }


def result_payload(
    *,
    command_id: str,
    execution_id: str,
    step: str,
    message_id: str = "msg-result-0001",
    trace_id: str = "trace-result-0001",
    device_id: str = "esp32-1",
    zone_id: str = "tray_1",
    status: str = "completed",
    local_timestamp_ms: int = 1_200,
    observed_state: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_message: str = "",
    status_code: str = "",
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "correlation_id": trace_id,
        "command_id": command_id,
        "execution_id": execution_id,
        "step": step,
        "device_id": device_id,
        "zone_id": zone_id,
        "status": status,
        "local_timestamp": local_timestamp_ms,
        "observed_state": observed_state or {},
        "metrics": metrics or {},
        "source": "device",
        "error_code": error_code,
        "error_message": error_message,
        "status_code": status_code or (error_code or status),
    }
