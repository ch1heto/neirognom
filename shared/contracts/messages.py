"""Canonical runtime DTOs for the backend-centric greenhouse architecture.

These models are the source of truth for telemetry, command, ACK/RESULT, and
event transport contracts used by the active backend path.
The models accept legacy field aliases where needed so MQTT/device migration can
stay soft while the canonical envelope shape remains stable.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _as_payload(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        return dict(data)
    return {}


def _coerce_counter(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


class CommandLifecycle(StrEnum):
    PLANNED = "PLANNED"
    DISPATCHED = "DISPATCHED"
    ACKED = "ACKED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    ABORTED = "ABORTED"


class DeviceConnectivity(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    SAFE_MODE = "safe_mode"


class DecisionOrigin(StrEnum):
    DETERMINISTIC = "deterministic"
    LLAMA = "llama"
    OPERATOR = "operator"
    MCP = "mcp"
    AUTOMATION = "automation"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class TelemetryMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    timestamp: int = Field(ge=0)
    message_counter: int = Field(ge=0)
    sensors: dict[str, float | int | bool | str]
    status: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)
    local_timestamp: int | None = Field(default=None, ge=0)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        ts_ms = payload.pop("ts_ms", None)
        local_ts_ms = payload.pop("local_ts_ms", None)
        legacy_meta = payload.pop("metadata", None) if isinstance(payload.get("metadata"), dict) else {}
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("timestamp", ts_ms if ts_ms is not None else local_ts_ms if local_ts_ms is not None else 0)
        if "local_timestamp" not in payload and local_ts_ms is not None:
            payload["local_timestamp"] = local_ts_ms
        payload.setdefault("meta", legacy_meta if isinstance(legacy_meta, dict) else {})
        payload.setdefault("status", {})
        payload.setdefault(
            "message_counter",
            _coerce_counter(payload.get("message_counter", payload.get("meta", {}).get("message_counter", legacy_meta.get("message_counter", 0)))),
        )
        return payload

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def ts_ms(self) -> int:
        return self.timestamp

    @property
    def local_ts_ms(self) -> int:
        return int(self.local_timestamp if self.local_timestamp is not None else self.timestamp)

    @property
    def metadata(self) -> dict[str, Any]:
        return self.meta


class DeviceStateMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    timestamp: int = Field(ge=0)
    connectivity: DeviceConnectivity
    state: dict[str, Any] = Field(default_factory=dict)
    firmware_version: str | None = None
    message_counter: int = Field(default=0, ge=0)
    status: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        ts_ms = payload.pop("ts_ms", None)
        legacy_meta = payload.pop("metadata", None) if isinstance(payload.get("metadata"), dict) else {}
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("timestamp", ts_ms if ts_ms is not None else 0)
        payload.setdefault("meta", legacy_meta if isinstance(legacy_meta, dict) else {})
        payload.setdefault("status", {})
        payload.setdefault(
            "message_counter",
            _coerce_counter(payload.get("message_counter", payload.get("meta", {}).get("message_counter", legacy_meta.get("message_counter", 0)))),
        )
        return payload

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def ts_ms(self) -> int:
        return self.timestamp

    @property
    def metadata(self) -> dict[str, Any]:
        return self.meta


class PresenceMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    timestamp: int = Field(ge=0)
    message_counter: int = Field(ge=0)
    connectivity: DeviceConnectivity
    status: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        ts_ms = payload.pop("ts_ms", None)
        legacy_meta = payload.pop("metadata", None) if isinstance(payload.get("metadata"), dict) else {}
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("timestamp", ts_ms if ts_ms is not None else payload.get("timestamp", 0))
        payload.setdefault("zone_id", payload.get("zone_id") or payload.get("tray_id") or payload.get("device_id") or "unknown")
        status_payload = payload.get("status")
        if not payload.get("connectivity") and isinstance(status_payload, dict):
            payload["connectivity"] = status_payload.get("connectivity") or status_payload.get("state") or "online"
        payload["connectivity"] = str(payload.get("connectivity") or "online").lower()
        payload.setdefault("meta", legacy_meta if isinstance(legacy_meta, dict) else {})
        payload.setdefault("status", {})
        payload.setdefault(
            "message_counter",
            _coerce_counter(payload.get("message_counter", payload.get("meta", {}).get("message_counter", legacy_meta.get("message_counter", 0)))),
        )
        return payload

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def ts_ms(self) -> int:
        return self.timestamp


class SafetyCaps(StrictModel):
    require_backend_safety: bool = True
    reject_if_offline: bool = True
    reject_if_stale_heartbeat: bool = True
    reject_if_leak_suspected: bool = True
    reject_if_empty_tank: bool = True
    local_hard_max_duration_ms: int = Field(ge=0)
    allowed_runtime_window_ms: int = Field(ge=0)


SafetyConstraints = SafetyCaps


class ActuatorCommandMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    source: str = Field(min_length=1, max_length=64)
    target_device_id: str = Field(min_length=1, max_length=64)
    target_zone_id: str = Field(min_length=1, max_length=64)
    action: str = Field(min_length=1, max_length=64)
    duration_sec: int = Field(ge=0)
    ttl_sec: int = Field(ge=0)
    created_at: int = Field(ge=0)
    safety_caps: SafetyCaps
    execution_id: str = Field(min_length=8, max_length=128)
    actuator: str = Field(min_length=1, max_length=64)
    step: str = Field(min_length=1, max_length=64)
    nonce: str = Field(min_length=8, max_length=128)
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("source", str(payload.get("source") or "backend"))
        payload.setdefault("target_device_id", payload.pop("device_id", None))
        payload.setdefault("target_zone_id", payload.pop("zone_id", None))
        issued_at_ms = payload.pop("issued_at_ms", None)
        expires_at_ms = payload.pop("expires_at_ms", None)
        max_duration_ms = payload.pop("max_duration_ms", None)
        safety_constraints = payload.pop("safety_constraints", None)
        payload.setdefault("created_at", issued_at_ms if issued_at_ms is not None else 0)
        if "ttl_sec" not in payload:
            created_at = int(payload.get("created_at") or 0)
            payload["ttl_sec"] = max(0, (int(expires_at_ms) - created_at + 999) // 1000) if expires_at_ms is not None else 0
        if "duration_sec" not in payload:
            payload["duration_sec"] = max(0, int(max_duration_ms or 0) // 1000)
        if "safety_caps" not in payload and safety_constraints is not None:
            payload["safety_caps"] = safety_constraints
        return payload

    @model_validator(mode="after")
    def _validate_runtime_contract(self) -> "ActuatorCommandMessage":
        if self.action.upper() in {"START", "OPEN", "ON", "DIM_50"} and self.ttl_sec <= 0:
            raise ValueError("ttl_sec_must_be_positive_for_activating_commands")
        return self

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def device_id(self) -> str:
        return self.target_device_id

    @property
    def zone_id(self) -> str:
        return self.target_zone_id

    @property
    def issued_at_ms(self) -> int:
        return self.created_at

    @property
    def expires_at_ms(self) -> int:
        return self.created_at + (self.ttl_sec * 1000)

    @property
    def max_duration_ms(self) -> int:
        return self.duration_sec * 1000

    @property
    def safety_constraints(self) -> SafetyCaps:
        return self.safety_caps


class CommandAck(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    status: Literal["received", "acked", "running", "rejected", "failed", "expired"]
    local_timestamp: int = Field(ge=0)
    observed_state: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(default="device", min_length=1, max_length=64)
    error_code: str | None = None
    error_message: str = ""
    execution_id: str | None = Field(default=None, min_length=8, max_length=128)
    step: str | None = None
    status_code: str = ""

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        local_timestamp_ms = payload.pop("local_timestamp_ms", None)
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("local_timestamp", local_timestamp_ms if local_timestamp_ms is not None else 0)
        payload.setdefault("source", str(payload.get("source") or "device"))
        payload.setdefault("status_code", str(payload.get("status_code") or payload.get("error_code") or payload.get("status") or ""))
        return payload

    @model_validator(mode="after")
    def _validate_status_fields(self) -> "CommandAck":
        if not self.status_code:
            raise ValueError("status_code_required")
        return self

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def local_timestamp_ms(self) -> int:
        return self.local_timestamp


class CommandResult(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    status: Literal["completed", "failed", "expired", "aborted"]
    local_timestamp: int = Field(ge=0)
    observed_state: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(default="device", min_length=1, max_length=64)
    error_code: str | None = None
    error_message: str = ""
    execution_id: str | None = Field(default=None, min_length=8, max_length=128)
    step: str | None = None
    status_code: str = ""

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        trace_id = payload.pop("trace_id", None)
        local_timestamp_ms = payload.pop("local_timestamp_ms", None)
        payload.setdefault("correlation_id", trace_id or payload.get("message_id") or "trace-unknown")
        payload.setdefault("local_timestamp", local_timestamp_ms if local_timestamp_ms is not None else 0)
        payload.setdefault("source", str(payload.get("source") or "device"))
        payload.setdefault("status_code", str(payload.get("status_code") or payload.get("error_code") or payload.get("status") or ""))
        return payload

    @model_validator(mode="after")
    def _validate_status_fields(self) -> "CommandResult":
        if not self.status_code:
            raise ValueError("status_code_required")
        return self

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def local_timestamp_ms(self) -> int:
        return self.local_timestamp


class EventEnvelope(StrictModel):
    event_id: str = Field(min_length=8, max_length=128)
    correlation_id: str = Field(min_length=8, max_length=128)
    source: str = Field(min_length=1, max_length=64)
    category: str = Field(min_length=1, max_length=64)
    severity: Literal["info", "warning", "critical", "emergency"]
    message: str = Field(min_length=1, max_length=512)
    timestamp: int = Field(ge=0)
    device_id: str = Field(default="", max_length=64)
    zone_id: str = Field(default="", max_length=64)
    status: str = ""
    error_code: str | None = None
    error_message: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        alert_id = payload.pop("alert_id", None)
        message_id = payload.pop("message_id", None)
        trace_id = payload.pop("trace_id", None)
        created_at_ms = payload.pop("created_at_ms", None)
        ts_ms = payload.pop("ts_ms", None)
        details = payload.pop("details", None)
        metadata = payload.pop("metadata", None)
        payload.setdefault("event_id", alert_id or message_id or "event-unknown")
        payload.setdefault("correlation_id", trace_id or message_id or payload.get("event_id") or "trace-unknown")
        payload.setdefault("timestamp", created_at_ms if created_at_ms is not None else ts_ms if ts_ms is not None else 0)
        payload.setdefault("payload", details if isinstance(details, dict) else payload.get("payload", {}))
        payload.setdefault("meta", metadata if isinstance(metadata, dict) else payload.get("meta", {}))
        payload.setdefault("status", str(payload.get("status") or ""))
        payload.setdefault("source", str(payload.get("source") or "backend"))
        return payload

    @property
    def trace_id(self) -> str:
        return self.correlation_id

    @property
    def created_at_ms(self) -> int:
        return self.timestamp

    @property
    def details(self) -> dict[str, Any]:
        return self.payload


class SafetyDecision(StrictModel):
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    allowed: bool
    lifecycle_state: CommandLifecycle
    origin: DecisionOrigin
    reasons: list[str] = Field(default_factory=list)
    actuator: str | None = None
    action: str | None = None
    max_duration_ms: int | None = Field(default=None, ge=0)


class LlmAllowedAction(StrictModel):
    decision: Literal["no_action", "open_valve", "close_valve", "dose_solution"]
    actuator: str | None = None
    action: str | None = None
    max_duration_sec: int = Field(default=0, ge=0, le=3600)
    description: str = ""


class LlmDecisionRequest(StrictModel):
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    zone_state: dict[str, Any] = Field(default_factory=dict)
    device_state: dict[str, Any] = Field(default_factory=dict)
    global_state: dict[str, Any] = Field(default_factory=dict)
    zone_limits: dict[str, Any] = Field(default_factory=dict)
    global_limits: dict[str, Any] = Field(default_factory=dict)
    active_safety_locks: list[dict[str, Any]] = Field(default_factory=list)
    active_alarms: list[dict[str, Any]] = Field(default_factory=list)
    automation_flags: dict[str, Any] = Field(default_factory=dict)
    recent_zone_commands: list[dict[str, Any]] = Field(default_factory=list)
    recent_device_commands: list[dict[str, Any]] = Field(default_factory=list)
    telemetry_windows: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    allowed_actions: list[LlmAllowedAction] = Field(default_factory=list)
    current_state: dict[str, Any] = Field(default_factory=dict)
    telemetry_window: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        payload = _as_payload(data)
        if not payload:
            return data
        current_state = payload.get("current_state")
        if not payload.get("zone_state") and isinstance(current_state, dict):
            payload["zone_state"] = ((current_state.get("zones") or {}).get(payload.get("zone_id")) or {})
        if not payload.get("device_state") and isinstance(current_state, dict):
            payload["device_state"] = ((current_state.get("devices") or {}).get(payload.get("device_id")) or {})
        if not payload.get("global_state") and isinstance(current_state, dict):
            payload["global_state"] = current_state.get("global") or {}
        if not payload.get("telemetry_windows") and isinstance(payload.get("telemetry_window"), list):
            payload["telemetry_windows"] = {"ph": payload.get("telemetry_window", [])}
        return payload


class LlmDecisionResponse(StrictModel):
    decision: Literal["no_action", "open_valve", "close_valve", "dose_solution"]
    zone_id: str = Field(min_length=1, max_length=64)
    requested_duration_sec: int | None = Field(default=None, ge=0, le=3600)
    dose_ml: int | None = Field(default=None, ge=0, le=10_000)
    rationale: str = Field(min_length=1, max_length=512)
    confidence: float = Field(ge=0.0, le=1.0)


class AlertEvent(StrictModel):
    alert_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    severity: Literal["info", "warning", "critical", "emergency"]
    category: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=512)
    created_at_ms: int = Field(ge=0)
    details: dict[str, Any] = Field(default_factory=dict)
