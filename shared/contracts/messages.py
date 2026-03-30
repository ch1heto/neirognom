from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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
    AUTOMATION = "automation"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class TelemetryMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    ts_ms: int = Field(ge=0)
    local_ts_ms: int = Field(ge=0)
    sensors: dict[str, float | int | bool | str]
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeviceStateMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    ts_ms: int = Field(ge=0)
    connectivity: DeviceConnectivity
    firmware_version: str | None = None
    state: dict[str, Any] = Field(default_factory=dict)


class SafetyConstraints(StrictModel):
    require_backend_safety: bool = True
    reject_if_offline: bool = True
    reject_if_stale_heartbeat: bool = True
    reject_if_leak_suspected: bool = True
    reject_if_empty_tank: bool = True
    local_hard_max_duration_ms: int = Field(ge=0)
    allowed_runtime_window_ms: int = Field(ge=0)


class ActuatorCommandMessage(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    execution_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    actuator: str = Field(min_length=1, max_length=64)
    action: str = Field(min_length=1, max_length=64)
    step: str = Field(min_length=1, max_length=64)
    issued_at_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)
    nonce: str = Field(min_length=8, max_length=128)
    max_duration_ms: int = Field(ge=0)
    safety_constraints: SafetyConstraints
    parameters: dict[str, Any] = Field(default_factory=dict)


class CommandAck(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    execution_id: str | None = Field(default=None, min_length=8, max_length=128)
    step: str | None = None
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    status: Literal["received", "acked", "running", "rejected", "failed", "expired"]
    local_timestamp_ms: int = Field(ge=0)
    observed_state: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_message: str = ""


class CommandResult(StrictModel):
    message_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    execution_id: str | None = Field(default=None, min_length=8, max_length=128)
    step: str | None = None
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    status: Literal["completed", "failed", "expired", "aborted"]
    local_timestamp_ms: int = Field(ge=0)
    observed_state: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_message: str = ""


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


class LlmDecisionRequest(StrictModel):
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    current_state: dict[str, Any]
    telemetry_window: list[dict[str, Any]] = Field(default_factory=list)
    zone_limits: dict[str, Any] = Field(default_factory=dict)
    global_limits: dict[str, Any] = Field(default_factory=dict)


class LlmDecisionResponse(StrictModel):
    decision: Literal["no_action", "water_zone", "stop_zone", "ventilate_zone", "block_zone"]
    zone_id: str = Field(min_length=1, max_length=64)
    actuator: str | None = None
    action: str | None = None
    duration_sec: int | None = Field(default=None, ge=0, le=3600)
    reason: str = ""
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
