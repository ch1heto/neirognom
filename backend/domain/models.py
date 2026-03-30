from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from shared.contracts.messages import CommandLifecycle, DecisionOrigin


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CommandType(StrEnum):
    IRRIGATE_ZONE = "IRRIGATE_ZONE"
    SET_ACTUATOR = "SET_ACTUATOR"


class ExecutionPhase(StrEnum):
    VALIDATE_REQUEST = "VALIDATE_REQUEST"
    RESERVE_ZONE = "RESERVE_ZONE"
    OPEN_VALVE = "OPEN_VALVE"
    WAIT_SETTLE_DELAY = "WAIT_SETTLE_DELAY"
    START_PUMP = "START_PUMP"
    CONFIRM_FLOW = "CONFIRM_FLOW"
    MONITOR_RUN = "MONITOR_RUN"
    STOP_PUMP = "STOP_PUMP"
    CLOSE_VALVE = "CLOSE_VALVE"
    VERIFY_SAFE_STOP = "VERIFY_SAFE_STOP"
    FINISHED = "FINISHED"


class ExecutionState(StrEnum):
    PENDING = "pending"
    DISPATCHED = "dispatched"
    ACKED = "acked"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    FAULTED = "faulted"
    CANCELLED = "cancelled"


class DeviceRecord(DomainModel):
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str | None = None
    connectivity: str = "offline"
    firmware_version: str | None = None
    last_seen_ms: int | None = Field(default=None, ge=0)
    last_telemetry_ms: int | None = Field(default=None, ge=0)
    telemetry: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)


class TrayZoneRecord(DomainModel):
    zone_id: str = Field(min_length=1, max_length=64)
    device_id: str | None = None
    pump_id: str = "pump_main"
    line_id: str = ""
    mutually_exclusive_zones: list[str] = Field(default_factory=list)
    shared_line_restricted: bool = False
    blocked: bool = False
    maintenance_mode: bool = False
    cooldown_sec: int = Field(default=0, ge=0)
    max_duration_per_run_sec: int = Field(default=0, ge=0)
    max_open_duration_sec: int = Field(default=0, ge=0)
    max_runs_per_hour: int = Field(default=0, ge=0)
    max_total_water_per_day_ml: int = Field(default=0, ge=0)
    settle_delay_ms: int = Field(default=0, ge=0)
    flow_confirm_timeout_ms: int = Field(default=0, ge=0)
    min_flow_ml_per_min: float = Field(default=0.0, ge=0.0)
    last_watering_at_ms: int | None = Field(default=None, ge=0)
    last_error_at_ms: int | None = Field(default=None, ge=0)
    telemetry: dict[str, Any] = Field(default_factory=dict)
    device_state: dict[str, Any] = Field(default_factory=dict)
    reserved_by_execution: str | None = None


class CommandRecord(DomainModel):
    command_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    idempotency_key: str = Field(min_length=8, max_length=128)
    command_type: CommandType
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    requested_by: DecisionOrigin
    reason: str = ""
    requested_at_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)
    lifecycle: CommandLifecycle = CommandLifecycle.PLANNED
    requested_payload: dict[str, Any] = Field(default_factory=dict)
    current_execution_id: str | None = None
    last_error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CommandExecutionRecord(DomainModel):
    execution_id: str = Field(min_length=8, max_length=128)
    command_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    lifecycle: CommandLifecycle = CommandLifecycle.PLANNED
    execution_state: ExecutionState = ExecutionState.PENDING
    phase: ExecutionPhase = ExecutionPhase.VALIDATE_REQUEST
    step_index: int = Field(default=0, ge=0)
    active_step: str | None = None
    started_at_ms: int = Field(ge=0)
    updated_at_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)
    phase_deadline_ms: int | None = Field(default=None, ge=0)
    monitor_until_ms: int | None = Field(default=None, ge=0)
    target_duration_ms: int = Field(ge=0)
    target_volume_ml: float | None = Field(default=None, ge=0.0)
    delivered_volume_ml: float = Field(default=0.0, ge=0.0)
    flow_confirmed: bool = False
    reserved_lock_id: str | None = None
    last_error: str | None = None
    result_payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SafetyLockRecord(DomainModel):
    lock_id: str = Field(min_length=8, max_length=128)
    scope: Literal["global", "zone", "device"]
    scope_id: str = Field(min_length=1, max_length=128)
    kind: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=512)
    active: bool = True
    created_at_ms: int = Field(ge=0)
    expires_at_ms: int | None = Field(default=None, ge=0)
    owner: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ManualLeaseRecord(DomainModel):
    lease_id: str = Field(min_length=8, max_length=128)
    zone_id: str = Field(min_length=1, max_length=64)
    holder: str = Field(min_length=1, max_length=128)
    active: bool = True
    created_at_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)


class AlarmRecord(DomainModel):
    alarm_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    device_id: str = Field(min_length=1, max_length=64)
    zone_id: str = Field(min_length=1, max_length=64)
    severity: str = Field(min_length=1, max_length=32)
    category: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=512)
    active: bool = True
    created_at_ms: int = Field(ge=0)
    cleared_at_ms: int | None = Field(default=None, ge=0)
    details: dict[str, Any] = Field(default_factory=dict)


class AuditLogRecord(DomainModel):
    audit_id: str = Field(min_length=8, max_length=128)
    trace_id: str = Field(min_length=8, max_length=128)
    action_type: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=512)
    created_at_ms: int = Field(ge=0)
    command_id: str | None = None
    execution_id: str | None = None
    device_id: str | None = None
    zone_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class AutomationFlagRecord(DomainModel):
    flag_name: str = Field(min_length=1, max_length=64)
    enabled: bool
    updated_at_ms: int = Field(ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)
