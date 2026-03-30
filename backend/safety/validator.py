from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from backend.config import BackendConfig
from backend.domain.models import CommandRecord, CommandType
from backend.state.store import StateStore
from shared.contracts.messages import CommandLifecycle, DecisionOrigin, SafetyDecision


DEFAULT_ALLOWED_ACTIONS: dict[str, set[str]] = {
    "irrigation_sequence": {"START", "ABORT"},
    "irrigation_valve": {"OPEN", "CLOSE"},
    "master_pump": {"ON", "OFF"},
    "vent_fan": {"ON", "OFF"},
    "grow_light": {"ON", "OFF", "DIM_50"},
    "alarm": {"ON", "OFF"},
}


@dataclass(frozen=True)
class ActionProposal:
    trace_id: str
    device_id: str
    zone_id: str
    actuator: str
    action: str
    duration_sec: int
    origin: DecisionOrigin
    reason: str
    requested_at_ms: int
    command_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class SafetyValidator:
    def __init__(self, config: BackendConfig, allowed_actions: dict[str, set[str]] | None = None) -> None:
        self._config = config
        self._allowed_actions = allowed_actions or DEFAULT_ALLOWED_ACTIONS

    def validate(self, store: StateStore, proposal: ActionProposal) -> SafetyDecision:
        reasons: list[str] = []
        zone = store.get_zone_state(proposal.zone_id)
        device = store.get_device_state(proposal.device_id)
        current_state = store.get_current_state()
        telemetry = zone.get("telemetry", {})
        now_ms = int(proposal.requested_at_ms if proposal.requested_at_ms is not None else time.time() * 1000)
        activating_action = self._is_activating_action(proposal.action)

        if proposal.actuator not in self._allowed_actions:
            reasons.append(f"actuator_not_allowed:{proposal.actuator}")
        elif proposal.action.upper() not in self._allowed_actions[proposal.actuator]:
            reasons.append(f"action_not_allowed:{proposal.actuator}:{proposal.action}")

        automation_enabled = current_state.get("automation_flags", {}).get("automation_enabled", {}).get("enabled", True)
        if proposal.origin != DecisionOrigin.OPERATOR and not automation_enabled:
            reasons.append("automation_disabled")

        if activating_action and current_state.get("global", {}).get("emergency_stop"):
            reasons.append("global_emergency_stop")
        if activating_action and zone.get("blocked"):
            reasons.append(f"zone_blocked:{proposal.zone_id}")

        if activating_action:
            for lock in store.get_active_safety_locks("global"):
                reasons.append(f"global_lock:{lock['kind']}")
            for lock in store.get_active_safety_locks("zone", proposal.zone_id):
                if lock.get("kind") != "execution_reservation":
                    reasons.append(f"zone_lock:{lock['kind']}")

        manual_lease = store.get_active_manual_lease(proposal.zone_id)
        if manual_lease and proposal.origin != DecisionOrigin.OPERATOR:
            reasons.append("manual_lease_active")

        if proposal.duration_sec > int(zone.get("max_duration_per_run_sec") or 0):
            reasons.append("duration_exceeds_zone_limit")
        if proposal.actuator == "master_pump" and proposal.action.upper() == "ON":
            if proposal.duration_sec > int(self._config.global_safety.master_pump_timeout_sec):
                reasons.append("duration_exceeds_pump_limit")

        if activating_action:
            connectivity = str(device.get("connectivity") or "").lower()
            if connectivity in {"offline", "safe_mode"}:
                reasons.append("device_offline")

        last_watering_at_ms = zone.get("last_watering_at_ms")
        cooldown_sec = int(zone.get("cooldown_sec") or 0)
        if last_watering_at_ms and cooldown_sec > 0 and proposal.action.upper() in {"START", "OPEN", "ON"}:
            if now_ms < int(last_watering_at_ms) + cooldown_sec * 1000:
                reasons.append("zone_cooldown_active")

        if proposal.actuator == "irrigation_valve" and proposal.action.upper() == "OPEN":
            if zone.get("device_state", {}).get("valve_open") is True:
                reasons.append("valve_already_open")
            elif any(
                other_zone.get("zone_id") != proposal.zone_id and other_zone.get("reserved_by_execution")
                for other_zone in current_state.get("zones", {}).values()
            ):
                reasons.append("valve_interlock_active")

        if proposal.actuator == "master_pump" and proposal.action.upper() == "ON":
            if device.get("state", {}).get("pump_on") is True:
                reasons.append("pump_already_on")
            if not zone.get("device_state", {}).get("valve_open") and not zone.get("reserved_by_execution"):
                reasons.append("pump_precondition_no_open_valve")
            if telemetry.get("leak") is True and self._config.global_safety.leak_shutdown_enabled:
                reasons.append("leak_detected")
            if telemetry.get("overflow") is True:
                reasons.append("overflow_detected")
            tank_level = telemetry.get("tank_level")
            if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
                reasons.append("water_source_unavailable")
            if telemetry.get("water_available") is False:
                reasons.append("water_source_unavailable")

        if proposal.actuator == "irrigation_sequence" and proposal.action.upper() == "START":
            if store.count_active_irrigation_executions() >= int(current_state.get("global", {}).get("max_simultaneous_zones") or 1):
                reasons.append("max_simultaneous_zones_reached")
            if zone.get("device_state", {}).get("valve_open") is True:
                reasons.append("valve_already_open")
            if telemetry.get("leak") is True and self._config.global_safety.leak_shutdown_enabled:
                reasons.append("leak_detected")
            if telemetry.get("overflow") is True:
                reasons.append("overflow_detected")
            tank_level = telemetry.get("tank_level")
            if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
                reasons.append("water_source_unavailable")
            if telemetry.get("water_available") is False:
                reasons.append("water_source_unavailable")
            hour_ago_ms = now_ms - 3600_000
            day_ago_ms = now_ms - 86_400_000
            if store.count_completed_zone_runs_since(proposal.zone_id, hour_ago_ms) >= int(zone.get("max_runs_per_hour") or 0):
                reasons.append("zone_hourly_run_limit_reached")
            if store.sum_zone_volume_since(proposal.zone_id, day_ago_ms) >= float(zone.get("max_total_water_per_day_ml") or 0):
                reasons.append("zone_daily_water_limit_reached")
            flow_window_ms = int(current_state.get("global", {}).get("flow_window_sec") or 3600) * 1000
            if store.total_completed_volume_since(now_ms - flow_window_ms) >= float(current_state.get("global", {}).get("total_flow_limit_per_window_ml") or 0):
                reasons.append("global_flow_limit_reached")

        allowed = not reasons
        return SafetyDecision(
            trace_id=proposal.trace_id,
            device_id=proposal.device_id,
            zone_id=proposal.zone_id,
            allowed=allowed,
            lifecycle_state=CommandLifecycle.PLANNED if allowed else CommandLifecycle.ABORTED,
            origin=proposal.origin,
            reasons=reasons,
            actuator=proposal.actuator,
            action=proposal.action.upper(),
            max_duration_ms=proposal.duration_sec * 1000,
        )

    @staticmethod
    def _is_activating_action(action: str) -> bool:
        return action.upper() in {"START", "OPEN", "ON", "DIM_50"}

    def build_command_record(self, proposal: ActionProposal) -> CommandRecord:
        created_at_ms = proposal.requested_at_ms if proposal.requested_at_ms is not None else int(time.time() * 1000)
        expires_at_ms = created_at_ms + self._config.global_safety.command_ttl_sec * 1000
        command_id = proposal.command_id or f"cmd-{uuid.uuid4().hex[:12]}"
        command_type = CommandType.IRRIGATE_ZONE if proposal.actuator == "irrigation_sequence" else CommandType.SET_ACTUATOR
        proposal_metadata = dict(proposal.metadata)
        return CommandRecord(
            command_id=command_id,
            trace_id=proposal.trace_id,
            idempotency_key=command_id,
            command_type=command_type,
            device_id=proposal.device_id,
            zone_id=proposal.zone_id,
            requested_by=proposal.origin,
            reason=proposal.reason,
            requested_at_ms=created_at_ms,
            expires_at_ms=expires_at_ms,
            lifecycle=CommandLifecycle.PLANNED,
            requested_payload={
                "actuator": proposal.actuator,
                "action": proposal.action.upper(),
                "duration_sec": proposal.duration_sec,
                "metadata": proposal.metadata,
            },
            metadata={
                "nonce": f"nonce-{uuid.uuid4().hex[:16]}",
                "device_binding": proposal.device_id,
                "zone_binding": proposal.zone_id,
                "replay_window_ms": self._config.global_safety.command_ttl_sec * 1000,
                "operator_id": proposal_metadata.get("operator_id"),
                "operator_name": proposal_metadata.get("operator_name"),
                "submitted_via": proposal_metadata.get("submitted_via"),
            },
        )
