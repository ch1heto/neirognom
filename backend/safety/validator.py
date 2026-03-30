"""Canonical backend safety validator for all actuator decisions.

AI, web UI, MCP, and any other operator surface must converge on this layer
before actuator commands reach the dispatcher.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from backend.config import BackendConfig, ZoneSafetyConfig
from backend.domain.models import CommandRecord, CommandType, ExecutionPhase
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

TERMINAL_LIFECYCLES = {
    CommandLifecycle.FAILED.value,
    CommandLifecycle.EXPIRED.value,
    CommandLifecycle.ABORTED.value,
}
SAFE_STOP_PENDING_PHASES = {
    ExecutionPhase.STOP_PUMP.value,
    ExecutionPhase.CLOSE_VALVE.value,
    ExecutionPhase.VERIFY_SAFE_STOP.value,
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
        self._zone_configs = {zone.zone_id: zone for zone in config.zone_configs()}

    def validate(self, store: StateStore, proposal: ActionProposal) -> SafetyDecision:
        reasons: list[str] = []
        current_state = store.get_current_state()
        zones = current_state.get("zones", {})
        zone = zones.get(proposal.zone_id)
        topology = self._zone_configs.get(proposal.zone_id)
        device = current_state.get("devices", {}).get(proposal.device_id) or store.get_device_state(proposal.device_id)
        now_ms = int(proposal.requested_at_ms if proposal.requested_at_ms is not None else time.time() * 1000)
        activating_action = self._is_activating_action(proposal.action)
        irrigation_activation = self._is_irrigation_activation(proposal)

        if zone is None or topology is None:
            reasons.append(f"zone_not_in_topology:{proposal.zone_id}")
            zone = store.get_zone_state(proposal.zone_id)
        telemetry = dict(zone.get("telemetry", {}))
        zone_device_id = str(zone.get("device_id") or (topology.device_id if topology else "") or "")
        pump_id = str(zone.get("pump_id") or (topology.pump_id if topology else "pump_main") or "pump_main")
        line_id = str(zone.get("line_id") or (topology.line_id if topology else "") or "")
        exclusive_zones = self._exclusive_zones(proposal.zone_id, zone, zones)
        effective_duration_sec = self._effective_duration_sec(proposal, zone, topology)

        if proposal.actuator not in self._allowed_actions:
            reasons.append(f"actuator_not_allowed:{proposal.actuator}")
        elif proposal.action.upper() not in self._allowed_actions[proposal.actuator]:
            reasons.append(f"action_not_allowed:{proposal.actuator}:{proposal.action}")

        if topology is not None and zone_device_id and proposal.device_id != zone_device_id:
            reasons.append(f"zone_device_mismatch:{proposal.zone_id}:{zone_device_id}:{proposal.device_id}")

        if activating_action and irrigation_activation and effective_duration_sec <= 0:
            reasons.append(f"duration_not_allowed:{proposal.zone_id}")

        automation_enabled = current_state.get("automation_flags", {}).get("automation_enabled", {}).get("enabled", True)
        if not self._is_manual_origin(proposal.origin) and not automation_enabled:
            reasons.append("automation_disabled")

        if activating_action and current_state.get("global", {}).get("emergency_stop"):
            reasons.append("global_emergency_stop")
        if activating_action and zone.get("blocked"):
            reasons.append(f"zone_blocked:{proposal.zone_id}")

        if activating_action:
            self._append_lock_reasons(reasons, store.get_active_safety_locks("global"), "global")
            self._append_lock_reasons(reasons, store.get_active_safety_locks("device", proposal.device_id), "device")
            self._append_lock_reasons(reasons, store.get_active_safety_locks("zone", proposal.zone_id), "zone")

        if activating_action and self._maintenance_mode_active(current_state, zone, device):
            reasons.append("maintenance_mode_active")

        manual_lease = store.get_active_manual_lease(proposal.zone_id)
        if manual_lease and not self._manual_lease_allows(proposal, manual_lease):
            holder = str(manual_lease.get("holder") or "operator")
            reasons.append(f"manual_lock_active:{holder}")
            reasons.append("manual_lease_active")

        if activating_action:
            connectivity = str(device.get("connectivity") or "").lower()
            if connectivity in {"offline", "safe_mode"}:
                reasons.append("device_offline")

        last_watering_at_ms = zone.get("last_watering_at_ms")
        cooldown_sec = int(zone.get("cooldown_sec") or 0)
        if last_watering_at_ms and cooldown_sec > 0 and proposal.action.upper() in {"START", "OPEN", "ON"}:
            if now_ms < int(last_watering_at_ms) + cooldown_sec * 1000:
                reasons.append("zone_cooldown_active")

        if irrigation_activation:
            conflict_zone = self._find_conflicting_exclusive_zone(proposal.zone_id, exclusive_zones, zones)
            if conflict_zone:
                reasons.append(f"mutually_exclusive_zone_active:{conflict_zone}")

            shared_line_zone = self._find_shared_line_conflict(proposal.zone_id, line_id, zone, zones)
            if shared_line_zone:
                reasons.append(f"shared_line_conflict:{line_id}:{shared_line_zone}")

            active_zone_count = store.count_active_irrigation_executions()
            if active_zone_count >= int(current_state.get("global", {}).get("max_simultaneous_zones") or 1):
                reasons.append("max_simultaneous_zones_reached")
                reasons.append(f"max_simultaneous_zones_reached:{active_zone_count}")

            active_lines = self._active_lines(proposal.zone_id, zones)
            if line_id and line_id not in active_lines:
                active_line_count = len(active_lines) + 1
            else:
                active_line_count = len(active_lines)
            max_active_lines = int(current_state.get("global", {}).get("max_active_lines") or 1)
            if active_line_count > max_active_lines:
                reasons.append("max_active_lines_reached")
                reasons.append(f"max_active_lines_reached:{active_line_count}:{max_active_lines}")

            cooldown_reason = self._pump_cooldown_reason(current_state, zones, pump_id, now_ms)
            if cooldown_reason:
                reasons.append(cooldown_reason)

            unsafe_contour_reason = self._unsafe_contour_reason(current_state, zones, proposal.zone_id, proposal.device_id, pump_id)
            if unsafe_contour_reason:
                reasons.append(unsafe_contour_reason)

            if telemetry.get("leak") is True and self._config.global_safety.leak_shutdown_enabled:
                reasons.append("leak_detected")
            if telemetry.get("overflow") is True:
                reasons.append("overflow_detected")
            tank_level = telemetry.get("tank_level")
            if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
                reasons.append("water_source_unavailable")
            if telemetry.get("water_available") is False:
                reasons.append("water_source_unavailable")

        if proposal.actuator == "irrigation_valve" and proposal.action.upper() == "OPEN":
            if zone.get("device_state", {}).get("valve_open") is True:
                reasons.append("valve_already_open")

        if proposal.actuator == "master_pump" and proposal.action.upper() == "ON":
            if device.get("state", {}).get("pump_on") is True:
                reasons.append("pump_already_on")
            if not zone.get("device_state", {}).get("valve_open") and not zone.get("reserved_by_execution"):
                reasons.append("pump_precondition_no_open_valve")

        if proposal.actuator == "irrigation_sequence" and proposal.action.upper() == "START":
            if zone.get("device_state", {}).get("valve_open") is True:
                reasons.append("valve_already_open")
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
            max_duration_ms=effective_duration_sec * 1000,
        )

    @staticmethod
    def _is_activating_action(action: str) -> bool:
        return action.upper() in {"START", "OPEN", "ON", "DIM_50"}

    def _is_irrigation_activation(self, proposal: ActionProposal) -> bool:
        return self._is_activating_action(proposal.action) and proposal.actuator in {"irrigation_sequence", "irrigation_valve", "master_pump"}

    def _effective_duration_sec(self, proposal: ActionProposal, zone: dict[str, Any], topology: ZoneSafetyConfig | None) -> int:
        requested = max(0, int(proposal.duration_sec))
        if not self._is_activating_action(proposal.action):
            return 0
        if proposal.actuator == "master_pump":
            return min(requested, int(self._config.global_safety.master_pump_timeout_sec))
        if proposal.actuator in {"irrigation_sequence", "irrigation_valve"}:
            zone_cap = int(zone.get("max_open_duration_sec") or zone.get("max_duration_per_run_sec") or (topology.max_open_duration_sec if topology else 0) or 0)
            return min(requested, zone_cap) if zone_cap > 0 else 0
        return requested

    def _exclusive_zones(self, zone_id: str, zone: dict[str, Any], zones: dict[str, Any]) -> set[str]:
        configured = set(zone.get("mutually_exclusive_zones") or [])
        for other_zone_id, other_zone in zones.items():
            if zone_id in set(other_zone.get("mutually_exclusive_zones") or []):
                configured.add(other_zone_id)
        configured.discard(zone_id)
        return configured

    def _find_conflicting_exclusive_zone(self, zone_id: str, exclusive_zones: set[str], zones: dict[str, Any]) -> str | None:
        for other_zone_id in sorted(exclusive_zones):
            other_zone = zones.get(other_zone_id)
            if other_zone and self._zone_has_active_contour(other_zone, zones):
                return other_zone_id
        return None

    def _find_shared_line_conflict(self, zone_id: str, line_id: str, zone: dict[str, Any], zones: dict[str, Any]) -> str | None:
        if not line_id:
            return None
        restrict_current = bool(zone.get("shared_line_restricted"))
        for other_zone_id, other_zone in zones.items():
            if other_zone_id == zone_id:
                continue
            if str(other_zone.get("line_id") or "") != line_id:
                continue
            if not (restrict_current or bool(other_zone.get("shared_line_restricted"))):
                continue
            if self._zone_has_active_contour(other_zone, zones):
                return other_zone_id
        return None

    def _active_lines(self, current_zone_id: str, zones: dict[str, Any]) -> set[str]:
        active_lines: set[str] = set()
        for zone_id, zone in zones.items():
            if zone_id == current_zone_id:
                continue
            if self._zone_has_active_contour(zone, zones):
                line_id = str(zone.get("line_id") or "")
                if line_id:
                    active_lines.add(line_id)
        return active_lines

    def _zone_has_active_contour(self, zone: dict[str, Any], zones: dict[str, Any]) -> bool:
        if zone.get("reserved_by_execution"):
            return True
        device_state = zone.get("device_state", {})
        return bool(device_state.get("valve_open") or device_state.get("pump_on"))

    def _pump_cooldown_reason(self, current_state: dict[str, Any], zones: dict[str, Any], pump_id: str, now_ms: int) -> str | None:
        cooldown_sec = int(current_state.get("global", {}).get("pump_cooldown_sec") or 0)
        if cooldown_sec <= 0:
            return None
        last_stop_ms = self._last_pump_stop_ms(current_state, zones, pump_id)
        if last_stop_ms is None:
            return None
        remaining_ms = (last_stop_ms + cooldown_sec * 1000) - now_ms
        if remaining_ms > 0:
            return f"pump_cooldown_active:{pump_id}:{remaining_ms}"
        return None

    def _last_pump_stop_ms(self, current_state: dict[str, Any], zones: dict[str, Any], pump_id: str) -> int | None:
        latest: int | None = None
        for execution in current_state.get("executions", {}).values():
            zone = zones.get(execution.get("zone_id"))
            if not zone or str(zone.get("pump_id") or "pump_main") != pump_id:
                continue
            lifecycle = str(execution.get("lifecycle") or "")
            if lifecycle not in TERMINAL_LIFECYCLES and str(execution.get("phase") or "") != ExecutionPhase.FINISHED.value:
                continue
            updated_at_ms = execution.get("updated_at_ms")
            if updated_at_ms is None:
                continue
            latest = max(latest or 0, int(updated_at_ms))
        return latest

    def _unsafe_contour_reason(self, current_state: dict[str, Any], zones: dict[str, Any], zone_id: str, device_id: str, pump_id: str) -> str | None:
        for execution in current_state.get("executions", {}).values():
            execution_zone_id = str(execution.get("zone_id") or "")
            zone = zones.get(execution_zone_id, {})
            same_pump = str(zone.get("pump_id") or "pump_main") == pump_id
            same_zone = execution_zone_id == zone_id
            same_device = str(execution.get("device_id") or "") == device_id
            if not (same_pump or same_zone or same_device):
                continue
            phase = str(execution.get("phase") or "")
            lifecycle = str(execution.get("lifecycle") or "")
            zone_open = bool(zone.get("device_state", {}).get("valve_open"))
            if phase in SAFE_STOP_PENDING_PHASES:
                return f"safe_stop_pending:{execution_zone_id or zone_id}"
            if lifecycle in TERMINAL_LIFECYCLES and (phase != ExecutionPhase.FINISHED.value or zone_open):
                return f"faulted_contour_open:{execution_zone_id or zone_id}"
        return None

    def _append_lock_reasons(self, reasons: list[str], locks: list[dict[str, Any]], scope: str) -> None:
        for lock in locks:
            if lock.get("kind") == "execution_reservation":
                continue
            kind = str(lock.get("kind") or "unknown")
            owner = str(lock.get("owner") or "")
            if kind == "manual_lock":
                reasons.append(f"manual_lock_active:{owner or scope}")
            elif "maintenance" in kind:
                reasons.append("maintenance_mode_active")
            elif kind == "manual_emergency_stop":
                reasons.append("global_emergency_stop")
            else:
                reasons.append(f"{scope}_lock:{kind}")

    def _maintenance_mode_active(self, current_state: dict[str, Any], zone: dict[str, Any], device: dict[str, Any]) -> bool:
        flags = current_state.get("automation_flags", {})
        return bool(
            flags.get("maintenance_mode", {}).get("enabled")
            or zone.get("maintenance_mode")
            or zone.get("device_state", {}).get("maintenance_mode")
            or device.get("state", {}).get("maintenance_mode")
        )

    def _manual_lease_allows(self, proposal: ActionProposal, lease: dict[str, Any]) -> bool:
        if not self._is_manual_origin(proposal.origin):
            return False
        operator_id = str(proposal.metadata.get("operator_id") or "")
        operator_name = str(proposal.metadata.get("operator_name") or "")
        holder = str(lease.get("holder") or "")
        return holder in {operator_id, operator_name}

    def build_command_record(self, proposal: ActionProposal) -> CommandRecord:
        created_at_ms = proposal.requested_at_ms if proposal.requested_at_ms is not None else int(time.time() * 1000)
        ttl_sec = self._command_ttl_sec(proposal.origin)
        expires_at_ms = created_at_ms + ttl_sec * 1000
        command_id = proposal.command_id or f"cmd-{uuid.uuid4().hex[:12]}"
        command_type = CommandType.IRRIGATE_ZONE if proposal.actuator == "irrigation_sequence" else CommandType.SET_ACTUATOR
        proposal_metadata = dict(proposal.metadata)
        topology = self._zone_configs.get(proposal.zone_id)
        effective_duration_sec = self._effective_duration_sec(proposal, {}, topology)
        requested_duration_sec = max(0, int(proposal.duration_sec))
        command_metadata = {
            "nonce": f"nonce-{uuid.uuid4().hex[:16]}",
            "device_binding": proposal.device_id,
            "zone_binding": proposal.zone_id,
            "replay_window_ms": ttl_sec * 1000,
            "operator_id": proposal_metadata.get("operator_id"),
            "operator_name": proposal_metadata.get("operator_name"),
            "submitted_via": proposal_metadata.get("submitted_via"),
            "command_source": proposal_metadata.get("command_source") or proposal.origin.value,
            "command_ttl_sec": ttl_sec,
            "requested_duration_sec": proposal_metadata.get("requested_duration_sec", requested_duration_sec),
            "effective_duration_sec": proposal_metadata.get("effective_duration_sec", effective_duration_sec),
            "manual_ttl_sec": proposal_metadata.get("manual_ttl_sec", ttl_sec if self._is_manual_origin(proposal.origin) else None),
            "topology": {
                "pump_id": topology.pump_id if topology else "pump_main",
                "line_id": topology.line_id if topology else "",
                "shared_line_restricted": bool(topology.shared_line_restricted) if topology else False,
                "mutually_exclusive_zones": list(topology.mutually_exclusive_zones) if topology else [],
            },
        }
        if effective_duration_sec != requested_duration_sec:
            command_metadata["duration_clamped_from_sec"] = requested_duration_sec
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
                "duration_sec": effective_duration_sec,
                "metadata": proposal.metadata,
            },
            metadata=command_metadata,
        )

    @staticmethod
    def _is_manual_origin(origin: DecisionOrigin) -> bool:
        return origin in {DecisionOrigin.OPERATOR, DecisionOrigin.MCP}

    def _command_ttl_sec(self, origin: DecisionOrigin) -> int:
        if origin == DecisionOrigin.MCP:
            return int(self._config.global_safety.mcp_command_ttl_sec)
        if origin == DecisionOrigin.OPERATOR:
            return int(self._config.global_safety.manual_command_ttl_sec)
        return int(self._config.global_safety.command_ttl_sec)
