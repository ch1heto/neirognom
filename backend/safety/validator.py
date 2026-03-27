from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from backend.config import BackendConfig
from backend.state.store import StateStore
from shared.contracts.messages import CommandLifecycle, CommandRequest, DecisionOrigin, SafetyDecision


DEFAULT_ALLOWED_ACTIONS: dict[str, set[str]] = {
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
    metadata: dict[str, Any] = field(default_factory=dict)


class SafetyValidator:
    def __init__(self, config: BackendConfig, allowed_actions: dict[str, set[str]] | None = None) -> None:
        self._config = config
        self._allowed_actions = allowed_actions or DEFAULT_ALLOWED_ACTIONS

    def validate(self, store: StateStore, proposal: ActionProposal) -> SafetyDecision:
        reasons: list[str] = []
        zone = store.get_zone_state(proposal.zone_id)
        current_state = store.get_current_state()
        global_state = current_state.get("global", {})
        telemetry = zone.get("telemetry", {})

        if proposal.actuator not in self._allowed_actions:
            reasons.append(f"actuator_not_allowed:{proposal.actuator}")
        elif proposal.action.upper() not in self._allowed_actions[proposal.actuator]:
            reasons.append(f"action_not_allowed:{proposal.actuator}:{proposal.action}")

        if global_state.get("emergency_stop"):
            reasons.append("global_emergency_stop")
        if zone.get("blocked"):
            reasons.append(f"zone_blocked:{proposal.zone_id}")

        if proposal.duration_sec > int(zone.get("max_duration_per_run_sec") or 0 or 0):
            reasons.append(
                f"duration_exceeds_zone_limit:{proposal.duration_sec}>{zone.get('max_duration_per_run_sec')}"
            )

        last_watering_at_ms = zone.get("last_watering_at_ms")
        cooldown_sec = int(zone.get("cooldown_sec") or 0)
        if last_watering_at_ms and cooldown_sec > 0:
            cooldown_until = int(last_watering_at_ms) + cooldown_sec * 1000
            if proposal.requested_at_ms < cooldown_until and proposal.action.upper() in {"OPEN", "ON"}:
                reasons.append("zone_cooldown_active")

        if proposal.action.upper() in {"OPEN", "ON"}:
            if store.count_active_zone_commands() >= int(global_state.get("max_simultaneous_zones") or 1):
                reasons.append("max_simultaneous_zones_reached")
            if telemetry.get("leak") is True and self._config.global_safety.leak_shutdown_enabled:
                reasons.append("leak_detected")
            if telemetry.get("overflow") is True:
                reasons.append("overflow_detected")
            tank_level = telemetry.get("tank_level")
            if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
                reasons.append("water_source_unavailable")
            if telemetry.get("water_available") is False:
                reasons.append("water_source_unavailable")

        allowed = not reasons
        return SafetyDecision(
            trace_id=proposal.trace_id,
            device_id=proposal.device_id,
            zone_id=proposal.zone_id,
            allowed=allowed,
            lifecycle_state=CommandLifecycle.QUEUED if allowed else CommandLifecycle.REJECTED_BY_SAFETY,
            origin=proposal.origin,
            reasons=reasons,
            actuator=proposal.actuator,
            action=proposal.action,
            max_duration_ms=proposal.duration_sec * 1000,
        )

    def build_command(self, proposal: ActionProposal) -> CommandRequest:
        created_at_ms = proposal.requested_at_ms or int(time.time() * 1000)
        expires_at_ms = created_at_ms + self._config.global_safety.command_ttl_sec * 1000
        return CommandRequest(
            command_id=f"cmd-{uuid.uuid4().hex[:12]}",
            trace_id=proposal.trace_id,
            device_id=proposal.device_id,
            zone_id=proposal.zone_id,
            actuator=proposal.actuator,
            action=proposal.action.upper(),
            created_at_ms=created_at_ms,
            expires_at_ms=expires_at_ms,
            max_duration_ms=proposal.duration_sec * 1000,
            idempotency_key=f"{proposal.zone_id}:{proposal.actuator}:{proposal.requested_at_ms}",
            origin=proposal.origin,
            reason=proposal.reason,
            parameters=proposal.metadata | {"duration_sec": proposal.duration_sec},
        )
