from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.config import BackendConfig
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from shared.contracts.messages import LlmAllowedAction, LlmDecisionRequest, TelemetryMessage


class DecisionContextBuilder:
    def __init__(self, config: BackendConfig, store: StateStore, telemetry_history: TelemetryHistoryStore) -> None:
        self._config = config
        self._store = store
        self._telemetry_history = telemetry_history
        self._critical_thresholds = self._load_critical_thresholds()

    def _load_critical_thresholds(self) -> dict[str, Any]:
        path = Path(__file__).resolve().parents[2] / "knowledge_base" / "alerts" / "critical_thresholds.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def build(self, message: TelemetryMessage) -> LlmDecisionRequest:
        current_state = self._store.get_current_state()
        zone_state = dict(self._store.get_zone_state(message.zone_id))
        zone_state["grow_map"] = zone_state.get("grow_map") or {}
        device_state = self._store.get_device_state(message.device_id)
        global_state = current_state.get("global", {})
        automation_flags = current_state.get("automation_flags", {})
        telemetry_windows = self._telemetry_windows(message)
        global_limits = dict(global_state)
        global_limits["critical_thresholds"] = self._critical_thresholds

        return LlmDecisionRequest(
            trace_id=message.trace_id,
            device_id=message.device_id,
            zone_id=message.zone_id,
            zone_state=zone_state,
            device_state=device_state,
            global_state=global_state,
            zone_limits={k: v for k, v in zone_state.items() if k not in {"telemetry", "device_state"}},
            global_limits=global_limits,
            active_safety_locks=self._active_safety_locks(message.device_id, message.zone_id),
            active_alarms=self._active_alarms(message.device_id, message.zone_id),
            automation_flags=automation_flags,
            recent_zone_commands=self._recent_commands(zone_id=message.zone_id),
            recent_device_commands=self._recent_commands(device_id=message.device_id),
            telemetry_windows=telemetry_windows,
            allowed_actions=self._allowed_actions(zone_state, global_state),
            current_state=current_state,
            telemetry_window=telemetry_windows.get("ph", []),
        )

    def _telemetry_windows(self, message: TelemetryMessage) -> dict[str, list[dict[str, Any]]]:
        start_ms = max(0, message.ts_ms - 3_600_000)
        end_ms = message.ts_ms
        sensors = ("ph", "ec", "water_level")
        return {
            sensor: self._telemetry_history.get_sensor_history(message.device_id, sensor, start_ms, end_ms, limit=3)
            for sensor in sensors
        }

    def _active_safety_locks(self, device_id: str, zone_id: str) -> list[dict[str, Any]]:
        return [
            *self._store.get_active_safety_locks("global"),
            *self._store.get_active_safety_locks("device", device_id),
            *self._store.get_active_safety_locks("zone", zone_id),
        ]

    def _active_alarms(self, device_id: str, zone_id: str) -> list[dict[str, Any]]:
        alarms = self._store.get_active_alarms()
        return [alarm for alarm in alarms if alarm.get("device_id") in {"", device_id} or alarm.get("zone_id") in {"", zone_id}]

    def _recent_commands(self, *, zone_id: str | None = None, device_id: str | None = None, limit: int = 5) -> list[dict[str, Any]]:
        commands = list((self._store.get_current_state().get("commands") or {}).values())
        if zone_id is not None:
            commands = [command for command in commands if command.get("zone_id") == zone_id]
        if device_id is not None:
            commands = [command for command in commands if command.get("device_id") == device_id]
        commands.sort(key=lambda item: int(item.get("requested_at_ms") or 0), reverse=True)
        return [
            {
                "command_id": command.get("command_id"),
                "command_type": command.get("command_type"),
                "requested_by": command.get("requested_by"),
                "reason": command.get("reason"),
                "requested_at_ms": command.get("requested_at_ms"),
                "lifecycle": command.get("lifecycle"),
                "last_error": command.get("last_error"),
                "requested_payload": command.get("requested_payload", {}),
            }
            for command in commands[:limit]
        ]

    def _allowed_actions(self, zone_state: dict[str, Any], global_state: dict[str, Any]) -> list[LlmAllowedAction]:
        actions = [
            LlmAllowedAction(decision="no_action", actuator=None, action=None, max_duration_sec=0, description="Do nothing."),
            LlmAllowedAction(decision="close_valve", actuator="irrigation_valve", action="CLOSE", max_duration_sec=0, description="Close the tray valve."),
        ]
        if global_state.get("emergency_stop"):
            return actions

        if not zone_state.get("blocked") and not zone_state.get("maintenance_mode"):
            actions.append(
                LlmAllowedAction(
                    decision="open_valve",
                    actuator="irrigation_valve",
                    action="OPEN",
                    max_duration_sec=max(0, int(zone_state.get("max_open_duration_sec") or zone_state.get("max_duration_per_run_sec") or 0)),
                    description="Open the tray valve for a bounded interval.",
                )
            )
            actions.append(
                LlmAllowedAction(
                    decision="dose_solution",
                    actuator="nutrient_doser",
                    action="START",
                    max_duration_sec=5,
                    description="Dose nutrient solution into the active tray.",
                )
            )
        return actions
