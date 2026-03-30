from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any

from backend.api.tools import BackendToolService
from backend.config import BackendConfig
from backend.domain.models import AuditLogRecord, AutomationFlagRecord, SafetyLockRecord
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import ACTIVE_LIFECYCLES, StateStore


class OperatorControlService:
    def __init__(
        self,
        config: BackendConfig,
        state_store: StateStore,
        telemetry_history: TelemetryHistoryStore,
        backend_tools: BackendToolService,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._telemetry_history = telemetry_history
        self._backend_tools = backend_tools

    def list_devices_zones(self) -> dict[str, Any]:
        current_state = self._state_store.get_current_state()
        commands = self._sorted_commands()
        locks = self._state_store.get_active_safety_locks()
        devices = [self._build_device_view(device, commands, locks) for device in current_state.get("devices", {}).values()]
        zones = [self._build_zone_view(zone, current_state, commands, locks) for zone in current_state.get("zones", {}).values()]
        devices.sort(key=lambda item: item["device_id"])
        zones.sort(key=lambda item: item["zone_id"])
        return {"devices": devices, "zones": zones, "generated_at_ms": self._now_ms()}

    def get_current_state(self) -> dict[str, Any]:
        return self._plain(self._backend_tools.get_current_state())

    def get_device_status(self, device_id: str) -> dict[str, Any]:
        snapshot = self.list_devices_zones()
        for device in snapshot["devices"]:
            if device["device_id"] == device_id:
                return device
        return {
            "device_id": device_id,
            "status": "not_found",
            "generated_at_ms": self._now_ms(),
        }

    def get_zone_status(self, zone_id: str) -> dict[str, Any]:
        snapshot = self.list_devices_zones()
        for zone in snapshot["zones"]:
            if zone["zone_id"] == zone_id:
                return zone
        return {
            "zone_id": zone_id,
            "status": "not_found",
            "generated_at_ms": self._now_ms(),
        }

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> dict[str, Any]:
        return {
            "device_id": device_id,
            "sensor": sensor,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "samples": self._plain(self._backend_tools.get_sensor_history(device_id, sensor, start_ms, end_ms, limit=limit)),
            "generated_at_ms": self._now_ms(),
        }

    def get_control_safety_state(self) -> dict[str, Any]:
        current_state = self._state_store.get_current_state()
        commands = self.command_history(limit=50)["commands"]
        return {
            "global": self._plain(current_state.get("global", {})),
            "automation_flags": self._plain(current_state.get("automation_flags", {})),
            "active_safety_locks": self._plain(self._state_store.get_active_safety_locks()),
            "active_alarms": self._plain(self._state_store.get_active_alarms()),
            "active_commands": [command for command in commands if command.get("lifecycle") in ACTIVE_LIFECYCLES],
            "generated_at_ms": self._now_ms(),
        }

    def command_history(self, limit: int = 50) -> dict[str, Any]:
        commands = [self._serialize_command(command) for command in self._sorted_commands()[: max(1, min(limit, 200))]]
        return {"commands": commands, "generated_at_ms": self._now_ms()}

    def submit_manual_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = self._normalize_manual_action(payload)
        metadata = dict(action.get("metadata") or {})
        self._state_store.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=str(action["trace_id"]),
                action_type="MANUAL_ACTION_SUBMITTED",
                message=str(action["reason"]),
                created_at_ms=self._now_ms(),
                command_id=str(action["command_id"]),
                device_id=str(action["device_id"]),
                zone_id=str(action["zone_id"]),
                payload={
                    "submitted_via": metadata.get("submitted_via"),
                    "command_source": metadata.get("command_source"),
                    "operator_id": metadata.get("operator_id"),
                    "operator_name": metadata.get("operator_name"),
                    "requested_duration_sec": metadata.get("requested_duration_sec"),
                    "effective_duration_sec": metadata.get("effective_duration_sec"),
                    "manual_ttl_sec": metadata.get("manual_ttl_sec"),
                },
            )
        )
        result = self._backend_tools.execute_manual_action(
            action,
            source=str(metadata.get("command_source") or "operator"),
        )
        command_id = result.get("command_id")
        if command_id:
            status = self._backend_tools.get_command_status(str(command_id))
            if status is not None:
                result["command"] = self._serialize_command(status)
        return self._plain(result)

    def propose_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = self._normalize_manual_action(payload)
        proposal = self._backend_tools.propose_action(action)
        zone_status = self.get_zone_status(action["zone_id"])
        proposal["normalized_action"] = action
        proposal["zone_status"] = zone_status
        return self._plain(proposal)

    def execute_manual_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.submit_manual_command(payload)

    def emergency_stop(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_ms = self._now_ms()
        operator_id = str(payload.get("operator_id") or "operator")
        operator_name = str(payload.get("operator_name") or operator_id)
        reason = str(payload.get("reason") or "manual emergency stop")
        trace_id = str(payload.get("trace_id") or f"trace-emergency-{now_ms}")
        submitted_via = str(payload.get("submitted_via") or "operator_ui")

        lock = SafetyLockRecord(
            lock_id="lock-global-backend-runtime-manual-emergency-stop",
            scope="global",
            scope_id="backend-runtime",
            kind="manual_emergency_stop",
            reason=reason,
            active=True,
            created_at_ms=now_ms,
            expires_at_ms=None,
            owner=operator_id,
            payload={"operator_id": operator_id, "operator_name": operator_name},
        )
        self._state_store.create_safety_lock(lock)
        self._state_store.set_automation_flag(
            AutomationFlagRecord(
                flag_name="automation_enabled",
                enabled=False,
                updated_at_ms=now_ms,
                payload={"disabled_by": operator_id, "reason": reason, "source": submitted_via},
            )
        )
        self._state_store.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=trace_id,
                action_type="EMERGENCY_STOP",
                message=reason,
                created_at_ms=now_ms,
                payload={"operator_id": operator_id, "operator_name": operator_name},
            )
        )

        responses: list[dict[str, Any]] = []
        current_state = self._state_store.get_current_state()
        seen_devices: set[str] = set()
        for zone_id, zone in current_state.get("zones", {}).items():
            device_id = str(zone.get("device_id") or "")
            if not device_id:
                continue
            responses.append(
                self.submit_manual_command(
                    {
                        "trace_id": trace_id,
                        "operator_id": operator_id,
                        "operator_name": operator_name,
                        "zone_id": zone_id,
                        "device_id": device_id,
                        "actuator": "irrigation_valve",
                        "action": "CLOSE",
                        "duration_sec": 0,
                        "reason": f"emergency stop close valve: {reason}",
                        "metadata": {"emergency_stop": True},
                        "submitted_via": submitted_via,
                    }
                )
            )
            if device_id not in seen_devices:
                seen_devices.add(device_id)
                responses.append(
                    self.submit_manual_command(
                        {
                            "trace_id": trace_id,
                            "operator_id": operator_id,
                            "operator_name": operator_name,
                            "zone_id": zone_id,
                            "device_id": device_id,
                            "actuator": "nutrient_doser",
                            "action": "STOP",
                            "duration_sec": 0,
                            "reason": f"emergency stop stop doser: {reason}",
                            "metadata": {"emergency_stop": True},
                            "submitted_via": submitted_via,
                        }
                    )
                )

        return {
            "status": "emergency_stop_active",
            "lock": self._plain(lock.model_dump()),
            "responses": responses,
            "generated_at_ms": now_ms,
        }

    def overview(self) -> dict[str, Any]:
        devices_zones = self.list_devices_zones()
        return {
            "devices": devices_zones["devices"],
            "zones": devices_zones["zones"],
            "state": self.get_control_safety_state(),
            "command_history": self.command_history(limit=25)["commands"],
            "generated_at_ms": self._now_ms(),
        }

    def _normalize_manual_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_ms = self._now_ms()
        ui_action = str(payload.get("ui_action") or "").strip().lower()
        zone_id = str(payload.get("zone_id") or "")
        device_id = str(payload.get("device_id") or "")
        zone = self._state_store.get_zone_state(zone_id) if zone_id else {}
        if not device_id and zone.get("device_id"):
            device_id = str(zone["device_id"])
        action_map = {
            "open_valve": ("irrigation_valve", "OPEN", min(10, int(zone.get("max_open_duration_sec") or zone.get("max_duration_per_run_sec") or 10))),
            "close_valve": ("irrigation_valve", "CLOSE", 0),
            "dose_solution": ("nutrient_doser", "START", min(5, int(self._config.global_safety.max_manual_duration_sec))),
        }
        if ui_action:
            if ui_action not in action_map:
                raise ValueError(f"unsupported_ui_action:{ui_action}")
            actuator, action, default_duration_sec = action_map[ui_action]
        else:
            actuator = str(payload["actuator"])
            action = str(payload["action"]).upper()
            default_duration_sec = int(payload.get("duration_sec", 0))

        requested_duration_sec = int(payload.get("duration_sec", default_duration_sec) or default_duration_sec)
        duration_cap = self._duration_cap(actuator, zone)
        duration_sec = min(max(0, requested_duration_sec), duration_cap) if duration_cap > 0 else 0

        metadata = dict(payload.get("metadata", {}))
        submitted_via = self._normalize_submitted_via(payload.get("submitted_via"))
        command_source = "mcp" if submitted_via == "openclaw_mcp" else "operator"
        manual_ttl_sec = self._manual_ttl_sec(command_source)
        metadata.update(
            {
                "operator_id": str(payload.get("operator_id") or "operator"),
                "operator_name": str(payload.get("operator_name") or payload.get("operator_id") or "operator"),
                "submitted_via": submitted_via,
                "command_source": command_source,
                "requested_duration_sec": requested_duration_sec,
                "effective_duration_sec": duration_sec,
                "manual_ttl_sec": manual_ttl_sec,
                "duration_cap_sec": duration_cap,
                "ui_action": ui_action or None,
            }
        )
        return {
            "trace_id": str(payload.get("trace_id") or f"trace-operator-{now_ms}"),
            "command_id": payload.get("command_id") or f"cmd-operator-{uuid.uuid4().hex[:12]}",
            "device_id": device_id,
            "zone_id": zone_id,
            "actuator": actuator,
            "action": action,
            "duration_sec": duration_sec,
            "reason": str(payload.get("reason") or f"operator action: {ui_action or action.lower()}"),
            "requested_at_ms": int(payload.get("requested_at_ms", now_ms)),
            "metadata": metadata,
        }

    def _duration_cap(self, actuator: str, zone: dict[str, Any]) -> int:
        global_manual_cap = max(0, int(self._config.global_safety.max_manual_duration_sec))
        if actuator == "nutrient_doser":
            base_cap = global_manual_cap
        else:
            base_cap = int(zone.get("max_open_duration_sec") or zone.get("max_duration_per_run_sec") or 0)
        if global_manual_cap <= 0:
            return base_cap
        if base_cap <= 0:
            return global_manual_cap
        return min(base_cap, global_manual_cap)

    @staticmethod
    def _normalize_submitted_via(value: Any) -> str:
        submitted_via = str(value or "operator_ui").strip().lower()
        if submitted_via in {"openclaw_mcp", "mcp"}:
            return "openclaw_mcp"
        return "operator_ui"

    def _manual_ttl_sec(self, command_source: str) -> int:
        if command_source == "mcp":
            return int(self._config.global_safety.mcp_command_ttl_sec)
        return int(self._config.global_safety.manual_command_ttl_sec)

    def _sorted_commands(self) -> list[dict[str, Any]]:
        commands = self._state_store.get_current_state().get("commands", {}).values()
        return sorted(commands, key=lambda item: int(item.get("requested_at_ms") or 0), reverse=True)

    def _serialize_command(self, command: dict[str, Any]) -> dict[str, Any]:
        execution = command.get("execution") or self._state_store.get_execution(str(command.get("current_execution_id") or ""))
        metadata = dict(command.get("metadata") or {})
        result_payload = dict((execution or {}).get("result_payload") or {})
        return self._plain(
            {
                "command_id": command.get("command_id"),
                "trace_id": command.get("trace_id"),
                "device_id": command.get("device_id"),
                "zone_id": command.get("zone_id"),
                "requested_by": command.get("requested_by"),
                "reason": command.get("reason"),
                "requested_at_ms": command.get("requested_at_ms"),
                "expires_at_ms": command.get("expires_at_ms"),
                "lifecycle": command.get("lifecycle"),
                "current_execution_id": command.get("current_execution_id"),
                "last_error": command.get("last_error"),
                "requested_payload": command.get("requested_payload", {}),
                "operator": {
                    "operator_id": metadata.get("operator_id"),
                    "operator_name": metadata.get("operator_name"),
                    "submitted_via": metadata.get("submitted_via"),
                    "command_source": metadata.get("command_source"),
                    "requested_duration_sec": metadata.get("requested_duration_sec"),
                    "effective_duration_sec": metadata.get("effective_duration_sec"),
                    "manual_ttl_sec": metadata.get("manual_ttl_sec"),
                },
                "command_source": metadata.get("command_source"),
                "execution": execution or None,
                "last_ack": result_payload.get("last_ack"),
                "last_result": result_payload.get("last_result"),
            }
        )

    def _build_device_view(self, device: dict[str, Any], commands: list[dict[str, Any]], locks: list[dict[str, Any]]) -> dict[str, Any]:
        device_id = str(device.get("device_id") or "")
        device_commands = [command for command in commands if command.get("device_id") == device_id]
        last_command = self._serialize_command(device_commands[0]) if device_commands else None
        active_command = next((self._serialize_command(command) for command in device_commands if command.get("lifecycle") in ACTIVE_LIFECYCLES), None)
        device_locks = [lock for lock in locks if lock.get("scope") == "device" and lock.get("scope_id") == device_id]
        zone_id = str(device.get("zone_id") or "")
        return self._plain(
            {
                "device_id": device_id,
                "zone_id": zone_id,
                "connectivity": device.get("connectivity"),
                "firmware_version": device.get("firmware_version"),
                "last_seen_ms": device.get("last_seen_ms"),
                "last_telemetry_ms": device.get("last_telemetry_ms"),
                "state": device.get("state", {}),
                "telemetry": self._telemetry_summary(device.get("telemetry", {})),
                "active_command": active_command,
                "last_command": last_command,
                "safety_locks": device_locks,
            }
        )

    def _build_zone_view(self, zone: dict[str, Any], current_state: dict[str, Any], commands: list[dict[str, Any]], locks: list[dict[str, Any]]) -> dict[str, Any]:
        zone_id = str(zone.get("zone_id") or "")
        device_id = str(zone.get("device_id") or "")
        device = current_state.get("devices", {}).get(device_id, {})
        zone_commands = [command for command in commands if command.get("zone_id") == zone_id]
        last_command = self._serialize_command(zone_commands[0]) if zone_commands else None
        active_command = next((self._serialize_command(command) for command in zone_commands if command.get("lifecycle") in ACTIVE_LIFECYCLES), None)
        zone_locks = [
            lock
            for lock in locks
            if (lock.get("scope") == "zone" and lock.get("scope_id") == zone_id)
            or (lock.get("scope") == "device" and lock.get("scope_id") == device_id)
            or lock.get("scope") == "global"
        ]
        action_guards = self._zone_action_guards(zone, device, current_state, zone_locks)
        return self._plain(
            {
                "zone_id": zone_id,
                "device_id": device_id,
                "blocked": zone.get("blocked", False),
                "reserved_by_execution": zone.get("reserved_by_execution"),
                "last_watering_at_ms": zone.get("last_watering_at_ms"),
                "last_error_at_ms": zone.get("last_error_at_ms"),
                "connectivity": device.get("connectivity"),
                "device_state": zone.get("device_state", {}),
                "telemetry": self._telemetry_summary(zone.get("telemetry", {})),
                "active_command": active_command,
                "last_command": last_command,
                "safety_locks": zone_locks,
                "actions": action_guards,
            }
        )

    def _zone_action_guards(self, zone: dict[str, Any], device: dict[str, Any], current_state: dict[str, Any], locks: list[dict[str, Any]]) -> dict[str, Any]:
        blocking_lock_reasons = [f"{lock.get('scope')}:{lock.get('kind')}" for lock in locks if lock.get("kind") != "execution_reservation"]
        telemetry = zone.get("telemetry", {})
        device_state = zone.get("device_state", {})
        online = str(device.get("connectivity") or "").lower() == "online"
        valve_open = bool(device_state.get("valve_open"))
        doser_active = bool(device.get("state", {}).get("doser_active") or device_state.get("doser_active"))
        water_level = telemetry.get("water_level")
        water_available = not isinstance(water_level, (int, float)) or float(water_level) > 10.0
        any_other_reserved = any(
            other_zone.get("zone_id") != zone.get("zone_id") and other_zone.get("reserved_by_execution")
            for other_zone in current_state.get("zones", {}).values()
        )

        def guard(enabled: bool, reasons: list[str]) -> dict[str, Any]:
            return {"enabled": enabled, "reasons": [reason for reason in reasons if reason]}

        return {
            "open_valve": guard(online and not valve_open and not blocking_lock_reasons and not any_other_reserved and not zone.get("blocked"), [
                "" if online else "device_offline",
                "" if not valve_open else "valve_already_open",
                "" if not blocking_lock_reasons else ",".join(blocking_lock_reasons),
                "" if not any_other_reserved else "valve_interlock_active",
                "" if not zone.get("blocked") else "zone_blocked",
            ]),
            "close_valve": guard(valve_open or bool(zone.get("reserved_by_execution")), [
                "" if (valve_open or bool(zone.get("reserved_by_execution"))) else "valve_not_open",
            ]),
            "dose_solution": guard(
                online and not doser_active and not blocking_lock_reasons and not zone.get("blocked") and water_available,
                [
                    "" if online else "device_offline",
                    "" if not doser_active else "doser_already_active",
                    "" if not blocking_lock_reasons else ",".join(blocking_lock_reasons),
                    "" if not zone.get("blocked") else "zone_blocked",
                    "" if water_available else "water_level_too_low",
                ],
            ),
        }

    @staticmethod
    def _telemetry_summary(telemetry: dict[str, Any]) -> dict[str, Any]:
        keys = ("ph", "ec", "water_level")
        return {key: telemetry.get(key) for key in keys if key in telemetry}

    @classmethod
    def _plain(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): cls._plain(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._plain(item) for item in value]
        if isinstance(value, tuple):
            return [cls._plain(item) for item in value]
        if isinstance(value, Enum):
            return value.value
        return value

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)
