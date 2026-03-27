from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from backend.config import GlobalSafetyConfig, SqliteConfig, ZoneSafetyConfig
from backend.domain.models import (
    AlarmRecord,
    AuditLogRecord,
    AutomationFlagRecord,
    CommandExecutionRecord,
    CommandRecord,
    CommandType,
    DeviceRecord,
    ManualLeaseRecord,
    SafetyLockRecord,
    TrayZoneRecord,
)
from shared.contracts.messages import AlertEvent, CommandLifecycle, DeviceStateMessage, TelemetryMessage


log = logging.getLogger("backend.state")
ACTIVE_LIFECYCLES = {
    CommandLifecycle.PLANNED.value,
    CommandLifecycle.DISPATCHED.value,
    CommandLifecycle.ACKED.value,
    CommandLifecycle.EXECUTING.value,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


class StateStore(Protocol):
    def initialize(self, zones: list[ZoneSafetyConfig], global_safety: GlobalSafetyConfig) -> None: ...
    def seen_message(self, message_id: str) -> bool: ...
    def write_telemetry_snapshot(self, message: TelemetryMessage) -> dict[str, Any]: ...
    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]: ...
    def get_zone_state(self, zone_id: str) -> dict[str, Any]: ...
    def get_device_state(self, device_id: str) -> dict[str, Any]: ...
    def get_current_state(self) -> dict[str, Any]: ...
    def create_or_get_command(self, command: CommandRecord) -> tuple[dict[str, Any], bool]: ...
    def update_command(self, command_id: str, **changes: Any) -> dict[str, Any] | None: ...
    def get_command(self, command_id: str) -> dict[str, Any] | None: ...
    def get_command_status(self, command_id: str) -> dict[str, Any] | None: ...
    def list_active_commands(self) -> list[dict[str, Any]]: ...
    def create_execution(self, execution: CommandExecutionRecord) -> dict[str, Any]: ...
    def update_execution(self, execution_id: str, **changes: Any) -> dict[str, Any] | None: ...
    def get_execution(self, execution_id: str) -> dict[str, Any] | None: ...
    def list_active_executions(self) -> list[dict[str, Any]]: ...
    def list_recoverable_executions(self) -> list[dict[str, Any]]: ...
    def create_safety_lock(self, lock: SafetyLockRecord) -> dict[str, Any]: ...
    def release_safety_lock(self, lock_id: str) -> dict[str, Any] | None: ...
    def get_active_safety_locks(self, scope: str | None = None, scope_id: str | None = None) -> list[dict[str, Any]]: ...
    def create_manual_lease(self, lease: ManualLeaseRecord) -> dict[str, Any]: ...
    def release_manual_lease(self, lease_id: str) -> dict[str, Any] | None: ...
    def get_active_manual_lease(self, zone_id: str) -> dict[str, Any] | None: ...
    def set_automation_flag(self, flag: AutomationFlagRecord) -> dict[str, Any]: ...
    def get_automation_flags(self) -> dict[str, dict[str, Any]]: ...
    def get_active_alarms(self) -> list[dict[str, Any]]: ...
    def record_alarm(self, alert: AlertEvent) -> dict[str, Any]: ...
    def append_audit_log(self, audit: AuditLogRecord) -> dict[str, Any]: ...
    def note_incident(self, category: str, payload: dict[str, Any]) -> None: ...
    def count_active_irrigation_executions(self) -> int: ...
    def count_completed_zone_runs_since(self, zone_id: str, since_ms: int) -> int: ...
    def sum_zone_volume_since(self, zone_id: str, since_ms: int) -> float: ...
    def total_completed_volume_since(self, since_ms: int) -> float: ...
    def mark_zone_watering(self, zone_id: str, ts_ms: int) -> None: ...
    def mark_zone_error(self, zone_id: str, ts_ms: int) -> None: ...


class MemoryStateStore:
    def __init__(self) -> None:
        self._seen_messages: set[str] = set()
        self._devices: dict[str, dict[str, Any]] = {}
        self._zones: dict[str, dict[str, Any]] = {}
        self._commands: dict[str, dict[str, Any]] = {}
        self._executions: dict[str, dict[str, Any]] = {}
        self._safety_locks: dict[str, dict[str, Any]] = {}
        self._manual_leases: dict[str, dict[str, Any]] = {}
        self._alarms: dict[str, dict[str, Any]] = {}
        self._audit_logs: list[dict[str, Any]] = []
        self._automation_flags: dict[str, dict[str, Any]] = {}
        self._global_state: dict[str, Any] = {}

    def initialize(self, zones: list[ZoneSafetyConfig], global_safety: GlobalSafetyConfig) -> None:
        self._global_state = {
            "emergency_stop": global_safety.emergency_stop,
            "max_simultaneous_zones": global_safety.max_simultaneous_zones,
            "master_pump_timeout_sec": global_safety.master_pump_timeout_sec,
            "total_flow_limit_per_window_ml": global_safety.total_flow_limit_per_window_ml,
            "flow_window_sec": global_safety.flow_window_sec,
            "leak_shutdown_enabled": global_safety.leak_shutdown_enabled,
            "command_ttl_sec": global_safety.command_ttl_sec,
            "device_heartbeat_timeout_sec": global_safety.device_heartbeat_timeout_sec,
        }
        self.set_automation_flag(
            AutomationFlagRecord(
                flag_name="automation_enabled",
                enabled=True,
                updated_at_ms=_now_ms(),
                payload={},
            )
        )
        for zone in zones:
            existing = self._zones.get(zone.zone_id) or {}
            zone_record = TrayZoneRecord(
                zone_id=zone.zone_id,
                device_id=existing.get("device_id"),
                blocked=zone.blocked,
                cooldown_sec=zone.cooldown_sec,
                max_duration_per_run_sec=zone.max_duration_per_run_sec,
                max_runs_per_hour=zone.max_runs_per_hour,
                max_total_water_per_day_ml=zone.max_total_water_per_day_ml,
                settle_delay_ms=zone.settle_delay_ms,
                flow_confirm_timeout_ms=zone.flow_confirm_timeout_ms,
                min_flow_ml_per_min=zone.min_flow_ml_per_min,
                last_watering_at_ms=existing.get("last_watering_at_ms"),
                last_error_at_ms=existing.get("last_error_at_ms"),
                telemetry=existing.get("telemetry", {}),
                device_state=existing.get("device_state", {}),
                reserved_by_execution=existing.get("reserved_by_execution"),
            )
            self._zones[zone.zone_id] = zone_record.model_dump()

    def seen_message(self, message_id: str) -> bool:
        if message_id in self._seen_messages:
            return True
        self._seen_messages.add(message_id)
        return False

    def write_telemetry_snapshot(self, message: TelemetryMessage) -> dict[str, Any]:
        zone = self._zones.setdefault(message.zone_id, TrayZoneRecord(zone_id=message.zone_id).model_dump())
        zone.update({"device_id": message.device_id, "telemetry": dict(message.sensors)})
        device = self._devices.setdefault(message.device_id, DeviceRecord(device_id=message.device_id).model_dump())
        device.update(
            {
                "zone_id": message.zone_id,
                "last_seen_ms": message.ts_ms,
                "last_telemetry_ms": message.ts_ms,
                "telemetry": dict(message.sensors),
            }
        )
        return deepcopy(zone)

    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]:
        zone = self._zones.setdefault(message.zone_id, TrayZoneRecord(zone_id=message.zone_id).model_dump())
        zone.update({"device_id": message.device_id, "device_state": dict(message.state)})
        device = self._devices.setdefault(message.device_id, DeviceRecord(device_id=message.device_id).model_dump())
        device.update(
            {
                "zone_id": message.zone_id,
                "connectivity": message.connectivity.value,
                "last_seen_ms": message.ts_ms,
                "firmware_version": message.firmware_version,
                "state": dict(message.state),
            }
        )
        return deepcopy(device)

    def get_zone_state(self, zone_id: str) -> dict[str, Any]:
        return deepcopy(self._zones.get(zone_id, TrayZoneRecord(zone_id=zone_id).model_dump()))

    def get_device_state(self, device_id: str) -> dict[str, Any]:
        return deepcopy(self._devices.get(device_id, DeviceRecord(device_id=device_id).model_dump()))

    def get_current_state(self) -> dict[str, Any]:
        return {
            "devices": deepcopy(self._devices),
            "zones": deepcopy(self._zones),
            "global": deepcopy(self._global_state),
            "commands": deepcopy(self._commands),
            "executions": deepcopy(self._executions),
            "safety_locks": deepcopy(self._safety_locks),
            "manual_leases": deepcopy(self._manual_leases),
            "automation_flags": deepcopy(self._automation_flags),
            "alarms": self.get_active_alarms(),
        }

    def create_or_get_command(self, command: CommandRecord) -> tuple[dict[str, Any], bool]:
        existing = self._commands.get(command.command_id)
        if existing is not None:
            return deepcopy(existing), False
        record = command.model_dump()
        self._commands[command.command_id] = record
        return deepcopy(record), True

    def update_command(self, command_id: str, **changes: Any) -> dict[str, Any] | None:
        record = self._commands.get(command_id)
        if record is None:
            return None
        metadata_update = dict(changes.pop("metadata_update", {}))
        requested_payload_update = dict(changes.pop("requested_payload_update", {}))
        record.update({key: value.value if hasattr(value, "value") else value for key, value in changes.items() if value is not None})
        if metadata_update:
            record.setdefault("metadata", {}).update(metadata_update)
        if requested_payload_update:
            record.setdefault("requested_payload", {}).update(requested_payload_update)
        return deepcopy(record)

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        record = self._commands.get(command_id)
        return deepcopy(record) if record else None

    def get_command_status(self, command_id: str) -> dict[str, Any] | None:
        record = self.get_command(command_id)
        if record is None:
            return None
        execution_id = record.get("current_execution_id")
        if execution_id:
            record["execution"] = self.get_execution(execution_id)
        return record

    def list_active_commands(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self._commands.values() if record.get("lifecycle") in ACTIVE_LIFECYCLES]

    def create_execution(self, execution: CommandExecutionRecord) -> dict[str, Any]:
        record = execution.model_dump()
        self._executions[execution.execution_id] = record
        return deepcopy(record)

    def update_execution(self, execution_id: str, **changes: Any) -> dict[str, Any] | None:
        record = self._executions.get(execution_id)
        if record is None:
            return None
        result_payload = dict(changes.pop("result_payload_update", {}))
        metadata_update = dict(changes.pop("metadata_update", {}))
        record.update({key: value.value if hasattr(value, "value") else value for key, value in changes.items() if value is not None})
        if result_payload:
            record.setdefault("result_payload", {}).update(result_payload)
        if metadata_update:
            record.setdefault("metadata", {}).update(metadata_update)
        return deepcopy(record)

    def get_execution(self, execution_id: str) -> dict[str, Any] | None:
        record = self._executions.get(execution_id)
        return deepcopy(record) if record else None

    def list_active_executions(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self._executions.values() if record.get("lifecycle") in ACTIVE_LIFECYCLES]

    def list_recoverable_executions(self) -> list[dict[str, Any]]:
        return self.list_active_executions()

    def create_safety_lock(self, lock: SafetyLockRecord) -> dict[str, Any]:
        record = lock.model_dump()
        self._safety_locks[lock.lock_id] = record
        if record["scope"] == "zone" and record["kind"] == "execution_reservation":
            zone = self._zones.setdefault(record["scope_id"], TrayZoneRecord(zone_id=record["scope_id"]).model_dump())
            zone["reserved_by_execution"] = record.get("owner")
        return deepcopy(record)

    def release_safety_lock(self, lock_id: str) -> dict[str, Any] | None:
        record = self._safety_locks.get(lock_id)
        if record is None:
            return None
        record["active"] = False
        if record["scope"] == "zone" and record["kind"] == "execution_reservation":
            zone = self._zones.get(record["scope_id"])
            if zone:
                zone["reserved_by_execution"] = None
        return deepcopy(record)

    def _prune_inactive_items(self) -> None:
        now_ms = _now_ms()
        for lock in self._safety_locks.values():
            expires_at_ms = lock.get("expires_at_ms")
            if lock.get("active") and expires_at_ms and int(expires_at_ms) <= now_ms:
                lock["active"] = False
        for lease in self._manual_leases.values():
            if lease.get("active") and int(lease.get("expires_at_ms") or 0) <= now_ms:
                lease["active"] = False

    def get_active_safety_locks(self, scope: str | None = None, scope_id: str | None = None) -> list[dict[str, Any]]:
        self._prune_inactive_items()
        locks = []
        for lock in self._safety_locks.values():
            if not lock.get("active"):
                continue
            if scope and lock.get("scope") != scope:
                continue
            if scope_id and lock.get("scope_id") != scope_id:
                continue
            locks.append(deepcopy(lock))
        return locks

    def create_manual_lease(self, lease: ManualLeaseRecord) -> dict[str, Any]:
        record = lease.model_dump()
        self._manual_leases[lease.lease_id] = record
        return deepcopy(record)

    def release_manual_lease(self, lease_id: str) -> dict[str, Any] | None:
        record = self._manual_leases.get(lease_id)
        if record is None:
            return None
        record["active"] = False
        return deepcopy(record)

    def get_active_manual_lease(self, zone_id: str) -> dict[str, Any] | None:
        self._prune_inactive_items()
        for lease in self._manual_leases.values():
            if lease.get("zone_id") == zone_id and lease.get("active"):
                return deepcopy(lease)
        return None

    def set_automation_flag(self, flag: AutomationFlagRecord) -> dict[str, Any]:
        record = flag.model_dump()
        self._automation_flags[flag.flag_name] = record
        return deepcopy(record)

    def get_automation_flags(self) -> dict[str, dict[str, Any]]:
        return deepcopy(self._automation_flags)

    def get_active_alarms(self) -> list[dict[str, Any]]:
        active = [alarm for alarm in self._alarms.values() if alarm.get("active")]
        return deepcopy(sorted(active, key=lambda item: item["created_at_ms"], reverse=True))

    def record_alarm(self, alert: AlertEvent) -> dict[str, Any]:
        record = AlarmRecord(
            alarm_id=alert.alert_id,
            trace_id=alert.trace_id,
            device_id=alert.device_id,
            zone_id=alert.zone_id,
            severity=alert.severity,
            category=alert.category,
            message=alert.message,
            active=True,
            created_at_ms=alert.created_at_ms,
            details=alert.details,
        ).model_dump()
        self._alarms[alert.alert_id] = record
        return deepcopy(record)

    def append_audit_log(self, audit: AuditLogRecord) -> dict[str, Any]:
        record = audit.model_dump()
        self._audit_logs.append(record)
        self._audit_logs = self._audit_logs[-5000:]
        return deepcopy(record)

    def note_incident(self, category: str, payload: dict[str, Any]) -> None:
        self.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=str(payload.get("trace_id") or f"trace-{uuid.uuid4().hex[:12]}"),
                action_type="INCIDENT",
                message=category,
                created_at_ms=_now_ms(),
                command_id=payload.get("command_id"),
                execution_id=payload.get("execution_id"),
                device_id=payload.get("device_id"),
                zone_id=payload.get("zone_id"),
                payload=payload,
            )
        )

    def count_active_irrigation_executions(self) -> int:
        total = 0
        for execution in self._executions.values():
            if execution.get("lifecycle") not in ACTIVE_LIFECYCLES:
                continue
            command = self._commands.get(execution["command_id"]) or {}
            if command.get("command_type") == CommandType.IRRIGATE_ZONE.value:
                total += 1
        return total

    def count_completed_zone_runs_since(self, zone_id: str, since_ms: int) -> int:
        total = 0
        for command in self._commands.values():
            if command.get("zone_id") != zone_id:
                continue
            if command.get("command_type") != CommandType.IRRIGATE_ZONE.value:
                continue
            if command.get("lifecycle") != CommandLifecycle.COMPLETED.value:
                continue
            execution = self._executions.get(command.get("current_execution_id") or "")
            completed_at = ((execution or {}).get("result_payload") or {}).get("completed_at_ms")
            if completed_at and int(completed_at) >= since_ms:
                total += 1
        return total

    def sum_zone_volume_since(self, zone_id: str, since_ms: int) -> float:
        total = 0.0
        for command in self._commands.values():
            if command.get("zone_id") != zone_id:
                continue
            if command.get("command_type") != CommandType.IRRIGATE_ZONE.value:
                continue
            if command.get("lifecycle") != CommandLifecycle.COMPLETED.value:
                continue
            execution = self._executions.get(command.get("current_execution_id") or "")
            completed_at = ((execution or {}).get("result_payload") or {}).get("completed_at_ms")
            if completed_at and int(completed_at) >= since_ms:
                total += float(((execution or {}).get("result_payload") or {}).get("delivered_volume_ml") or 0.0)
        return total

    def total_completed_volume_since(self, since_ms: int) -> float:
        total = 0.0
        for execution in self._executions.values():
            if execution.get("lifecycle") != CommandLifecycle.COMPLETED.value:
                continue
            completed_at = (execution.get("result_payload") or {}).get("completed_at_ms")
            if completed_at and int(completed_at) >= since_ms:
                total += float((execution.get("result_payload") or {}).get("delivered_volume_ml") or 0.0)
        return total

    def mark_zone_watering(self, zone_id: str, ts_ms: int) -> None:
        zone = self._zones.setdefault(zone_id, TrayZoneRecord(zone_id=zone_id).model_dump())
        zone["last_watering_at_ms"] = ts_ms

    def mark_zone_error(self, zone_id: str, ts_ms: int) -> None:
        zone = self._zones.setdefault(zone_id, TrayZoneRecord(zone_id=zone_id).model_dump())
        zone["last_error_at_ms"] = ts_ms


class SQLiteStateStore(MemoryStateStore):
    def __init__(self, config: SqliteConfig) -> None:
        super().__init__()
        self._path = Path(config.path)
        self._init_db()
        self._load_from_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS devices (device_id TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS zones (zone_id TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS commands (command_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL, lifecycle TEXT NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS command_executions (execution_id TEXT PRIMARY KEY, command_id TEXT NOT NULL, lifecycle TEXT NOT NULL, phase TEXT NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS safety_locks (lock_id TEXT PRIMARY KEY, scope TEXT NOT NULL, scope_id TEXT NOT NULL, active INTEGER NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS manual_leases (lease_id TEXT PRIMARY KEY, zone_id TEXT NOT NULL, active INTEGER NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS automation_flags (flag_name TEXT PRIMARY KEY, enabled INTEGER NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS alarms (alarm_id TEXT PRIMARY KEY, active INTEGER NOT NULL, payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS audit_logs (audit_id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processed_messages (message_id TEXT PRIMARY KEY);
            """
        )
        conn.commit()
        conn.close()

    def _load_rows(self, table: str, key: str) -> dict[str, dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute(f"SELECT {key}, payload FROM {table}").fetchall()
        conn.close()
        data: dict[str, dict[str, Any]] = {}
        for row in rows:
            data[str(row[key])] = json.loads(row["payload"])
        return data

    def _load_from_db(self) -> None:
        self._devices = self._load_rows("devices", "device_id")
        self._zones = self._load_rows("zones", "zone_id")
        self._commands = self._load_rows("commands", "command_id")
        self._executions = self._load_rows("command_executions", "execution_id")
        self._safety_locks = self._load_rows("safety_locks", "lock_id")
        self._manual_leases = self._load_rows("manual_leases", "lease_id")
        self._automation_flags = self._load_rows("automation_flags", "flag_name")
        self._alarms = self._load_rows("alarms", "alarm_id")
        conn = self._conn()
        self._seen_messages = {str(row["message_id"]) for row in conn.execute("SELECT message_id FROM processed_messages").fetchall()}
        self._audit_logs = [json.loads(row["payload"]) for row in conn.execute("SELECT payload FROM audit_logs ORDER BY created_at_ms ASC").fetchall()]
        conn.close()

    def _upsert_payload(self, table: str, key_field: str, key_value: str, payload: dict[str, Any], updated_at_ms: int, extra: dict[str, Any] | None = None) -> None:
        columns = [key_field]
        placeholders = ["?"]
        values: list[Any] = [key_value]
        for column, value in (extra or {}).items():
            columns.append(column)
            placeholders.append("?")
            values.append(value)
        columns.extend(["payload", "updated_at_ms"])
        placeholders.extend(["?", "?"])
        values.extend([json.dumps(payload, ensure_ascii=False), updated_at_ms])
        update_columns = ",".join(f"{column}=excluded.{column}" for column in columns[1:])
        conn = self._conn()
        conn.execute(
            f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join(placeholders)}) ON CONFLICT({key_field}) DO UPDATE SET {update_columns}",
            tuple(values),
        )
        conn.commit()
        conn.close()

    def initialize(self, zones: list[ZoneSafetyConfig], global_safety: GlobalSafetyConfig) -> None:
        super().initialize(zones, global_safety)
        now_ms = _now_ms()
        for device_id, payload in self._devices.items():
            self._upsert_payload("devices", "device_id", device_id, payload, int(payload.get("last_seen_ms") or now_ms))
        for zone_id, payload in self._zones.items():
            self._upsert_payload("zones", "zone_id", zone_id, payload, int(payload.get("last_watering_at_ms") or payload.get("last_error_at_ms") or now_ms))
        for flag_name, payload in self._automation_flags.items():
            self._upsert_payload("automation_flags", "flag_name", flag_name, payload, int(payload.get("updated_at_ms") or now_ms), {"enabled": 1 if payload.get("enabled") else 0})

    def seen_message(self, message_id: str) -> bool:
        if super().seen_message(message_id):
            return True
        conn = self._conn()
        row = conn.execute("SELECT 1 FROM processed_messages WHERE message_id=?", (message_id,)).fetchone()
        if row:
            conn.close()
            return True
        conn.execute("INSERT INTO processed_messages(message_id) VALUES(?)", (message_id,))
        conn.commit()
        conn.close()
        return False

    def write_telemetry_snapshot(self, message: TelemetryMessage) -> dict[str, Any]:
        zone = super().write_telemetry_snapshot(message)
        self._upsert_payload("zones", "zone_id", message.zone_id, zone, message.ts_ms)
        self._upsert_payload("devices", "device_id", message.device_id, self._devices[message.device_id], message.ts_ms)
        return zone

    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]:
        device = super().write_device_state(message)
        self._upsert_payload("devices", "device_id", message.device_id, device, message.ts_ms)
        self._upsert_payload("zones", "zone_id", message.zone_id, self._zones[message.zone_id], message.ts_ms)
        return device

    def create_or_get_command(self, command: CommandRecord) -> tuple[dict[str, Any], bool]:
        record, created = super().create_or_get_command(command)
        self._upsert_payload("commands", "command_id", command.command_id, record, int(record.get("requested_at_ms") or _now_ms()), {"idempotency_key": record["idempotency_key"], "lifecycle": record["lifecycle"]})
        return record, created

    def update_command(self, command_id: str, **changes: Any) -> dict[str, Any] | None:
        record = super().update_command(command_id, **changes)
        if record is None:
            return None
        self._upsert_payload("commands", "command_id", command_id, record, _now_ms(), {"idempotency_key": record["idempotency_key"], "lifecycle": record["lifecycle"]})
        return record

    def create_execution(self, execution: CommandExecutionRecord) -> dict[str, Any]:
        record = super().create_execution(execution)
        self._upsert_payload("command_executions", "execution_id", execution.execution_id, record, execution.updated_at_ms, {"command_id": record["command_id"], "lifecycle": record["lifecycle"], "phase": record["phase"]})
        return record

    def update_execution(self, execution_id: str, **changes: Any) -> dict[str, Any] | None:
        record = super().update_execution(execution_id, **changes)
        if record is None:
            return None
        self._upsert_payload("command_executions", "execution_id", execution_id, record, int(record.get("updated_at_ms") or _now_ms()), {"command_id": record["command_id"], "lifecycle": record["lifecycle"], "phase": record["phase"]})
        return record

    def create_safety_lock(self, lock: SafetyLockRecord) -> dict[str, Any]:
        record = super().create_safety_lock(lock)
        self._upsert_payload("safety_locks", "lock_id", lock.lock_id, record, int(record.get("created_at_ms") or _now_ms()), {"scope": record["scope"], "scope_id": record["scope_id"], "active": 1 if record["active"] else 0})
        if record["scope"] == "zone":
            self._upsert_payload("zones", "zone_id", record["scope_id"], self._zones[record["scope_id"]], _now_ms())
        return record

    def release_safety_lock(self, lock_id: str) -> dict[str, Any] | None:
        record = super().release_safety_lock(lock_id)
        if record is None:
            return None
        self._upsert_payload("safety_locks", "lock_id", lock_id, record, _now_ms(), {"scope": record["scope"], "scope_id": record["scope_id"], "active": 0})
        if record["scope"] == "zone":
            self._upsert_payload("zones", "zone_id", record["scope_id"], self._zones[record["scope_id"]], _now_ms())
        return record

    def create_manual_lease(self, lease: ManualLeaseRecord) -> dict[str, Any]:
        record = super().create_manual_lease(lease)
        self._upsert_payload("manual_leases", "lease_id", lease.lease_id, record, lease.created_at_ms, {"zone_id": record["zone_id"], "active": 1 if record["active"] else 0})
        return record

    def release_manual_lease(self, lease_id: str) -> dict[str, Any] | None:
        record = super().release_manual_lease(lease_id)
        if record is None:
            return None
        self._upsert_payload("manual_leases", "lease_id", lease_id, record, _now_ms(), {"zone_id": record["zone_id"], "active": 0})
        return record

    def set_automation_flag(self, flag: AutomationFlagRecord) -> dict[str, Any]:
        record = super().set_automation_flag(flag)
        self._upsert_payload("automation_flags", "flag_name", flag.flag_name, record, flag.updated_at_ms, {"enabled": 1 if flag.enabled else 0})
        return record

    def record_alarm(self, alert: AlertEvent) -> dict[str, Any]:
        record = super().record_alarm(alert)
        self._upsert_payload("alarms", "alarm_id", alert.alert_id, record, alert.created_at_ms, {"active": 1})
        return record

    def append_audit_log(self, audit: AuditLogRecord) -> dict[str, Any]:
        record = super().append_audit_log(audit)
        conn = self._conn()
        conn.execute("INSERT OR REPLACE INTO audit_logs(audit_id,created_at_ms,payload) VALUES(?,?,?)", (record["audit_id"], record["created_at_ms"], json.dumps(record, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return record

    def mark_zone_watering(self, zone_id: str, ts_ms: int) -> None:
        super().mark_zone_watering(zone_id, ts_ms)
        self._upsert_payload("zones", "zone_id", zone_id, self._zones[zone_id], ts_ms)

    def mark_zone_error(self, zone_id: str, ts_ms: int) -> None:
        super().mark_zone_error(zone_id, ts_ms)
        self._upsert_payload("zones", "zone_id", zone_id, self._zones[zone_id], ts_ms)


def build_state_store(config: SqliteConfig, backend: str = "sqlite") -> StateStore:
    if backend == "memory":
        return MemoryStateStore()
    return SQLiteStateStore(config)
