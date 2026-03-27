from __future__ import annotations

import json
import logging
import sqlite3
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from backend.config import DatabaseConfig, GlobalSafetyConfig, ZoneSafetyConfig
from shared.contracts.messages import (
    AlertEvent,
    CommandLifecycle,
    CommandRequest,
    DeviceStateMessage,
    TelemetryMessage,
)

try:  # pragma: no cover - optional runtime dependency
    import psycopg
except ImportError:  # pragma: no cover - optional runtime dependency
    psycopg = None


log = logging.getLogger("backend.state")


class StateStore(Protocol):
    def initialize(self, zones: list[ZoneSafetyConfig], global_safety: GlobalSafetyConfig) -> None: ...
    def seen_message(self, message_id: str) -> bool: ...
    def write_telemetry(self, message: TelemetryMessage) -> dict[str, Any]: ...
    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]: ...
    def get_zone_state(self, zone_id: str) -> dict[str, Any]: ...
    def get_current_state(self) -> dict[str, Any]: ...
    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict[str, Any]]: ...
    def create_command(self, command: CommandRequest, lifecycle: CommandLifecycle) -> dict[str, Any]: ...
    def transition_command(self, command_id: str, lifecycle: CommandLifecycle, payload: dict[str, Any] | None = None) -> dict[str, Any] | None: ...
    def get_command_status(self, command_id: str) -> dict[str, Any] | None: ...
    def get_active_alerts(self) -> list[dict[str, Any]]: ...
    def record_alert(self, alert: AlertEvent) -> None: ...
    def note_incident(self, category: str, payload: dict[str, Any]) -> None: ...
    def count_active_zone_commands(self) -> int: ...


class MemoryStateStore:
    def __init__(self) -> None:
        self._seen_messages: set[str] = set()
        self._devices: dict[str, dict[str, Any]] = {}
        self._zones: dict[str, dict[str, Any]] = {}
        self._global_state: dict[str, Any] = {}
        self._telemetry_history: list[dict[str, Any]] = []
        self._commands: dict[str, dict[str, Any]] = {}
        self._alerts: dict[str, dict[str, Any]] = {}
        self._incidents: list[dict[str, Any]] = []

    def initialize(self, zones: list[ZoneSafetyConfig], global_safety: GlobalSafetyConfig) -> None:
        self._global_state = {
            "emergency_stop": global_safety.emergency_stop,
            "max_simultaneous_zones": global_safety.max_simultaneous_zones,
            "master_pump_timeout_sec": global_safety.master_pump_timeout_sec,
            "total_flow_limit_per_window_ml": global_safety.total_flow_limit_per_window_ml,
            "leak_shutdown_enabled": global_safety.leak_shutdown_enabled,
        }
        for zone in zones:
            self._zones.setdefault(
                zone.zone_id,
                {
                    "zone_id": zone.zone_id,
                    "blocked": zone.blocked,
                    "cooldown_sec": zone.cooldown_sec,
                    "max_duration_per_run_sec": zone.max_duration_per_run_sec,
                    "max_runs_per_hour": zone.max_runs_per_hour,
                    "max_total_water_per_day_ml": zone.max_total_water_per_day_ml,
                    "last_watering_at_ms": None,
                    "last_error_at_ms": None,
                    "telemetry": {},
                    "device_id": None,
                    "connectivity": "offline",
                },
            )

    def seen_message(self, message_id: str) -> bool:
        if message_id in self._seen_messages:
            return True
        self._seen_messages.add(message_id)
        return False

    def write_telemetry(self, message: TelemetryMessage) -> dict[str, Any]:
        zone = self._zones.setdefault(message.zone_id, {"zone_id": message.zone_id})
        zone.update(
            {
                "device_id": message.device_id,
                "last_telemetry_ts_ms": message.ts_ms,
                "telemetry": dict(message.sensors),
            }
        )
        device = self._devices.setdefault(message.device_id, {"device_id": message.device_id})
        device.update(
            {
                "zone_id": message.zone_id,
                "last_telemetry_ts_ms": message.ts_ms,
                "telemetry": dict(message.sensors),
            }
        )
        for sensor, value in message.sensors.items():
            self._telemetry_history.append(
                {
                    "device_id": message.device_id,
                    "zone_id": message.zone_id,
                    "sensor": sensor,
                    "value": value,
                    "ts_ms": message.ts_ms,
                    "trace_id": message.trace_id,
                    "message_id": message.message_id,
                }
            )
        return deepcopy(zone)

    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]:
        zone = self._zones.setdefault(message.zone_id, {"zone_id": message.zone_id})
        zone.update(
            {
                "device_id": message.device_id,
                "connectivity": message.connectivity.value,
                "last_state_ts_ms": message.ts_ms,
                "device_state": dict(message.state),
            }
        )
        device = self._devices.setdefault(message.device_id, {"device_id": message.device_id})
        device.update(
            {
                "zone_id": message.zone_id,
                "connectivity": message.connectivity.value,
                "last_state_ts_ms": message.ts_ms,
                "state": dict(message.state),
                "firmware_version": message.firmware_version,
            }
        )
        return deepcopy(device)

    def get_zone_state(self, zone_id: str) -> dict[str, Any]:
        return deepcopy(self._zones.get(zone_id, {"zone_id": zone_id}))

    def get_current_state(self) -> dict[str, Any]:
        return {
            "devices": deepcopy(self._devices),
            "zones": deepcopy(self._zones),
            "global": deepcopy(self._global_state),
            "commands": deepcopy(self._commands),
            "alerts": self.get_active_alerts(),
        }

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict[str, Any]]:
        filtered = [
            item
            for item in self._telemetry_history
            if item["device_id"] == device_id and item["sensor"] == sensor and start_ms <= int(item["ts_ms"]) <= end_ms
        ]
        return deepcopy(filtered[-limit:])

    def create_command(self, command: CommandRequest, lifecycle: CommandLifecycle) -> dict[str, Any]:
        record = command.model_dump() | {"lifecycle": lifecycle.value}
        self._commands[command.command_id] = record
        return deepcopy(record)

    def transition_command(
        self,
        command_id: str,
        lifecycle: CommandLifecycle,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        record = self._commands.get(command_id)
        if record is None:
            return None
        record["lifecycle"] = lifecycle.value
        if payload:
            record.setdefault("updates", []).append(deepcopy(payload))
            if lifecycle == CommandLifecycle.COMPLETED:
                zone = self._zones.get(record["zone_id"])
                if zone and record["action"].upper() in {"ON", "OPEN"}:
                    zone["last_watering_at_ms"] = payload.get("ts_ms")
            if lifecycle in {CommandLifecycle.FAILED, CommandLifecycle.EXPIRED, CommandLifecycle.REJECTED_BY_SAFETY}:
                zone = self._zones.get(record["zone_id"])
                if zone:
                    zone["last_error_at_ms"] = payload.get("ts_ms")
        return deepcopy(record)

    def get_command_status(self, command_id: str) -> dict[str, Any] | None:
        record = self._commands.get(command_id)
        return deepcopy(record) if record else None

    def get_active_alerts(self) -> list[dict[str, Any]]:
        return deepcopy(sorted(self._alerts.values(), key=lambda item: item["created_at_ms"], reverse=True))

    def record_alert(self, alert: AlertEvent) -> None:
        self._alerts[alert.alert_id] = alert.model_dump()

    def note_incident(self, category: str, payload: dict[str, Any]) -> None:
        self._incidents.append({"category": category, "payload": deepcopy(payload)})

    def count_active_zone_commands(self) -> int:
        active_states = {
            CommandLifecycle.SENT.value,
            CommandLifecycle.ACKED.value,
            CommandLifecycle.RUNNING.value,
        }
        return sum(1 for command in self._commands.values() if command.get("lifecycle") in active_states)


class SQLiteCompatibilityStateStore(MemoryStateStore):
    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = Path(path)
        self._init_db()
        log.warning("Using deprecated sqlite compatibility store at %s; PostgreSQL should be used in production", self._path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS current_state (
                scope TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(scope, subject_id)
            );
            CREATE TABLE IF NOT EXISTS telemetry_history (
                message_id TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                sensor TEXT NOT NULL,
                value TEXT NOT NULL,
                ts_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS actuator_commands (
                command_id TEXT PRIMARY KEY,
                zone_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                payload TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                zone_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            """
        )
        conn.commit()
        conn.close()

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

    def write_telemetry(self, message: TelemetryMessage) -> dict[str, Any]:
        zone = super().write_telemetry(message)
        conn = self._conn()
        for sensor, value in message.sensors.items():
            conn.execute(
                "INSERT INTO telemetry_history(message_id,trace_id,device_id,zone_id,sensor,value,ts_ms) VALUES(?,?,?,?,?,?,?)",
                (message.message_id, message.trace_id, message.device_id, message.zone_id, sensor, json.dumps(value), message.ts_ms),
            )
        conn.execute(
            "INSERT OR REPLACE INTO current_state(scope,subject_id,payload,updated_at_ms) VALUES(?,?,?,?)",
            ("zone", message.zone_id, json.dumps(zone, ensure_ascii=False), message.ts_ms),
        )
        conn.commit()
        conn.close()
        return zone

    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]:
        device = super().write_device_state(message)
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO current_state(scope,subject_id,payload,updated_at_ms) VALUES(?,?,?,?)",
            ("device", message.device_id, json.dumps(device, ensure_ascii=False), message.ts_ms),
        )
        conn.commit()
        conn.close()
        return device

    def create_command(self, command: CommandRequest, lifecycle: CommandLifecycle) -> dict[str, Any]:
        record = super().create_command(command, lifecycle)
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO actuator_commands(command_id,zone_id,device_id,lifecycle,payload,updated_at_ms) VALUES(?,?,?,?,?,?)",
            (command.command_id, command.zone_id, command.device_id, lifecycle.value, json.dumps(record, ensure_ascii=False), command.created_at_ms),
        )
        conn.commit()
        conn.close()
        return record

    def transition_command(self, command_id: str, lifecycle: CommandLifecycle, payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        record = super().transition_command(command_id, lifecycle, payload)
        if record is None:
            return None
        conn = self._conn()
        updated_at_ms = int((payload or {}).get("ts_ms") or record.get("created_at_ms") or 0)
        conn.execute(
            "INSERT OR REPLACE INTO actuator_commands(command_id,zone_id,device_id,lifecycle,payload,updated_at_ms) VALUES(?,?,?,?,?,?)",
            (command_id, record["zone_id"], record["device_id"], lifecycle.value, json.dumps(record, ensure_ascii=False), updated_at_ms),
        )
        conn.commit()
        conn.close()
        return record

    def record_alert(self, alert: AlertEvent) -> None:
        super().record_alert(alert)
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO alerts(alert_id,zone_id,device_id,severity,payload,created_at_ms) VALUES(?,?,?,?,?,?)",
            (alert.alert_id, alert.zone_id, alert.device_id, alert.severity, json.dumps(alert.model_dump(), ensure_ascii=False), alert.created_at_ms),
        )
        conn.commit()
        conn.close()

    def note_incident(self, category: str, payload: dict[str, Any]) -> None:
        super().note_incident(category, payload)
        conn = self._conn()
        conn.execute(
            "INSERT INTO incidents(category,payload) VALUES(?,?)",
            (category, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()


class PostgresStateStore(MemoryStateStore):
    def __init__(self, dsn: str) -> None:
        super().__init__()
        if psycopg is None:
            raise RuntimeError("psycopg is required for PostgreSQL state store")
        self._dsn = dsn
        self._init_db()

    def _connect(self):
        return psycopg.connect(self._dsn)

    def _init_db(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS devices (
                        device_id TEXT PRIMARY KEY,
                        zone_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        updated_at_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS zones (
                        zone_id TEXT PRIMARY KEY,
                        payload JSONB NOT NULL,
                        updated_at_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS current_state (
                        scope TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        updated_at_ms BIGINT NOT NULL,
                        PRIMARY KEY(scope, subject_id)
                    );
                    CREATE TABLE IF NOT EXISTS telemetry_history (
                        message_id TEXT NOT NULL,
                        trace_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        zone_id TEXT NOT NULL,
                        sensor TEXT NOT NULL,
                        value JSONB NOT NULL,
                        ts_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS actuator_commands (
                        command_id TEXT PRIMARY KEY,
                        zone_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        lifecycle TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        updated_at_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS command_results (
                        id BIGSERIAL PRIMARY KEY,
                        command_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        ts_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS alerts (
                        alert_id TEXT PRIMARY KEY,
                        zone_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at_ms BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS incidents (
                        id BIGSERIAL PRIMARY KEY,
                        category TEXT NOT NULL,
                        payload JSONB NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS plant_profiles (
                        profile_id TEXT PRIMARY KEY,
                        payload JSONB NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS safety_limits (
                        scope TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        PRIMARY KEY(scope, subject_id)
                    );
                    CREATE TABLE IF NOT EXISTS processed_messages (
                        message_id TEXT PRIMARY KEY
                    );
                    """
                )
            conn.commit()

    def seen_message(self, message_id: str) -> bool:
        if super().seen_message(message_id):
            return True
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM processed_messages WHERE message_id=%s", (message_id,))
                if cur.fetchone():
                    return True
                cur.execute("INSERT INTO processed_messages(message_id) VALUES(%s)", (message_id,))
            conn.commit()
        return False

    def write_telemetry(self, message: TelemetryMessage) -> dict[str, Any]:
        zone = super().write_telemetry(message)
        with self._connect() as conn:
            with conn.cursor() as cur:
                for sensor, value in message.sensors.items():
                    cur.execute(
                        "INSERT INTO telemetry_history(message_id,trace_id,device_id,zone_id,sensor,value,ts_ms) VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s)",
                        (message.message_id, message.trace_id, message.device_id, message.zone_id, sensor, json.dumps(value), message.ts_ms),
                    )
                cur.execute(
                    "INSERT INTO zones(zone_id,payload,updated_at_ms) VALUES(%s,%s::jsonb,%s) ON CONFLICT(zone_id) DO UPDATE SET payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    (message.zone_id, json.dumps(zone, ensure_ascii=False), message.ts_ms),
                )
                cur.execute(
                    "INSERT INTO current_state(scope,subject_id,payload,updated_at_ms) VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT(scope,subject_id) DO UPDATE SET payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    ("zone", message.zone_id, json.dumps(zone, ensure_ascii=False), message.ts_ms),
                )
            conn.commit()
        return zone

    def write_device_state(self, message: DeviceStateMessage) -> dict[str, Any]:
        device = super().write_device_state(message)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO devices(device_id,zone_id,payload,updated_at_ms) VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT(device_id) DO UPDATE SET zone_id=EXCLUDED.zone_id, payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    (message.device_id, message.zone_id, json.dumps(device, ensure_ascii=False), message.ts_ms),
                )
                cur.execute(
                    "INSERT INTO current_state(scope,subject_id,payload,updated_at_ms) VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT(scope,subject_id) DO UPDATE SET payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    ("device", message.device_id, json.dumps(device, ensure_ascii=False), message.ts_ms),
                )
            conn.commit()
        return device

    def create_command(self, command: CommandRequest, lifecycle: CommandLifecycle) -> dict[str, Any]:
        record = super().create_command(command, lifecycle)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO actuator_commands(command_id,zone_id,device_id,lifecycle,payload,updated_at_ms) VALUES(%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT(command_id) DO UPDATE SET lifecycle=EXCLUDED.lifecycle, payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    (command.command_id, command.zone_id, command.device_id, lifecycle.value, json.dumps(record, ensure_ascii=False), command.created_at_ms),
                )
            conn.commit()
        return record

    def transition_command(self, command_id: str, lifecycle: CommandLifecycle, payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        record = super().transition_command(command_id, lifecycle, payload)
        if record is None:
            return None
        updated_at_ms = int((payload or {}).get("ts_ms") or record.get("created_at_ms") or 0)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO actuator_commands(command_id,zone_id,device_id,lifecycle,payload,updated_at_ms) VALUES(%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT(command_id) DO UPDATE SET lifecycle=EXCLUDED.lifecycle, payload=EXCLUDED.payload, updated_at_ms=EXCLUDED.updated_at_ms",
                    (command_id, record["zone_id"], record["device_id"], lifecycle.value, json.dumps(record, ensure_ascii=False), updated_at_ms),
                )
                if payload:
                    cur.execute(
                        "INSERT INTO command_results(command_id,payload,ts_ms) VALUES(%s,%s::jsonb,%s)",
                        (command_id, json.dumps(payload, ensure_ascii=False), updated_at_ms),
                    )
            conn.commit()
        return record

    def record_alert(self, alert: AlertEvent) -> None:
        super().record_alert(alert)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO alerts(alert_id,zone_id,device_id,severity,payload,created_at_ms) VALUES(%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT(alert_id) DO UPDATE SET severity=EXCLUDED.severity, payload=EXCLUDED.payload, created_at_ms=EXCLUDED.created_at_ms",
                    (alert.alert_id, alert.zone_id, alert.device_id, alert.severity, json.dumps(alert.model_dump(), ensure_ascii=False), alert.created_at_ms),
                )
            conn.commit()

    def note_incident(self, category: str, payload: dict[str, Any]) -> None:
        super().note_incident(category, payload)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO incidents(category,payload) VALUES(%s,%s::jsonb)",
                    (category, json.dumps(payload, ensure_ascii=False)),
                )
            conn.commit()


def build_state_store(config: DatabaseConfig) -> StateStore:
    if config.backend == "postgres":
        if not config.postgres_dsn:
            raise RuntimeError("STATE_STORE_BACKEND=postgres requires POSTGRES_DSN")
        return PostgresStateStore(config.postgres_dsn)
    if config.backend == "memory":
        return MemoryStateStore()
    return SQLiteCompatibilityStateStore(config.sqlite_path)
