from __future__ import annotations

import re
import time
import uuid
from typing import Any

from backend.config import BackendConfig
from backend.domain.models import AuditLogRecord, SafetyLockRecord
from backend.state.influx import TelemetryHistoryStore
from backend.state.store import StateStore
from shared.contracts.messages import AlertEvent, TelemetryMessage


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", value).strip("-").lower() or "unknown"


class SecurityMonitor:
    def __init__(self, config: BackendConfig, state_store: StateStore, telemetry_history: TelemetryHistoryStore) -> None:
        self._config = config
        self._state_store = state_store
        self._telemetry_history = telemetry_history

    def process_telemetry(self, message: TelemetryMessage) -> None:
        self.device_seen(message.device_id, message.zone_id, message.trace_id)
        sensors = message.sensors
        if sensors.get("leak") is True:
            self._raise_alarm_and_lock(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                scope="zone",
                scope_id=message.zone_id,
                lock_kind="leak_suspicion",
                severity="emergency",
                category="leak_suspicion",
                message_text="Leak suspicion detected; backend lock applied",
            )
        tank_level = sensors.get("tank_level")
        if isinstance(tank_level, (int, float)) and float(tank_level) <= 0:
            self._raise_alarm_and_lock(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                scope="global",
                scope_id="water_source",
                lock_kind="empty_tank",
                severity="critical",
                category="empty_tank",
                message_text="Tank empty; irrigation commands must fail safe",
            )
        flow_rate = sensors.get("flow_rate_ml_per_min")
        if isinstance(flow_rate, (int, float)) and float(flow_rate) > 0:
            active_execution = any(
                execution["zone_id"] == message.zone_id
                for execution in self._state_store.list_active_executions()
            )
            if not active_execution:
                self._raise_alarm_and_lock(
                    trace_id=message.trace_id,
                    device_id=message.device_id,
                    zone_id=message.zone_id,
                    scope="zone",
                    scope_id=message.zone_id,
                    lock_kind="flow_anomaly",
                    severity="critical",
                    category="flow_anomaly",
                    message_text="Unexpected flow detected without active command",
                )
        pressure = sensors.get("pressure_kpa")
        if isinstance(pressure, (int, float)) and (
            float(pressure) < self._config.global_safety.min_pressure_kpa
            or float(pressure) > self._config.global_safety.max_pressure_kpa
        ):
            self._raise_alarm_and_lock(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                scope="device",
                scope_id=message.device_id,
                lock_kind="pressure_anomaly",
                severity="critical",
                category="pressure_anomaly",
                message_text="Pressure anomaly detected from telemetry",
            )
        self._detect_history_anomalies(message)

    def device_seen(self, device_id: str, zone_id: str | None, trace_id: str) -> None:
        self._release_lock("device", device_id, "stale_heartbeat")
        self._clear_alarm(self._alarm_id("stale_heartbeat", device_id, zone_id))
        self._release_lock("device", device_id, "device_offline")
        self._clear_alarm(self._alarm_id("device_offline", device_id, zone_id))
        self._release_lock("device", device_id, "device_safe_mode")
        self._clear_alarm(self._alarm_id("device_safe_mode", device_id, zone_id))
        self._release_lock("global", "backend-runtime", "mqtt_disconnected")
        self._clear_alarm(self._alarm_id("mqtt_disconnected", "backend", "broker"))
        self._release_lock("global", "backend-runtime", "mqtt_auth_failure")

    def device_offline(self, device_id: str, zone_id: str, trace_id: str, reason: str = "device_offline") -> None:
        category = "device_safe_mode" if reason == "device_safe_mode" else "device_offline"
        message_text = "Device entered safe mode; backend fault path applied" if reason == "device_safe_mode" else "Device reported offline; backend fault path applied"
        self._raise_alarm_and_lock(
            trace_id=trace_id,
            device_id=device_id,
            zone_id=zone_id,
            scope="device",
            scope_id=device_id,
            lock_kind=category,
            severity="critical",
            category=category,
            message_text=message_text,
        )

    def broker_disconnected(self, reason_text: str) -> None:
        now_ms = int(time.time() * 1000)
        self._raise_alarm_and_lock(
            trace_id=f"trace-{uuid.uuid4().hex[:12]}",
            device_id="backend",
            zone_id="broker",
            scope="global",
            scope_id="backend-runtime",
            lock_kind="mqtt_disconnected",
            severity="critical",
            category="mqtt_disconnected",
            message_text=f"MQTT broker disconnected: {reason_text}",
            expires_at_ms=now_ms + self._config.global_safety.broker_reconnect_lock_sec * 1000,
        )

    def broker_connected(self) -> None:
        self._release_lock("global", "backend-runtime", "mqtt_disconnected")
        self._clear_alarm(self._alarm_id("mqtt_disconnected", "backend", "broker"))
        self._release_lock("global", "backend-runtime", "mqtt_auth_failure")
        self._clear_alarm(self._alarm_id("mqtt_auth_failure", "backend", "broker"))

    def auth_failed(self, reason_text: str) -> None:
        self._raise_alarm_and_lock(
            trace_id=f"trace-{uuid.uuid4().hex[:12]}",
            device_id="backend",
            zone_id="broker",
            scope="global",
            scope_id="backend-runtime",
            lock_kind="mqtt_auth_failure",
            severity="critical",
            category="mqtt_auth_failure",
            message_text=f"MQTT authentication failed: {reason_text}",
        )

    def sweep(self, now_ms: int | None = None) -> None:
        now_ms = int(now_ms or time.time() * 1000)
        for device in self._state_store.get_current_state().get("devices", {}).values():
            last_seen_ms = device.get("last_seen_ms")
            if not last_seen_ms:
                continue
            if now_ms - int(last_seen_ms) > self._config.global_safety.device_heartbeat_timeout_sec * 1000:
                device_id = str(device["device_id"])
                zone_id = str(device.get("zone_id") or "unknown")
                self._raise_alarm_and_lock(
                    trace_id=f"trace-{uuid.uuid4().hex[:12]}",
                    device_id=device_id,
                    zone_id=zone_id,
                    scope="device",
                    scope_id=device_id,
                    lock_kind="stale_heartbeat",
                    severity="critical",
                    category="stale_heartbeat",
                    message_text="Heartbeat stale; backend fail-safe lock applied",
                )

    def _detect_history_anomalies(self, message: TelemetryMessage) -> None:
        lookback_ms = self._config.global_safety.anomaly_lookback_sec * 1000
        start_ms = max(0, message.ts_ms - lookback_ms)
        tank_history = self._telemetry_history.get_sensor_history(message.device_id, "tank_level", start_ms, message.ts_ms, limit=20)
        if len(tank_history) >= 2:
            drop = float(tank_history[0]["value"]) - float(tank_history[-1]["value"])
            if drop >= self._config.global_safety.tank_depletion_drop_threshold:
                self._raise_alarm_and_lock(
                    trace_id=message.trace_id,
                    device_id=message.device_id,
                    zone_id=message.zone_id,
                    scope="global",
                    scope_id="water_source",
                    lock_kind="tank_depletion_trend",
                    severity="warning",
                    category="tank_depletion_trend",
                    message_text="Rapid tank depletion trend detected from InfluxDB history",
                )
        self._check_sensor_quality(message, start_ms, "soil_moisture")
        self._check_sensor_quality(message, start_ms, "pressure_kpa")

    def _check_sensor_quality(self, message: TelemetryMessage, start_ms: int, sensor: str) -> None:
        history = self._telemetry_history.get_sensor_history(message.device_id, sensor, start_ms, message.ts_ms, limit=20)
        if len(history) < 5:
            return
        values = [float(item["value"]) for item in history if isinstance(item["value"], (int, float))]
        if len(values) < 5:
            return
        if max(values) == min(values):
            self._raise_alarm_and_lock(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                scope="device",
                scope_id=message.device_id,
                lock_kind=f"sensor_stale_{sensor}",
                severity="warning",
                category=f"sensor_stale_{sensor}",
                message_text=f"Sensor {sensor} appears stale from recent InfluxDB history",
            )
            return
        if max(values) - min(values) >= self._config.global_safety.noisy_sensor_delta_threshold:
            self._raise_alarm_and_lock(
                trace_id=message.trace_id,
                device_id=message.device_id,
                zone_id=message.zone_id,
                scope="device",
                scope_id=message.device_id,
                lock_kind=f"sensor_noisy_{sensor}",
                severity="warning",
                category=f"sensor_noisy_{sensor}",
                message_text=f"Sensor {sensor} appears noisy from recent InfluxDB history",
            )

    def _raise_alarm_and_lock(
        self,
        trace_id: str,
        device_id: str,
        zone_id: str,
        scope: str,
        scope_id: str,
        lock_kind: str,
        severity: str,
        category: str,
        message_text: str,
        expires_at_ms: int | None = None,
    ) -> None:
        alarm_id = self._alarm_id(category, device_id, zone_id)
        self._state_store.record_alarm(
            AlertEvent(
                alert_id=alarm_id,
                trace_id=trace_id,
                device_id=device_id,
                zone_id=zone_id,
                severity=severity,
                category=category,
                message=message_text,
                created_at_ms=int(time.time() * 1000),
                details={"scope": scope, "scope_id": scope_id, "lock_kind": lock_kind},
            )
        )
        lock_id = self._lock_id(scope, scope_id, lock_kind)
        self._state_store.create_safety_lock(
            SafetyLockRecord(
                lock_id=lock_id,
                scope=scope,  # type: ignore[arg-type]
                scope_id=scope_id,
                kind=lock_kind,
                reason=message_text,
                active=True,
                created_at_ms=int(time.time() * 1000),
                expires_at_ms=expires_at_ms,
                owner="security-monitor",
                payload={"alarm_id": alarm_id},
            )
        )
        self._audit(trace_id, "SECURITY_ALARM", message_text, device_id=device_id, zone_id=zone_id, payload={"scope": scope, "scope_id": scope_id, "kind": lock_kind})

    def _release_lock(self, scope: str, scope_id: str, kind: str) -> None:
        lock_id = self._lock_id(scope, scope_id, kind)
        self._state_store.release_safety_lock(lock_id)

    def _clear_alarm(self, alarm_id: str) -> None:
        self._state_store.clear_alarm(alarm_id)

    @staticmethod
    def _lock_id(scope: str, scope_id: str, kind: str) -> str:
        return f"lock-{_slug(scope)}-{_slug(scope_id)}-{_slug(kind)}"[:120]

    @staticmethod
    def _alarm_id(category: str, device_id: str, zone_id: str | None) -> str:
        return f"alarm-{_slug(category)}-{_slug(device_id)}-{_slug(zone_id or 'unknown')}"[:120]

    def _audit(self, trace_id: str, action_type: str, message: str, device_id: str | None = None, zone_id: str | None = None, payload: dict[str, Any] | None = None) -> None:
        self._state_store.append_audit_log(
            AuditLogRecord(
                audit_id=f"audit-{uuid.uuid4().hex[:12]}",
                trace_id=trace_id,
                action_type=action_type,
                message=message,
                created_at_ms=int(time.time() * 1000),
                device_id=device_id,
                zone_id=zone_id,
                payload=payload or {},
            )
        )
