from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Protocol

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from backend.config import InfluxConfig
from shared.contracts.messages import TelemetryMessage


log = logging.getLogger("backend.influx")


class TelemetryHistoryStore(Protocol):
    def write_telemetry(self, message: TelemetryMessage) -> None: ...
    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict[str, Any]]: ...


class MemoryTelemetryHistoryStore:
    def __init__(self) -> None:
        self._history: list[dict[str, Any]] = []

    def write_telemetry(self, message: TelemetryMessage) -> None:
        for sensor, value in message.sensors.items():
            self._history.append(
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
        self._history = self._history[-10000:]

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict[str, Any]]:
        filtered = [
            item
            for item in self._history
            if item["device_id"] == device_id and item["sensor"] == sensor and start_ms <= int(item["ts_ms"]) <= end_ms
        ]
        return deepcopy(filtered[-limit:])


class InfluxTelemetryHistoryStore(MemoryTelemetryHistoryStore):
    def __init__(self, config: InfluxConfig) -> None:
        super().__init__()
        self._config = config
        self._client = InfluxDBClient(url=config.url, token=config.token, org=config.org, timeout=config.timeout_ms)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()

    def write_telemetry(self, message: TelemetryMessage) -> None:
        super().write_telemetry(message)
        try:
            points: list[Point] = []
            for sensor, value in message.sensors.items():
                if isinstance(value, bool):
                    field_value: float | str = float(int(value))
                elif isinstance(value, (int, float)):
                    field_value = float(value)
                else:
                    field_value = str(value)
                points.append(
                    Point("greenhouse_telemetry")
                    .tag("device_id", message.device_id)
                    .tag("zone_id", message.zone_id)
                    .tag("sensor", sensor)
                    .field("value", field_value)
                    .field("trace_id", message.trace_id)
                    .field("message_id", message.message_id)
                    .time(message.ts_ms * 1_000_000)
                )
            if points:
                self._write_api.write(bucket=self._config.bucket, record=points)
        except Exception as exc:  # pragma: no cover
            log.error("influx write failed device_id=%s zone_id=%s error=%s", message.device_id, message.zone_id, exc)

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict[str, Any]]:
        try:
            query = f'''
from(bucket: "{self._config.bucket}")
  |> range(start: time(v: {start_ms}ms), stop: time(v: {end_ms}ms))
  |> filter(fn: (r) => r["_measurement"] == "greenhouse_telemetry")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> filter(fn: (r) => r["sensor"] == "{sensor}")
  |> filter(fn: (r) => r["_field"] == "value")
  |> sort(columns: ["_time"])
  |> limit(n: {limit})
'''
            tables = self._query_api.query(query=query, org=self._config.org)
            rows: list[dict[str, Any]] = []
            for table in tables:
                for record in table.records:
                    rows.append(
                        {
                            "device_id": device_id,
                            "zone_id": record.values.get("zone_id"),
                            "sensor": sensor,
                            "value": record.get_value(),
                            "ts_ms": int(record.get_time().timestamp() * 1000),
                        }
                    )
            if rows:
                return rows
        except Exception as exc:  # pragma: no cover
            log.error("influx query failed device_id=%s sensor=%s error=%s", device_id, sensor, exc)
        return super().get_sensor_history(device_id, sensor, start_ms, end_ms, limit=limit)


def build_telemetry_history_store(config: InfluxConfig, backend: str = "influx") -> TelemetryHistoryStore:
    if backend == "memory" or not config.enabled or not config.token:
        return MemoryTelemetryHistoryStore()
    return InfluxTelemetryHistoryStore(config)
