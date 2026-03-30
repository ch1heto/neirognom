import logging
import os
import threading
import time
from typing import Any, Optional

log = logging.getLogger("influx_writer")

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "neuroagronom")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "farm_telemetry")

RETRY_INTERVAL_SEC  = 120
FLUSH_INTERVAL_SEC  = 10
BATCH_SIZE          = 200

_client       = None
_write_api    = None
_available    = False
_enabled      = False          # False если токен не задан
_lock         = threading.Lock()
_last_warn_ts = 0.0            # подавляем повторные WARNING
_WARN_SUPPRESS_SEC = 300       # одно WARNING раз в 5 минут


def _maybe_warn(msg: str) -> None:
    global _last_warn_ts
    now = time.time()
    if now - _last_warn_ts >= _WARN_SUPPRESS_SEC:
        log.warning(msg)
        _last_warn_ts = now


def _try_connect() -> bool:
    global _client, _write_api, _available
    if not _enabled:
        return False
    try:
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS

        client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            timeout=5_000,
        )
        health = client.health()
        if health.status == "pass":
            _client    = client
            _write_api = client.write_api(write_options=SYNCHRONOUS)
            _available = True
            log.info("InfluxDB connected: %s  org=%s  bucket=%s",
                     INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET)
            return True
        else:
            _maybe_warn(f"InfluxDB health check failed: {health.status}")
            return False
    except Exception as exc:
        _maybe_warn(f"InfluxDB not available: {exc}")
        return False


def _snapshot_to_lines(snapshot: dict[str, Any], tray_id: str, crop: str, stage: str) -> list[str]:
    lines = []
    ts_ns = int(time.time() * 1e9)
    tags  = f"tray={tray_id},crop={crop},stage={stage}"

    for sensor, data in snapshot.items():
        last = data.get("last")
        avg  = data.get("avg")
        if last is None:
            continue
        fields = f"last={last}"
        if avg  is not None: fields += f",avg={avg}"
        if data.get("min") is not None: fields += f",min_val={data['min']}"
        if data.get("max") is not None: fields += f",max_val={data['max']}"
        trend_map = {"rising": 1, "falling": -1, "stable": 0, "unknown": 0}
        trend_n   = trend_map.get(data.get("trend", "unknown"), 0)
        fields   += f",trend_n={trend_n}"
        lines.append(f"telemetry,{tags},sensor={sensor} {fields} {ts_ns}")

    return lines


def write_snapshot(
    snapshot: dict,
    tray_id: str,
    crop: str,
    stage: str,
) -> None:
    from legacy import database as db

    lines = _snapshot_to_lines(snapshot, tray_id, crop, stage)
    if not lines:
        return

    if not _enabled:
        db.buffer_influx_lines(lines)
        return

    if _available and _write_api is not None:
        try:
            _write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=lines)
            log.debug("InfluxDB: wrote %d lines", len(lines))
            return
        except Exception as exc:
            _maybe_warn(f"InfluxDB write failed — buffering: {exc}")

    db.buffer_influx_lines(lines)
    log.debug("Buffered %d lines to SQLite for retry", len(lines))


def write_alert(sensor: str, level: str, value: float, tray_id: str) -> None:
    from legacy import database as db

    if not _enabled:
        return

    ts_ns = int(time.time() * 1e9)
    line  = (
        f"alerts,tray={tray_id},sensor={sensor},level={level}"
        f" value={value} {ts_ns}"
    )

    if _available and _write_api is not None:
        try:
            _write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=[line])
            return
        except Exception as exc:
            _maybe_warn(f"InfluxDB alert write failed — buffering: {exc}")

    db.buffer_influx_lines([line])


class RetryWorker(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True, name="InfluxRetry")
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        from legacy import database as db

        if not _enabled:
            log.debug("InfluxDB disabled — retry worker idle")
            self._stop_event.wait()
            return

        log.info("InfluxDB retry worker started (interval=%ds)", RETRY_INTERVAL_SEC)

        while not self._stop_event.is_set():
            self._stop_event.wait(RETRY_INTERVAL_SEC)

            if not _available:
                with _lock:
                    reconnected = _try_connect()
                if not reconnected:
                    continue

            buffered = db.get_buffered_lines(limit=BATCH_SIZE)
            if not buffered:
                continue

            ids   = [row[0] for row in buffered]
            lines = [row[1] for row in buffered]

            try:
                _write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=lines)
                db.ack_buffered_lines(ids)
                log.info("InfluxDB retry: flushed %d buffered lines", len(lines))
            except Exception as exc:
                db.increment_buffer_retries(ids)
                _maybe_warn(f"InfluxDB retry failed ({len(lines)} lines buffered): {exc}")


_retry_worker: Optional[RetryWorker] = None


def start(tray_id: str = "tray01", crop: str = "lettuce_nft", stage: str = "vegetative") -> None:
    global _retry_worker, _enabled

    if not INFLUX_TOKEN:
        log.warning(
            "INFLUX_TOKEN not set — InfluxDB disabled (telemetry buffered in SQLite). "
            "To enable: set INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET"
        )
        _enabled = False
    else:
        _enabled = True
        _try_connect()

    _retry_worker = RetryWorker()
    _retry_worker.start()


def stop() -> None:
    if _retry_worker:
        _retry_worker.stop()
    if _client:
        try:
            _client.close()
        except Exception:
            pass
    log.info("InfluxDB writer stopped")
