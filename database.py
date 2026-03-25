import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("database")

DB_PATH            = Path("neuroagronom.db")
DB_MAX_SIZE_MB     = 500
DB_RETENTION_DAYS  = 90

_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-16000")
    return conn


def init_db() -> None:
    with _lock:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts       INTEGER NOT NULL,
                level    TEXT    NOT NULL,
                sensor   TEXT    NOT NULL,
                value    REAL,
                trend    TEXT,
                reason   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_alert_ts ON alerts(ts);

            CREATE TABLE IF NOT EXISTS llm_requests (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           INTEGER NOT NULL,
                request_type TEXT,
                crop         TEXT,
                stage        TEXT,
                growth_day   INTEGER,
                prompt_len   INTEGER,
                response_raw TEXT,
                analysis     TEXT,
                status       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_llm_ts ON llm_requests(ts);

            CREATE TABLE IF NOT EXISTS commands (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           INTEGER NOT NULL,
                llm_req_id   INTEGER REFERENCES llm_requests(id),
                actuator     TEXT    NOT NULL,
                action       TEXT    NOT NULL,
                duration_sec INTEGER,
                reason       TEXT,
                mqtt_topic   TEXT,
                exec_status  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_cmd_ts ON commands(ts);

            CREATE TABLE IF NOT EXISTS actuator_state (
                actuator   TEXT    PRIMARY KEY,
                state      TEXT    NOT NULL DEFAULT 'OFF',
                changed_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS control_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           INTEGER NOT NULL,
                event_type   TEXT    NOT NULL,
                source       TEXT    NOT NULL,
                metric       TEXT,
                status       TEXT,
                details      TEXT,
                context_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_control_events_ts ON control_events(ts);

            CREATE TABLE IF NOT EXISTS influx_buffer (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts       INTEGER NOT NULL,
                line     TEXT    NOT NULL,
                retries  INTEGER NOT NULL DEFAULT 0
            );
        """)
        conn.commit()
        conn.close()
    _maintenance()


def _maintenance() -> None:
    cutoff = int(time.time()) - DB_RETENTION_DAYS * 86400
    with _lock:
        conn = get_conn()
        for tbl in ("alerts", "llm_requests", "commands", "control_events"):
            conn.execute(f"DELETE FROM {tbl} WHERE ts < ?", (cutoff,))
        conn.execute("DELETE FROM influx_buffer WHERE retries >= 10")
        conn.commit()
        conn.close()

    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        if size_mb > DB_MAX_SIZE_MB:
            log.warning("DB %.1f MB > limit %d MB — VACUUM", size_mb, DB_MAX_SIZE_MB)
            with _lock:
                conn = get_conn()
                conn.execute("VACUUM")
                conn.close()


def save_alert(level: str, sensor: str, value: float, trend: str, reason: str) -> None:
    with _lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO alerts(ts,level,sensor,value,trend,reason) VALUES(?,?,?,?,?,?)",
            (int(time.time()), level, sensor, value, trend, reason),
        )
        conn.commit()
        conn.close()


def save_llm_request(
    request_type: str, crop: str, stage: str, growth_day: int,
    prompt_len: int, response_raw: str, analysis: str, status: str,
) -> int:
    with _lock:
        conn = get_conn()
        cur = conn.execute(
            "INSERT INTO llm_requests"
            "(ts,request_type,crop,stage,growth_day,prompt_len,response_raw,analysis,status)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (int(time.time()), request_type, crop, stage, growth_day,
             prompt_len, response_raw[:4000], analysis[:500], status),
        )
        row_id = cur.lastrowid
        conn.commit()
        conn.close()
        return row_id


def save_command(
    llm_req_id: Optional[int], actuator: str, action: str,
    duration_sec: Optional[int], reason: str, mqtt_topic: str, exec_status: str,
) -> None:
    with _lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO commands"
            "(ts,llm_req_id,actuator,action,duration_sec,reason,mqtt_topic,exec_status)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (int(time.time()), llm_req_id, actuator, action,
             duration_sec, reason[:200], mqtt_topic, exec_status),
        )
        conn.commit()
        conn.close()


def save_control_event(
    event_type: str,
    source: str,
    metric: Optional[str] = None,
    status: Optional[str] = None,
    details: str = "",
    context_json: str = "",
) -> None:
    with _lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO control_events(ts,event_type,source,metric,status,details,context_json)"
            " VALUES(?,?,?,?,?,?,?)",
            (
                int(time.time()),
                event_type[:80],
                source[:80],
                metric[:40] if metric else None,
                status[:40] if status else None,
                details[:500],
                context_json[:4000],
            ),
        )
        conn.commit()
        conn.close()


def get_recent_control_events(limit: int = 50) -> list[dict]:
    with _lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT ts,event_type,source,metric,status,details,context_json"
            " FROM control_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def get_current_escalation_state(metrics: Optional[list[str]] = None) -> dict[str, dict]:
    tracked_metrics = [str(metric).strip().lower() for metric in (metrics or ["ph", "ec"])]
    state = {
        metric: {
            "metric": metric,
            "active": False,
            "state": "clear",
            "since": None,
            "reason": "",
            "last_reset_ts": None,
            "last_event_ts": None,
        }
        for metric in tracked_metrics
    }

    placeholders = ",".join("?" for _ in tracked_metrics)
    params = [*tracked_metrics, "all"]
    query = (
        "SELECT ts,event_type,metric,status,details"
        " FROM control_events"
        " WHERE source='control_runtime'"
        " AND event_type IN ('escalation','operator_reset')"
        f" AND metric IN ({placeholders}, ?)"
        " ORDER BY id ASC"
    )

    with _lock:
        conn = get_conn()
        rows = conn.execute(query, params).fetchall()
        conn.close()

    for row in rows:
        ts = row["ts"]
        event_type = (row["event_type"] or "").strip().lower()
        metric = (row["metric"] or "").strip().lower()
        status = (row["status"] or "").strip().lower()
        details = row["details"] or ""

        if event_type == "escalation" and metric in state and status == "operator_required":
            state[metric].update(
                {
                    "active": True,
                    "state": "operator_required",
                    "since": ts,
                    "reason": details,
                    "last_event_ts": ts,
                }
            )
            continue

        if event_type != "operator_reset" or status != "applied":
            continue

        if metric == "all":
            for item in state.values():
                item.update(
                    {
                        "active": False,
                        "state": "clear",
                        "since": None,
                        "reason": "",
                        "last_reset_ts": ts,
                        "last_event_ts": ts,
                    }
                )
            continue

        if metric in state:
            state[metric].update(
                {
                    "active": False,
                    "state": "clear",
                    "since": None,
                    "reason": "",
                    "last_reset_ts": ts,
                    "last_event_ts": ts,
                }
            )

    return state


def update_actuator_state(actuator: str, state: str) -> None:
    with _lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO actuator_state(actuator,state,changed_at) VALUES(?,?,?)"
            " ON CONFLICT(actuator) DO UPDATE"
            " SET state=excluded.state, changed_at=excluded.changed_at",
            (actuator, state, int(time.time())),
        )
        conn.commit()
        conn.close()


def get_actuator_states() -> dict:
    with _lock:
        conn = get_conn()
        rows = conn.execute("SELECT actuator, state FROM actuator_state").fetchall()
        conn.close()
        return {r["actuator"]: r["state"] for r in rows}


def get_recent_alerts(hours: int = 6) -> list:
    since = int(time.time()) - hours * 3600
    with _lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT ts,level,sensor,value,trend,reason FROM alerts"
            " WHERE ts>=? ORDER BY ts DESC",
            (since,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def buffer_influx_lines(lines: list[str]) -> None:
    if not lines:
        return
    ts = int(time.time())
    with _lock:
        conn = get_conn()
        conn.executemany(
            "INSERT INTO influx_buffer(ts,line) VALUES(?,?)",
            [(ts, ln) for ln in lines],
        )
        conn.commit()
        conn.close()


def get_buffered_lines(limit: int = 500) -> list[tuple[int, str]]:
    with _lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT id, line FROM influx_buffer ORDER BY id LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [(r["id"], r["line"]) for r in rows]


def ack_buffered_lines(ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _lock:
        conn = get_conn()
        conn.execute(f"DELETE FROM influx_buffer WHERE id IN ({placeholders})", ids)
        conn.commit()
        conn.close()


def increment_buffer_retries(ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _lock:
        conn = get_conn()
        conn.execute(
            f"UPDATE influx_buffer SET retries=retries+1 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        conn.close()
