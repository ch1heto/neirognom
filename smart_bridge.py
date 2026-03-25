from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt
import requests

from command_gateway import HybridCommandGateway
from control_config import KnowledgeBaseConfig
from control_runtime import ControlSafetyManager
import database as db
import influx_writer as influx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log       = logging.getLogger("smart_bridge")
audit_log = logging.getLogger("audit")
_ah = logging.FileHandler("audit.log", encoding="utf-8")
_ah.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
audit_log.addHandler(_ah)
audit_log.setLevel(logging.DEBUG)

MQTT_HOST            = "localhost"
MQTT_PORT            = 1883
MQTT_TELEMETRY_TOPIC = "farm/tray01/telemetry/#"
MQTT_RECONNECT_MAX   = 30

OPENCLAW_URL         = os.getenv("OPENCLAW_URL",   "http://localhost:8080/v1/chat/completions")
OPENCLAW_MODEL       = os.getenv("OPENCLAW_MODEL", "llama3.1")
OLLAMA_URL           = os.getenv("OLLAMA_URL",     "http://localhost:11434/api/generate")
OLLAMA_MODEL         = os.getenv("OLLAMA_MODEL",   "llama3.1")
LLM_TIMEOUT_SEC      = 90

SYSTEM_PROMPT_CORE   = "system_prompt_core.txt"
SYSTEM_PROMPT_SCHEMA = "system_prompt_schema.txt"
KNOWLEDGE_BASE_DIR   = Path("knowledge_base/grow_maps")

WINDOW_SIZE          = 60
TREND_WINDOW         = 5
DEAD_MAN_THRESHOLD   = 3
ALERT_COOLDOWN_SEC   = 15 * 60
INFLUX_WRITE_SEC     = 60

CURRENT_CROP         = os.getenv("CROP",         "lettuce_nft")
CURRENT_GROWTH_STAGE = os.getenv("GROWTH_STAGE", "vegetative")
CURRENT_GROWTH_DAY   = int(os.getenv("GROWTH_DAY", "14"))
TRAY_ID              = os.getenv("TRAY_ID",       "tray01")
DRY_RUN              = os.getenv("DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "on"}
OPERATOR_RESET_TOPIC = f"farm/{TRAY_ID}/operator/reset"

KB_CONFIG = KnowledgeBaseConfig()

THRESHOLDS: dict[str, dict[str, tuple[float, float]]] = KB_CONFIG.threshold_levels
ALLOWED_ACTUATORS: dict[str, list[str]] = KB_CONFIG.allowed_actions
ACTUATOR_TOPIC_MAP: dict[str, str] = KB_CONFIG.actuator_topics
MAX_DURATION: dict[str, int] = KB_CONFIG.max_duration_sec
MAX_CONSECUTIVE_DOSES: dict[str, int] = KB_CONFIG.max_consecutive_doses
SAFETY_RULES: dict[str, Any] = KB_CONFIG.safety_rules
SENSOR_SILENT_SEC = int(SAFETY_RULES.get("sensor_silence_sec", 300))
MULTI_WARNING_THRESHOLD = KB_CONFIG.multi_parameter_stress_count

DOSE_RESET_SEC = 3600

CONFLICTING_PAIRS: list[tuple[str, str]] = KB_CONFIG.conflicts


class SensorWindow:
    def __init__(self, maxlen: int = WINDOW_SIZE) -> None:
        self._data: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def push(self, v: float) -> None:
        self._data.append((time.time(), v))

    @property
    def last(self) -> Optional[float]:
        return self._data[-1][1] if self._data else None

    @property
    def previous(self) -> Optional[float]:
        return self._data[-2][1] if len(self._data) >= 2 else None

    @property
    def last_ts(self) -> Optional[float]:
        return self._data[-1][0] if self._data else None

    @property
    def values(self) -> list[float]:
        return [v for _, v in self._data]

    @property
    def avg(self) -> Optional[float]:
        vals = self.values
        return sum(vals) / len(vals) if vals else None

    @property
    def min(self) -> Optional[float]:
        vals = self.values
        return min(vals) if vals else None

    @property
    def max(self) -> Optional[float]:
        vals = self.values
        return max(vals) if vals else None

    @property
    def stdev(self) -> Optional[float]:
        vals = self.values
        if len(vals) < 2: return None
        m = sum(vals) / len(vals)
        return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))

    @property
    def trend(self) -> str:
        vals = self.values[-TREND_WINDOW:]
        if len(vals) < 2: return "unknown"
        d   = vals[-1] - vals[0]
        thr = 0.01 * abs(vals[0]) if vals[0] != 0 else 0.01
        if d > thr:  return "rising"
        if d < -thr: return "falling"
        return "stable"

    def snapshot(self) -> dict[str, Any]:
        return {
            "last":    self.last,
            "prev":    self.previous,
            "avg":     round(self.avg,   3) if self.avg   is not None else None,
            "min":     self.min,
            "max":     self.max,
            "stdev":   round(self.stdev, 4) if self.stdev is not None else None,
            "trend":   self.trend,
            "delta":   round(self.last - self.previous, 4)
                       if self.last is not None and self.previous is not None else None,
            "last_ts": self.last_ts,
        }


class Aggregator:
    SENSORS = ["temp", "humidity", "ph", "ec", "water_level", "soil"]

    def __init__(self) -> None:
        self._windows: dict[str, SensorWindow] = {s: SensorWindow() for s in self.SENSORS}
        self._lock   = threading.Lock()
        self._dirty: set[str] = set()

    def ingest(self, sensor: str, value: float) -> None:
        if sensor not in self._windows: return
        with self._lock:
            self._windows[sensor].push(value)
            self._dirty.add(sensor)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {s: w.snapshot() for s, w in self._windows.items()}

    def has_new_data(self) -> bool:
        with self._lock: return bool(self._dirty)

    def clear_dirty(self) -> None:
        with self._lock: self._dirty.clear()

    def is_silent(self, sensor: str) -> bool:
        with self._lock:
            ts = self._windows[sensor].last_ts
            return ts is None or (time.time() - ts) > SENSOR_SILENT_SEC


class AlertLevel:
    OK       = "OK"
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"


def _check_level(value: float, thresholds: dict) -> str:
    cl, ch = thresholds["critical"]
    wl, wh = thresholds["warning"]
    if value < cl or value > ch: return AlertLevel.CRITICAL
    if value < wl or value > wh: return AlertLevel.WARNING
    return AlertLevel.OK


class AlertEngine:
    def __init__(self, aggregator: Aggregator) -> None:
        self._agg         = aggregator
        self._cooldowns: dict[str, float] = {}
        self._warning_log: list[dict]     = []
        self._pending     = False

    def mark_pending(self) -> None:
        self._pending = True

    def should_evaluate(self) -> bool:
        if self._pending:
            self._pending = False
            return True
        return False

    def _in_cd(self, key: str) -> bool:
        return (time.time() - self._cooldowns.get(key, 0.0)) < ALERT_COOLDOWN_SEC

    def _mark_cd(self, key: str) -> None:
        self._cooldowns[key] = time.time()

    def evaluate(self) -> dict[str, Any]:
        snapshot = self._agg.snapshot()
        critical_params, warning_params, silent_sensors = [], [], []

        for sensor, thresholds in THRESHOLDS.items():
            if sensor not in snapshot:
                continue
            if self._agg.is_silent(sensor):
                silent_sensors.append(sensor)
                continue
            val = snapshot[sensor]["last"]
            if val is None: continue
            level = _check_level(val, thresholds)
            entry = {
                "sensor": sensor, "value": val,
                "trend": snapshot[sensor]["trend"], "level": level,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            if level == AlertLevel.CRITICAL and not self._in_cd(sensor):
                critical_params.append(entry)
                self._mark_cd(sensor)
                log.warning("CRITICAL: %s = %.3f", sensor, val)
                db.save_alert("CRITICAL", sensor, val, entry["trend"], f"value={val}")
                influx.write_alert(sensor, "CRITICAL", val, TRAY_ID)
            elif level == AlertLevel.WARNING and not self._in_cd(f"warn_{sensor}"):
                warning_params.append(entry)
                self._warning_log.append(entry)
                self._mark_cd(f"warn_{sensor}")
                db.save_alert("WARNING", sensor, val, entry["trend"], f"value={val}")

        should_alert = bool(critical_params)
        alert_reason = ""
        if critical_params:
            alert_reason = "CRITICAL: " + ", ".join(f"{e['sensor']}={e['value']}" for e in critical_params)
        elif len(warning_params) >= MULTI_WARNING_THRESHOLD and not self._in_cd("combined"):
            should_alert  = True
            alert_reason  = f"Multi-WARNING: {len(warning_params)} params"
            self._mark_cd("combined")

        if silent_sensors and not self._in_cd("silent"):
            should_alert  = True
            alert_reason += (" | " if alert_reason else "") + "Silent: " + ", ".join(silent_sensors)
            self._mark_cd("silent")

        return {
            "critical_params": critical_params,
            "warning_params":  warning_params,
            "silent_sensors":  silent_sensors,
            "should_alert":    should_alert,
            "alert_reason":    alert_reason,
        }

    def drain_warning_log(self) -> list[dict]:
        log_copy = list(self._warning_log)
        self._warning_log.clear()
        return log_copy


class GrowMapCache:
    def __init__(self) -> None:
        self._cache: dict[str, dict] = {}

    def _load(self, crop: str) -> dict:
        if crop in self._cache: return self._cache[crop]
        path = KNOWLEDGE_BASE_DIR / f"{crop}.json"
        if not path.exists():
            log.warning("Grow map '%s' not found", crop)
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self._cache[crop] = data
            log.info("Grow map '%s' loaded (%d bytes)", crop, path.stat().st_size)
            return data
        except json.JSONDecodeError as exc:
            log.error("Grow map parse error '%s': %s", crop, exc)
            return {}

    def get_stage_compact(self, crop: str, stage: str) -> dict:
        full = self._load(crop)
        if not full: return {}
        s  = full.get(stage, {})
        ns = s.get("nutrient_solution", {})
        cl = s.get("climate", {})

        def _r(obj, key):
            v = obj.get(key, {})
            if isinstance(v, dict):
                return v.get("acceptable_range") or v.get("range") or (
                    [v["value"], v["value"]] if v.get("value") is not None else None
                )
            return v

        return {
            "ph_range":   _r(ns, "ph_setpoint")  or [5.5, 6.5],
            "ec_range":   _r(ns, "ec_absolute")   or [1.2, 1.8],
            "temp_day":   _r(cl, "temp_day_C")    or [20.0, 26.0],
            "temp_night": _r(cl, "temp_night_C")  or [17.0, 20.0],
            "humidity":   _r(cl, "humidity_pct")  or [50.0, 70.0],
        }


class PromptBuilder:
    def __init__(self, grow_map: GrowMapCache, kb: KnowledgeBaseConfig) -> None:
        self._core = self._load_file(SYSTEM_PROMPT_CORE, "You are a hydroponics dosing policy engine.")
        self._schema = self._load_file(
            SYSTEM_PROMPT_SCHEMA,
            "Return JSON with situation_summary, selected_control_strategy, risks, commands, recheck_interval_sec, stop_conditions.",
        )
        self._gm = grow_map
        self._kb = kb
        log.info("Prompts loaded: core=%d chars, schema=%d chars", len(self._core), len(self._schema))

    @staticmethod
    def _load_file(path: str, fallback: str) -> str:
        p = Path(path)
        if not p.exists():
            log.warning("%s not found — using fallback", path)
            return fallback
        return p.read_text(encoding="utf-8").strip()

    @staticmethod
    def _sanitize(val: Any) -> Any:
        if isinstance(val, (int, float)): return round(float(val), 4)
        if isinstance(val, str):          return val[:80].replace("{","").replace("}","")
        return None

    def _telemetry(self, snapshot: dict) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for sensor, data in snapshot.items():
            last = self._sanitize(data.get("last"))
            if last is None:
                continue
            result[sensor] = {
                "last": last,
                "avg": self._sanitize(data.get("avg")),
                "trend": data.get("trend"),
                "delta": self._sanitize(data.get("delta")),
                "last_ts": self._sanitize(data.get("last_ts")),
            }
        return result

    def build_control(
        self,
        snapshot: dict[str, Any],
        control_state: dict[str, Any],
        crop: str,
        stage: str,
        day: int,
        tray_id: str,
        actuator_state: dict[str, str],
        actuator_capabilities: dict[str, Any],
    ) -> tuple[str, str]:
        stage_targets = self._gm.get_stage_compact(crop, stage)
        context = {
            "request_type": "CONTROL_POLICY",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "tray_id": tray_id,
            "crop": crop,
            "stage": stage,
            "growth_day": day,
            "control_scope": ["ph", "ec"],
            "telemetry": self._telemetry(snapshot),
            "stage_targets": {
                "ph_range": stage_targets.get("ph_range"),
                "ec_range": stage_targets.get("ec_range"),
            },
            "deviations": control_state.get("deviations", []),
            "risk_state": {
                "risks": control_state.get("risks", []),
                "blocked_metrics": control_state.get("blocked_metrics", {}),
                "escalations": control_state.get("escalations", []),
                "pending_effects": control_state.get("pending_effects", []),
                "last_effect_results": control_state.get("last_effect_results", []),
            },
            "recent_actions": control_state.get("recent_actions", []),
            "recovery_protocol_hints": control_state.get("recovery_hints", {}),
            "general_rules": self._kb.general_rules,
            "actuator_state": actuator_state,
            "actuator_capabilities": actuator_capabilities,
        }
        return self._core, json.dumps(context, ensure_ascii=False, indent=2) + "\n\n" + self._schema


class LLMClient:
    def __init__(self) -> None:
        self._failures = 0

    @property
    def consecutive_failures(self) -> int:
        return self._failures

    def generate(self, system_prompt: str, user_message: str) -> Optional[str]:
        result = self._openclaw(system_prompt, user_message)
        if result is not None:
            self._failures = 0
            return result
        log.warning("OpenClaw unavailable — Ollama fallback")
        result = self._ollama(system_prompt, user_message)
        if result is not None:
            self._failures = 0
            return result
        self._failures += 1
        log.error("Both LLM backends failed. streak=%d/%d", self._failures, DEAD_MAN_THRESHOLD)
        return None

    def _openclaw(self, system_prompt: str, user_message: str) -> Optional[str]:
        payload = {
            "model": OPENCLAW_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        try:
            resp = requests.post(OPENCLAW_URL, json=payload, timeout=LLM_TIMEOUT_SEC)
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()
            log.info("OpenClaw OK (%d chars)", len(text))
            audit_log.info("OPENCLAW_OK | model=%s | chars=%d", OPENCLAW_MODEL, len(text))
            return text
        except requests.exceptions.ConnectionError:
            log.debug("OpenClaw not available at %s", OPENCLAW_URL)
            return None
        except Exception as exc:
            log.debug("OpenClaw error: %s", exc)
            return None

    def _ollama(self, system_prompt: str, user_message: str) -> Optional[str]:
        payload = {
            "model":  OLLAMA_MODEL,
            "prompt": f"{system_prompt}\n\n{user_message}",
            "format": "json",
            "stream": False,
        }
        try:
            resp = requests.post(OLLAMA_URL, json=payload, timeout=LLM_TIMEOUT_SEC)
            resp.raise_for_status()
            text = resp.json().get("response", "").strip()
            if not text: return None
            log.info("Ollama fallback OK (%d chars)", len(text))
            audit_log.info("OLLAMA_FALLBACK_OK | model=%s | chars=%d", OLLAMA_MODEL, len(text))
            return text
        except requests.exceptions.ConnectionError:
            log.error("Ollama not available at %s", OLLAMA_URL)
            return None
        except Exception as exc:
            log.error("Ollama error: %s", exc)
            return None

class SmartMQTTBridge:
    def __init__(self) -> None:
        db.init_db()
        influx.start(TRAY_ID, CURRENT_CROP, CURRENT_GROWTH_STAGE)

        self._aggregator     = Aggregator()
        self._alert_engine   = AlertEngine(self._aggregator)
        self._grow_map       = GrowMapCache()
        self._control        = ControlSafetyManager(KB_CONFIG)
        self._prompt_builder = PromptBuilder(self._grow_map, KB_CONFIG)
        self._llm            = LLMClient()
        self._llm_busy       = threading.Lock()
        self._last_influx_ts = 0.0
        self._reconnect_delay = 1
        self._telemetry_version = 0
        self._last_control_version = -1

        saved_states = db.get_actuator_states()
        self._active_actuators: dict[str, str] = {
            a: saved_states.get(a, "OFF") for a in ALLOWED_ACTUATORS
        }
        if saved_states:
            log.info("Restored actuator states from DB: %s",
                     {k: v for k, v in self._active_actuators.items() if v != "OFF"})

        self._mqtt = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="neuroagronom_smart_bridge",
        )
        self._mqtt.on_connect    = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_message    = self._on_message
        self._gateway = HybridCommandGateway(
            self._mqtt,
            self._control,
            dry_run=DRY_RUN,
            actuator_topics=ACTUATOR_TOPIC_MAP,
            allowed_actions=ALLOWED_ACTUATORS,
            max_duration=MAX_DURATION,
            max_consecutive_doses=MAX_CONSECUTIVE_DOSES,
            conflicting_pairs=CONFLICTING_PAIRS,
            dose_reset_sec=DOSE_RESET_SEC,
            recovery_protocols=KB_CONFIG.recovery_protocols,
            check_level=_check_level,
            thresholds=THRESHOLDS,
        )

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            self._reconnect_delay = 1
            log.info("Connected to MQTT %s:%d", MQTT_HOST, MQTT_PORT)
            client.subscribe(MQTT_TELEMETRY_TOPIC)
            client.subscribe(f"farm/{TRAY_ID}/status/#")
            client.subscribe(OPERATOR_RESET_TOPIC)
            log.info("Subscribed to operator reset topic %s", OPERATOR_RESET_TOPIC)
        else:
            log.error("MQTT connect failed rc=%s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code != 0:
            delay = self._reconnect_delay
            log.warning("MQTT disconnect rc=%s — reconnect in %ds", reason_code, delay)
            time.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, MQTT_RECONNECT_MAX)
            try:
                client.reconnect()
            except Exception as exc:
                log.error("MQTT reconnect failed: %s", exc)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        if "telemetry" in topic:
            sensor_key = topic.split("/")[-1]
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return
            raw_val = payload.get("value") if isinstance(payload, dict) else payload
            try:
                value = float(raw_val)
            except (TypeError, ValueError):
                return
            self._aggregator.ingest(sensor_key, value)
            self._alert_engine.mark_pending()
            self._telemetry_version += 1
        elif "status" in topic:
            key = topic.split("/")[-1]
            if key in self._active_actuators:
                try:
                    self._active_actuators[key] = msg.payload.decode("utf-8").strip()
                except Exception:
                    pass
        elif topic == OPERATOR_RESET_TOPIC:
            self._handle_operator_reset(msg.payload)

    def _handle_operator_reset(self, payload: bytes) -> None:
        raw = payload.decode("utf-8", errors="replace").strip()
        metric = "all"
        operator = "operator"
        try:
            data = json.loads(raw) if raw.startswith("{") else {"metric": raw}
        except json.JSONDecodeError:
            data = {"metric": raw}

        if isinstance(data, dict):
            metric = str(data.get("metric", "all")).strip().lower() or "all"
            operator = str(data.get("operator", "operator")).strip() or "operator"

        if metric == "all":
            reset_metrics = self._control.reset_all(operator=operator)
            self._last_control_version = -1
            log.warning("Operator reset received: metric=all operator=%s reset=%s", operator, reset_metrics or "none")
            db.save_control_event(
                "operator_reset",
                "smart_bridge",
                metric="all",
                status="applied" if reset_metrics else "noop",
                details=f"operator={operator}",
            )
            return

        changed = self._control.reset_metric(metric, operator=operator)
        self._last_control_version = -1
        log.warning("Operator reset received: metric=%s operator=%s changed=%s", metric, operator, changed)
        db.save_control_event(
            "operator_reset",
            "smart_bridge",
            metric=metric,
            status="applied" if changed else "noop",
            details=f"operator={operator}",
        )

    def _eval_alerts(self) -> None:
        if not self._alert_engine.should_evaluate():
            return
        self._alert_engine.evaluate()

    def _maybe_write_influx(self) -> None:
        now = time.time()
        if (now - self._last_influx_ts) >= INFLUX_WRITE_SEC and self._aggregator.has_new_data():
            snapshot = self._aggregator.snapshot()
            influx.write_snapshot(snapshot, TRAY_ID, CURRENT_CROP, CURRENT_GROWTH_STAGE)
            self._aggregator.clear_dirty()
            self._last_influx_ts = now

    def _current_stage_targets(self) -> dict[str, Any]:
        return self._grow_map.get_stage_compact(CURRENT_CROP, CURRENT_GROWTH_STAGE)

    def _log_control_decision(
        self,
        status: str,
        reason: str,
        control_state: dict[str, Any],
    ) -> None:
        details = (
            f"{reason} | actionable={control_state.get('actionable_metrics', [])} "
            f"blocked={control_state.get('blocked_metrics', {})}"
        )
        log.info("Control decision: %s | %s", status, details)
        db.save_control_event(
            "llm_decision",
            "smart_bridge",
            status=status,
            details=details[:500],
            context_json=json.dumps(
                {
                    "risks": control_state.get("risks", []),
                    "deviations": control_state.get("deviations", []),
                },
                ensure_ascii=False,
            ),
        )

    def _maybe_control(self) -> None:
        snapshot = self._aggregator.snapshot()
        stage_targets = self._current_stage_targets()
        control_state = self._control.assess(snapshot, stage_targets)
        deviation_metrics = [item["metric"] for item in control_state.get("deviations", [])]
        control_state["recovery_hints"] = self._control.recovery_hints(deviation_metrics)

        telemetry_changed = self._telemetry_version != self._last_control_version
        if telemetry_changed:
            self._last_control_version = self._telemetry_version
            if control_state["actionable_metrics"]:
                reason = "fresh telemetry with actionable pH/EC deviation"
                if not self._fire_llm(
                    "CONTROL",
                    {
                        "snapshot": snapshot,
                        "stage_targets": stage_targets,
                        "control_state": control_state,
                    },
                ):
                    self._log_control_decision("busy", reason, control_state)
                else:
                    self._log_control_decision("called", reason, control_state)
                return
            if control_state["deviations"]:
                self._log_control_decision("skipped", "fresh telemetry but all deviations blocked by deterministic safety", control_state)
            else:
                self._log_control_decision("skipped", "fresh telemetry but pH/EC already inside target", control_state)
        elif control_state["actionable_metrics"] and self._control.heartbeat_due():
            self._log_control_decision("waiting", "actionable deviation present but no new telemetry since last decision", control_state)

        if self._control.heartbeat_due():
            self._log_heartbeat(control_state, snapshot)
            self._control.mark_heartbeat()

    def _log_heartbeat(self, control_state: dict[str, Any], snapshot: dict[str, Any]) -> None:
        ph = (snapshot.get("ph") or {}).get("last")
        ec = (snapshot.get("ec") or {}).get("last")
        risks = control_state.get("risks", [])
        deviations = control_state.get("deviations", [])
        if not deviations and not risks:
            log.info(
                "Heartbeat: bridge healthy, no pH/EC action needed | ph=%s ec=%s",
                round(float(ph), 3) if ph is not None else "n/a",
                round(float(ec), 3) if ec is not None else "n/a",
            )
            return

        log.info(
            "Heartbeat: no automatic policy call | deviations=%s risks=%s",
            json.dumps(deviations, ensure_ascii=False),
            json.dumps(risks, ensure_ascii=False),
        )

    def _fire_llm(self, request_type: str, context: Optional[dict]) -> bool:
        if not self._llm_busy.acquire(blocking=False):
            return False
        threading.Thread(
            target=self._llm_pipeline,
            args=(request_type, context),
            daemon=True,
            name=f"LLM-{request_type}",
        ).start()
        return True

    def _llm_pipeline(self, request_type: str, context: Optional[dict]) -> None:
        try:
            if request_type != "CONTROL" or not context:
                return

            snapshot = context["snapshot"]
            stage_targets = context["stage_targets"]
            control_state = context["control_state"]
            sys_p, user_m = self._prompt_builder.build_control(
                snapshot,
                control_state,
                CURRENT_CROP,
                CURRENT_GROWTH_STAGE,
                CURRENT_GROWTH_DAY,
                TRAY_ID,
                self._active_actuators,
                self._control.actuator_capabilities(),
            )

            audit_log.info(
                "LLM_REQUEST | type=%s | crop=%s | stage=%s | day=%d | sys=%d | user=%d",
                request_type, CURRENT_CROP, CURRENT_GROWTH_STAGE, CURRENT_GROWTH_DAY,
                len(sys_p), len(user_m),
            )

            raw = self._llm.generate(sys_p, user_m)

            req_id = db.save_llm_request(
                request_type, CURRENT_CROP, CURRENT_GROWTH_STAGE, CURRENT_GROWTH_DAY,
                len(user_m), raw or "", "", "OK" if raw else "LLM_UNAVAILABLE",
            )

            if raw is None:
                db.save_control_event(
                    "llm_result",
                    "smart_bridge",
                    status="unavailable",
                    details=f"failures={self._llm.consecutive_failures}",
                )
                if self._llm.consecutive_failures >= DEAD_MAN_THRESHOLD:
                    log.critical("Dead man's switch triggered — fallback rules")
                    self._gateway.set_req_id(req_id)
                    db.save_control_event(
                        "fallback",
                        "smart_bridge",
                        status="triggered",
                        details=f"dead_man_threshold={DEAD_MAN_THRESHOLD}",
                    )
                    self._gateway.fallback(snapshot, stage_targets)
                return

            self._gateway.set_req_id(req_id)
            db.save_control_event(
                "llm_result",
                "smart_bridge",
                status="received",
                details=f"chars={len(raw)}",
            )
            self._gateway.process(raw, snapshot, request_type)

        except Exception as exc:
            log.exception("LLM pipeline error: %s", exc)
        finally:
            try:
                self._llm_busy.release()
            except RuntimeError:
                pass

    def run(self) -> None:
        log.info("=== Neuroagronom Smart Bridge v3 ===")
        log.info("crop=%s  stage=%s  day=%d  tray=%s",
                 CURRENT_CROP, CURRENT_GROWTH_STAGE, CURRENT_GROWTH_DAY, TRAY_ID)
        log.info("OpenClaw: %s  Ollama fallback: %s", OPENCLAW_URL, OLLAMA_URL)
        log.info("InfluxDB: %s  bucket=%s", influx.INFLUX_URL, influx.INFLUX_BUCKET)
        log.info("Dry-run mode: %s", DRY_RUN)
        log.info("Operator reset topic: %s", OPERATOR_RESET_TOPIC)

        try:
            self._mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        except (ConnectionRefusedError, OSError) as exc:
            log.critical("Cannot connect to MQTT: %s", exc)
            return

        self._mqtt.loop_start()
        log.info("MQTT loop started")

        try:
            while True:
                self._eval_alerts()
                self._maybe_write_influx()
                self._maybe_control()
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutdown")
        finally:
            influx.stop()
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
            log.info("=== Smart Bridge stopped ===")


if __name__ == "__main__":
    SmartMQTTBridge().run()
