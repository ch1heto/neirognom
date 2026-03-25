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
from pydantic import BaseModel, Field, ValidationError, field_validator

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


class CommandItem(BaseModel):
    actuator:     str
    action:       str
    duration_sec: Optional[int] = Field(default=None, ge=0)
    reason:       str = ""

    @field_validator("reason")
    @classmethod
    def _trim(cls, v: str) -> str:
        return v[:200]


CommandItem.model_rebuild()


class PolicyResponse(BaseModel):
    situation_summary: str               = ""
    selected_control_strategy: str       = ""
    risks:             list[str]         = []
    commands:          list[CommandItem] = []
    recheck_interval_sec: Optional[int]  = Field(default=None, ge=0)
    stop_conditions:   list[str]         = []

    @field_validator("situation_summary", "selected_control_strategy")
    @classmethod
    def _trim(cls, v: str) -> str:
        return v[:2000]

    @field_validator("risks", "stop_conditions", mode="before")
    @classmethod
    def _coerce_rec(cls, v: Any) -> list[str]:
        if isinstance(v, list):   return [str(x) for x in v]
        if isinstance(v, dict):   return [f"{k}: {val}" for k, val in v.items()]
        if isinstance(v, str):    return [v]
        return []

    @field_validator("commands", mode="before")
    @classmethod
    def _coerce_cmd(cls, v: Any) -> list:
        if isinstance(v, list): return v
        if isinstance(v, dict):
            return [{"actuator": k, "action": a, "duration_sec": None, "reason": "coerced"}
                    for k, a in v.items() if isinstance(a, str)]
        return []


PolicyResponse.model_rebuild()


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


class CommandGateway:
    def __init__(self, mqtt_client: mqtt.Client, safety: ControlSafetyManager) -> None:
        self._client = mqtt_client
        self._safety = safety
        self._req_id: Optional[int] = None
        self._dose_counts: dict[str, int] = {}
        self._dose_reset: dict[str, float] = {}

    def set_req_id(self, req_id: Optional[int]) -> None:
        self._req_id = req_id

    def _dose_ok(self, actuator: str) -> bool:
        max_d = MAX_CONSECUTIVE_DOSES.get(actuator)
        if max_d is None:
            return True
        now = time.time()
        if now - self._dose_reset.get(actuator, 0) > DOSE_RESET_SEC:
            self._dose_counts[actuator] = 0
            self._dose_reset[actuator] = now
        count = self._dose_counts.get(actuator, 0)
        if count >= max_d:
            log.warning("BLOCKED %s: consecutive doses %d/%d — operator required", actuator, count, max_d)
            return False
        return True

    def _inc_dose(self, actuator: str) -> None:
        self._dose_counts[actuator] = self._dose_counts.get(actuator, 0) + 1

    def process(self, raw: str, snapshot: dict, request_type: str) -> tuple[bool, Optional[PolicyResponse]]:
        audit_log.info("REQUEST_TYPE=%s | RAW_RESPONSE=%s", request_type, raw[:2000])
        parsed = self._parse(raw)
        if parsed is None:
            db.save_control_event(
                "policy_result",
                "smart_bridge",
                status="parse_failed",
                details=f"request_type={request_type}",
            )
            return False, None

        if parsed.situation_summary:
            log.info("Policy: %s", parsed.situation_summary[:160])
        if parsed.selected_control_strategy:
            log.info("Strategy: %s", parsed.selected_control_strategy[:160])
        db.save_control_event(
            "policy_result",
            "smart_bridge",
            status="parsed",
            details=parsed.selected_control_strategy[:200] or parsed.situation_summary[:200],
            context_json=json.dumps(
                {
                    "risks": parsed.risks[:10],
                    "recheck_interval_sec": parsed.recheck_interval_sec,
                    "command_count": len(parsed.commands),
                },
                ensure_ascii=False,
            ),
        )
        for risk in parsed.risks:
            log.warning("Policy risk: %s", risk)
        for condition in parsed.stop_conditions:
            log.warning("Stop condition: %s", condition)

        if not parsed.commands:
            log.info("Policy returned no commands")
            db.save_control_event(
                "policy_result",
                "smart_bridge",
                status="no_commands",
                details=parsed.situation_summary[:200],
            )
            return True, parsed

        validated = self._validate(parsed.commands, snapshot)
        if not validated:
            log.warning("All policy commands rejected")
            db.save_control_event(
                "command_batch",
                "smart_bridge",
                status="rejected",
                details="all commands rejected by deterministic validation",
            )
            return False, parsed

        published = False
        published_commands: list[dict[str, Any]] = []
        for cmd in validated:
            topic = ACTUATOR_TOPIC_MAP[cmd.actuator]
            ok, status = self._publish_action(topic, cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason)

            if ok:
                log.info("✅ PUBLISH  %-46s → %-8s | %s", topic, cmd.action, cmd.reason)
                db.update_actuator_state(cmd.actuator, cmd.action)
                published = True
                published_commands.append(cmd.model_dump())
                if cmd.action in ("ON", "OPEN"):
                    self._inc_dose(cmd.actuator)
                if cmd.duration_sec and cmd.action not in ("OFF", "CLOSE"):
                    self._auto_off(cmd.actuator, cmd.duration_sec)
            else:
                log.error("PUBLISH FAILED %s", topic)

            db.save_command(self._req_id, cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason, topic, status)
            audit_log.info("CMD_%s | %s | %s | %s", status, cmd.actuator, cmd.action, cmd.reason)

        if published_commands:
            self._safety.register_published_commands(published_commands, snapshot, parsed.recheck_interval_sec)
        return published, parsed

    def _parse(self, raw: str) -> Optional[PolicyResponse]:
        text = raw.strip()
        if text.startswith("```"):
            import re
            m = re.search(r"```(?:json)?\s*([\s\S]+?)```", text)
            text = m.group(1).strip() if m else text.replace("```", "").strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            import re
            m = re.search(r"\{[\s\S]+\}", text)
            if not m:
                log.error("No JSON in LLM response")
                return None
            try:
                data = json.loads(m.group())
            except json.JSONDecodeError as exc:
                log.error("JSON parse error: %s", exc)
                return None

        if "analysis" in data and "situation_summary" not in data:
            data["situation_summary"] = data["analysis"]
        if "summary" in data and "situation_summary" not in data:
            data["situation_summary"] = data["summary"]
        if "strategy" in data and "selected_control_strategy" not in data:
            data["selected_control_strategy"] = data["strategy"]
        if "recommendations" in data and "stop_conditions" not in data:
            data["stop_conditions"] = data["recommendations"]
        if "actuators" in data and "commands" not in data:
            data["commands"] = [
                {"actuator": k, "action": v, "duration_sec": None, "reason": "from actuators"}
                for k, v in data["actuators"].items()
                if isinstance(v, str) and v in ("ON", "OFF", "OPEN", "CLOSE", "DIM_50")
            ]
        try:
            return PolicyResponse.model_validate(data)
        except ValidationError as exc:
            log.error("Pydantic validation failed: %s", exc)
            return None

    def _validate(self, commands: list[CommandItem], snapshot: dict) -> list[CommandItem]:
        accepted = []
        applied: dict[str, str] = {}
        has_ec_dose = any(cmd.actuator in ("nutrient_a", "nutrient_b") and cmd.action == "ON" for cmd in commands)

        for cmd in commands:
            if cmd.actuator not in ALLOWED_ACTUATORS:
                log.warning("REJECTED not-in-allowlist: %s", cmd.actuator)
                db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="not_allowed", details=cmd.action)
                continue
            if cmd.action not in ALLOWED_ACTUATORS[cmd.actuator]:
                log.warning("REJECTED bad action: %s.%s", cmd.actuator, cmd.action)
                db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="bad_action", details=cmd.action)
                continue
            if has_ec_dose and cmd.actuator in ("ph_down_pump", "ph_up_pump") and cmd.action == "ON":
                log.warning("REJECTED %s.%s: pH correction must wait until after EC recheck", cmd.actuator, cmd.action)
                db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="ph_after_ec_block", details=cmd.action)
                continue
            if cmd.action in ("ON", "OPEN") and not self._dose_ok(cmd.actuator):
                db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="dose_limit", details=cmd.action)
                continue

            allowed, reason = self._safety.can_execute(cmd.actuator, cmd.action, snapshot)
            if not allowed:
                log.warning("REJECTED %s.%s: %s", cmd.actuator, cmd.action, reason)
                db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="safety_block", details=reason)
                continue

            if cmd.duration_sec is not None:
                max_d = MAX_DURATION.get(cmd.actuator)
                if max_d and cmd.duration_sec > max_d:
                    log.warning("CLAMPED %s: %d→%d sec", cmd.actuator, cmd.duration_sec, max_d)
                    cmd = CommandItem(
                        actuator=cmd.actuator,
                        action=cmd.action,
                        duration_sec=max_d,
                        reason=cmd.reason,
                    )

            if cmd.actuator == "drain_valve" and cmd.action == "OPEN":
                wl = (snapshot.get("water_level") or {}).get("last")
                if wl is not None and wl < 25.0:
                    log.warning("REJECTED drain_valve OPEN: water_level=%.1f%%", wl)
                    db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="low_water", details=str(wl))
                    continue

            conflict = False
            for a, b in CONFLICTING_PAIRS:
                other = b if cmd.actuator == a else (a if cmd.actuator == b else None)
                if other and applied.get(other) == "ON" and cmd.action == "ON":
                    log.warning("REJECTED conflict: %s vs %s", cmd.actuator, other)
                    db.save_control_event("command_rejected", "smart_bridge", metric=cmd.actuator, status="conflict", details=other)
                    conflict = True
                    break
            if conflict:
                continue

            applied[cmd.actuator] = cmd.action
            accepted.append(cmd)

        return accepted

    def _publish_action(
        self,
        topic: str,
        actuator: str,
        action: str,
        duration_sec: Optional[int],
        reason: str,
    ) -> tuple[bool, str]:
        if DRY_RUN:
            log.info("[DRY_RUN] %-46s -> %-8s | %s", topic, action, reason)
            return True, "DRY_RUN"

        result = self._client.publish(topic, action)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("PUBLISH FAILED %s rc=%d", topic, result.rc)
            return False, "FAIL"
        log.info("PUBLISH %-46s -> %-8s | %s", topic, action, reason)
        return True, "OK"

    def _publish_direct(self, actuator: str, action: str, duration_sec: Optional[int], reason: str) -> bool:
        topic = ACTUATOR_TOPIC_MAP.get(actuator)
        if not topic:
            return False
        result = self._client.publish(topic, action)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("❌ PUBLISH FAILED %s rc=%d", topic, result.rc)
            return False
        log.warning("FALLBACK PUBLISH %-46s → %-8s | %s", topic, action, reason)
        db.update_actuator_state(actuator, action)
        if action in ("ON", "OPEN"):
            self._inc_dose(actuator)
        if duration_sec and action not in ("OFF", "CLOSE"):
            self._auto_off(actuator, duration_sec)
        db.save_command(self._req_id, actuator, action, duration_sec, reason, topic, "OK")
        return True

    def _fallback_spec(self, protocol_key: str, severity: str) -> tuple[list[dict[str, Any]], int | None]:
        protocol = KB_CONFIG.recovery_protocols.get(protocol_key, {})
        spec = protocol.get("severity_map", {}).get(severity) or protocol.get("severity_map", {}).get("WARNING") or {}
        recheck_sec = None
        if spec.get("wait_min") is not None:
            recheck_sec = int(spec["wait_min"]) * 60

        commands = []
        actuator = spec.get("actuator")
        if actuator:
            commands.append(
                {
                    "actuator": actuator,
                    "action": spec.get("action", "ON"),
                    "duration_sec": spec.get("duration_sec"),
                    "reason": f"fallback:{protocol_key}:{severity}",
                }
            )
        also = spec.get("also")
        if also:
            commands.append(
                {
                    "actuator": also,
                    "action": "ON",
                    "duration_sec": spec.get("duration_sec"),
                    "reason": f"fallback:{protocol_key}:{severity}",
                }
            )
        return commands, recheck_sec

    def fallback(self, snapshot: dict, stage_targets: dict[str, Any]) -> bool:
        log.warning("=== FALLBACK deterministic rules ===")
        commands: list[dict[str, Any]] = []
        recheck_sec: Optional[int] = None

        ph = (snapshot.get("ph") or {}).get("last")
        ec = (snapshot.get("ec") or {}).get("last")
        ph_range = stage_targets.get("ph_range") or [5.5, 6.5]
        ec_range = stage_targets.get("ec_range") or [1.2, 1.8]

        if ph is not None:
            if float(ph) > float(ph_range[1]):
                severity = _check_level(float(ph), THRESHOLDS["ph"])
                commands, recheck_sec = self._fallback_spec("ph_high", severity)
            elif float(ph) < float(ph_range[0]):
                severity = _check_level(float(ph), THRESHOLDS["ph"])
                commands, recheck_sec = self._fallback_spec("ph_low", severity)

        if not commands and ec is not None and float(ec) < float(ec_range[0]):
            severity = _check_level(float(ec), THRESHOLDS["ec"])
            commands, recheck_sec = self._fallback_spec("ec_low", severity)

        validated = self._validate(
            [CommandItem.model_validate(cmd) for cmd in commands],
            snapshot,
        )
        if not validated:
            log.warning("Fallback did not find any safe commands")
            return False

        published_commands: list[dict[str, Any]] = []
        for cmd in validated:
            ok = self._publish_direct(cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason)
            if ok:
                published_commands.append(cmd.model_dump())

        if published_commands:
            self._safety.register_published_commands(published_commands, snapshot, recheck_sec)
            return True
        return False

    def _auto_off(self, actuator: str, duration_sec: int) -> None:
        def _off():
            time.sleep(duration_sec)
            off_action = "CLOSE" if actuator == "drain_valve" else "OFF"
            topic = ACTUATOR_TOPIC_MAP.get(actuator)
            if topic:
                r = self._client.publish(topic, off_action)
                if r.rc == mqtt.MQTT_ERR_SUCCESS:
                    log.info("⏱ AUTO-%-5s %-46s (after %ds)", off_action, topic, duration_sec)
                    db.update_actuator_state(actuator, off_action)
        threading.Thread(target=_off, daemon=True, name=f"AutoOff-{actuator}").start()


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
