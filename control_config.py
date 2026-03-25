from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("control_config")

KB_ROOT = Path("knowledge_base")
RUNTIME_PROFILES_PATH = Path("runtime_profiles.json")


BUILTIN_PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    "test": {
        "mqtt": {
            "host": "localhost",
            "port": 1883,
            "reconnect_max": 10,
        },
        "logging": {
            "level": "DEBUG",
        },
        "llm": {
            "timeout_sec": 30,
            "dead_man_threshold": 2,
        },
        "telemetry": {
            "bootstrap_grace_sec": 20,
            "llm_min_fresh": {
                "ph": ["ph"],
                "ec": ["ec"],
            },
            "actuation_min_fresh": {
                "ph": ["ph", "water_level"],
                "ec": ["ec", "water_level"],
            },
        },
        "safety_overrides": {
            "heartbeat_sec": 15,
            "sensor_silence_sec": 45,
            "effect_tracking": {
                "ph": {"default_recheck_sec": 60},
                "ec": {"default_recheck_sec": 90},
            },
        },
    },
    "prod": {
        "mqtt": {
            "host": "mqtt.production.internal",
            "port": 1883,
            "reconnect_max": 30,
        },
        "logging": {
            "level": "INFO",
        },
        "llm": {
            "timeout_sec": 90,
            "dead_man_threshold": 3,
        },
        "telemetry": {
            "bootstrap_grace_sec": 180,
            "llm_min_fresh": {
                "ph": ["ph"],
                "ec": ["ec"],
            },
            "actuation_min_fresh": {
                "ph": ["ph", "water_level"],
                "ec": ["ec", "water_level"],
            },
        },
        "safety_overrides": {},
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    settings: dict[str, Any]

    @property
    def mqtt_host(self) -> str:
        return str(self.settings.get("mqtt", {}).get("host", "localhost"))

    @property
    def mqtt_port(self) -> int:
        return int(self.settings.get("mqtt", {}).get("port", 1883))

    @property
    def mqtt_reconnect_max(self) -> int:
        return int(self.settings.get("mqtt", {}).get("reconnect_max", 30))

    @property
    def log_level(self) -> str:
        return str(self.settings.get("logging", {}).get("level", "INFO")).upper()

    @property
    def llm_timeout_sec(self) -> int:
        return int(self.settings.get("llm", {}).get("timeout_sec", 90))

    @property
    def dead_man_threshold(self) -> int:
        return int(self.settings.get("llm", {}).get("dead_man_threshold", 3))

    @property
    def telemetry_bootstrap_grace_sec(self) -> int:
        return int(self.settings.get("telemetry", {}).get("bootstrap_grace_sec", 180))

    @property
    def safety_overrides(self) -> dict[str, Any]:
        return dict(self.settings.get("safety_overrides", {}))

    def llm_min_fresh(self, metric: str) -> list[str]:
        return list(self.settings.get("telemetry", {}).get("llm_min_fresh", {}).get(metric, [metric]))

    def actuation_min_fresh(self, metric: str) -> list[str]:
        return list(self.settings.get("telemetry", {}).get("actuation_min_fresh", {}).get(metric, [metric]))


def load_profile_definitions(path: Path = RUNTIME_PROFILES_PATH) -> dict[str, dict[str, Any]]:
    profiles = copy.deepcopy(BUILTIN_PROFILE_DEFAULTS)
    if not path.exists():
        return profiles
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        log.error("Runtime profiles JSON parse error in %s: %s", path, exc)
        return profiles

    if not isinstance(loaded, dict):
        log.error("Runtime profiles file must contain a JSON object: %s", path)
        return profiles

    for name, profile in loaded.items():
        if not isinstance(profile, dict):
            continue
        base = profiles.get(name, {})
        profiles[name] = _deep_merge(base, profile)
    return profiles


def load_runtime_profile() -> RuntimeProfile:
    profile_definitions = load_profile_definitions()
    name = os.getenv("APP_PROFILE", "test").strip().lower() or "test"
    if name not in profile_definitions:
        log.warning("Unknown APP_PROFILE '%s' - falling back to 'test'", name)
        name = "test"

    settings = copy.deepcopy(profile_definitions[name])
    settings["mqtt"]["host"] = os.getenv("MQTT_HOST", settings["mqtt"]["host"])
    settings["mqtt"]["port"] = int(os.getenv("MQTT_PORT", str(settings["mqtt"]["port"])))
    settings["mqtt"]["reconnect_max"] = int(os.getenv("MQTT_RECONNECT_MAX", str(settings["mqtt"]["reconnect_max"])))
    settings["logging"]["level"] = os.getenv("LOG_LEVEL", settings["logging"]["level"]).upper()
    settings["llm"]["timeout_sec"] = int(os.getenv("LLM_TIMEOUT_SEC", str(settings["llm"]["timeout_sec"])))
    settings["llm"]["dead_man_threshold"] = int(os.getenv("DEAD_MAN_THRESHOLD", str(settings["llm"]["dead_man_threshold"])))
    settings["telemetry"]["bootstrap_grace_sec"] = int(
        os.getenv("TELEMETRY_BOOTSTRAP_GRACE_SEC", str(settings["telemetry"]["bootstrap_grace_sec"]))
    )
    return RuntimeProfile(name=name, settings=settings)


class KnowledgeBaseConfig:
    def __init__(self, root: Path = KB_ROOT) -> None:
        self._root = root
        self._thresholds = self._load_json(root / "alerts" / "critical_thresholds.json")
        self._protocols = self._load_json(root / "alerts" / "recovery_protocols.json")
        self._safety = self._load_json(root / "alerts" / "control_safety.json")
        self._equipment = self._load_json(root / "equipment" / "actuator_registry.json")

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            log.warning("Knowledge base file missing: %s", path)
            return {}
        except json.JSONDecodeError as exc:
            log.error("Knowledge base JSON parse error in %s: %s", path, exc)
            return {}

    @staticmethod
    def _range(low: Any, high: Any) -> tuple[float, float]:
        lo = float("-inf") if low is None else float(low)
        hi = float("inf") if high is None else float(high)
        return (lo, hi)

    @property
    def threshold_levels(self) -> dict[str, dict[str, tuple[float, float]]]:
        result: dict[str, dict[str, tuple[float, float]]] = {}
        for sensor, spec in self._thresholds.get("thresholds", {}).items():
            result[sensor] = {
                "warning": self._range(spec.get("warning_low"), spec.get("warning_high")),
                "critical": self._range(spec.get("critical_low"), spec.get("critical_high")),
                "emergency": self._range(spec.get("emergency_low"), spec.get("emergency_high")),
            }
        return result

    @property
    def actuators(self) -> dict[str, dict[str, Any]]:
        return dict(self._equipment.get("actuators", {}))

    @property
    def allowed_actions(self) -> dict[str, list[str]]:
        return {
            actuator: list(spec.get("allowed_actions", []))
            for actuator, spec in self.actuators.items()
        }

    @property
    def actuator_topics(self) -> dict[str, str]:
        return {
            actuator: str(spec.get("mqtt_topic"))
            for actuator, spec in self.actuators.items()
            if spec.get("mqtt_topic")
        }

    @property
    def max_duration_sec(self) -> dict[str, int]:
        return {
            actuator: int(spec["max_duration_sec"])
            for actuator, spec in self.actuators.items()
            if spec.get("max_duration_sec") is not None
        }

    @property
    def conflicts(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for rule in self._equipment.get("conflict_rules", []):
            actuators = rule.get("actuators") or []
            if len(actuators) == 2:
                pairs.append((str(actuators[0]), str(actuators[1])))
        return pairs

    @property
    def recovery_protocols(self) -> dict[str, Any]:
        return dict(self._protocols.get("protocols", {}))

    @property
    def general_rules(self) -> dict[str, Any]:
        return dict(self._protocols.get("general_rules", {}))

    @property
    def safety_rules(self) -> dict[str, Any]:
        return dict(self._safety)

    def safety_rules_for_profile(self, profile: RuntimeProfile) -> dict[str, Any]:
        return _deep_merge(self.safety_rules, profile.safety_overrides)

    @property
    def multi_parameter_stress_count(self) -> int:
        spec = self._thresholds.get("multi_parameter_stress", {})
        return int(spec.get("threshold_count", 2))

    @property
    def max_consecutive_doses(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for protocol in self.recovery_protocols.values():
            max_doses = protocol.get("max_consecutive_doses")
            if max_doses is None:
                continue
            severity_map = protocol.get("severity_map", {})
            actuators = {
                str(spec.get("actuator"))
                for spec in severity_map.values()
                if isinstance(spec, dict) and spec.get("actuator")
            }
            also = {
                str(spec.get("also"))
                for spec in severity_map.values()
                if isinstance(spec, dict) and spec.get("also")
            }
            for actuator in actuators | also:
                result[actuator] = int(max_doses)
        return result
