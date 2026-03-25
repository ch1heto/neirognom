from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger("control_config")

KB_ROOT = Path("knowledge_base")


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
