from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from control_config import KnowledgeBaseConfig, RuntimeProfile
import database as db

log = logging.getLogger("control_runtime")

CONTROL_EFFECTS = {
    "ph_down_pump": ("ph", "decrease"),
    "ph_up_pump": ("ph", "increase"),
    "nutrient_a": ("ec", "increase"),
    "nutrient_b": ("ec", "increase"),
}


@dataclass
class PendingEffect:
    metric: str
    actuator: str
    baseline: float
    expected_direction: str
    created_at: float
    recheck_after_sec: int
    min_effect: float
    wrong_direction_margin: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "actuator": self.actuator,
            "baseline": round(self.baseline, 4),
            "expected_direction": self.expected_direction,
            "created_at": self.created_at,
            "recheck_after_sec": self.recheck_after_sec,
            "reason": self.reason,
        }


class ControlSafetyManager:
    CONTROL_METRICS = ("ph", "ec")

    def __init__(self, config: KnowledgeBaseConfig, profile: RuntimeProfile) -> None:
        self._config = config
        self._profile = profile
        self._rules = config.safety_rules_for_profile(profile)
        self._effect_rules = self._rules.get("effect_tracking", {})
        self._pending: dict[str, PendingEffect] = {}
        self._recent_actions: deque[dict[str, Any]] = deque(maxlen=20)
        self._escalations: dict[str, dict[str, Any]] = {}
        self._effect_failures: dict[str, int] = {}
        self._jump_counts: dict[str, int] = {}
        self._current_jump_issues: dict[str, str] = {}
        self._last_jump_ts: dict[str, float] = {}
        self._last_effect_results: dict[str, dict[str, Any]] = {}
        self._last_heartbeat_ts = 0.0
        self._telemetry_state: dict[str, dict[str, Any]] = {}

    @property
    def sensor_silence_sec(self) -> int:
        return int(self._rules.get("sensor_silence_sec", 300))

    @property
    def heartbeat_sec(self) -> int:
        return int(self._rules.get("heartbeat_sec", 60))

    @property
    def low_water_limit_pct(self) -> float:
        return float(self._rules.get("low_water_pct_for_correction", 30.0))

    def heartbeat_due(self) -> bool:
        return (time.time() - self._last_heartbeat_ts) >= self.heartbeat_sec

    def mark_heartbeat(self) -> None:
        self._last_heartbeat_ts = time.time()

    def refresh(self, snapshot: dict[str, Any], telemetry_state: dict[str, Any] | None = None) -> None:
        self._telemetry_state = dict((telemetry_state or {}).get("sensors", {}))
        self._refresh_jump_anomalies(snapshot)
        self._refresh_pending_effects(snapshot)

    def _sensor_state(self, snapshot: dict[str, Any], sensor: str) -> str:
        state = self._telemetry_state.get(sensor, {}).get("state")
        if state:
            return str(state)

        ts = (snapshot.get(sensor) or {}).get("last_ts")
        if ts is None:
            return "stale_missing"
        return "stale_missing" if (time.time() - float(ts)) > self.sensor_silence_sec else "fresh"

    def _sensor_fresh(self, snapshot: dict[str, Any], sensor: str) -> bool:
        return self._sensor_state(snapshot, sensor) == "fresh"

    def _refresh_jump_anomalies(self, snapshot: dict[str, Any]) -> None:
        self._current_jump_issues = {}
        thresholds = self._rules.get("implausible_jump_thresholds", {})
        max_events = int(self._rules.get("escalation", {}).get("max_implausible_jump_events", 2))

        for sensor, threshold in thresholds.items():
            data = snapshot.get(sensor) or {}
            prev = data.get("prev")
            last = data.get("last")
            last_ts = data.get("last_ts")
            if prev is None or last is None or last_ts is None:
                continue
            if self._last_jump_ts.get(sensor) == float(last_ts):
                continue
            self._last_jump_ts[sensor] = float(last_ts)

            delta = abs(float(last) - float(prev))
            if delta > float(threshold):
                self._jump_counts[sensor] = self._jump_counts.get(sensor, 0) + 1
                issue = f"{sensor} jump too large ({prev} -> {last})"
                self._current_jump_issues[sensor] = issue
                log.warning("Implausible jump detected: %s", issue)
                db.save_control_event(
                    "sensor_anomaly",
                    "control_runtime",
                    metric=sensor,
                    status="implausible_jump",
                    details=issue,
                )
                if sensor in self.CONTROL_METRICS and self._jump_counts[sensor] >= max_events:
                    self._set_escalation(sensor, issue)
            else:
                self._jump_counts[sensor] = 0

    def _refresh_pending_effects(self, snapshot: dict[str, Any]) -> None:
        now = time.time()
        max_failed = int(self._rules.get("escalation", {}).get("max_failed_effect_cycles", 2))

        for metric, pending in list(self._pending.items()):
            if (now - pending.created_at) < pending.recheck_after_sec:
                continue

            data = snapshot.get(metric) or {}
            current = data.get("last")
            if current is None:
                continue

            delta = float(current) - pending.baseline
            effective = False
            wrong_direction = False

            if pending.expected_direction == "increase":
                effective = delta >= pending.min_effect
                wrong_direction = delta <= -pending.wrong_direction_margin
            else:
                effective = delta <= -pending.min_effect
                wrong_direction = delta >= pending.wrong_direction_margin

            if effective:
                self._effect_failures[metric] = 0
                self._last_effect_results[metric] = {
                    "metric": metric,
                    "status": "effective",
                    "delta": round(delta, 4),
                    "baseline": round(pending.baseline, 4),
                    "current": round(float(current), 4),
                    "actuator": pending.actuator,
                }
                log.info(
                    "Dose effect confirmed: metric=%s actuator=%s delta=%.4f",
                    metric,
                    pending.actuator,
                    delta,
                )
                db.save_control_event(
                    "effect_check",
                    "control_runtime",
                    metric=metric,
                    status="effective",
                    details=f"{pending.actuator} delta={round(delta, 4)}",
                )
            else:
                self._effect_failures[metric] = self._effect_failures.get(metric, 0) + 1
                status = "wrong_direction" if wrong_direction else "no_effect"
                result = {
                    "metric": metric,
                    "status": status,
                    "delta": round(delta, 4),
                    "baseline": round(pending.baseline, 4),
                    "current": round(float(current), 4),
                    "actuator": pending.actuator,
                    "failures": self._effect_failures[metric],
                }
                self._last_effect_results[metric] = result
                log.warning(
                    "Dose effect check failed: metric=%s actuator=%s status=%s delta=%.4f failures=%d",
                    metric,
                    pending.actuator,
                    status,
                    delta,
                    self._effect_failures[metric],
                )
                db.save_control_event(
                    "effect_check",
                    "control_runtime",
                    metric=metric,
                    status=status,
                    details=(
                        f"{pending.actuator} delta={round(delta, 4)} "
                        f"failures={self._effect_failures[metric]}"
                    ),
                )
                if wrong_direction or self._effect_failures[metric] >= max_failed:
                    self._set_escalation(metric, f"{status} after {pending.actuator}")

            del self._pending[metric]

    def _set_escalation(self, metric: str, reason: str) -> None:
        if metric in self._escalations:
            return
        self._escalations[metric] = {
            "metric": metric,
            "state": "operator_required",
            "reason": reason,
            "since": time.time(),
        }
        log.error("Operator escalation activated: metric=%s reason=%s", metric, reason)
        db.save_control_event(
            "escalation",
            "control_runtime",
            metric=metric,
            status="operator_required",
            details=reason,
        )

    def _level(self, metric: str, value: float) -> str:
        levels = self._config.threshold_levels.get(metric, {})
        critical = levels.get("critical", (float("-inf"), float("inf")))
        warning = levels.get("warning", (float("-inf"), float("inf")))
        if value < critical[0] or value > critical[1]:
            return "CRITICAL"
        if value < warning[0] or value > warning[1]:
            return "WARNING"
        return "OK"

    def assess(
        self,
        snapshot: dict[str, Any],
        stage_targets: dict[str, Any],
        telemetry_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.refresh(snapshot, telemetry_state)

        deviations = []
        reasoning_metrics = []
        actionable_metrics = []
        blocked_metrics: dict[str, list[str]] = {}
        risks: list[str] = []

        water_level = (snapshot.get("water_level") or {}).get("last")
        water_level_state = self._sensor_state(snapshot, "water_level")
        water_level_fresh = water_level_state == "fresh"
        low_water = water_level is not None and float(water_level) < self.low_water_limit_pct
        if low_water:
            risks.append(f"low_water_level_for_correction:{round(float(water_level), 2)}")

        for sensor in ("ph", "ec", "water_level"):
            sensor_state = self._sensor_state(snapshot, sensor)
            if sensor_state == "stale_missing":
                risks.append(f"sensor_stale:{sensor}")
            elif sensor_state == "expected_not_seen_yet":
                risks.append(f"sensor_expected_not_seen:{sensor}")

        for sensor, issue in self._current_jump_issues.items():
            risks.append(f"implausible_jump:{issue}")

        for metric, escalation in self._escalations.items():
            risks.append(f"operator_escalation:{metric}:{escalation['reason']}")

        for metric, pending in self._pending.items():
            risks.append(f"pending_effect_recheck:{metric}:{pending.actuator}")

        for metric in self.CONTROL_METRICS:
            data = snapshot.get(metric) or {}
            last = data.get("last")
            target = stage_targets.get(f"{metric}_range") or []
            if last is None or len(target) != 2:
                continue

            low, high = float(target[0]), float(target[1])
            direction = None
            if float(last) < low:
                direction = "increase"
            elif float(last) > high:
                direction = "decrease"
            if direction is None:
                continue

            metric_state = self._sensor_state(snapshot, metric)
            reasoning_allowed = metric_state == "fresh"
            reasons = []
            if metric_state != "fresh":
                reasons.append(f"telemetry_{metric_state}")
            if metric in self._current_jump_issues:
                reasons.append("implausible_jump")
            if metric in self._pending:
                reasons.append("waiting_effect_window")
            if metric in self._escalations:
                reasons.append("operator_required")
            if low_water:
                reasons.append("low_water")
            if not water_level_fresh:
                reasons.append("water_level_unknown")
            if metric == "ec" and direction == "decrease":
                ec_high = self._config.recovery_protocols.get("ec_high", {})
                if ec_high.get("automated_action") is False:
                    reasons.append("manual_only_protocol")

            deviation = {
                "metric": metric,
                "current": round(float(last), 4),
                "target_range": [low, high],
                "direction": direction,
                "severity": self._level(metric, float(last)),
                "reasoning_allowed": reasoning_allowed,
                "telemetry_state": metric_state,
                "blocked": bool(reasons),
                "blocked_reasons": reasons,
            }
            deviations.append(deviation)
            if reasoning_allowed:
                reasoning_metrics.append(metric)
            if reasons:
                blocked_metrics[metric] = reasons
            if reasoning_allowed and not reasons:
                actionable_metrics.append(metric)

        return {
            "deviations": deviations,
            "reasoning_metrics": reasoning_metrics,
            "actionable_metrics": actionable_metrics,
            "blocked_metrics": blocked_metrics,
            "pending_effects": [pending.to_dict() for pending in self._pending.values()],
            "escalations": list(self._escalations.values()),
            "recent_actions": list(self._recent_actions)[-5:],
            "last_effect_results": list(self._last_effect_results.values())[-5:],
            "risks": risks,
            "low_water": low_water,
            "telemetry_state": telemetry_state or {"sensors": {}},
        }

    def recovery_hints(self, metrics: list[str]) -> dict[str, Any]:
        mapping = {
            "ph": ["ph_high", "ph_low"],
            "ec": ["ec_low", "ec_high"],
        }
        protocols = self._config.recovery_protocols
        hints = {}
        for metric in metrics:
            for key in mapping.get(metric, []):
                if key in protocols:
                    hints[key] = protocols[key]
        return hints

    def actuator_capabilities(self) -> dict[str, Any]:
        actuators = self._config.actuators
        return {
            name: {
                "allowed_actions": spec.get("allowed_actions", []),
                "max_duration_sec": spec.get("max_duration_sec"),
                "safety_note": spec.get("safety_note"),
            }
            for name, spec in actuators.items()
        }

    def can_execute(self, actuator: str, action: str, snapshot: dict[str, Any]) -> tuple[bool, str]:
        effect = CONTROL_EFFECTS.get(actuator)
        if action not in ("ON", "OPEN"):
            return True, ""
        if not effect:
            return True, ""

        metric, _ = effect
        if not self._sensor_fresh(snapshot, metric):
            return False, f"metric_telemetry_unavailable:{metric}"
        if metric in self._escalations:
            return False, f"operator_escalation_active:{metric}"
        if metric in self._pending:
            return False, f"pending_effect_recheck:{metric}"
        if metric in self._current_jump_issues:
            return False, f"implausible_jump:{metric}"
        if not self._sensor_fresh(snapshot, "water_level"):
            return False, "water_level_unknown"

        water_level = (snapshot.get("water_level") or {}).get("last")
        if water_level is not None and float(water_level) < self.low_water_limit_pct:
            return False, "low_water_level_for_correction"

        return True, ""

    def register_published_commands(
        self,
        commands: list[dict[str, Any]],
        snapshot: dict[str, Any],
        recheck_interval_sec: int | None,
    ) -> None:
        registered_metrics: set[str] = set()

        for cmd in commands:
            actuator = str(cmd.get("actuator"))
            action = str(cmd.get("action"))
            if action != "ON":
                continue
            effect = CONTROL_EFFECTS.get(actuator)
            if not effect:
                continue

            metric, expected_direction = effect
            if metric in registered_metrics:
                continue

            baseline = (snapshot.get(metric) or {}).get("last")
            if baseline is None:
                continue

            rules = self._effect_rules.get(metric, {})
            pending = PendingEffect(
                metric=metric,
                actuator=actuator,
                baseline=float(baseline),
                expected_direction=expected_direction,
                created_at=time.time(),
                recheck_after_sec=int(recheck_interval_sec or rules.get("default_recheck_sec", 900)),
                min_effect=float(rules.get("minimum_effect", 0.05)),
                wrong_direction_margin=float(rules.get("wrong_direction_margin", 0.03)),
                reason=str(cmd.get("reason", ""))[:200],
            )
            self._pending[metric] = pending
            registered_metrics.add(metric)
            self._recent_actions.append(
                {
                    "ts": pending.created_at,
                    "metric": metric,
                    "actuator": actuator,
                    "action": action,
                    "duration_sec": cmd.get("duration_sec"),
                    "reason": pending.reason,
                }
            )
            db.save_control_event(
                "dose_registered",
                "control_runtime",
                metric=metric,
                status="pending_effect",
                details=f"{actuator} recheck={pending.recheck_after_sec}s",
            )

    def reset_effect_failures(self, metric: str) -> None:
        self._effect_failures[metric] = 0

    def reset_metric(self, metric: str, operator: str = "operator") -> bool:
        if metric not in self.CONTROL_METRICS:
            return False

        changed = False
        if metric in self._escalations:
            del self._escalations[metric]
            changed = True
        if metric in self._pending:
            del self._pending[metric]
            changed = True
        if metric in self._last_effect_results:
            del self._last_effect_results[metric]
            changed = True
        self._effect_failures[metric] = 0
        self._jump_counts[metric] = 0
        self._current_jump_issues.pop(metric, None)

        db.save_control_event(
            "operator_reset",
            "control_runtime",
            metric=metric,
            status="applied" if changed else "noop",
            details=f"reset by {operator}",
        )
        return changed

    def reset_all(self, operator: str = "operator") -> list[str]:
        reset_metrics = []
        for metric in self.CONTROL_METRICS:
            if self.reset_metric(metric, operator=operator):
                reset_metrics.append(metric)
        if not reset_metrics:
            db.save_control_event(
                "operator_reset",
                "control_runtime",
                metric="all",
                status="noop",
                details=f"reset by {operator}",
            )
        return reset_metrics

    def state_snapshot(self) -> dict[str, Any]:
        return {
            "pending_effects": [pending.to_dict() for pending in self._pending.values()],
            "escalations": list(self._escalations.values()),
            "effect_failures": dict(self._effect_failures),
            "last_effect_results": dict(self._last_effect_results),
        }
