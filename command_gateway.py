from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Optional

import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, ValidationError

import database as db

log = logging.getLogger("command_gateway")


class ParsedCommand(BaseModel):
    actuator: str
    action: str
    duration_sec: Optional[int] = Field(default=None, ge=0)
    reason: str = ""


class ParsedPolicy(BaseModel):
    situation_summary: str = ""
    selected_control_strategy: str = ""
    risks: list[str] = []
    commands: list[ParsedCommand] = []
    recheck_interval_sec: Optional[int] = Field(default=None, ge=0)
    stop_conditions: list[str] = []


class HybridCommandGateway:
    def __init__(
        self,
        mqtt_client: mqtt.Client,
        safety: Any,
        *,
        dry_run: bool,
        actuator_topics: dict[str, str],
        allowed_actions: dict[str, list[str]],
        max_duration: dict[str, int],
        max_consecutive_doses: dict[str, int],
        conflicting_pairs: list[tuple[str, str]],
        dose_reset_sec: int,
        recovery_protocols: dict[str, Any],
        check_level: Any,
        thresholds: dict[str, Any],
    ) -> None:
        self._client = mqtt_client
        self._safety = safety
        self._dry_run = dry_run
        self._actuator_topics = actuator_topics
        self._allowed_actions = allowed_actions
        self._max_duration = max_duration
        self._max_consecutive_doses = max_consecutive_doses
        self._conflicting_pairs = conflicting_pairs
        self._dose_reset_sec = dose_reset_sec
        self._recovery_protocols = recovery_protocols
        self._check_level = check_level
        self._thresholds = thresholds
        self._req_id: Optional[int] = None
        self._dose_counts: dict[str, int] = {}
        self._dose_reset: dict[str, float] = {}

    def set_req_id(self, req_id: Optional[int]) -> None:
        self._req_id = req_id

    def _dose_ok(self, actuator: str) -> bool:
        max_d = self._max_consecutive_doses.get(actuator)
        if max_d is None:
            return True
        now = time.time()
        if now - self._dose_reset.get(actuator, 0) > self._dose_reset_sec:
            self._dose_counts[actuator] = 0
            self._dose_reset[actuator] = now
        count = self._dose_counts.get(actuator, 0)
        if count >= max_d:
            log.warning("Blocked %s: consecutive doses %d/%d", actuator, count, max_d)
            return False
        return True

    def _inc_dose(self, actuator: str) -> None:
        self._dose_counts[actuator] = self._dose_counts.get(actuator, 0) + 1

    def process(self, raw: str, snapshot: dict, request_type: str) -> tuple[bool, Optional[ParsedPolicy]]:
        parsed = self._parse(raw)
        if parsed is None:
            db.save_control_event("policy_result", "command_gateway", status="parse_failed", details=request_type)
            return False, None

        if parsed.situation_summary:
            log.info("Policy: %s", parsed.situation_summary[:160])
        if parsed.selected_control_strategy:
            log.info("Strategy: %s", parsed.selected_control_strategy[:160])
        db.save_control_event(
            "policy_result",
            "command_gateway",
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
            db.save_control_event("policy_result", "command_gateway", status="no_commands", details=parsed.situation_summary[:200])
            return True, parsed

        validated = self._validate(parsed.commands, snapshot)
        if not validated:
            db.save_control_event("command_batch", "command_gateway", status="rejected", details="all commands rejected")
            return False, parsed

        published = False
        published_commands: list[dict[str, Any]] = []
        for cmd in validated:
            topic = self._actuator_topics[cmd.actuator]
            ok, status = self._publish_action(topic, cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason)
            if ok:
                db.update_actuator_state(cmd.actuator, cmd.action)
                published = True
                published_commands.append(cmd.model_dump())
                if cmd.action in ("ON", "OPEN"):
                    self._inc_dose(cmd.actuator)
                if cmd.duration_sec and cmd.action not in ("OFF", "CLOSE"):
                    self._auto_off(cmd.actuator, cmd.duration_sec)

            db.save_command(self._req_id, cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason, topic, status)
            db.save_control_event(
                "command_execution",
                "command_gateway",
                metric=cmd.actuator,
                status=status,
                details=f"{cmd.action} duration={cmd.duration_sec} reason={cmd.reason[:120]}",
            )

        if published_commands and not self._dry_run:
            self._safety.register_published_commands(published_commands, snapshot, parsed.recheck_interval_sec)
        elif published_commands and self._dry_run:
            log.info("Dry-run active: effect tracking skipped for %d command(s)", len(published_commands))
            db.save_control_event("dry_run", "command_gateway", status="effect_tracking_skipped", details=f"commands={len(published_commands)}")
        return published, parsed

    def _parse(self, raw: str) -> Optional[ParsedPolicy]:
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
                return None
            try:
                data = json.loads(m.group())
            except json.JSONDecodeError:
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
            return ParsedPolicy.model_validate(data)
        except ValidationError:
            return None

    def _validate(self, commands: list[ParsedCommand], snapshot: dict) -> list[ParsedCommand]:
        accepted = []
        applied: dict[str, str] = {}
        has_ec_dose = any(cmd.actuator in ("nutrient_a", "nutrient_b") and cmd.action == "ON" for cmd in commands)

        for cmd in commands:
            if cmd.actuator not in self._allowed_actions:
                db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="not_allowed", details=cmd.action)
                continue
            if cmd.action not in self._allowed_actions[cmd.actuator]:
                db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="bad_action", details=cmd.action)
                continue
            if has_ec_dose and cmd.actuator in ("ph_down_pump", "ph_up_pump") and cmd.action == "ON":
                db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="ph_after_ec_block", details=cmd.action)
                continue
            if cmd.action in ("ON", "OPEN") and not self._dose_ok(cmd.actuator):
                db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="dose_limit", details=cmd.action)
                continue

            allowed, reason = self._safety.can_execute(cmd.actuator, cmd.action, snapshot)
            if not allowed:
                db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="safety_block", details=reason)
                continue

            if cmd.duration_sec is not None:
                max_d = self._max_duration.get(cmd.actuator)
                if max_d and cmd.duration_sec > max_d:
                    cmd = ParsedCommand(actuator=cmd.actuator, action=cmd.action, duration_sec=max_d, reason=cmd.reason)

            if cmd.actuator == "drain_valve" and cmd.action == "OPEN":
                wl = (snapshot.get("water_level") or {}).get("last")
                if wl is not None and wl < 25.0:
                    db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="low_water", details=str(wl))
                    continue

            conflict = False
            for a, b in self._conflicting_pairs:
                other = b if cmd.actuator == a else (a if cmd.actuator == b else None)
                if other and applied.get(other) == "ON" and cmd.action == "ON":
                    db.save_control_event("command_rejected", "command_gateway", metric=cmd.actuator, status="conflict", details=other)
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
        if self._dry_run:
            log.info("[DRY_RUN] %-46s -> %-8s | %s", topic, action, reason)
            return True, "DRY_RUN"

        result = self._client.publish(topic, action)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("Publish failed %s rc=%d", topic, result.rc)
            return False, "FAIL"
        log.info("Publish %-46s -> %-8s | %s", topic, action, reason)
        return True, "OK"

    def _publish_direct(self, actuator: str, action: str, duration_sec: Optional[int], reason: str) -> bool:
        topic = self._actuator_topics.get(actuator)
        if not topic:
            return False
        ok, status = self._publish_action(topic, actuator, action, duration_sec, reason)
        if not ok:
            return False
        db.update_actuator_state(actuator, action)
        if action in ("ON", "OPEN"):
            self._inc_dose(actuator)
        if duration_sec and action not in ("OFF", "CLOSE"):
            self._auto_off(actuator, duration_sec)
        db.save_command(self._req_id, actuator, action, duration_sec, reason, topic, status)
        db.save_control_event("fallback_command", "command_gateway", metric=actuator, status=status, details=f"{action} duration={duration_sec} reason={reason[:120]}")
        return True

    def _fallback_spec(self, protocol_key: str, severity: str) -> tuple[list[dict[str, Any]], int | None]:
        protocol = self._recovery_protocols.get(protocol_key, {})
        spec = protocol.get("severity_map", {}).get(severity) or protocol.get("severity_map", {}).get("WARNING") or {}
        recheck_sec = int(spec["wait_min"]) * 60 if spec.get("wait_min") is not None else None
        commands = []
        actuator = spec.get("actuator")
        if actuator:
            commands.append({"actuator": actuator, "action": spec.get("action", "ON"), "duration_sec": spec.get("duration_sec"), "reason": f"fallback:{protocol_key}:{severity}"})
        also = spec.get("also")
        if also:
            commands.append({"actuator": also, "action": "ON", "duration_sec": spec.get("duration_sec"), "reason": f"fallback:{protocol_key}:{severity}"})
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
                commands, recheck_sec = self._fallback_spec("ph_high", self._check_level(float(ph), self._thresholds["ph"]))
            elif float(ph) < float(ph_range[0]):
                commands, recheck_sec = self._fallback_spec("ph_low", self._check_level(float(ph), self._thresholds["ph"]))

        if not commands and ec is not None and float(ec) < float(ec_range[0]):
            commands, recheck_sec = self._fallback_spec("ec_low", self._check_level(float(ec), self._thresholds["ec"]))

        validated = self._validate([ParsedCommand.model_validate(cmd) for cmd in commands], snapshot)
        if not validated:
            log.warning("Fallback did not find any safe commands")
            return False

        published_commands: list[dict[str, Any]] = []
        for cmd in validated:
            if self._publish_direct(cmd.actuator, cmd.action, cmd.duration_sec, cmd.reason):
                published_commands.append(cmd.model_dump())

        if published_commands and not self._dry_run:
            self._safety.register_published_commands(published_commands, snapshot, recheck_sec)
        elif published_commands and self._dry_run:
            db.save_control_event("dry_run", "command_gateway", status="fallback_effect_tracking_skipped", details=f"commands={len(published_commands)}")
            return True
        return bool(published_commands)

    def _auto_off(self, actuator: str, duration_sec: int) -> None:
        def _off() -> None:
            time.sleep(duration_sec)
            off_action = "CLOSE" if actuator == "drain_valve" else "OFF"
            topic = self._actuator_topics.get(actuator)
            if not topic:
                return
            if self._dry_run:
                log.info("[DRY_RUN] AUTO-%-5s %-46s (after %ds)", off_action, topic, duration_sec)
                db.update_actuator_state(actuator, off_action)
                return
            result = self._client.publish(topic, off_action)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log.info("AUTO-%-5s %-46s (after %ds)", off_action, topic, duration_sec)
                db.update_actuator_state(actuator, off_action)

        threading.Thread(target=_off, daemon=True, name=f"AutoOff-{actuator}").start()
