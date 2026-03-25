from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, ValidationError

import database as db

log = logging.getLogger("command_gateway")
ACK_PENDING_STATES = {"sent", "received", "executing"}
ACK_FINAL_STATES = {"done", "failed", "timeout"}


@dataclass
class PendingCommand:
    command_id: str
    actuator: str
    action: str
    topic: str
    reason: str
    duration_sec: Optional[int]
    created_at: float
    timeout_at: float
    snapshot: Optional[dict[str, Any]]
    recheck_interval_sec: Optional[int]
    llm_req_id: Optional[int]
    ack_state: str = "sent"


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
        ack_timeout_sec: int,
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
        self._ack_timeout_sec = ack_timeout_sec
        self._req_id: Optional[int] = None
        self._dose_counts: dict[str, int] = {}
        self._dose_reset: dict[str, float] = {}
        self._pending_commands: dict[str, PendingCommand] = {}
        self._pending_lock = threading.Lock()

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

    @staticmethod
    def _new_command_id() -> str:
        return f"cmd-{uuid.uuid4().hex[:12]}"

    def poll_ack_timeouts(self) -> None:
        if self._dry_run:
            return

        now = time.time()
        timed_out: list[PendingCommand] = []
        with self._pending_lock:
            for command_id, pending in list(self._pending_commands.items()):
                if pending.timeout_at <= now and pending.ack_state in ACK_PENDING_STATES:
                    timed_out.append(self._pending_commands.pop(command_id))

        for pending in timed_out:
            log.error(
                "Command ack timeout: command_id=%s actuator=%s action=%s topic=%s",
                pending.command_id,
                pending.actuator,
                pending.action,
                pending.topic,
            )
            db.update_command_status(pending.command_id, "ACK_TIMEOUT", ack_state="timeout")
            db.save_control_event(
                "command_ack",
                "command_gateway",
                metric=pending.actuator,
                status="timeout",
                details=f"command_id={pending.command_id} action={pending.action}",
                context_json=json.dumps(
                    {
                        "command_id": pending.command_id,
                        "topic": pending.topic,
                        "action": pending.action,
                        "reason": pending.reason,
                    },
                    ensure_ascii=False,
                ),
            )

    def handle_ack(self, topic: str, payload: bytes) -> None:
        try:
            data = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            log.warning("Ignoring malformed command ack on %s", topic)
            return

        if not isinstance(data, dict):
            return

        command_id = str(data.get("command_id", "")).strip()
        ack_state = str(data.get("ack_state") or data.get("state") or data.get("status") or "").strip().lower()
        if not command_id or ack_state not in ACK_PENDING_STATES | ACK_FINAL_STATES:
            log.warning("Ignoring command ack with missing command_id/state on %s", topic)
            return

        with self._pending_lock:
            pending = self._pending_commands.get(command_id)
            if pending is None:
                db.save_control_event(
                    "command_ack",
                    "command_gateway",
                    status="unknown_command",
                    details=f"command_id={command_id} state={ack_state}",
                    context_json=json.dumps(data, ensure_ascii=False)[:4000],
                )
                return
            pending.ack_state = ack_state
            if ack_state in ACK_FINAL_STATES:
                self._pending_commands.pop(command_id, None)

        details = str(data.get("message") or data.get("error") or "").strip()
        log.info(
            "Command ack: command_id=%s actuator=%s state=%s %s",
            command_id,
            pending.actuator,
            ack_state,
            details,
        )
        db.update_command_status(
            command_id,
            "EXECUTED" if ack_state == "done" else ("FAILED" if ack_state == "failed" else f"ACK_{ack_state.upper()}"),
            ack_state=ack_state,
        )
        db.save_control_event(
            "command_ack",
            "command_gateway",
            metric=pending.actuator,
            status=ack_state,
            details=f"command_id={command_id} action={pending.action} {details}".strip(),
            context_json=json.dumps(data, ensure_ascii=False)[:4000],
        )

        if ack_state == "done":
            db.update_actuator_state(pending.actuator, pending.action)
            if pending.action in ("ON", "OPEN"):
                self._inc_dose(pending.actuator)
            if pending.duration_sec and pending.action not in ("OFF", "CLOSE"):
                self._auto_off(pending.actuator, pending.duration_sec)
            if pending.snapshot is not None:
                self._safety.register_published_commands(
                    [
                        {
                            "actuator": pending.actuator,
                            "action": pending.action,
                            "duration_sec": pending.duration_sec,
                            "reason": pending.reason,
                        }
                    ],
                    pending.snapshot,
                    pending.recheck_interval_sec,
                )

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
            ok, status, command_id = self._publish_action(
                topic,
                cmd.actuator,
                cmd.action,
                cmd.duration_sec,
                cmd.reason,
                snapshot=snapshot,
                recheck_interval_sec=parsed.recheck_interval_sec,
            )
            if ok:
                published = True
                published_commands.append(cmd.model_dump())

            db.save_command(
                self._req_id,
                cmd.actuator,
                cmd.action,
                cmd.duration_sec,
                cmd.reason,
                topic,
                status,
                command_id=command_id,
                ack_state="done" if status == "DRY_RUN" else ("sent" if ok else None),
            )
            db.save_control_event(
                "command_execution",
                "command_gateway",
                metric=cmd.actuator,
                status=status,
                details=f"{cmd.action} duration={cmd.duration_sec} reason={cmd.reason[:120]} command_id={command_id or '-'}",
            )

        if published_commands and self._dry_run:
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
        *,
        snapshot: Optional[dict[str, Any]] = None,
        recheck_interval_sec: Optional[int] = None,
    ) -> tuple[bool, str, Optional[str]]:
        if self._dry_run:
            log.info("[DRY_RUN] %-46s -> %-8s | %s", topic, action, reason)
            return True, "DRY_RUN", None

        command_id = self._new_command_id()
        payload = json.dumps(
            {
                "command_id": command_id,
                "actuator": actuator,
                "action": action,
                "duration_sec": duration_sec,
                "reason": reason[:200],
                "llm_req_id": self._req_id,
                "issued_at": int(time.time()),
            },
            ensure_ascii=False,
        )
        result = self._client.publish(topic, payload)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("Publish failed %s rc=%d", topic, result.rc)
            return False, "FAIL", command_id
        with self._pending_lock:
            self._pending_commands[command_id] = PendingCommand(
                command_id=command_id,
                actuator=actuator,
                action=action,
                topic=topic,
                reason=reason[:200],
                duration_sec=duration_sec,
                created_at=time.time(),
                timeout_at=time.time() + self._ack_timeout_sec,
                snapshot=snapshot,
                recheck_interval_sec=recheck_interval_sec,
                llm_req_id=self._req_id,
            )
        log.info("Publish %-46s -> %-8s | %s | command_id=%s", topic, action, reason, command_id)
        db.save_control_event(
            "command_dispatch",
            "command_gateway",
            metric=actuator,
            status="pending_ack",
            details=f"{action} command_id={command_id}",
            context_json=payload[:4000],
        )
        return True, "PENDING_ACK", command_id

    def _publish_direct(
        self,
        actuator: str,
        action: str,
        duration_sec: Optional[int],
        reason: str,
        *,
        snapshot: Optional[dict[str, Any]] = None,
        recheck_interval_sec: Optional[int] = None,
    ) -> bool:
        topic = self._actuator_topics.get(actuator)
        if not topic:
            return False
        ok, status, command_id = self._publish_action(
            topic,
            actuator,
            action,
            duration_sec,
            reason,
            snapshot=snapshot,
            recheck_interval_sec=recheck_interval_sec,
        )
        if not ok:
            return False
        if self._dry_run:
            db.update_actuator_state(actuator, action)
            if action in ("ON", "OPEN"):
                self._inc_dose(actuator)
            if duration_sec and action not in ("OFF", "CLOSE"):
                self._auto_off(actuator, duration_sec)
        db.save_command(
            self._req_id,
            actuator,
            action,
            duration_sec,
            reason,
            topic,
            status,
            command_id=command_id,
            ack_state="done" if status == "DRY_RUN" else ("sent" if ok else None),
        )
        db.save_control_event(
            "fallback_command",
            "command_gateway",
            metric=actuator,
            status=status,
            details=f"{action} duration={duration_sec} reason={reason[:120]} command_id={command_id or '-'}",
        )
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
            if self._publish_direct(
                cmd.actuator,
                cmd.action,
                cmd.duration_sec,
                cmd.reason,
                snapshot=snapshot,
                recheck_interval_sec=recheck_sec,
            ):
                published_commands.append(cmd.model_dump())

        if published_commands and self._dry_run:
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
            ok, status, command_id = self._publish_action(
                topic,
                actuator,
                off_action,
                None,
                f"auto_off:{duration_sec}s",
            )
            if ok:
                db.save_command(
                    self._req_id,
                    actuator,
                    off_action,
                    None,
                    f"auto_off:{duration_sec}s",
                    topic,
                    status,
                    command_id=command_id,
                    ack_state="sent",
                )
                log.info("AUTO-%-5s %-46s (after %ds) | command_id=%s", off_action, topic, duration_sec, command_id)

        threading.Thread(target=_off, daemon=True, name=f"AutoOff-{actuator}").start()
