from __future__ import annotations

import argparse
import json
import os
import random
import signal
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from pydantic import ValidationError

from mqtt.topics import command_ack_topic, command_result_topic, command_topic, presence_topic, state_topic, telemetry_topic
from shared.contracts.messages import ActuatorCommandMessage, CommandAck, CommandResult, DeviceConnectivity, DeviceStateMessage, PresenceMessage, TelemetryMessage


@dataclass(frozen=True)
class SimulatorMode:
    ph: float
    ec: float
    water_level: float
    description: str


@dataclass
class TraySimState:
    device_id: str
    zone_id: str
    ph: float
    ec: float
    water_level: float
    valve_open: bool = False
    doser_active: bool = False
    connectivity: DeviceConnectivity = DeviceConnectivity.ONLINE
    last_command_ids: deque[str] = field(default_factory=lambda: deque(maxlen=128))
    last_nonces: deque[str] = field(default_factory=lambda: deque(maxlen=128))
    counter: int = 0


MODES: dict[str, SimulatorMode] = {
    "normal": SimulatorMode(ph=6.1, ec=1.7, water_level=68.0, description="Нормальный режим с плавным дрейфом."),
    "low_ph": SimulatorMode(ph=5.1, ec=1.7, water_level=66.0, description="Низкий pH, подходит для Llama path."),
    "high_ph": SimulatorMode(ph=7.1, ec=1.7, water_level=66.0, description="Высокий pH, подходит для Llama path."),
    "low_ec": SimulatorMode(ph=6.0, ec=0.9, water_level=67.0, description="Низкий EC, подходит для Llama path."),
    "high_ec": SimulatorMode(ph=6.0, ec=2.8, water_level=67.0, description="Высокий EC, подходит для Llama path."),
    "low_water": SimulatorMode(ph=6.0, ec=1.6, water_level=8.0, description="Низкий water_level, детерминированная защита."),
}

ACTIVATING_ACTIONS = {"START", "OPEN", "ON", "DIM_50"}
SAFE_ACTIONS = {"CLOSE", "OFF", "STOP", "ABORT"}
DEFAULT_TRAYS: tuple[tuple[str, str], ...] = (
    ("esp32-1", "tray_1"),
    ("esp32-2", "tray_2"),
    ("esp32-3", "tray_3"),
    ("esp32-4", "tray_4"),
)


class FakeEsp32Simulator:
    def __init__(
        self,
        *,
        broker_host: str,
        broker_port: int,
        broker_username: str,
        broker_password: str,
        publish_interval_sec: float,
        mode: str,
        valve_limit_sec: int,
        dose_limit_sec: int,
        tray_id: str | None = None,
    ) -> None:
        self.publish_interval_sec = publish_interval_sec
        self.mode_name = mode
        self.mode = MODES[mode]
        self.max_valve_open_sec = max(1, valve_limit_sec)
        self.max_dose_duration_sec = max(1, dose_limit_sec)
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._lock = threading.Lock()
        self._running = True
        self._tray_states = self._build_tray_states(tray_id)
        self._device_index = {state.device_id: state for state in self._tray_states.values()}

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-hydroponics-{uuid.uuid4().hex[:8]}")
        if broker_username:
            self.client.username_pw_set(broker_username, broker_password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def run(self) -> None:
        trays = ", ".join(f"{state.zone_id}:{state.device_id}" for state in self._tray_states.values())
        print(
            f"[SIM] starting broker={self._broker_host}:{self._broker_port} "
            f"mode={self.mode_name} trays={trays} note={self.mode.description}"
        )
        self.client.connect(self._broker_host, self._broker_port, keepalive=60)
        self.client.loop_start()
        try:
            while self._running:
                for tray in self._tray_states.values():
                    self.publish_presence(tray)
                    self.publish_state(tray)
                    self.publish_telemetry(tray)
                time.sleep(self.publish_interval_sec)
        except KeyboardInterrupt:
            print("\n[SIM] shutdown requested")
        finally:
            self._running = False
            for tray in self._tray_states.values():
                try:
                    self.publish_presence(tray, connectivity=DeviceConnectivity.OFFLINE, reason="simulator_shutdown", retain=True)
                except Exception:
                    pass
            self.client.loop_stop()
            self.client.disconnect()

    def stop(self, *_args) -> None:
        print("[SIM] stopping")
        self._running = False

    def _build_tray_states(self, tray_id: str | None) -> dict[str, TraySimState]:
        tray_states: dict[str, TraySimState] = {}
        for device_id, zone_id in DEFAULT_TRAYS:
            if tray_id and zone_id != tray_id:
                continue
            tray_states[zone_id] = TraySimState(
                device_id=device_id,
                zone_id=zone_id,
                ph=self.mode.ph + random.uniform(-0.05, 0.05),
                ec=self.mode.ec + random.uniform(-0.03, 0.03),
                water_level=self.mode.water_level + random.uniform(-1.0, 1.0),
            )
        if not tray_states:
            raise ValueError(f"unknown_tray_id:{tray_id}")
        return tray_states

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        print(f"[MQTT] connected reason_code={reason_code}")
        for tray in self._tray_states.values():
            topic = command_topic(tray.device_id)
            client.subscribe(topic, qos=1)
            print(f"[MQTT] subscribed {topic}")
            tray.connectivity = DeviceConnectivity.ONLINE
            self.publish_presence(tray)
            self.publish_state(tray)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        print(f"[MQTT] disconnected reason_code={reason_code}")
        with self._lock:
            for tray in self._tray_states.values():
                tray.connectivity = DeviceConnectivity.SAFE_MODE
                tray.valve_open = False
                tray.doser_active = False

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        raw = msg.payload.decode("utf-8", errors="replace")
        print(f"[CMD] topic={msg.topic} payload={raw}")
        try:
            payload = json.loads(raw)
            command = ActuatorCommandMessage.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            print(f"[CMD] malformed command ignored error={exc}")
            return

        tray = self._device_index.get(command.target_device_id)
        if tray is None:
            print(f"[CMD] unknown target device={command.target_device_id}")
            return
        worker = threading.Thread(target=self._handle_command, args=(tray, command), daemon=True)
        worker.start()

    def _handle_command(self, tray: TraySimState, command: ActuatorCommandMessage) -> None:
        with self._lock:
            now_ms = self._now_ms()
            if command.target_device_id != tray.device_id:
                self._publish_rejected_ack(tray, command, "invalid_target_device", "target_device_id does not match simulator")
                return
            if command.command_id in tray.last_command_ids:
                self._publish_rejected_ack(tray, command, "duplicate_command", "duplicate command_id ignored")
                return
            if command.nonce in tray.last_nonces:
                self._publish_rejected_ack(tray, command, "duplicate_command", "duplicate nonce ignored")
                return
            if command.expires_at_ms <= now_ms:
                self._publish_ack(tray, command, status="expired", status_code="stale_command", error_code="stale_command", error_message="command expired locally")
                return
            if tray.connectivity == DeviceConnectivity.SAFE_MODE and command.action.upper() in ACTIVATING_ACTIONS:
                self._publish_rejected_ack(tray, command, "device_in_safe_mode", "device currently in safe mode")
                return
            local_limit_sec = self._local_duration_limit_sec(command.actuator)
            if command.duration_sec > local_limit_sec and command.action.upper() in ACTIVATING_ACTIONS:
                self._publish_rejected_ack(
                    tray,
                    command,
                    "duration_exceeds_local_limit",
                    f"requested duration {command.duration_sec}s exceeds local limit {local_limit_sec}s",
                )
                return
            tray.last_command_ids.append(command.command_id)
            tray.last_nonces.append(command.nonce)
            if command.actuator == "nutrient_doser" and command.action.upper() == "START":
                tray.doser_active = True

        self._publish_ack(tray, command, status="received", status_code="received")
        time.sleep(0.1)
        self._publish_ack(tray, command, status="acked", status_code="acked")
        time.sleep(0.1)
        if command.action.upper() in ACTIVATING_ACTIONS:
            self._publish_ack(tray, command, status="running", status_code="running")
            time.sleep(min(max(command.duration_sec, 0), 1.5))

        with self._lock:
            self._apply_command_effect(tray, command)
            observed_state = self._observed_state(tray)

        self._publish_result(tray, command, status="completed", status_code="completed", observed_state=observed_state)
        self.publish_state(tray)
        self._log_state(tray, "post-command")

    def publish_presence(self, tray: TraySimState, *, connectivity: DeviceConnectivity | None = None, reason: str = "heartbeat", retain: bool = True) -> None:
        payload = self._presence_payload(tray, connectivity or tray.connectivity, reason=reason)
        topic = presence_topic(tray.device_id)
        self.client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=1, retain=retain)
        print(f"[PRESENCE] {tray.zone_id} {topic} -> {json.dumps(payload, ensure_ascii=False)}")

    def publish_state(self, tray: TraySimState) -> None:
        message = DeviceStateMessage(
            message_id=self._next_id("msg-state"),
            correlation_id=self._next_id("trace-state"),
            device_id=tray.device_id,
            zone_id=tray.zone_id,
            timestamp=self._now_ms(),
            connectivity=tray.connectivity,
            state=self._observed_state(tray),
            firmware_version="sim-hydroponics-1.0",
            message_counter=self._next_counter(tray),
            status={"mode": self.mode_name},
            meta={"simulator": True},
        )
        topic = state_topic(tray.device_id)
        self.client.publish(topic, json.dumps(message.model_dump(), ensure_ascii=False), qos=1, retain=True)
        print(f"[STATE] {tray.zone_id} {topic} -> {json.dumps(message.model_dump(), ensure_ascii=False)}")

    def publish_telemetry(self, tray: TraySimState) -> None:
        with self._lock:
            self._advance_environment(tray)
            message = self._build_telemetry_message(tray)
        topic = telemetry_topic(tray.device_id)
        self.client.publish(topic, json.dumps(message.model_dump(), ensure_ascii=False), qos=1)
        print(f"[TELEMETRY] {tray.zone_id} {topic} -> {json.dumps(message.model_dump(), ensure_ascii=False)}")
        self._log_state(tray, "telemetry")

    def _build_telemetry_message(self, tray: TraySimState) -> TelemetryMessage:
        return TelemetryMessage(
            message_id=self._next_id("msg-telemetry"),
            correlation_id=self._next_id("trace-telemetry"),
            device_id=tray.device_id,
            zone_id=tray.zone_id,
            timestamp=self._now_ms(),
            message_counter=self._next_counter(tray),
            sensors={
                "ph": round(tray.ph, 2),
                "ec": round(tray.ec, 2),
                "water_level": round(tray.water_level, 2),
            },
            status={
                "mode": self.mode_name,
                "valve_open": tray.valve_open,
                "doser_active": tray.doser_active,
            },
            meta={"simulator": True},
            local_timestamp=self._now_ms(),
        )

    def _publish_rejected_ack(self, tray: TraySimState, command: ActuatorCommandMessage, error_code: str, error_message: str) -> None:
        self._publish_ack(tray, command, status="rejected", status_code=error_code, error_code=error_code, error_message=error_message)
        self._publish_result(tray, command, status="aborted", status_code=error_code, error_code=error_code, error_message=error_message)

    def _publish_ack(
        self,
        tray: TraySimState,
        command: ActuatorCommandMessage,
        *,
        status: str,
        status_code: str,
        error_code: str | None = None,
        error_message: str = "",
    ) -> None:
        ack = CommandAck(
            message_id=self._next_id("msg-ack"),
            correlation_id=command.correlation_id,
            command_id=command.command_id,
            device_id=tray.device_id,
            zone_id=tray.zone_id,
            status=status,
            local_timestamp=self._now_ms(),
            observed_state=self._observed_state(tray),
            source="device",
            error_code=error_code,
            error_message=error_message,
            execution_id=command.execution_id,
            step=command.step,
            status_code=status_code,
        )
        topic = command_ack_topic(tray.device_id)
        self.client.publish(topic, json.dumps(ack.model_dump(), ensure_ascii=False), qos=1)
        print(f"[ACK] {tray.zone_id} {topic} -> {json.dumps(ack.model_dump(), ensure_ascii=False)}")

    def _publish_result(
        self,
        tray: TraySimState,
        command: ActuatorCommandMessage,
        *,
        status: str,
        status_code: str,
        error_code: str | None = None,
        error_message: str = "",
        observed_state: dict[str, Any] | None = None,
    ) -> None:
        result = CommandResult(
            message_id=self._next_id("msg-result"),
            correlation_id=command.correlation_id,
            command_id=command.command_id,
            device_id=tray.device_id,
            zone_id=tray.zone_id,
            status=status,
            local_timestamp=self._now_ms(),
            observed_state=observed_state or self._observed_state(tray),
            metrics={"dose_ml": int(command.parameters.get("dose_ml") or 0)},
            source="device",
            error_code=error_code,
            error_message=error_message,
            execution_id=command.execution_id,
            step=command.step,
            status_code=status_code,
        )
        topic = command_result_topic(tray.device_id)
        self.client.publish(topic, json.dumps(result.model_dump(), ensure_ascii=False), qos=1)
        print(f"[RESULT] {tray.zone_id} {topic} -> {json.dumps(result.model_dump(), ensure_ascii=False)}")

    def _apply_command_effect(self, tray: TraySimState, command: ActuatorCommandMessage) -> None:
        actuator = command.actuator
        action = command.action.upper()
        if actuator == "irrigation_valve":
            if action == "OPEN":
                tray.valve_open = True
            elif action == "CLOSE":
                tray.valve_open = False
        elif actuator == "nutrient_doser":
            if action == "START":
                self._apply_nutrient_dose(tray, command)
                tray.doser_active = False
            elif action == "STOP":
                tray.doser_active = False

    def _apply_nutrient_dose(self, tray: TraySimState, command: ActuatorCommandMessage) -> None:
        dose_ml = max(0.0, float(command.parameters.get("dose_ml") or 25.0))
        dose_factor = max(1.0, dose_ml / 25.0)
        tray.ec = min(3.4, tray.ec + 0.12 * dose_factor)
        tray.ph += (6.0 - tray.ph) * 0.22
        tray.water_level = max(0.0, min(100.0, tray.water_level - 0.4 * dose_factor))

    def _advance_environment(self, tray: TraySimState) -> None:
        if tray.valve_open:
            tray.water_level = min(100.0, tray.water_level + random.uniform(0.6, 1.4))
            tray.ec += (self.mode.ec - tray.ec) * 0.08
            tray.ph += (self.mode.ph - tray.ph) * 0.06
        else:
            tray.water_level = max(0.0, tray.water_level - random.uniform(0.25, 0.6))
            tray.ec += (self.mode.ec - tray.ec) * 0.03 + random.uniform(-0.02, 0.02)
            tray.ph += (self.mode.ph - tray.ph) * 0.03 + random.uniform(-0.03, 0.03)
        tray.ph = max(4.5, min(8.0, tray.ph))
        tray.ec = max(0.4, min(3.6, tray.ec))

    def _presence_payload(self, tray: TraySimState, connectivity: DeviceConnectivity, *, reason: str) -> dict[str, Any]:
        message = PresenceMessage(
            message_id=self._next_id("msg-presence"),
            correlation_id=self._next_id("trace-presence"),
            device_id=tray.device_id,
            zone_id=tray.zone_id,
            timestamp=self._now_ms(),
            message_counter=self._next_counter(tray),
            connectivity=connectivity,
            status={"reason": reason},
            meta={"simulator": True},
        )
        return message.model_dump()

    def _observed_state(self, tray: TraySimState) -> dict[str, Any]:
        return {
            "valve_open": tray.valve_open,
            "doser_active": tray.doser_active,
            "connectivity": tray.connectivity.value,
        }

    def _local_duration_limit_sec(self, actuator: str) -> int:
        if actuator == "nutrient_doser":
            return self.max_dose_duration_sec
        if actuator == "irrigation_valve":
            return self.max_valve_open_sec
        return max(self.max_valve_open_sec, self.max_dose_duration_sec)

    @staticmethod
    def _next_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _next_counter(tray: TraySimState) -> int:
        tray.counter += 1
        return tray.counter

    def _log_state(self, tray: TraySimState, label: str) -> None:
        print(
            f"[STATE:{label}] {tray.zone_id} connectivity={tray.connectivity.value} "
            f"valve_open={tray.valve_open} doser_active={tray.doser_active} "
            f"ph={tray.ph:.2f} ec={tray.ec:.2f} water_level={tray.water_level:.2f}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fake ESP32 hydroponic simulator for the current backend contracts.")
    parser.add_argument("--broker-host", default=os.getenv("MQTT_HOST", "localhost"))
    parser.add_argument("--broker-port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--broker-username", default=os.getenv("MQTT_USERNAME", ""))
    parser.add_argument("--broker-password", default=os.getenv("MQTT_PASSWORD", ""))
    parser.add_argument("--tray-id", choices=[zone_id for _, zone_id in DEFAULT_TRAYS], default=None)
    parser.add_argument("--mode", choices=sorted(MODES.keys()), default="normal")
    parser.add_argument("--interval-sec", type=float, default=float(os.getenv("SIM_PUBLISH_INTERVAL_SEC", "3")))
    parser.add_argument("--max-valve-open-sec", type=int, default=int(os.getenv("SIM_MAX_VALVE_OPEN_SEC", "15")))
    parser.add_argument("--max-dose-duration-sec", type=int, default=int(os.getenv("SIM_MAX_DOSE_DURATION_SEC", "10")))
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    simulator = FakeEsp32Simulator(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        broker_username=args.broker_username,
        broker_password=args.broker_password,
        publish_interval_sec=args.interval_sec,
        mode=args.mode,
        valve_limit_sec=args.max_valve_open_sec,
        dose_limit_sec=args.max_dose_duration_sec,
        tray_id=args.tray_id,
    )
    signal.signal(signal.SIGINT, simulator.stop)
    signal.signal(signal.SIGTERM, simulator.stop)
    simulator.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
