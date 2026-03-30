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
from dataclasses import dataclass
from typing import Any

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from pydantic import ValidationError

from mqtt.topics import command_topic, command_ack_topic, command_result_topic, presence_topic, state_topic, telemetry_topic
from shared.contracts.messages import ActuatorCommandMessage, CommandAck, CommandResult, DeviceConnectivity, DeviceStateMessage, PresenceMessage, TelemetryMessage


@dataclass(frozen=True)
class SimulatorMode:
    soil_moisture: float
    temperature: float
    tank_level: float
    humidity: float
    pressure_kpa: float


MODES: dict[str, SimulatorMode] = {
    "normal": SimulatorMode(soil_moisture=24.0, temperature=24.0, tank_level=82.0, humidity=55.0, pressure_kpa=145.0),
    "dry": SimulatorMode(soil_moisture=15.5, temperature=24.0, tank_level=78.0, humidity=48.0, pressure_kpa=150.0),
    "wet": SimulatorMode(soil_moisture=42.0, temperature=23.0, tank_level=85.0, humidity=60.0, pressure_kpa=140.0),
    "hot": SimulatorMode(soil_moisture=26.0, temperature=32.0, tank_level=80.0, humidity=44.0, pressure_kpa=150.0),
}

ACTIVATING_ACTIONS = {"START", "OPEN", "ON", "DIM_50"}
SAFE_ACTIONS = {"CLOSE", "OFF", "ABORT"}


class FakeEsp32Simulator:
    def __init__(
        self,
        *,
        broker_host: str,
        broker_port: int,
        broker_username: str,
        broker_password: str,
        publish_interval_sec: float,
        device_id: str,
        zone_id: str,
        mode: str,
        valve_limit_sec: int,
        pump_limit_sec: int,
    ) -> None:
        self.device_id = device_id
        self.zone_id = zone_id
        self.publish_interval_sec = publish_interval_sec
        self.mode_name = mode
        self.mode = MODES[mode]
        self.max_valve_open_sec = max(1, valve_limit_sec)
        self.max_pump_run_sec = max(1, pump_limit_sec)
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._counter = 0
        self._lock = threading.Lock()
        self._running = True
        self._connectivity = DeviceConnectivity.ONLINE
        self._valve_open = False
        self._pump_on = False
        self._maintenance_mode = False
        self._last_command_ids: deque[str] = deque(maxlen=128)
        self._last_nonces: deque[str] = deque(maxlen=128)
        self._active_command_threads: dict[str, threading.Thread] = {}
        self._soil_moisture = self.mode.soil_moisture
        self._tank_level = self.mode.tank_level
        self._temperature = self.mode.temperature
        self._humidity = self.mode.humidity
        self._pressure_kpa = self.mode.pressure_kpa

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{device_id}")
        if broker_username:
            self.client.username_pw_set(broker_username, broker_password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.will_set(
            presence_topic(self.device_id),
            payload=json.dumps(self._presence_payload(DeviceConnectivity.OFFLINE, reason="mqtt_disconnect", lwt=True)),
            qos=1,
            retain=True,
        )

    def run(self) -> None:
        print(
            f"[SIM] starting broker={self._broker_host}:{self._broker_port} "
            f"device_id={self.device_id} zone_id={self.zone_id} mode={self.mode_name}"
        )
        self.client.connect(self._broker_host, self._broker_port, keepalive=60)
        self.client.loop_start()
        try:
            while self._running:
                self.publish_presence()
                self.publish_state()
                self.publish_telemetry()
                time.sleep(self.publish_interval_sec)
        except KeyboardInterrupt:
            print("\n[SIM] shutdown requested")
        finally:
            self._running = False
            self.client.loop_stop()
            self.client.disconnect()

    def stop(self, *_args) -> None:
        print("[SIM] stopping")
        self._running = False

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        print(f"[MQTT] connected reason_code={reason_code}")
        client.subscribe(command_topic(self.device_id), qos=1)
        print(f"[MQTT] subscribed {command_topic(self.device_id)}")
        self._connectivity = DeviceConnectivity.ONLINE
        self.publish_presence()
        self.publish_state()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        print(f"[MQTT] disconnected reason_code={reason_code}")
        with self._lock:
            self._connectivity = DeviceConnectivity.SAFE_MODE
            self._pump_on = False
            self._valve_open = False

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        print(f"[CMD] topic={msg.topic} payload={msg.payload.decode('utf-8', errors='replace')}")
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            command = ActuatorCommandMessage.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            print(f"[CMD] malformed command ignored error={exc}")
            return

        worker = threading.Thread(target=self._handle_command, args=(command,), daemon=True)
        self._active_command_threads[command.command_id] = worker
        worker.start()

    def _handle_command(self, command: ActuatorCommandMessage) -> None:
        with self._lock:
            now_ms = self._now_ms()
            if command.target_device_id != self.device_id:
                self._publish_rejected_ack(command, "invalid_target_device", "target_device_id does not match simulator")
                return
            if command.command_id in self._last_command_ids:
                self._publish_rejected_ack(command, "duplicate_command", "duplicate command_id ignored")
                return
            if command.nonce in self._last_nonces:
                self._publish_rejected_ack(command, "duplicate_command", "duplicate nonce ignored")
                return
            if command.expires_at_ms <= now_ms:
                self._publish_ack(command, status="expired", status_code="stale_command", error_code="stale_command", error_message="command expired locally")
                return
            if self._connectivity == DeviceConnectivity.SAFE_MODE and command.action.upper() in ACTIVATING_ACTIONS:
                self._publish_rejected_ack(command, "device_in_safe_mode", "device currently in safe mode")
                return
            local_limit_sec = self._local_duration_limit_sec(command.actuator)
            if command.duration_sec > local_limit_sec and command.action.upper() in ACTIVATING_ACTIONS:
                self._publish_rejected_ack(
                    command,
                    "duration_exceeds_local_limit",
                    f"requested duration {command.duration_sec}s exceeds local limit {local_limit_sec}s",
                )
                return
            self._last_command_ids.append(command.command_id)
            self._last_nonces.append(command.nonce)

        self._publish_ack(command, status="received", status_code="received")
        time.sleep(0.1)
        self._publish_ack(command, status="acked", status_code="acked")
        time.sleep(0.1)
        if command.action.upper() in ACTIVATING_ACTIONS:
            self._publish_ack(command, status="running", status_code="running")
            time.sleep(min(max(command.duration_sec, 0), 1))

        with self._lock:
            self._apply_command_effect(command)
            observed_state = self._observed_state()

        self._publish_result(command, status="completed", status_code="completed", observed_state=observed_state)
        self.publish_state()
        self._log_state("post-command")

    def publish_presence(self) -> None:
        payload = self._presence_payload(self._connectivity, reason="heartbeat")
        topic = presence_topic(self.device_id)
        self.client.publish(topic, json.dumps(payload), qos=1, retain=True)
        print(f"[PRESENCE] {topic} -> {json.dumps(payload, ensure_ascii=False)}")

    def publish_state(self) -> None:
        message = DeviceStateMessage(
            message_id=self._next_id("msg-state"),
            correlation_id=self._next_id("trace-state"),
            device_id=self.device_id,
            zone_id=self.zone_id,
            timestamp=self._now_ms(),
            connectivity=self._connectivity,
            state=self._observed_state(),
            firmware_version="sim-esp32-1.0",
            message_counter=self._next_counter(),
            status={"mode": self.mode_name},
            meta={"simulator": True},
        )
        topic = state_topic(self.device_id)
        self.client.publish(topic, json.dumps(message.model_dump(), ensure_ascii=False), qos=1, retain=True)
        print(f"[STATE] {topic} -> {json.dumps(message.model_dump(), ensure_ascii=False)}")

    def publish_telemetry(self) -> None:
        with self._lock:
            self._advance_environment()
            sensors = {
                "soil_moisture": round(self._soil_moisture, 2),
                "temperature": round(self._temperature, 2),
                "humidity": round(self._humidity, 2),
                "tank_level": round(self._tank_level, 2),
                "flow_rate_ml_per_min": 120.0 if self._pump_on and self._valve_open else 0.0,
                "pressure_kpa": round(self._pressure_kpa, 2),
                "leak": False,
                "overflow": False,
                "water_available": self._tank_level > 0.0,
            }
        message = TelemetryMessage(
            message_id=self._next_id("msg-telemetry"),
            correlation_id=self._next_id("trace-telemetry"),
            device_id=self.device_id,
            zone_id=self.zone_id,
            timestamp=self._now_ms(),
            message_counter=self._next_counter(),
            sensors=sensors,
            status={"mode": self.mode_name},
            meta={"simulator": True},
            local_timestamp=self._now_ms(),
        )
        topic = telemetry_topic(self.device_id)
        self.client.publish(topic, json.dumps(message.model_dump(), ensure_ascii=False), qos=1)
        print(f"[TELEMETRY] {topic} -> {json.dumps(message.model_dump(), ensure_ascii=False)}")
        self._log_state("telemetry")

    def _publish_rejected_ack(self, command: ActuatorCommandMessage, error_code: str, error_message: str) -> None:
        self._publish_ack(command, status="rejected", status_code=error_code, error_code=error_code, error_message=error_message)
        self._publish_result(command, status="aborted", status_code=error_code, error_code=error_code, error_message=error_message)

    def _publish_ack(
        self,
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
            device_id=self.device_id,
            zone_id=self.zone_id,
            status=status,
            local_timestamp=self._now_ms(),
            observed_state=self._observed_state(),
            source="device",
            error_code=error_code,
            error_message=error_message,
            execution_id=command.execution_id,
            step=command.step,
            status_code=status_code,
        )
        topic = command_ack_topic(self.device_id)
        self.client.publish(topic, json.dumps(ack.model_dump(), ensure_ascii=False), qos=1)
        print(f"[ACK] {topic} -> {json.dumps(ack.model_dump(), ensure_ascii=False)}")

    def _publish_result(
        self,
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
            device_id=self.device_id,
            zone_id=self.zone_id,
            status=status,
            local_timestamp=self._now_ms(),
            observed_state=observed_state or self._observed_state(),
            metrics={"flow_rate_ml_per_min": 120.0 if self._pump_on and self._valve_open else 0.0},
            source="device",
            error_code=error_code,
            error_message=error_message,
            execution_id=command.execution_id,
            step=command.step,
            status_code=status_code,
        )
        topic = command_result_topic(self.device_id)
        self.client.publish(topic, json.dumps(result.model_dump(), ensure_ascii=False), qos=1)
        print(f"[RESULT] {topic} -> {json.dumps(result.model_dump(), ensure_ascii=False)}")

    def _apply_command_effect(self, command: ActuatorCommandMessage) -> None:
        actuator = command.actuator
        action = command.action.upper()
        if actuator == "irrigation_valve":
            if action == "OPEN":
                self._valve_open = True
            elif action == "CLOSE":
                self._valve_open = False
        elif actuator == "master_pump":
            if action == "ON":
                self._pump_on = True
            elif action == "OFF":
                self._pump_on = False
        elif actuator == "irrigation_sequence":
            if action == "START":
                self._valve_open = True
                self._pump_on = True
            elif action == "ABORT":
                self._valve_open = False
                self._pump_on = False
        elif actuator == "vent_fan":
            pass

        if not self._valve_open:
            self._pump_on = False if command.step == "close_valve" else self._pump_on

    def _advance_environment(self) -> None:
        if self._pump_on and self._valve_open:
            self._soil_moisture = min(60.0, self._soil_moisture + random.uniform(1.0, 2.0))
            self._tank_level = max(0.0, self._tank_level - random.uniform(0.2, 0.8))
            self._pressure_kpa = max(90.0, min(220.0, self._pressure_kpa + random.uniform(-4.0, 4.0)))
        else:
            if self.mode_name == "dry":
                self._soil_moisture = max(8.0, self._soil_moisture - random.uniform(0.1, 0.4))
            elif self.mode_name == "wet":
                self._soil_moisture = max(30.0, min(55.0, self._soil_moisture + random.uniform(-0.3, 0.2)))
            else:
                self._soil_moisture = max(18.0, min(35.0, self._soil_moisture + random.uniform(-0.3, 0.3)))
            self._pressure_kpa = max(100.0, min(170.0, self._pressure_kpa + random.uniform(-1.5, 1.5)))

        temp_base = self.mode.temperature
        self._temperature = max(temp_base - 1.0, min(temp_base + 1.0, self._temperature + random.uniform(-0.4, 0.4)))
        self._humidity = max(35.0, min(75.0, self._humidity + random.uniform(-1.0, 1.0)))

    def _presence_payload(self, connectivity: DeviceConnectivity, *, reason: str, lwt: bool = False) -> dict[str, Any]:
        message = PresenceMessage(
            message_id=self._next_id("msg-presence"),
            correlation_id=self._next_id("trace-presence"),
            device_id=self.device_id,
            zone_id=self.zone_id,
            timestamp=self._now_ms(),
            message_counter=self._next_counter(),
            connectivity=connectivity,
            status={"reason": reason},
            meta={"simulator": True, "lwt": lwt},
        )
        return message.model_dump()

    def _observed_state(self) -> dict[str, Any]:
        return {
            "valve_open": self._valve_open,
            "pump_on": self._pump_on,
            "maintenance_mode": self._maintenance_mode,
            "connectivity": self._connectivity.value,
        }

    def _local_duration_limit_sec(self, actuator: str) -> int:
        if actuator == "master_pump":
            return self.max_pump_run_sec
        if actuator in {"irrigation_valve", "irrigation_sequence"}:
            return self.max_valve_open_sec
        return max(self.max_pump_run_sec, self.max_valve_open_sec)

    def _next_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    def _next_counter(self) -> int:
        self._counter += 1
        return self._counter

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _log_state(self, label: str) -> None:
        print(
            f"[STATE:{label}] mode={self.mode_name} connectivity={self._connectivity.value} "
            f"valve_open={self._valve_open} pump_on={self._pump_on} "
            f"soil={self._soil_moisture:.1f} temp={self._temperature:.1f} tank={self._tank_level:.1f}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fake ESP32 simulator for the current greenhouse backend contracts.")
    parser.add_argument("--broker-host", default=os.getenv("MQTT_HOST", "localhost"))
    parser.add_argument("--broker-port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--broker-username", default=os.getenv("MQTT_USERNAME", ""))
    parser.add_argument("--broker-password", default=os.getenv("MQTT_PASSWORD", ""))
    parser.add_argument("--device-id", default="esp32-1")
    parser.add_argument("--zone-id", default="tray_1")
    parser.add_argument("--mode", choices=sorted(MODES.keys()), default="normal")
    parser.add_argument("--interval-sec", type=float, default=float(os.getenv("SIM_PUBLISH_INTERVAL_SEC", "3")))
    parser.add_argument("--max-valve-open-sec", type=int, default=int(os.getenv("SIM_MAX_VALVE_OPEN_SEC", "30")))
    parser.add_argument("--max-pump-run-sec", type=int, default=int(os.getenv("SIM_MAX_PUMP_RUN_SEC", "60")))
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    simulator = FakeEsp32Simulator(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        broker_username=args.broker_username,
        broker_password=args.broker_password,
        publish_interval_sec=args.interval_sec,
        device_id=args.device_id,
        zone_id=args.zone_id,
        mode=args.mode,
        valve_limit_sec=args.max_valve_open_sec,
        pump_limit_sec=args.max_pump_run_sec,
    )
    signal.signal(signal.SIGINT, simulator.stop)
    signal.signal(signal.SIGTERM, simulator.stop)
    simulator.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
