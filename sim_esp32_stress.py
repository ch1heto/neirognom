import json
import os
import random
import time
from collections import defaultdict

import paho.mqtt.client as mqtt

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
PUBLISH_INTERVAL_SEC = int(os.getenv("PUBLISH_INTERVAL_SEC", "10"))
SCENARIO = os.getenv("SCENARIO", "sequence")
FAULT_SENSOR = os.getenv("FAULT_SENSOR", "ph")
TRAY_ID = os.getenv("TRAY_ID", "tray01")

BASE = {
    "temp": 23.5,
    "humidity": 62.0,
    "ph": 6.0,
    "ec": 1.6,
    "water_level": 75.0,
    "soil": 48.0,
}

NOISE = {
    "temp": 0.15,
    "humidity": 1.0,
    "ph": 0.02,
    "ec": 0.02,
    "water_level": 0.3,
    "soil": 0.5,
}

TOPIC_PREFIX = f"farm/{TRAY_ID}/telemetry"
CMD_TOPIC = f"farm/{TRAY_ID}/cmd/#"

_state = dict(BASE)
_step = 0
_seq_phase = 0
_seq_steps = 0
_fault_flags = {"jump_sent": False, "silence_started": False}
_command_counts = defaultdict(int)

_SEQUENCE = [
    (20, "NORMAL baseline", None),
    (25, "pH slow drift up", "ph_rise"),
    (15, "pH CRITICAL zone", None),
    (20, "RECOVERY pH", "ph_fall"),
    (20, "NORMAL stable", "reset_all"),
    (20, "EC slow fall", "ec_fall"),
    (15, "EC CRITICAL zone", None),
    (20, "RECOVERY EC", "ec_rise"),
    (15, "MULTI-WARNING ph+ec", "multi_warn"),
    (999, "NORMAL long tail", "reset_all"),
]


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def jitter(v, noise):
    return round(v + random.uniform(-noise, noise), 2)


def reset_fault_baseline():
    global _state
    _state = dict(BASE)
    if SCENARIO in {"fault_no_effect", "fault_wrong_direction", "fault_repeated_dose_exhaustion"}:
        _state["ph"] = 6.95
    if SCENARIO == "fault_sensor_silence":
        _state["ph"] = 6.85
    if SCENARIO == "fault_implausible_jump":
        _state["ph"] = 6.8


def apply_control_command(topic: str, payload: str):
    try:
        command = json.loads(payload)
    except json.JSONDecodeError:
        command = {"action": payload}

    action = str(command.get("action", payload))
    actuator = topic.split("/")[-1]
    if actuator == "ph_down":
        _apply_ph_down(action)
    elif actuator == "ph_up":
        _apply_ph_up(action)
    elif actuator in {"nutrient_a", "nutrient_b"}:
        _apply_ec_up(action, actuator)

    return command


def publish_ack(client: mqtt.Client, topic: str, command: dict, state: str, message: str = ""):
    command_id = str(command.get("command_id", "")).strip()
    if not command_id:
        return
    ack_topic = topic.replace("/cmd/", "/ack/", 1)
    ack_payload = json.dumps(
        {
            "command_id": command_id,
            "ack_state": state,
            "actuator": command.get("actuator"),
            "action": command.get("action"),
            "message": message,
            "timestamp": int(time.time()),
        }
    )
    client.publish(ack_topic, ack_payload)
    print(f"[ACK] {ack_topic} -> {ack_payload}")


def _apply_ph_down(payload: str):
    if payload != "ON":
        return
    _command_counts["ph_down"] += 1

    if SCENARIO == "fault_no_effect":
        print("[FAULT] ph_down command ignored: no measurable pH change")
        return
    if SCENARIO == "fault_wrong_direction":
        _state["ph"] = clamp(_state["ph"] + 0.15, 4.5, 8.5)
        print("[FAULT] ph_down moved pH in wrong direction")
        return
    if SCENARIO == "fault_repeated_dose_exhaustion":
        if _command_counts["ph_down"] == 1:
            _state["ph"] = clamp(_state["ph"] - 0.02, 4.5, 8.5)
            print("[FAULT] weak first pH-down response")
        else:
            print("[FAULT] exhausted pH-down buffer: no further effect")
        return

    _state["ph"] = clamp(_state["ph"] - 0.12, 4.5, 8.5)
    print("[SIM] pH-down applied")


def _apply_ph_up(payload: str):
    if payload != "ON":
        return
    _state["ph"] = clamp(_state["ph"] + 0.12, 4.5, 8.5)
    print("[SIM] pH-up applied")


def _apply_ec_up(payload: str, actuator: str):
    if payload != "ON":
        return
    _command_counts[actuator] += 1
    if SCENARIO == "fault_repeated_dose_exhaustion":
        if _command_counts[actuator] <= 1:
            _state["ec"] = clamp(_state["ec"] + 0.03, 0.1, 4.0)
            print(f"[FAULT] weak {actuator} response")
        else:
            print(f"[FAULT] {actuator} exhausted: no EC effect")
        return
    _state["ec"] = clamp(_state["ec"] + 0.12, 0.1, 4.0)
    print(f"[SIM] {actuator} applied")


def _update_sequence():
    global _seq_phase, _seq_steps

    _seq_steps += 1
    if _seq_steps >= _SEQUENCE[_seq_phase][0] and _seq_phase < len(_SEQUENCE) - 1:
        _seq_phase += 1
        _seq_steps = 0
        _, next_label, _ = _SEQUENCE[_seq_phase]
        print(f"\n{'=' * 60}")
        print(f"[SCENARIO] Phase: {next_label}")
        print(f"{'=' * 60}")

    _, label, mode = _SEQUENCE[_seq_phase]
    if mode == "ph_rise":
        _state["ph"] = clamp(_state["ph"] + 0.04, 4.5, 8.5)
    elif mode == "ph_fall":
        _state["ph"] = clamp(_state["ph"] - 0.06, 4.5, 8.5)
        if _state["ph"] <= BASE["ph"]:
            _state["ph"] = BASE["ph"]
    elif mode == "ec_fall":
        _state["ec"] = clamp(_state["ec"] - 0.06, 0.1, 4.0)
    elif mode == "ec_rise":
        _state["ec"] = clamp(_state["ec"] + 0.10, 0.1, 4.0)
        if _state["ec"] >= BASE["ec"]:
            _state["ec"] = BASE["ec"]
    elif mode == "multi_warn":
        _state["ph"] = clamp(BASE["ph"] + 0.65, 5.5, 8.5)
        _state["ec"] = clamp(BASE["ec"] - 0.65, 0.1, 4.0)
    elif mode == "reset_all":
        for k, v in BASE.items():
            _state[k] = v

    _state["water_level"] = clamp(_state["water_level"] - 0.05, 30.0, 100.0)
    return {s: jitter(_state[s], NOISE[s]) for s in BASE}, label


def _update_fault_scenario():
    label = SCENARIO.replace("_", " ").upper()
    _state["water_level"] = clamp(_state["water_level"] - 0.02, 20.0, 100.0)

    if SCENARIO in {"fault_no_effect", "fault_wrong_direction", "fault_repeated_dose_exhaustion"}:
        _state["ph"] = clamp(_state["ph"] + 0.01, 4.5, 8.5)
    elif SCENARIO == "fault_sensor_silence":
        _state["ph"] = clamp(_state["ph"] + 0.01, 4.5, 8.5)
    elif SCENARIO == "fault_implausible_jump":
        if _step >= 3 and not _fault_flags["jump_sent"]:
            _state["ph"] = clamp(_state["ph"] + 0.9, 4.5, 8.5)
            _fault_flags["jump_sent"] = True
            print("[FAULT] Implausible pH jump injected")

    return {s: jitter(_state[s], NOISE[s]) for s in BASE}, label


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[CONNECTED] MQTT {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe(CMD_TOPIC)
        print(f"[SUBSCRIBED] {CMD_TOPIC}")
    else:
        print(f"[ERROR] rc={reason_code}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"\n{'!' * 60}")
    print(f"[CMD] Topic:   {msg.topic}")
    print(f"[CMD] Payload: {payload}")
    print(f"{'!' * 60}\n")
    command = apply_control_command(msg.topic, payload)
    publish_ack(client, msg.topic, command, "received", "queued")
    time.sleep(0.1)
    publish_ack(client, msg.topic, command, "executing", "relay active")
    time.sleep(0.1)
    publish_ack(client, msg.topic, command, "done", "completed")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sim_esp32_stress")
client.on_connect = on_connect
client.on_message = on_message

reset_fault_baseline()

client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
client.loop_start()

print(f"[SIM] scenario='{SCENARIO}' interval={PUBLISH_INTERVAL_SEC}s")
if SCENARIO == "sequence":
    print(f"[SIM] Starting phase: {_SEQUENCE[0][1]}")
print("-" * 60)

try:
    while True:
        ts = time.time()
        _step += 1

        if SCENARIO == "normal":
            values = {s: jitter(BASE[s], NOISE[s]) for s in BASE}
            label = "NORMAL"
        elif SCENARIO == "sequence":
            values, label = _update_sequence()
        else:
            values, label = _update_fault_scenario()

        silent = False
        if SCENARIO == "fault_sensor_silence" and _step >= 4:
            silent = True
            _fault_flags["silence_started"] = True

        for sensor, value in values.items():
            if silent and sensor == FAULT_SENSOR:
                continue
            client.publish(
                f"{TOPIC_PREFIX}/{sensor}",
                json.dumps({"value": value, "timestamp": ts}),
            )

        if silent:
            print(f"[FAULT] Sensor silence active for '{FAULT_SENSOR}'")

        ph_m = "!! " if values["ph"] < 5.5 or values["ph"] > 6.5 else "   "
        ec_m = "!! " if values["ec"] < 1.0 or values["ec"] > 2.5 else "   "
        wl_m = "!! " if values["water_level"] < 30.0 else "   "

        print(f"[Step {_step:04d}] {label}")
        print(
            f"  {ph_m}pH={values['ph']:.2f}  "
            f"{ec_m}EC={values['ec']:.2f}  "
            f"T={values['temp']:.1f}C  "
            f"RH={values['humidity']:.1f}%  "
            f"{wl_m}WL={values['water_level']:.1f}%  "
            f"Soil={values['soil']:.1f}%"
        )

        time.sleep(PUBLISH_INTERVAL_SEC)

except KeyboardInterrupt:
    print("\n[SIM] Stopped.")
finally:
    client.loop_stop()
    client.disconnect()
