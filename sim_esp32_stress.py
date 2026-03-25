import json
import random
import time

import paho.mqtt.client as mqtt

MQTT_BROKER_HOST     = "localhost"
MQTT_BROKER_PORT     = 1883
PUBLISH_INTERVAL_SEC = 30

SCENARIO = "sequence"

BASE = {
    "temp":        23.5,
    "humidity":    62.0,
    "ph":          6.0,
    "ec":          1.6,
    "water_level": 75.0,
    "soil":        48.0,
}

NOISE = {
    "temp":        0.15,
    "humidity":    1.0,
    "ph":          0.02,
    "ec":          0.02,
    "water_level": 0.3,
    "soil":        0.5,
}

TOPIC_PREFIX = "farm/tray01/telemetry"

_state = dict(BASE)
_step  = 0

_SEQUENCE = [
    (20,  "NORMAL baseline",          None),
    (25,  "pH slow drift up",         "ph_rise"),
    (15,  "pH CRITICAL zone",         None),
    (20,  "RECOVERY pH",              "ph_fall"),
    (20,  "NORMAL stable",            "reset_all"),
    (20,  "EC slow fall",             "ec_fall"),
    (15,  "EC CRITICAL zone",         None),
    (20,  "RECOVERY EC",              "ec_rise"),
    (15,  "TEMP slow rise",           "temp_rise"),
    (10,  "TEMP CRITICAL zone",       None),
    (20,  "RECOVERY TEMP",            "temp_fall"),
    (15,  "MULTI-WARNING ph+ec",      "multi_warn"),
    (999, "NORMAL long tail",         "reset_all"),
]

_seq_phase = 0
_seq_steps = 0


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def jitter(v, noise):
    return round(v + random.uniform(-noise, noise), 2)


def _update_sequence():
    global _seq_phase, _seq_steps, _state

    # Сначала переключаем фазу, потом читаем label и mode текущей фазы.
    # Так label и дрейф всегда соответствуют одной и той же фазе.
    _seq_steps += 1
    if _seq_steps >= _SEQUENCE[_seq_phase][0] and _seq_phase < len(_SEQUENCE) - 1:
        _seq_phase += 1
        _seq_steps = 0
        _, next_label, _ = _SEQUENCE[_seq_phase]
        print(f"\n{'='*60}")
        print(f"[SCENARIO] Phase: {next_label}")
        print(f"{'='*60}")

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
    elif mode == "temp_rise":
        _state["temp"] = clamp(_state["temp"] + 0.4, 15.0, 40.0)
    elif mode == "temp_fall":
        _state["temp"] = clamp(_state["temp"] - 0.6, 15.0, 40.0)
        if _state["temp"] <= BASE["temp"]:
            _state["temp"] = BASE["temp"]
    elif mode == "multi_warn":
        _state["ph"] = clamp(BASE["ph"] + 0.65, 5.5, 8.5)
        _state["ec"] = clamp(BASE["ec"] - 0.65, 0.1, 4.0)
    elif mode == "reset_all":
        for k in BASE:
            _state[k] = BASE[k]

    _state["water_level"] = clamp(_state["water_level"] - 0.05, 30.0, 100.0)
    values = {s: jitter(_state[s], NOISE[s]) for s in BASE}
    return values, label


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[CONNECTED] MQTT {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe("farm/tray01/cmd/#")
        print("[SUBSCRIBED] farm/tray01/cmd/#")
    else:
        print(f"[ERROR] rc={reason_code}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"\n{'!'*60}")
    print(f"[CMD] Topic:   {msg.topic}")
    print(f"[CMD] Payload: {payload}")
    print(f"{'!'*60}\n")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sim_esp32_stress")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
client.loop_start()

print(f"[SIM] scenario='{SCENARIO}'  interval={PUBLISH_INTERVAL_SEC}s")
print(f"[SIM] pH drift: +0.04/step  EC drift: -0.06/step  (1 step={PUBLISH_INTERVAL_SEC}s)")
if SCENARIO == "sequence":
    print(f"[SIM] Starting phase: {_SEQUENCE[0][1]}")
print("-" * 60)

try:
    while True:
        ts = time.time()

        if SCENARIO == "normal":
            values = {s: jitter(BASE[s], NOISE[s]) for s in BASE}
            label  = "NORMAL"
        elif SCENARIO == "sequence":
            values, label = _update_sequence()
        else:
            values = {s: jitter(BASE[s], NOISE[s]) for s in BASE}
            label  = SCENARIO

        _step += 1

        for sensor, value in values.items():
            client.publish(
                f"{TOPIC_PREFIX}/{sensor}",
                json.dumps({"value": value, "timestamp": ts}),
            )

        ph_m = "⚠️ " if values["ph"]  < 5.5 or values["ph"]  > 6.5 else "   "
        ec_m = "⚠️ " if values["ec"]  < 1.0 or values["ec"]  > 2.5 else "   "
        t_m  = "⚠️ " if values["temp"] > 28.0                       else "   "
        wl_m = "⚠️ " if values["water_level"] < 30.0                else "   "

        print(f"[Step {_step:04d}] {label}")
        print(
            f"  {ph_m}pH={values['ph']:.2f}  "
            f"{ec_m}EC={values['ec']:.2f}  "
            f"{t_m}T={values['temp']:.1f}°C  "
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