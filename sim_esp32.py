import json
import os
import random
import time

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
PUBLISH_INTERVAL_SEC = int(os.getenv("PUBLISH_INTERVAL_SEC", "7"))
TRAY_ID = os.getenv("TRAY_ID", "tray01")

SENSORS = {
    f"farm/{TRAY_ID}/telemetry/temp":        (20.0, 28.0),
    f"farm/{TRAY_ID}/telemetry/humidity":    (40.0, 70.0),
    f"farm/{TRAY_ID}/telemetry/soil":        (30.0, 60.0),
    f"farm/{TRAY_ID}/telemetry/water_level": (50.0, 100.0),
    f"farm/{TRAY_ID}/telemetry/ph":          (5.5,  6.5),
    f"farm/{TRAY_ID}/telemetry/ec":          (1.0,  2.5),
}
last_sensor_values = {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[CONNECTED] MQTT broker {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe(f"farm/{TRAY_ID}/cmd/#")
        print(f"[SUBSCRIBED] farm/{TRAY_ID}/cmd/#")
    else:
        print(f"[ERROR] Connection failed, rc={rc}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"[CMD RECEIVED] Topic: {msg.topic}, Payload: {payload}")
    try:
        command = json.loads(payload)
    except json.JSONDecodeError:
        return
    if not isinstance(command, dict):
        return
    command_id = str(command.get("command_id", "")).strip()
    if not command_id:
        return
    actuator = str(command.get("actuator", ""))
    action = str(command.get("action", ""))
    ack_topic = msg.topic.replace("/cmd/", "/ack/", 1)
    if actuator in {"ph_down_pump", "ph_up_pump", "nutrient_a", "nutrient_b"} and action == "ON":
        water_level = last_sensor_values.get("water_level")
        if water_level is not None and float(water_level) < 30.0:
            ack_payload = json.dumps(
                {
                    "command_id": command_id,
                    "ack_state": "failed",
                    "actuator": actuator,
                    "action": action,
                    "error": "low_water_edge_failsafe",
                    "timestamp": int(time.time()),
                }
            )
            client.publish(ack_topic, ack_payload)
            print(f"[ACK] {ack_topic} -> {ack_payload}")
            return
    for state in ("received", "executing", "done"):
        ack_payload = json.dumps(
            {
                "command_id": command_id,
                "ack_state": state,
                "actuator": command.get("actuator"),
                "action": command.get("action"),
                "timestamp": int(time.time()),
            }
        )
        client.publish(ack_topic, ack_payload)
        print(f"[ACK] {ack_topic} -> {ack_payload}")
        if state != "done":
            time.sleep(0.2)


client = mqtt.Client(client_id="sim_esp32")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
client.loop_start()

print(f"[SIM] ESP32 simulator started for tray='{TRAY_ID}'. Publishing every {PUBLISH_INTERVAL_SEC} seconds...")

try:
    while True:
        timestamp = time.time()
        for topic, (low, high) in SENSORS.items():
            value = round(random.uniform(low, high), 2)
            last_sensor_values[topic.split("/")[-1]] = value
            payload = json.dumps({"value": value, "timestamp": timestamp})
            client.publish(topic, payload)
            print(f"[PUB] {topic} → {payload}")
        print("-" * 60)
        time.sleep(PUBLISH_INTERVAL_SEC)
except KeyboardInterrupt:
    print("\n[SIM] Stopped.")
finally:
    client.loop_stop()
    client.disconnect()
