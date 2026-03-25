import json
import random
import time

import paho.mqtt.client as mqtt

MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 1883
PUBLISH_INTERVAL_SEC = 7

SENSORS = {
    "farm/tray01/telemetry/temp":        (20.0, 28.0),
    "farm/tray01/telemetry/humidity":    (40.0, 70.0),
    "farm/tray01/telemetry/soil":        (30.0, 60.0),
    "farm/tray01/telemetry/water_level": (50.0, 100.0),
    "farm/tray01/telemetry/ph":          (5.5,  6.5),
    "farm/tray01/telemetry/ec":          (1.0,  2.5),
}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[CONNECTED] MQTT broker {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe("farm/tray01/cmd/#")
        print("[SUBSCRIBED] farm/tray01/cmd/#")
    else:
        print(f"[ERROR] Connection failed, rc={rc}")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"[CMD RECEIVED] Topic: {msg.topic}, Payload: {payload}")


client = mqtt.Client(client_id="sim_esp32")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
client.loop_start()

print("[SIM] ESP32 simulator started. Publishing every 7 seconds...")

try:
    while True:
        timestamp = time.time()
        for topic, (low, high) in SENSORS.items():
            value = round(random.uniform(low, high), 2)
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