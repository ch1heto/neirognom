# Deployment Guide

This backend is designed to run the same control path in local simulation and on the real farm. The only intended switch is the `.env` file.

## Local Run With `sim_esp32.py`

Use local mode when you want to validate operator flows, manual mode, MQTT wiring, and backend safety decisions without touching hardware.

Recommended `.env` values:

```env
STATE_STORE_BACKEND=memory
TELEMETRY_HISTORY_BACKEND=memory
INFLUX_ENABLED=0
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
LLAMA_API_URL=http://127.0.0.1:11434/v1/chat/completions
LLAMA_MODEL=llama3.1
OPERATOR_UI_ENABLED=1
OPENCLAW_MCP_ENABLED=0
```

Local startup sequence:

1. Start Mosquitto on the local machine.
2. Copy `.env.example` to `.env` and keep the local block enabled.
3. Start the backend:
   ```powershell
   .\venv\Scripts\python.exe .\backend_server.py
   ```
4. Start the simulator in a second terminal:
   ```powershell
   .\venv\Scripts\python.exe .\sim_esp32.py
   ```
5. Open the operator UI at `http://127.0.0.1:8780`.

Local notes:

- `memory` store avoids touching production SQLite state.
- `INFLUX_ENABLED=0` keeps local runs simple.
- Manual mode in the UI still routes commands through `SafetyValidator` and MQTT.
- The simulator should not be modified to switch environments.

## Production Run On Ubuntu

Use production mode when the backend is connected to real ESP32 hardware, persistent SQLite state, InfluxDB history, and local Ollama.

Recommended `.env` values:

```env
STATE_STORE_BACKEND=sqlite
SQLITE_PATH=/opt/greenhouse/neuroagronom.db
TELEMETRY_HISTORY_BACKEND=influx
INFLUX_ENABLED=1
INFLUX_URL=http://127.0.0.1:8086
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
LLAMA_API_URL=http://127.0.0.1:11434/v1/chat/completions
LLAMA_MODEL=llama3.1
OPERATOR_UI_ENABLED=1
OPENCLAW_MCP_ENABLED=1
```

Ubuntu preparation:

1. Install and enable Mosquitto.
2. Install SQLite tools and make sure the backend process can write to `SQLITE_PATH`.
3. Install and configure InfluxDB for telemetry history.
4. Install Ollama locally and pull the selected Llama model.
5. Create the `.env` file from `.env.example` and switch to the production block.
6. Confirm each tray uses the correct `ZONE_TRAY_*_DEVICE_ID` binding before connecting hardware.

Production startup:

1. Start Mosquitto.
2. Start InfluxDB.
3. Start Ollama.
4. Start the backend service.
5. Power on ESP32 controllers and confirm they publish presence, state, and telemetry.
6. Open the operator UI and verify every tray is mapped to the expected device id.

Production notes:

- Keep `STATE_STORE_BACKEND=sqlite` so commands, alarms, and audit entries survive restart.
- Keep `TELEMETRY_HISTORY_BACKEND=influx` and `INFLUX_ENABLED=1` for anomaly checks and Llama context windows.
- Llama recommendations never actuate hardware directly. All commands still pass through `SafetyValidator`.
- Manual mode pauses automated telemetry-driven decisions, but manual actuator tests still go through the same safety gate.

## Environment Separation Checklist

Before switching between local and production, confirm:

- `.env` points to the correct MQTT broker.
- `.env` points to the correct SQLite path or memory backend.
- `INFLUX_ENABLED` matches the chosen telemetry history backend.
- `ZONE_IDS` and every `ZONE_TRAY_*_DEVICE_ID` match the intended tray-to-device mapping.
- Operator UI opens and shows the correct system mode.
- MQTT topics stay unchanged between environments.
