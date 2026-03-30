# Deployment Guide

This project is intended to run the same backend path in local simulation and on real hardware. The environment file is the main switch.

## Local Windows Run

Use local mode for single-PC testing with Mosquitto, the backend, and `sim_esp32.py` on the same machine.

Recommended local settings:

```env
STATE_STORE_BACKEND=memory
TELEMETRY_HISTORY_BACKEND=memory
INFLUX_ENABLED=0
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
OPENCLAW_MCP_ENABLED=0
OPERATOR_UI_ENABLED=1
ZONE_TRAY_1_CROP_ID=lettuce_nft
ZONE_TRAY_2_CROP_ID=lettuce_nft
ZONE_TRAY_3_CROP_ID=lettuce_nft
ZONE_TRAY_4_CROP_ID=lettuce_nft
```

Exact local PowerShell commands:

```powershell
& .\venv\Scripts\Activate.ps1; python .\backend_server.py
```

```powershell
& .\venv\Scripts\Activate.ps1; python .\sim_esp32.py
```

Dashboard URL:

```text
http://127.0.0.1:8780
```

Notes:

- The dashboard is hosted by the backend. There is no separate dashboard server.
- `memory` state/history keeps local tests disposable.
- If Ollama is unavailable, the backend still starts; LLM recommendations just fall back.
- Deprecated `OPENCLAW_OPERATOR_*` variables are ignored.

## Production Ubuntu Run

Use production mode when the backend is connected to real ESP32 hardware and persistent services.

Recommended production settings:

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
OPENCLAW_MCP_ENABLED=1
OPERATOR_UI_ENABLED=1
```

Preparation checklist:

1. Install and configure Mosquitto.
2. Install and configure SQLite storage permissions.
3. Install and configure InfluxDB if telemetry history is required.
4. Install Ollama locally if LLM recommendations are desired.
5. Populate `ZONE_TRAY_*_DEVICE_ID` and `ZONE_TRAY_*_CROP_ID` correctly before connecting hardware.
6. Confirm the operator UI opens and every tray maps to the intended device.

## Knowledge-Base Expectations

The live runtime currently loads:

- `knowledge_base/grow_maps/*.json`
- `knowledge_base/alerts/critical_thresholds.json`

Other JSON files under `knowledge_base/` are currently reference material only.