# Operator UI

The operator web UI is a thin HTTP layer hosted inside the backend runtime. It never publishes MQTT directly.

Runtime path:

- browser -> backend operator HTTP API
- backend operator service -> `BackendToolService.execute_manual_action()`
- backend safety validator -> dispatcher -> MQTT

## Available actions

- view device and zone status
- switch the backend between auto and manual mode
- open / close valve
- start / stop pump
- start / stop nutrient dosing
- review the live event log and export visible events to CSV
- emergency stop

All manual actions:

- create command journal entries in SQLite
- include operator metadata
- use the same safety checks as automated actions
- inherit command TTL from backend config
- cap durations to zone or pump safety limits

## Run

Set in `.env` if needed:

```env
OPERATOR_UI_ENABLED=1
OPERATOR_UI_HOST=127.0.0.1
OPERATOR_UI_PORT=8780
```

Start the backend:

```powershell
.\venv\Scripts\python.exe .\backend_server.py
```

Open:

```text
http://127.0.0.1:8780
```

## API

- `GET /api/operator/overview`
- `GET /api/operator/devices-zones`
- `GET /api/operator/state`
- `GET /api/operator/system-mode`
- `GET /api/operator/event-log?limit=200`
- `GET /api/operator/commands?limit=50`
- `POST /api/operator/system-mode`
- `POST /api/operator/manual-command`
- `POST /api/operator/command`
- `POST /api/operator/emergency-stop`

