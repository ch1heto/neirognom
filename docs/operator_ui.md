# Operator UI

The operator web UI is hosted inside the backend runtime. It never publishes MQTT directly.

Runtime path:

- browser -> backend operator HTTP API
- backend operator service -> `BackendToolService.execute_manual_action()`
- backend safety validator -> dispatcher -> MQTT

## Available Actions

- view device and zone status
- switch between auto and manual system mode
- open / close valve
- start / stop pump
- start / stop nutrient dosing
- review the live event log and export visible events to CSV
- emergency stop

All manual actions still pass through the backend safety path.

## Run Locally

Backend:

```powershell
& .\venv\Scripts\Activate.ps1; python .\backend_server.py
```

Open:

```text
http://127.0.0.1:8780
```

There is no standalone dashboard server. The backend serves `operator_ui.html` itself.

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