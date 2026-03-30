# Greenhouse Backend

Backend-centric greenhouse control runtime for local simulator testing and real ESP32 deployments.

## Runtime Summary

- ESP32 devices publish telemetry, state, presence, ACK, and RESULT over MQTT.
- The backend owns state, deterministic rules, LLM recommendations, safety validation, orchestration, audit logging, and the operator UI.
- Llama provides recommendations only. It never sends hardware commands directly.
- The operator dashboard is served by the backend itself. There is no separate dashboard server.

## Local Windows Quick Start

Use the local simulator profile from `.env` / `.env.example`.

Backend:

```powershell
& .\venv\Scripts\Activate.ps1; python .\backend_server.py
```

Simulator:

```powershell
& .\venv\Scripts\Activate.ps1; python .\sim_esp32.py
```

Dashboard URL:

```text
http://127.0.0.1:8780
```

## Local Defaults

The local profile is intended for a single Windows machine with Mosquitto, the backend, and `sim_esp32.py` all running locally.

- `MQTT_HOST=127.0.0.1`
- `MQTT_PORT=1883`
- `STATE_STORE_BACKEND=memory`
- `TELEMETRY_HISTORY_BACKEND=memory`
- `INFLUX_ENABLED=0`
- `OPENCLAW_MCP_ENABLED=0`
- `OPERATOR_UI_ENABLED=1`
- `ZONE_TRAY_*_CROP_ID=lettuce_nft`

Deprecated `OPENCLAW_OPERATOR_*` variables are ignored. Use `OPENCLAW_MCP_*` only.

## Runtime Entry Points

- `backend_server.py`: canonical backend entry point
- `backend/runtime.py`: runtime wiring
- `sim_esp32.py`: local ESP32 simulator
- `operator_ui.html`: operator dashboard served by the backend HTTP server

## Knowledge Base Status

Runtime-active knowledge-base files:

- `knowledge_base/grow_maps/lettuce_nft.json`
- `knowledge_base/alerts/critical_thresholds.json`

Reference-only files kept for future design work and documentation:

- `knowledge_base/alerts/control_safety.json`
- `knowledge_base/alerts/recovery_protocols.json`
- `knowledge_base/equipment/actuator_registry.json`

Those reference files are not loaded by the current runtime and should not be treated as the live command/topic contract.

## Safety Notes

- All actuator paths still pass through `SafetyValidator`.
- Manual mode pauses automated telemetry-driven decisions but keeps manual actions safety-gated.
- LLM responses are bound to the source telemetry zone. Cross-zone LLM actions are rejected and audit logged.
- Missing crop IDs now generate warnings instead of silently disabling grow-map context.

## Documentation

- Architecture: [docs/architecture_layers.md](docs/architecture_layers.md)
- Deployment: [docs/deployment.md](docs/deployment.md)
- Operator UI: [docs/operator_ui.md](docs/operator_ui.md)
- OpenClaw MCP: [docs/openclaw_mcp.md](docs/openclaw_mcp.md)
- ESP32 firmware checklist: [docs/esp32_firmware_checklist.md](docs/esp32_firmware_checklist.md)
- ESP32/backend protocol: [docs/esp32_backend_protocol.md](docs/esp32_backend_protocol.md)
- Security model: [docs/security_model.md](docs/security_model.md)
- Knowledge-base usage: [docs/knowledge_base.md](docs/knowledge_base.md)

## Testing

Automated tests were removed from this repository.
Use `sim_esp32.py` for local end-to-end verification.