# Greenhouse Backend-Centric Architecture

This repository now targets a backend-centric greenhouse control runtime.

## Target runtime path

- `ESP32` is edge only: sensors, actuators, local watchdogs, safe mode.
- `MQTT` is transport only.
- `Backend` is the single source of truth and final safety gate.
- `Llama 3.1` is a reasoning engine used only by backend.
- `OpenClaw` is an external MCP/operator layer that talks to backend tools and never touches devices directly.

## Package map

- `shared/contracts/messages.py`
  - strict DTOs for telemetry, state, commands, LLM recommendations, safety decisions, alerts
- `mqtt/topics.py`
  - canonical MQTT topic builder/parser
- `backend/config.py`
  - runtime config for MQTT, PostgreSQL/sqlite compatibility, zones, Llama, OpenClaw operator settings
- `backend/state/store.py`
  - state store abstraction
  - `PostgresStateStore` is the target backend
  - `SQLiteCompatibilityStateStore` is a deprecated migration adapter
- `backend/decision_engine/engine.py`
  - deterministic rules first, Llama only as fallback recommendation source
- `backend/safety/validator.py`
  - final backend safety gate
- `backend/dispatcher/service.py`
  - command state machine and MQTT dispatch
- `backend/ingestion/service.py`
  - MQTT ingestion, validation, dedup, routing
- `backend/api/tools.py`
  - backend tool/API surface for operators
- `integrations/llama/client.py`
  - local Llama API client
- `integrations/openclaw_mcp/tools.py`
  - OpenClaw operator adapter to backend tools
- `backend/runtime.py`
  - canonical runtime wiring
- `backend_server.py`
  - canonical entrypoint
- `smart_bridge.py`
  - deprecated compatibility adapter that delegates to backend runtime

## Data flow

1. Edge publishes `greenhouse/device/{device_id}/telemetry/raw` or `.../state`.
2. `backend.ingestion.service.IngestionService` validates and deduplicates payloads.
3. `backend.state.store` updates current state and telemetry history.
4. `backend.decision_engine.engine.DecisionEngine` applies deterministic rules.
5. Only if deterministic rules do not fully answer, backend asks `integrations.llama.client.LlamaDecisionClient`.
6. Backend converts any recommendation into `ActionProposal`.
7. `backend.safety.validator.SafetyValidator` performs final validation.
8. `backend.dispatcher.service.CommandDispatcher` creates `CommandRequest`, publishes MQTT, and manages lifecycle:
   - `queued`
   - `sent`
   - `acked`
   - `running`
   - `completed`
   - `failed`
   - `expired`
   - `rejected_by_safety`
9. ACK/RESULT topics update command lifecycle back into the state store.
10. OpenClaw, if used, consumes backend tools only:
   - `get_current_state`
   - `get_zone_state`
   - `get_sensor_history`
   - `get_active_alerts`
   - `propose_action`
   - `execute_manual_action`
   - `get_command_status`

## MQTT contracts

- `greenhouse/device/{device_id}/telemetry/raw`
- `greenhouse/device/{device_id}/state`
- `greenhouse/device/{device_id}/cmd/execute`
- `greenhouse/device/{device_id}/cmd/ack`
- `greenhouse/device/{device_id}/cmd/result`
- `greenhouse/device/{device_id}/event/error`

QoS recommendation: `1` for all runtime messages in this repository.

## Safety model

Per-zone:
- cooldown
- max duration per run
- max runs per hour
- max total water per day
- blocked flag
- last watering timestamp
- last error timestamp

Global:
- max simultaneous zones
- emergency stop
- master pump timeout
- flow limit window
- leak shutdown

## Environment

See [.env.example](/V:/work/DIPLOM/testMoskitto/.env.example).

Key variables:
- `STATE_STORE_BACKEND=postgres|sqlite_compat|memory`
- `POSTGRES_DSN`
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`
- `ZONE_IDS`
- `LLAMA_API_URL`, `LLAMA_MODEL`
- `OPENCLAW_OPERATOR_ENABLED`, `OPENCLAW_OPERATOR_URL`

## Local run

```powershell
.\venv\Scripts\python.exe .\backend_server.py
```

The old entrypoint still exists:

```powershell
.\venv\Scripts\python.exe .\smart_bridge.py
```

but it only forwards to the new backend runtime and should be treated as deprecated.

## Migration notes

- The old OpenClaw-central path is no longer the primary runtime path.
- `openclaw_client.py`, `command_gateway.py`, `control_runtime.py`, and the old SQLite audit flow are legacy artifacts until the remaining UI/diagnostic surfaces are migrated.
- Production deployment should use:
  - Ubuntu
  - Mosquitto
  - PostgreSQL
  - local Llama runtime
  - OpenClaw as an external operator client
