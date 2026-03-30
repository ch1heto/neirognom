# Greenhouse Backend Execution Architecture

This repository now targets a backend-centric greenhouse runtime with a strict execution safety boundary.

Architecture notes:

- Runtime layers: [architecture_layers.md](/V:/work/DIPLOM/testMoskitto/docs/architecture_layers.md)

## Runtime path

- `ESP32` stays edge-only: sensors, actuators, watchdogs, safe mode.
- `MQTT` is transport only.
- `SQLite` is the operational/control database and single source of truth.
- `InfluxDB` stores telemetry history and trend queries only.
- `Backend` owns state, decisions, safety validation, orchestration, dispatch, recovery.
- `Llama 3.1` provides recommendations only.
- `OpenClaw` is an operator/MCP client to backend tools and never publishes hardware commands directly.

## Package map

- `shared/contracts/messages.py`
  - strict MQTT/API DTOs
- `backend/domain/models.py`
  - `Device`, `TrayZone`, `Command`, `CommandExecution`, `SafetyLock`, `ManualLease`, `Alarm`, `AuditLog`, `AutomationFlag`
- `backend/config.py`
  - MQTT, SQLite, InfluxDB, Llama, MCP/operator adapter, zone/global safety config
- `backend/state/store.py`
  - SQLite operational store and memory test store
- `backend/state/influx.py`
  - Influx telemetry writer/query boundary
- `backend/security/monitor.py`
  - heartbeat, broker, replay, and anomaly-driven lock/alarm monitoring
- `backend/decision_engine/engine.py`
  - deterministic rules first, Llama fallback second
- `backend/safety/validator.py`
  - final backend safety gate
- `backend/execution/orchestrator.py`
  - irrigation sequencing, safe stop, TTL, restart recovery
- `backend/dispatcher/service.py`
  - orchestration-facing dispatch facade
- `backend/ingestion/service.py`
  - MQTT ingestion, validation, dedup, normalization
- `backend/api/tools.py`
  - operator/OpenClaw tool layer
- `backend/operator/service.py`
  - operator-facing safe control service
- `backend/operator/web.py`
  - runtime-hosted HTTP server for the operator UI
- `integrations/llama/client.py`
  - local Llama API adapter
- `integrations/openclaw_mcp/tools.py`
  - OpenClaw MCP adapter
- `integrations/openclaw_mcp/server.py`
  - HTTP/JSON-RPC MCP-compatible tool server for OpenClaw
- `backend/runtime.py`
  - canonical runtime wiring
- `backend_server.py`
  - canonical entrypoint

## Core entities

- `Device`
- `TrayZone`
- `Command`
- `CommandExecution`
- `SafetyLock`
- `ManualLease`
- `Alarm`
- `AuditLog`

Command lifecycle:

- `PLANNED`
- `DISPATCHED`
- `ACKED`
- `EXECUTING`
- `COMPLETED`
- `FAILED`
- `EXPIRED`
- `ABORTED`

## Data flow

1. Edge publishes telemetry/state over MQTT.
2. `IngestionService` validates JSON, deduplicates by `message_id`, updates SQLite current state, writes telemetry history to InfluxDB.
3. `DecisionEngine` applies deterministic rules.
4. If rules are insufficient, backend asks Llama for a JSON recommendation.
5. `SafetyValidator` checks locks, leases, cooldowns, source availability, leaks, overflow, run quotas, global limits.
6. `IrrigationOrchestrator` creates `Command` + `CommandExecution`, reserves the zone, and sequences:
   - validate request
   - reserve zone
   - open valve
   - wait settle delay
   - start pump
   - confirm flow
   - monitor duration / volume / anomalies
   - stop pump
   - close valve
   - verify safe stop
   - persist final result
7. ACK/RESULT messages update execution state in SQLite.
8. `SecurityMonitor` applies replay detection, broker fail-safe locks, heartbeat staleness checks, and Influx-driven anomaly locks/alarms.
9. On backend restart, active executions are restored from SQLite and reconciled before continuing or safely aborting.

## SQLite control boundary

SQLite stores:

- current device/zone state
- command records
- command execution records
- safety locks
- manual leases
- automation flags
- active alarms
- audit logs
- processed message ids

## InfluxDB boundary

InfluxDB stores only:

- sensor history
- flow history
- environmental trends
- telemetry lookback queries for reasoning/analytics

It is not used for command state, safety locks, leases, alarms, or execution recovery.

## Safety guarantees

- No LLM/OpenClaw/UI path sends actuator commands directly.
- All manual actions go through backend validation and orchestration.
- Command idempotency is keyed by `command_id`.
- Commands support `expires_at_ms`.
- Commands include a per-command `nonce` and explicit `safety_constraints`.
- Backend rejects unknown, mismatched, replay-suspected, and malformed ACK/RESULT messages.
- Broker disconnects, auth failures, stale heartbeats, empty tank, leak suspicion, and critical anomalies create persistent SQLite locks/alarms.
- Dangerous actions are audit logged.

## Security model

- Strict safe command channel and device/backend contract: [security_model.md](/V:/work/DIPLOM/testMoskitto/docs/security_model.md)
- Mosquitto ACL example: [mqtt_acl_example.txt](/V:/work/DIPLOM/testMoskitto/docs/mqtt_acl_example.txt)

## Environment

See `.env.example`.

Key variables:

- `STATE_STORE_BACKEND=sqlite|memory`
- `SQLITE_PATH`
- `TELEMETRY_HISTORY_BACKEND=influx|memory`
- `INFLUX_URL`, `INFLUX_ORG`, `INFLUX_BUCKET`, `INFLUX_TOKEN`
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`
- `ZONE_IDS`
- `LLAMA_API_URL`, `LLAMA_MODEL`
- `OPENCLAW_MCP_ENABLED`, `OPENCLAW_MCP_HOST`, `OPENCLAW_MCP_PORT`
- `OPENCLAW_MCP_ACTION_TOKEN`, `OPENCLAW_MCP_REQUIRE_ACTION_TOKEN`
- `BROKER_RECONNECT_LOCK_SEC`
- `ANOMALY_LOOKBACK_SEC`
- `MIN_PRESSURE_KPA`, `MAX_PRESSURE_KPA`
- `TANK_DEPLETION_DROP_THRESHOLD`
- `STALE_SENSOR_WINDOW_SEC`
- `NOISY_SENSOR_DELTA_THRESHOLD`

## Local run

```powershell
.\venv\Scripts\python.exe .\backend_server.py
```

If `OPERATOR_UI_ENABLED=1`, the backend also hosts the operator UI at `http://127.0.0.1:8780` by default.

Operator UI notes: [operator_ui.md](/V:/work/DIPLOM/testMoskitto/docs/operator_ui.md)

If `OPENCLAW_MCP_ENABLED=1`, the backend also hosts the OpenClaw MCP adapter at `http://127.0.0.1:8790/mcp` by default.

OpenClaw MCP notes: [openclaw_mcp.md](/V:/work/DIPLOM/testMoskitto/docs/openclaw_mcp.md)

## Testing

Layered backend tests live under `tests/`:

- `test_backend_unit.py`
- `test_backend_integration.py`
- `test_backend_ai_flow.py`
- `test_backend_operator_flow.py`
- `test_backend_architecture.py`
- `test_architecture_cleanup.py`
- `test_operator_ui_service.py`
- `test_openclaw_mcp.py`

Reusable test fixtures and the in-memory backend harness live in `tests/fixtures.py` and `tests/harness.py`.

Run the CI-friendly entrypoint:

```powershell
.\venv\Scripts\python.exe .\run_backend_tests.py
```

Or standard discovery:

```powershell
.\venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Additional notes: [testing.md](/V:/work/DIPLOM/testMoskitto/docs/testing.md)

## Migration notes

- Old `PostgreSQL`/`sqlite_compat` assumptions are removed from the runtime path.
- Old bridge-era runtime modules and prompts have been removed.
- If older tooling still expects direct actuator dispatch, route it through `BackendToolService.execute_manual_action()` or the dispatcher facade instead.
