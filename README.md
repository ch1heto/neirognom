# Greenhouse Backend Execution Architecture

This repository now targets a backend-centric greenhouse runtime with a strict execution safety boundary.

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
  - MQTT, SQLite, InfluxDB, Llama, OpenClaw, zone/global safety config
- `backend/state/store.py`
  - SQLite operational store and memory test store
- `backend/state/influx.py`
  - Influx telemetry writer/query boundary
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
- `integrations/llama/client.py`
  - local Llama API adapter
- `integrations/openclaw_mcp/tools.py`
  - OpenClaw MCP adapter
- `backend/runtime.py`
  - canonical runtime wiring
- `backend_server.py`
  - canonical entrypoint
- `smart_bridge.py`
  - deprecated adapter to the new backend runtime

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
8. On backend restart, active executions are restored from SQLite and reconciled before continuing or safely aborting.

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
- Dangerous actions are audit logged.

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
- `OPENCLAW_OPERATOR_ENABLED`, `OPENCLAW_OPERATOR_URL`

## Local run

```powershell
.\venv\Scripts\python.exe .\backend_server.py
```

Legacy entrypoint:

```powershell
.\venv\Scripts\python.exe .\smart_bridge.py
```

## Migration notes

- Old `PostgreSQL`/`sqlite_compat` assumptions are removed from the runtime path.
- Existing OpenClaw-specific modules remain deprecated compatibility artifacts only.
- If older code still expects direct actuator dispatch, route it through `BackendToolService.execute_manual_action()` or the dispatcher facade instead.
