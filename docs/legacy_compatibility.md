# Legacy Compatibility Notes

The modules below are deprecated compatibility artifacts and are not part of the active runtime path.
Their implementation now lives under `legacy/`. The old root-level filenames are thin shims only.

- `smart_bridge.py`
  - compatibility entrypoint that delegates to `backend.runtime.BackendRuntime`
- `legacy/command_gateway.py`
  - legacy bridge-era command gateway
- `legacy/control_runtime.py`
  - legacy bridge-era effect/escalation runtime
- `legacy/dashboard_server.py`
  - legacy bridge/OpenClaw diagnostic dashboard
- `legacy/operator_cli.py`
  - legacy inspection helper; direct reset path is disabled by default
- `legacy/openclaw_client.py`
  - diagnostics-only OpenClaw transport helper, not the runtime center
- `legacy/database.py`
  - legacy bridge audit/event store, separate from the current backend SQLite state store
- `legacy/control_config.py`
  - legacy bridge runtime profile/knowledge-base configuration
- `legacy/influx_writer.py`
  - legacy Influx retry/buffering helper

## Current replacements

- runtime entrypoint: `backend_server.py`
- state store: `backend/state/store.py`
- telemetry history: `backend/state/influx.py`
- decision engine: `backend/decision_engine/engine.py`
- safety gate: `backend/safety/validator.py`
- execution/dispatch: `backend/execution/orchestrator.py`
- operator web surface: `backend/operator/service.py` + `backend/operator/web.py`
- OpenClaw MCP adapter: `integrations/openclaw_mcp/tools.py` + `integrations/openclaw_mcp/server.py`

## Canonical DTOs

The active runtime contracts live in:

- `shared/contracts/messages.py`

Legacy compatibility modules may still define local parsing helpers for their own historical formats, but they are not the source of truth for the backend-centric runtime.
