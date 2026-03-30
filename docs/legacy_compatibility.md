# Legacy Compatibility Notes

The modules below are deprecated compatibility artifacts and are not part of the active runtime path:

- `smart_bridge.py`
  - compatibility entrypoint that delegates to `backend.runtime.BackendRuntime`
- `command_gateway.py`
  - legacy bridge-era command gateway
- `control_runtime.py`
  - legacy bridge-era effect/escalation runtime
- `dashboard_server.py`
  - legacy bridge/OpenClaw diagnostic dashboard
- `operator_cli.py`
  - legacy inspection helper; direct reset path is disabled by default
- `openclaw_client.py`
  - diagnostics-only OpenClaw transport helper, not the runtime center
- `database.py`
  - legacy bridge audit/event store, separate from the current backend SQLite state store

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
