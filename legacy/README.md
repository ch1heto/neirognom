# Legacy Compatibility Area

Files in this directory are bridge-era compatibility artifacts. They are not part
of the active backend runtime and must not be used for new work.

Canonical runtime path:

- `backend_server.py`
- `backend/runtime.py`
- `backend/ingestion/service.py`
- `backend/safety/validator.py`
- `backend/execution/orchestrator.py`
- `backend/dispatcher/service.py`
- `backend/operator/service.py`
- `backend/operator/web.py`
- `integrations/openclaw_mcp/`

The root-level modules with the same old names are thin deprecated shims that
forward here to avoid breaking old scripts during migration.
