# OpenClaw MCP Adapter

OpenClaw is now an MCP/operator client only. It does not publish MQTT and does not mutate SQLite directly.

Runtime path:

- OpenClaw MCP client -> backend MCP adapter
- backend MCP adapter -> `OperatorControlService`
- `OperatorControlService` -> `BackendToolService` / dispatcher
- backend safety validator -> MQTT dispatch

## Exposed tools

- `get_current_state`
- `get_device_status`
- `get_zone_status`
- `get_sensor_history`
- `get_command_history`
- `propose_action`
- `execute_manual_action`
- `emergency_stop`

Rules:

- read tools return structured JSON only
- `propose_action` never actuates hardware
- `execute_manual_action` uses the same validation path as the web UI
- `emergency_stop` goes through backend service logic and safe-stop dispatch
- MCP never gets direct MQTT or raw DB access

## Auth

Action tools require the configured token when `OPENCLAW_MCP_REQUIRE_ACTION_TOKEN=1`.

Set:

```env
OPENCLAW_MCP_ENABLED=1
OPENCLAW_MCP_HOST=127.0.0.1
OPENCLAW_MCP_PORT=8790
OPENCLAW_MCP_ACTION_TOKEN=change-me
OPENCLAW_MCP_REQUIRE_ACTION_TOKEN=1
```

## Endpoint

The backend hosts the adapter at:

```text
http://127.0.0.1:8790/mcp
```

Supported JSON-RPC methods:

- `initialize`
- `ping`
- `tools/list`
- `tools/call`

Example `tools/call` payload:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_manual_action",
    "arguments": {
      "auth_token": "change-me",
      "operator_id": "openclaw",
      "operator_name": "OpenClaw",
      "zone_id": "tray_1",
      "device_id": "esp32-1",
      "ui_action": "test_watering",
      "duration_sec": 8
    }
  }
}
```
