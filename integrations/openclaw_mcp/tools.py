from __future__ import annotations

from typing import Any

from backend.config import OpenClawMcpConfig
from backend.operator.service import OperatorControlService


class McpAuthError(RuntimeError):
    pass


class OpenClawMcpAdapter:
    """Operator-facing adapter. OpenClaw is a client of backend tools, not the core runtime."""

    def __init__(self, service: OperatorControlService, config: OpenClawMcpConfig) -> None:
        self._service = service
        self._config = config

    def get_current_state(self) -> dict:
        return self._service.get_current_state()

    def get_device_status(self, device_id: str) -> dict:
        return self._service.get_device_status(device_id)

    def get_zone_status(self, zone_id: str) -> dict:
        return self._service.get_zone_status(zone_id)

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> dict:
        return self._service.get_sensor_history(device_id, sensor, start_ms, end_ms, limit)

    def get_command_history(self, limit: int = 50) -> dict:
        return self._service.command_history(limit=limit)

    def propose_action(self, action: dict) -> dict:
        self._require_action_auth(action)
        payload = dict(action)
        payload.setdefault("submitted_via", "openclaw_mcp")
        return self._service.propose_action(payload)

    def execute_manual_action(self, action: dict) -> dict:
        self._require_action_auth(action)
        payload = dict(action)
        payload.setdefault("submitted_via", "openclaw_mcp")
        return self._service.execute_manual_action(payload)

    def emergency_stop(self, payload: dict[str, Any]) -> dict:
        self._require_action_auth(payload)
        normalized = dict(payload)
        normalized.setdefault("submitted_via", "openclaw_mcp")
        return self._service.emergency_stop(normalized)

    def tool_definitions(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "get_current_state",
                "description": "Return the current backend control and safety state as structured JSON.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            },
            {
                "name": "get_device_status",
                "description": "Return the current status summary for a single device.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"device_id": {"type": "string"}},
                    "required": ["device_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_zone_status",
                "description": "Return the current status summary for a single zone/tray.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"zone_id": {"type": "string"}},
                    "required": ["zone_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_sensor_history",
                "description": "Return historical sensor samples from the telemetry history store.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "device_id": {"type": "string"},
                        "sensor": {"type": "string"},
                        "start_ms": {"type": "integer"},
                        "end_ms": {"type": "integer"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["device_id", "sensor", "start_ms", "end_ms"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_command_history",
                "description": "Return recent backend command journal entries.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"limit": {"type": "integer"}},
                    "additionalProperties": False,
                },
            },
            {
                "name": "propose_action",
                "description": "Normalize and preview an operator action without actuating hardware.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "auth_token": {"type": "string"},
                        "operator_id": {"type": "string"},
                        "operator_name": {"type": "string"},
                        "zone_id": {"type": "string"},
                        "device_id": {"type": "string"},
                        "ui_action": {"type": "string"},
                        "actuator": {"type": "string"},
                        "action": {"type": "string"},
                        "duration_sec": {"type": "integer"},
                        "reason": {"type": "string"},
                        "metadata": {"type": "object"},
                    },
                    "required": ["zone_id"],
                    "additionalProperties": True,
                },
            },
            {
                "name": "execute_manual_action",
                "description": "Submit a manual action through the normal backend validation and dispatch pipeline.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "auth_token": {"type": "string"},
                        "operator_id": {"type": "string"},
                        "operator_name": {"type": "string"},
                        "zone_id": {"type": "string"},
                        "device_id": {"type": "string"},
                        "ui_action": {"type": "string"},
                        "actuator": {"type": "string"},
                        "action": {"type": "string"},
                        "duration_sec": {"type": "integer"},
                        "reason": {"type": "string"},
                        "metadata": {"type": "object"},
                    },
                    "required": ["zone_id"],
                    "additionalProperties": True,
                },
            },
            {
                "name": "emergency_stop",
                "description": "Trigger backend emergency stop and safe-stop commands for known zones/devices.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "auth_token": {"type": "string"},
                        "operator_id": {"type": "string"},
                        "operator_name": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                    "additionalProperties": True,
                },
            },
        ]

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        args = dict(arguments or {})
        handlers = {
            "get_current_state": lambda payload: self.get_current_state(),
            "get_device_status": lambda payload: self.get_device_status(str(payload["device_id"])),
            "get_zone_status": lambda payload: self.get_zone_status(str(payload["zone_id"])),
            "get_sensor_history": lambda payload: self.get_sensor_history(
                str(payload["device_id"]),
                str(payload["sensor"]),
                int(payload["start_ms"]),
                int(payload["end_ms"]),
                int(payload.get("limit", 500)),
            ),
            "get_command_history": lambda payload: self.get_command_history(int(payload.get("limit", 50))),
            "propose_action": lambda payload: self.propose_action(payload),
            "execute_manual_action": lambda payload: self.execute_manual_action(payload),
            "emergency_stop": lambda payload: self.emergency_stop(payload),
        }
        if name not in handlers:
            raise KeyError(f"unknown_tool:{name}")
        return handlers[name](args)

    def _require_action_auth(self, payload: dict[str, Any]) -> None:
        if not self._config.require_action_token:
            return
        expected = self._config.action_auth_token
        provided = str(payload.get("auth_token") or "")
        if not expected or provided != expected:
            raise McpAuthError("invalid_action_auth_token")
