from __future__ import annotations

from backend.api.tools import BackendToolService


class OpenClawMcpAdapter:
    """Operator-facing adapter. OpenClaw is a client of backend tools, not the core runtime."""

    def __init__(self, backend_tools: BackendToolService) -> None:
        self._tools = backend_tools

    def get_current_state(self) -> dict:
        return self._tools.get_current_state()

    def get_zone_state(self, zone_id: str) -> dict:
        return self._tools.get_zone_state(zone_id)

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict]:
        return self._tools.get_sensor_history(device_id, sensor, start_ms, end_ms, limit)

    def get_active_alerts(self) -> list[dict]:
        return self._tools.get_active_alerts()

    def propose_action(self, action: dict) -> dict:
        return self._tools.propose_action(action)

    def execute_manual_action(self, action: dict) -> dict:
        return self._tools.execute_manual_action(action)

    def get_command_status(self, command_id: str) -> dict | None:
        return self._tools.get_command_status(command_id)
