from __future__ import annotations

import time

from backend.dispatcher.service import CommandDispatcher
from backend.safety.validator import ActionProposal
from backend.state.store import StateStore
from shared.contracts.messages import DecisionOrigin


class BackendToolService:
    def __init__(self, store: StateStore, dispatcher: CommandDispatcher) -> None:
        self._store = store
        self._dispatcher = dispatcher

    def get_current_state(self) -> dict:
        return self._store.get_current_state()

    def get_zone_state(self, zone_id: str) -> dict:
        return self._store.get_zone_state(zone_id)

    def get_sensor_history(self, device_id: str, sensor: str, start_ms: int, end_ms: int, limit: int = 500) -> list[dict]:
        return self._store.get_sensor_history(device_id, sensor, start_ms, end_ms, limit=limit)

    def get_active_alerts(self) -> list[dict]:
        return self._store.get_active_alerts()

    def get_command_status(self, command_id: str) -> dict | None:
        return self._store.get_command_status(command_id)

    def propose_action(self, action: dict) -> dict:
        return {
            "status": "proposal_only",
            "action": action,
            "note": "proposal captured; execute_manual_action still passes through backend safety path",
        }

    def execute_manual_action(self, action: dict) -> dict:
        proposal = ActionProposal(
            trace_id=str(action.get("trace_id") or f"trace-{int(time.time() * 1000)}"),
            device_id=str(action["device_id"]),
            zone_id=str(action["zone_id"]),
            actuator=str(action["actuator"]),
            action=str(action["action"]),
            duration_sec=int(action.get("duration_sec", 0)),
            origin=DecisionOrigin.OPERATOR,
            reason=str(action.get("reason", "manual operator action")),
            requested_at_ms=int(action.get("requested_at_ms", int(time.time() * 1000))),
            metadata=dict(action.get("metadata", {})),
        )
        return self._dispatcher.dispatch_proposal(proposal)
