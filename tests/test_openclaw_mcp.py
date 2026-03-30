from __future__ import annotations

import unittest

from tests.fixtures import device_state_payload, telemetry_payload
from tests.harness import BackendTestHarness


class OpenClawMcpTests(unittest.TestCase):
    def test_tools_list_contains_safe_surface(self) -> None:
        harness = BackendTestHarness()
        response = harness.mcp_server.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})

        tools = response["result"]["tools"]
        names = {tool["name"] for tool in tools}
        self.assertEqual(
            names,
            {
                "get_current_state",
                "get_device_status",
                "get_zone_status",
                "get_sensor_history",
                "get_command_history",
                "propose_action",
                "execute_manual_action",
                "emergency_stop",
            },
        )

    def test_read_tools_return_structured_json(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        harness.ingest_telemetry(
            telemetry_payload(
                message_id="msg-mcp-read-0001",
                trace_id="trace-mcp-read-0001",
                sensors={"soil_moisture": 27.0, "temperature": 22.0, "tank_level": 88.0},
            )
        )
        response = harness.mcp_server.handle_rpc(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "get_zone_status", "arguments": {"zone_id": "tray_1"}},
            }
        )

        payload = response["result"]["content"][0]["json"]
        self.assertEqual(payload["zone_id"], "tray_1")
        self.assertEqual(payload["telemetry"]["tank_level"], 88.0)
        self.assertIn("actions", payload)

    def test_propose_action_does_not_dispatch_hardware(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.mcp_server.handle_rpc(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "propose_action",
                    "arguments": {
                        "auth_token": "test-mcp-token",
                        "operator_id": "openclaw",
                        "operator_name": "OpenClaw",
                        "zone_id": "tray_1",
                        "device_id": "esp32-1",
                        "ui_action": "test_watering",
                        "duration_sec": 7,
                    },
                },
            }
        )

        payload = response["result"]["content"][0]["json"]
        self.assertEqual(payload["status"], "proposal_only")
        self.assertEqual(payload["normalized_action"]["metadata"]["submitted_via"], "openclaw_mcp")
        self.assertEqual(len(harness.mqtt.published), 0)

    def test_execute_manual_action_requires_auth_and_uses_backend_pipeline(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())

        denied = harness.mcp_server.handle_rpc(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "execute_manual_action",
                    "arguments": {
                        "zone_id": "tray_1",
                        "device_id": "esp32-1",
                        "ui_action": "test_watering",
                    },
                },
            }
        )
        allowed = harness.mcp_server.handle_rpc(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "execute_manual_action",
                    "arguments": {
                        "auth_token": "test-mcp-token",
                        "operator_id": "openclaw",
                        "operator_name": "OpenClaw",
                        "zone_id": "tray_1",
                        "device_id": "esp32-1",
                        "ui_action": "test_watering",
                        "duration_sec": 8,
                    },
                },
            }
        )

        self.assertEqual(denied["error"]["message"], "invalid_action_auth_token")
        payload = allowed["result"]["content"][0]["json"]
        self.assertEqual(payload["status"], "DISPATCHED")
        self.assertEqual(harness.mqtt.published[-1]["payload"]["step"], "open_valve")
        command = harness.command_status(payload["command_id"])
        assert command is not None
        self.assertEqual(command["metadata"]["submitted_via"], "openclaw_mcp")

    def test_emergency_stop_flows_through_backend_service(self) -> None:
        harness = BackendTestHarness()
        harness.seed_device_state(device_state_payload())
        response = harness.mcp_server.handle_rpc(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "emergency_stop",
                    "arguments": {
                        "auth_token": "test-mcp-token",
                        "operator_id": "openclaw",
                        "operator_name": "OpenClaw",
                        "reason": "operator stop",
                    },
                },
            }
        )

        payload = response["result"]["content"][0]["json"]
        self.assertEqual(payload["status"], "emergency_stop_active")
        self.assertFalse(harness.store.get_automation_flags()["automation_enabled"]["enabled"])
        self.assertTrue(any(lock["kind"] == "manual_emergency_stop" for lock in harness.store.get_active_safety_locks("global")))


if __name__ == "__main__":
    unittest.main()
