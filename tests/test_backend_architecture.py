import unittest

from backend.config import load_backend_config
from backend.safety.validator import ActionProposal, SafetyValidator
from backend.state.store import MemoryStateStore
from shared.contracts.messages import DecisionOrigin, TelemetryMessage


class BackendArchitectureTests(unittest.TestCase):
    def test_telemetry_contract_requires_zone_and_device(self) -> None:
        message = TelemetryMessage.model_validate(
            {
                "message_id": "msg-12345678",
                "trace_id": "trace-12345678",
                "device_id": "esp32-1",
                "zone_id": "tray_1",
                "ts_ms": 1000,
                "local_ts_ms": 1000,
                "sensors": {"soil_moisture": 12.4, "temperature": 29.3},
            }
        )
        self.assertEqual(message.device_id, "esp32-1")
        self.assertEqual(message.zone_id, "tray_1")

    def test_safety_validator_blocks_leak_and_cooldown(self) -> None:
        config = load_backend_config()
        store = MemoryStateStore()
        store.initialize(config.zone_configs(), config.global_safety)
        zone = store.get_zone_state("tray_1")
        zone["telemetry"] = {"leak": True}
        zone["last_watering_at_ms"] = 10_000
        store._zones["tray_1"] = zone  # test-only state seed

        validator = SafetyValidator(config)
        decision = validator.validate(
            store,
            ActionProposal(
                trace_id="trace-12345678",
                device_id="esp32-1",
                zone_id="tray_1",
                actuator="irrigation_valve",
                action="OPEN",
                duration_sec=10,
                origin=DecisionOrigin.OPERATOR,
                reason="manual watering",
                requested_at_ms=11_000,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("leak_detected", decision.reasons)
        self.assertIn("zone_cooldown_active", decision.reasons)


if __name__ == "__main__":
    unittest.main()
