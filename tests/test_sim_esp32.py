from __future__ import annotations

import unittest

from sim_esp32 import FakeEsp32Simulator, MODES


class SimulatorContractTests(unittest.TestCase):
    def test_hydroponic_modes_are_available(self) -> None:
        self.assertEqual(
            set(MODES.keys()),
            {"normal", "low_ph", "high_ph", "low_ec", "high_ec", "low_water"},
        )

    def test_telemetry_message_uses_hydroponic_sensor_schema(self) -> None:
        simulator = FakeEsp32Simulator(
            broker_host="localhost",
            broker_port=1883,
            broker_username="",
            broker_password="",
            publish_interval_sec=3.0,
            mode="normal",
            valve_limit_sec=15,
            dose_limit_sec=10,
            tray_id="tray_1",
        )
        tray = simulator._tray_states["tray_1"]
        message = simulator._build_telemetry_message(tray)

        self.assertEqual(message.device_id, "esp32-1")
        self.assertEqual(message.zone_id, "tray_1")
        self.assertEqual(set(message.sensors.keys()), {"ph", "ec", "water_level"})


if __name__ == "__main__":
    unittest.main()
