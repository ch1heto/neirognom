# Backend Test Architecture

The backend test suite is split into small layers so safety and execution rules stay deterministic:

- `tests/test_backend_unit.py`: topic routing, command normalization, safety validation, deterministic policy precedence.
- `tests/test_backend_integration.py`: MQTT ingestion to state update to dispatch, duplicate MQTT handling, TTL expiry, max-duration enforcement, dry-run aborts, ACK lifecycle updates.
- `tests/test_backend_ai_flow.py`: Llama-driven decision path with mocked responses only.
- `tests/test_backend_operator_flow.py`: manual operator actions through the same validation and dispatcher path as automation.
- `tests/test_backend_architecture.py`: lower-level state/security regression coverage kept from the architecture refactor.
- `tests/test_sim_esp32.py`: local fake-device simulator contract for the hydroponic runtime.

Reusable helpers live in:

- `tests/fixtures.py`: deterministic telemetry, state, ACK, RESULT and mocked Llama payload builders.
- `tests/harness.py`: in-memory backend harness with `MemoryStateStore`, `MemoryTelemetryHistoryStore`, `SecurityMonitor`, `DecisionEngine`, `CommandDispatcher`, and `BackendToolService`.

## Run locally

Use the CI-friendly entrypoint:

```powershell
.\venv\Scripts\python.exe .\run_backend_tests.py
```

Or plain unittest discovery:

```powershell
.\venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

## Manual runtime test

Run the real backend runtime:

```powershell
.\venv\Scripts\python.exe .\backend_server.py
```

Run the current fake ESP32 simulator in a second terminal:

```powershell
.\venv\Scripts\python.exe .\sim_esp32.py --mode normal
```

Supported simulator modes:

- `normal`: pH/EC/water_level drift around target values for all four trays
- `low_ph`: keeps pH below target and is intended to exercise the Llama path
- `high_ph`: keeps pH above target and is intended to exercise the Llama path
- `low_ec`: keeps EC below target and is intended to exercise the Llama path
- `high_ec`: keeps EC above target and is intended to exercise the Llama path
- `low_water`: drops water level low enough to trigger deterministic safety handling

Useful examples:

```powershell
.\venv\Scripts\python.exe .\sim_esp32.py --mode low_ph
.\venv\Scripts\python.exe .\sim_esp32.py --mode low_ec
.\venv\Scripts\python.exe .\sim_esp32.py --tray-id tray_1 --mode low_water
```

Manual operator path:

- start the backend
- start the simulator
- open the operator UI, by default `http://127.0.0.1:8780`
- trigger `Открыть клапан`, `Закрыть клапан`, or `Дозировать раствор`
- watch simulator stdout for command, ACK, RESULT, and the current tray state

Llama path:

- run backend with a working local Llama endpoint
- start the simulator in `low_ph`, `high_ph`, `low_ec`, or `high_ec`
- backend should then build hydroponic context from `ph`, `ec`, and `water_level` and fall through to the Llama decision path

## Design rules

- Tests never send actuator commands outside the backend dispatcher.
- Llama is always mocked.
- MQTT is mocked through `DummyMqttClient`; published payloads still use the production contracts.
- SQLite and Influx production boundaries are preserved through the same store/history abstractions used at runtime.
