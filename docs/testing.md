# Backend Test Architecture

The backend test suite is split into small layers so safety and execution rules stay deterministic:

- `tests/test_backend_unit.py`: topic routing, command normalization, safety validation, deterministic policy precedence.
- `tests/test_backend_integration.py`: MQTT ingestion to state update to dispatch, duplicate MQTT handling, TTL expiry, max-duration enforcement, dry-run aborts, ACK lifecycle updates.
- `tests/test_backend_ai_flow.py`: Llama-driven decision path with mocked responses only.
- `tests/test_backend_operator_flow.py`: manual operator actions through the same validation and dispatcher path as automation.
- `tests/test_backend_architecture.py`: lower-level state/security regression coverage kept from the architecture refactor.

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

## Design rules

- Tests never send actuator commands outside the backend dispatcher.
- Llama is always mocked.
- MQTT is mocked through `DummyMqttClient`; published payloads still use the production contracts.
- SQLite and Influx production boundaries are preserved through the same store/history abstractions used at runtime.
