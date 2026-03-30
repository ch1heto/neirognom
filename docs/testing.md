# Local Verification

Automated tests were removed from this repository.
Use the backend plus `sim_esp32.py` for local verification.

## Start Locally

Backend:

```powershell
& .\venv\Scripts\Activate.ps1; python .\backend_server.py
```

Simulator:

```powershell
& .\venv\Scripts\Activate.ps1; python .\sim_esp32.py
```

Dashboard URL:

```text
http://127.0.0.1:8780
```

## Useful Simulator Modes

- `normal`
- `low_ph`
- `high_ph`
- `low_ec`
- `high_ec`
- `low_water`

Examples:

```powershell
& .\venv\Scripts\Activate.ps1; python .\sim_esp32.py --mode low_ph
```

```powershell
& .\venv\Scripts\Activate.ps1; python .\sim_esp32.py --tray-id tray_1 --mode low_water
```

## What To Check

- telemetry and presence arrive for all expected trays
- operator UI updates device and zone state
- event log fills with audit and alarm activity
- manual mode can open/close valves and toggle pump/doser actions through the backend
- irrigation sequences produce realistic `pump_on`, `flow_rate_ml_per_min`, and `pressure_kpa` telemetry
- LLM fallback still behaves safely when Ollama is unavailable