# Neuroagronom Hybrid Control Architecture

This project implements a hybrid control system for hydroponic nutrient-solution management.

Stage-1 product scope:
- measured directly: `ph`, `ec`, `water_level`
- automatically actuated: `ph`
- contextual/diagnostic only: `ec`
- optional / not onboarded by default: `temp`, `humidity`, `soil`

## Roles

- `ESP32`
  - edge device only
  - reads sensors
  - publishes telemetry over MQTT
  - receives commands over MQTT
  - executes low-level actuator commands
  - emits command acknowledgements over MQTT
  - applies local low-level failsafes such as low-water command rejection

- `MQTT`
  - transport layer only
  - does not own policy, safety, runtime state, or reasoning

- [`smart_bridge.py`](./smart_bridge.py)
  - mandatory control plane
  - consumes raw telemetry from MQTT
  - builds operational state from telemetry windows
  - computes freshness, completeness, readiness, risks, pending effects, and safety blocks
  - decides whether reasoning is needed
  - prepares structured context for OpenClaw
  - validates policy JSON from OpenClaw
  - publishes only safe commands
  - tracks acknowledgements, fallbacks, and post-dose effect windows

- `OpenClaw`
  - policy and reasoning layer only
  - never receives raw MQTT packets directly
  - receives structured operational context from `smart_bridge.py`
  - returns one structured JSON policy response

- `InfluxDB`
  - observability and history only
  - not part of decision-making

- `SQLite`
  - audit trail and runtime state
  - stores alerts, llm requests, commands, control events, and actuator state

## Control Philosophy

Deterministic Python owns:
- transport handling
- timing and retries
- command validation
- actuator allowlists
- duration bounds
- conflict checks
- telemetry freshness and completeness
- readiness state
- escalation/operator locks
- command acknowledgement tracking
- fallback rules
- post-dose effect tracking

OpenClaw owns:
- tactical interpretation
- selected correction strategy
- explanation
- stop and escalation recommendations

OpenClaw is explicitly forbidden from directly controlling MQTT or hardware semantics.

## Runtime Flow

1. ESP32 publishes telemetry on topics such as:
   - `farm/tray01/telemetry/ph`
   - `farm/tray01/telemetry/ec`
   - `farm/tray01/telemetry/water_level`
2. [`smart_bridge.py`](./smart_bridge.py) aggregates telemetry into sensor windows.
3. The bridge computes:
   - latest values
   - trends
   - freshness
   - completeness
   - readiness
   - deviations
   - risks
   - pending command acknowledgements
   - pending effect windows
   - recent corrective actions
   - escalation / operator lock state
4. The bridge decides whether reasoning is required.
5. If reasoning is required, the bridge sends structured context to OpenClaw.
6. OpenClaw returns a strict JSON policy response.
7. [`command_gateway.py`](./command_gateway.py) validates the response.
8. Only validated commands are published to MQTT.
9. ESP32 acknowledges command progress via MQTT: `received`, `executing`, `done`, or `failed`.
10. Only after `done` does the bridge start effect tracking.
11. Later telemetry confirms whether the expected physical effect occurred.

## Reasoning Trigger

The bridge calls OpenClaw only when all of the following are true:

- new telemetry has arrived since the last control decision
- there is a pH deviation outside the current stage target
- the metric is configured as a reasoning-trigger metric
- required reasoning inputs are fresh for that metric
- stage-1 readiness is sufficient for reasoning

Current stage-1 reasoning requirement for pH:
- `ph` fresh
- `ec` fresh
- `water_level` fresh

The bridge skips OpenClaw and logs the reason when:
- pH and EC are already inside policy
- deviations exist but required reasoning inputs are missing
- deterministic safety has already blocked action
- the LLM worker is already busy and no new decision can start

## OpenClaw Contract

OpenClaw receives a structured context prepared by the bridge. It includes:
- current telemetry values and trends
- available metrics
- required metrics
- telemetry completeness and readiness
- telemetry profile and not-onboarded metrics
- stage targets
- deviations
- active safety blocks
- risk state
- recent actions
- pending effect checks
- pending command acknowledgements
- operator lock / escalation conditions
- actuator state and actuator capabilities
- recovery protocol hints

OpenClaw must return exactly one JSON object matching [`openclaw_policy_schema.json`](./openclaw_policy_schema.json).

Required top-level fields:
- `summary`
- `selected_strategy`
- `risk_level`
- `risks`
- `recommended_commands`
- `recheck_interval_sec`
- `stop_conditions`
- `escalation_conditions`

`recommended_commands` contains logical actuator commands only. It must not contain MQTT topics or raw transport instructions.

## MQTT Command / Ack Flow

Command publish topic example:
- `farm/tray01/cmd/chemical/ph_down`

Published command payload example:

```json
{
  "command_id": "cmd-a1b2c3d4e5f6",
  "actuator": "ph_down_pump",
  "action": "ON",
  "duration_sec": 10,
  "reason": "lower pH toward stage target",
  "llm_req_id": 42,
  "issued_at": 1774435200
}
```

Ack topic example:
- `farm/tray01/ack/chemical/ph_down`

Ack payload example:

```json
{
  "command_id": "cmd-a1b2c3d4e5f6",
  "ack_state": "done",
  "actuator": "ph_down_pump",
  "action": "ON",
  "message": "completed",
  "timestamp": 1774435203
}
```

Ack states:
- `received`
- `executing`
- `done`
- `failed`

If final acknowledgement is missing before the timeout:
- the bridge records `ACK_TIMEOUT`
- the command remains unconfirmed
- effect tracking does not start

If acknowledgement is `failed`:
- the bridge records command failure
- effect tracking does not start
- the command is visible in SQLite audit history

## SQLite Audit Trail

Key `control_events` emitted by the bridge stack:
- `bridge_status`
- `llm_decision`
- `llm_context`
- `llm_result`
- `policy_result`
- `command_batch`
- `command_accepted`
- `command_rejected`
- `command_dispatch`
- `command_execution`
- `command_ack`
- `fallback`
- `effect_check`
- `escalation`
- `operator_reset`

## Local Windows Run Order

Environment defaults for local testing:

```powershell
$env:AI_BACKEND='openclaw'
$env:OPENCLAW_URL='http://127.0.0.1:18789'
$env:OPENCLAW_TRANSPORT='auto'
```

1. Start a local MQTT broker.
2. Start the bridge:

```powershell
$env:APP_PROFILE='test'
$env:AI_BACKEND='openclaw'
$env:OPENCLAW_URL='http://127.0.0.1:18789'
$env:OPENCLAW_TRANSPORT='auto'
$env:TRAY_ID='tray01'
$env:MQTT_HOST='localhost'
$env:MQTT_PORT='1883'
.\venv\Scripts\python.exe .\smart_bridge.py
```

3. Optional local dashboard:

```powershell
.\venv\Scripts\python.exe .\dashboard_server.py
```

4. Start the simulator:

```powershell
$env:TRAY_ID='tray01'
$env:MQTT_BROKER_HOST='localhost'
$env:MQTT_BROKER_PORT='1883'
$env:PUBLISH_INTERVAL_SEC='1'
.\venv\Scripts\python.exe .\sim_esp32_stress.py
```

5. Optional operator inspection:

```powershell
.\venv\Scripts\python.exe .\operator_cli.py status
.\venv\Scripts\python.exe .\operator_cli.py events --limit 20
```

6. Optional operator reset:

```powershell
.\venv\Scripts\python.exe .\operator_cli.py reset ph
```

## Production Notes

- `APP_PROFILE=prod` keeps the same control logic and safety flow.
- Only deployment/runtime configuration changes between `test` and `prod`.
- OpenClaw is the only AI entry point in the normal path.
- Missing optional metrics do not trigger panic when they are marked as not onboarded.
