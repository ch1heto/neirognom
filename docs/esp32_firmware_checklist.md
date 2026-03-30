# ESP32 Firmware Checklist

Use this checklist when validating firmware against the backend contract.

## Required MQTT Topics

The firmware must publish and subscribe to these canonical topics:

- Telemetry publish: `greenhouse/device/{device_id}/telemetry`
- State publish: `greenhouse/device/{device_id}/state`
- Presence publish: `greenhouse/device/{device_id}/presence`
- Command subscribe: `greenhouse/device/{device_id}/cmd`
- ACK publish: `greenhouse/device/{device_id}/ack`
- RESULT publish: `greenhouse/device/{device_id}/result`

## Required JSON Envelopes

### Telemetry payload

Required fields:

- `message_id`
- `correlation_id`
- `device_id`
- `zone_id`
- `timestamp`
- `message_counter`
- `sensors`
- `status`
- `meta`

Recommended sensor fields used by the backend:

- `ph`
- `ec`
- `water_level`
- `flow_rate_ml_per_min`
- `tank_level`
- `pressure_kpa`
- `leak`
- `overflow`
- `water_available`
- `soil_moisture`

### State payload

Required fields:

- `message_id`
- `correlation_id`
- `device_id`
- `zone_id`
- `timestamp`
- `connectivity`
- `state`
- `message_counter`
- `status`
- `meta`

State fields the operator UI and backend should receive when available:

- `valve_open`
- `pump_on`
- `doser_active`
- `maintenance_mode`

### Presence payload

Required fields:

- `message_id`
- `correlation_id`
- `device_id`
- `zone_id`
- `timestamp`
- `message_counter`
- `connectivity`
- `status`
- `meta`

Allowed `connectivity` values:

- `online`
- `offline`
- `degraded`
- `safe_mode`

### ACK payload

Required fields:

- `message_id`
- `correlation_id`
- `command_id`
- `device_id`
- `zone_id`
- `status`
- `local_timestamp`
- `observed_state`
- `source`
- `status_code`
- `execution_id`
- `step`

### RESULT payload

Required fields:

- `message_id`
- `correlation_id`
- `command_id`
- `device_id`
- `zone_id`
- `status`
- `local_timestamp`
- `observed_state`
- `metrics`
- `source`
- `status_code`
- `execution_id`
- `step`

## Required Actuators

Firmware must understand these actuator/action pairs without changing the shared contract:

- `irrigation_valve` -> `OPEN`, `CLOSE`
- `master_pump` -> `ON`, `OFF`
- `nutrient_doser` -> `START`, `STOP`

## Command Envelope Fields To Honor

The ESP32 command consumer must validate:

- `target_device_id`
- `target_zone_id`
- `command_id`
- `execution_id`
- `actuator`
- `action`
- `duration_sec`
- `ttl_sec`
- `created_at`
- `nonce`
- `step`
- `safety_caps`
- `parameters`

## Local Safety Expectations On Firmware

The firmware should reject or fail safe when:

- `target_device_id` does not match the device
- the command is stale by `ttl_sec`
- `command_id` is duplicated
- `nonce` is duplicated in the replay window
- the device is in `safe_mode`
- requested runtime exceeds local hard limits

## Backend-Facing Sensor Usage

These fields materially affect backend decisions, alarms, or safety locks:

- `water_level`: blocks irrigation and nutrient dosing at low level
- `flow_rate_ml_per_min`: used for flow confirmation and anomaly detection
- `tank_level`: used for empty-tank and depletion-trend alarms
- `pressure_kpa`: used for pressure anomaly detection
- `leak`: creates an emergency lock
- `overflow`: aborts active runs
- `soil_moisture`: checked for stale/noisy sensor patterns when history is available

## Final Validation Before Real Hardware

Confirm each tray/device pair can:

- receive a backend command on the canonical command topic
- reject stale and duplicate commands locally
- publish ACK and RESULT with matching `command_id`, `execution_id`, and `step`
- publish state reflecting `valve_open`, `pump_on`, and `doser_active`
- publish presence transitions for `online`, `offline`, and `safe_mode`
- keep `message_counter` monotonic across telemetry, state, and presence messages
