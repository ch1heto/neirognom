# ESP32 <-> Backend Protocol

This document is the practical firmware-facing contract for the current
backend-centric architecture. It complements, and does not replace,
[security_model.md](security_model.md).

## Canonical MQTT topics

- Telemetry: `greenhouse/device/{device_id}/telemetry`
- Device state: `greenhouse/device/{device_id}/state`
- Presence / LWT: `greenhouse/device/{device_id}/presence`
- Backend commands: `greenhouse/device/{device_id}/cmd`
- Device ACK: `greenhouse/device/{device_id}/ack`
- Device RESULT: `greenhouse/device/{device_id}/result`
- Backend/system events: `greenhouse/system/events`

Legacy aliases like `telemetry/raw`, `cmd/execute`, `cmd/ack`, and
`cmd/result` are still accepted by backend parsing for migration, but new
firmware should use only the canonical topics above.

## Telemetry envelope

Devices publish telemetry with:

- `message_id`
- `correlation_id`
- `device_id`
- `zone_id`
- `timestamp`
- `message_counter`
- `sensors`
- `status`
- `meta`
- `local_timestamp`

Rules:

- `message_id` must be unique for exact deduplication.
- `message_counter` should increase monotonically per device runtime.
- Backend may keep stale/out-of-order telemetry in history, but must not let it
  roll back current operational state.

## Presence envelope

Devices publish presence / heartbeat / LWT with:

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

`offline` and `safe_mode` are operationally important: backend faults active
executions for that device and blocks new activating commands.

## Command envelope

Backend publishes commands with:

- `message_id`
- `command_id`
- `correlation_id`
- `source`
- `target_device_id`
- `target_zone_id`
- `action`
- `duration_sec`
- `ttl_sec`
- `created_at`
- `safety_caps`
- `execution_id`
- `actuator`
- `step`
- `nonce`
- `parameters`

Field rules:

- `target_device_id` is mandatory and must match the receiving device.
- `target_zone_id` is mandatory and must match the controlled tray/zone.
- `actuator` and `action` are machine-readable and must not be inferred from
  free text.
- `nonce` is replay-resistance metadata for the command.
- `step` is the backend execution step name.

## ACK / RESULT envelopes

Devices publish ACK and RESULT with these common fields:

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
- `error_code`
- `error_message`

ACK also includes:

- `execution_id`
- `step`

RESULT also includes:

- `execution_id`
- `step`
- `metrics`

`status_code` must always be present and machine-readable. `error_code` may be
empty/null on success. `error_message` should stay short and operational.

## TTL semantics

- A command is stale if `created_at + ttl_sec*1000 <= local_time_ms`.
- ESP32 must reject stale commands before actuation.
- Backend requires `ttl_sec > 0` for activating commands such as `OPEN`,
  `START`, `ON`, `DIM_50`.
- Non-activating safe-stop commands like `OFF`, `CLOSE`, or `ABORT` may use
  `ttl_sec = 0` when emitted during a fault/expiry path.

## message_counter semantics

- `message_counter` is per-device and monotonic.
- Backend uses it together with `message_id` to detect duplicates, stale
  telemetry, and acceptable short-range reordering.
- Firmware should not reset it except on reboot.

## command_id / nonce / replay handling

ESP32 must:

- reject commands with a mismatched `target_device_id`
- reject duplicate `command_id`
- reject duplicate `nonce` inside the active replay window
- reject stale commands

Backend must:

- deduplicate inbound `message_id`
- reject ACK/RESULT for unknown `command_id`
- reject ACK/RESULT with mismatched `device_id`, `zone_id`, `execution_id`, or
  `step`

## Local hard limits expected on ESP32

Firmware must enforce local hard limits even if backend asks for more:

- max valve open duration
- max pump run duration
- watchdog timeout for active actuators
- local safe stop on Wi-Fi or MQTT loss

Effective local runtime should be:

- `min(duration_sec * 1000, safety_caps.local_hard_max_duration_ms, device_local_limit_ms)`

## Safe mode behavior

ESP32 should enter `safe_mode` when safe execution is not possible, including:

- Wi-Fi loss
- MQTT session loss
- stale backend command channel / heartbeat
- local actuator or sensor fault that makes watering unsafe

In `safe_mode`, firmware should:

- close irrigation valves
- stop shared pump
- refuse new activating commands
- publish presence with `connectivity = safe_mode`

## Required device error codes

At minimum, firmware should emit these codes:

- `stale_command`
- `duplicate_command`
- `invalid_target_device`
- `duration_exceeds_local_limit`
- `device_in_safe_mode`
- `malformed_command`
- `execution_timeout`

Additional codes are allowed, but these should remain stable for backend audit,
operator UI, and MCP consumers.

## Expected ACK / RESULT transitions

Normal path:

1. ACK `received` or `acked`
2. optional ACK `running`
3. RESULT `completed`

Rejection / failure path:

1. ACK `rejected`, `failed`, or `expired`
2. or RESULT `failed`, `expired`, or `aborted`

Rules:

- Duplicate ACK/RESULT for the same `(command_id, execution_id, step, status)`
  should be safe and idempotent.
- RESULT must correspond to the same `execution_id` and `step` that backend
  currently expects.
- If local actuation times out, use machine-readable timeout status together
  with `execution_timeout`.

## Manual test checklist

- telemetry with increasing `message_counter`
- presence `online -> offline -> online`
- stale command rejection by TTL
- duplicate command rejection by `command_id`
- invalid target device rejection
- local duration clamp / rejection
- safe-mode refusal of new activating commands
- ACK / RESULT correlation by `command_id` and `execution_id`
