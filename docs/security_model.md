# Safe Command Channel

## Command contract

Backend publishes only `ActuatorCommandMessage`:

- `message_id`
- `trace_id`
- `command_id`
- `execution_id`
- `device_id`
- `zone_id`
- `actuator`
- `action`
- `step`
- `issued_at_ms`
- `expires_at_ms`
- `nonce`
- `max_duration_ms`
- `safety_constraints`
- `parameters`

`safety_constraints` contains:

- `require_backend_safety`
- `reject_if_offline`
- `reject_if_stale_heartbeat`
- `reject_if_leak_suspected`
- `reject_if_empty_tank`
- `local_hard_max_duration_ms`
- `allowed_runtime_window_ms`

## ACK contract

Devices publish only `CommandAck`:

- `message_id`
- `trace_id`
- `command_id`
- `execution_id`
- `step`
- `device_id`
- `zone_id`
- `status`
- `local_timestamp_ms`
- `observed_state`
- `error_code`
- `error_message`

Allowed `status`:

- `received`
- `acked`
- `running`
- `rejected`
- `failed`
- `expired`

## RESULT contract

Devices publish only `CommandResult`:

- `message_id`
- `trace_id`
- `command_id`
- `execution_id`
- `step`
- `device_id`
- `zone_id`
- `status`
- `local_timestamp_ms`
- `observed_state`
- `metrics`
- `error_code`
- `error_message`

Allowed `status`:

- `completed`
- `failed`
- `expired`
- `aborted`

## Device-side rules

- Accept only commands where `device_id` matches the device identity burned into firmware or secure config.
- Reject any command where `expires_at_ms <= local_time_ms`.
- Keep a bounded replay cache of recent `command_id` values and reject duplicates.
- Keep a bounded replay cache of recent `nonce` values inside the active TTL window.
- Apply `effective_max_runtime_ms = min(max_duration_ms, safety_constraints.local_hard_max_duration_ms, device_local_hard_limit_ms)`.
- Enter fail-safe if backend heartbeat or MQTT session is stale.
- Never execute commands that arrive from any topic other than the backend command topic.

## Backend-side validation

Backend rejects and audit-logs:

- ACK/RESULT for unknown `command_id`
- ACK/RESULT with mismatched `device_id`
- ACK/RESULT with mismatched `zone_id`
- ACK/RESULT with mismatched `execution_id`
- ACK/RESULT with mismatched `step`
- malformed lifecycle transitions
- replay-suspected duplicate `message_id`

## Replay protection

- Backend issues a fresh `nonce` per command.
- Backend stores `command_id` and `execution_id` in SQLite.
- Backend rejects duplicate inbound `message_id` values.
- Devices should maintain a local recent-command cache keyed by `command_id` and `nonce`.
- All commands are TTL-bound by `expires_at_ms`.

## Heartbeat staleness

- `last_seen_ms` is updated from telemetry and state messages.
- Backend creates a `stale_heartbeat` device lock if `now - last_seen_ms > DEVICE_HEARTBEAT_TIMEOUT_SEC`.
- Active executions on that device are aborted through the normal safe-stop path.

## InfluxDB anomaly inputs

Backend queries InfluxDB history for:

- unexpected flow without active execution
- rapid tank depletion trend
- pressure out-of-range
- stale sensor values
- noisy sensor values

When detected, backend persists:

- active `Alarm`
- active `SafetyLock` when the anomaly is safety-relevant

## MQTT ACL example

See [mqtt_acl_example.txt](/V:/work/DIPLOM/testMoskitto/docs/mqtt_acl_example.txt).

## Assumptions and limitations

- MQTT ACLs and device authentication are broker-enforced; this repository does not provision broker credentials automatically.
- `nonce` improves replay resistance but is not a cryptographic signature. If command authenticity must survive broker compromise, add message signing or mTLS-backed broker auth.
- Heartbeat staleness depends on reasonably synchronized device clocks and regular telemetry/state traffic.
- Influx-based anomaly detection is heuristic and should be tuned against real sensor behavior.
