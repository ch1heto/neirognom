# Architecture Layers

## Active runtime path

1. `ESP32 devices`
   - publish telemetry and device state over MQTT
   - execute backend-issued commands only
2. `MQTT`
   - transport only
3. `Backend runtime`
   - ingestion
   - state persistence in SQLite
   - telemetry history writes to InfluxDB
   - deterministic decision rules
   - optional Llama recommendation step
   - safety validation
   - command dispatch / execution tracking
4. `Operator adapters`
   - web UI
   - OpenClaw MCP client

## Safety boundary

All actuator commands must flow through:

```text
operator/AI input -> backend service/tool layer -> safety validator -> dispatcher -> MQTT -> device
```

Forbidden bypasses:

- browser -> MQTT
- OpenClaw/MCP -> MQTT
- Llama -> device
- direct SQLite mutation for control execution

## Operator UI role

- reads backend control/safety state
- submits manual actions through backend APIs
- shows rejection reasons from the backend
- does not own safety logic

## MCP role

- adapter surface for OpenClaw as an operator/reasoning client
- read-only tools plus a minimal authenticated action surface
- action tools use the same backend validation path as the web UI
- no direct transport or orchestrator role
