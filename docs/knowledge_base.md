# Knowledge Base Usage

## Loaded By Runtime

These files are actively loaded by the current backend runtime:

- `knowledge_base/grow_maps/*.json`
  Used by `backend/state/store.py` to attach `zone_state.grow_map`.
- `knowledge_base/alerts/critical_thresholds.json`
  Used by `backend/decision_engine/engine.py` and `backend/decision_engine/context_builder.py`.

## Currently Reference-Only

These files are present but are not loaded by the active runtime code:

- `knowledge_base/alerts/control_safety.json`
- `knowledge_base/alerts/recovery_protocols.json`
- `knowledge_base/equipment/actuator_registry.json`

They are kept as design/reference assets only.
Their actuator names and MQTT topics should not be treated as the active runtime contract.

## Active Runtime Contract Sources

Use these files for the live contract instead:

- `shared/contracts/messages.py`
- `mqtt/topics.py`
- `backend/safety/validator.py`
- `docs/esp32_backend_protocol.md`
- `docs/esp32_firmware_checklist.md`