from __future__ import annotations
import logging
from typing import Any

import requests

from backend.config import LlamaConfig
from shared.contracts.messages import LlmDecisionRequest, LlmDecisionResponse


log = logging.getLogger("integrations.llama")


class LlamaDecisionClient:
    def __init__(self, config: LlamaConfig) -> None:
        self._config = config

    def recommend(self, request: LlmDecisionRequest) -> LlmDecisionResponse | None:
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["Authorization"] = f"Bearer {self._config.api_key}"

        payload = {
            "model": self._config.model,
            "messages": [
                {
                    "role": "system",
                    "content": self._system_prompt(),
                },
                {
                    "role": "user",
                    "content": request.model_dump_json(),
                },
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        try:
            response = requests.post(
                self._config.api_url,
                headers=headers,
                json=payload,
                timeout=self._config.timeout_sec,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            log.error("Llama request failed: %s", exc)
            return None

        body = response.json()
        content = self._extract_content(body)
        if content is None:
            log.error("Llama response did not contain extractable content")
            return None
        try:
            return LlmDecisionResponse.model_validate_json(content.strip())
        except Exception as exc:
            log.error("Llama response validation failed: %s", exc)
            return None

    @staticmethod
    def _system_prompt() -> str:
        return (
            "You are a hydroponic tray reasoning engine. "
            "Return exactly one JSON object and nothing else. "
            "Do not output markdown, code fences, prose, comments, explanations, bullet points, or surrounding text. "
            "CRITICAL: DO NOT echo back the input telemetry. "
            "CRITICAL: DO NOT include input fields like device_id, ts_ms, sensor, value, message_id, trace_id, current_state, telemetry_windows, or allowed_actions in your output. "
            "CRITICAL: ONLY return fields allowed by the response schema. "
            "The JSON must match this exact schema with no extra fields: "
            '{"decision":"no_action|open_valve|close_valve|dose_solution",'
            '"zone_id":"string","requested_duration_sec":"integer|null","dose_ml":"integer|null",'
            '"rationale":"string","confidence":"number"}. '
            "Required fields: decision, zone_id, rationale, confidence. "
            "Optional fields: requested_duration_sec, dose_ml. "
            "Do not use alternate field names like reason, duration_sec, actuator, action, metadata, notes, description, or max_duration_sec. "
            "If decision is no_action, still include zone_id, rationale, and confidence. "
            "Use requested_duration_sec only for open_valve. "
            "Use dose_ml only for dose_solution when nutrient dosing is justified by pH or EC context. "
            "EXAMPLE OUTPUT: "
            '{"decision":"open_valve","zone_id":"tray_1","requested_duration_sec":30,"dose_ml":null,"rationale":"Water level is safe and the tray needs irrigation.","confidence":0.95}'
        )

    @staticmethod
    def _extract_content(body: dict[str, Any]) -> str | None:
        choices = body.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] or {}
            message = first.get("message") or {}
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    text_parts: list[str] = []
                    for part in content:
                        if isinstance(part, dict) and isinstance(part.get("text"), str):
                            text_parts.append(part["text"])
                    if text_parts:
                        return "".join(text_parts)
        for key in ("content", "response"):
            value = body.get(key)
            if isinstance(value, str):
                return value
        return None

