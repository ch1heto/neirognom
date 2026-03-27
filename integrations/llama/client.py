from __future__ import annotations

import json
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
                    "content": (
                        "You are a greenhouse reasoning engine. Return exactly one JSON object with keys: "
                        "decision, zone_id, actuator, action, duration_sec, reason, confidence."
                    ),
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
            return LlmDecisionResponse.model_validate_json(content)
        except Exception as exc:
            log.error("Llama response validation failed: %s", exc)
            return None

    @staticmethod
    def _extract_content(body: dict[str, Any]) -> str | None:
        choices = body.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] or {}
            message = first.get("message") or {}
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                return message["content"]
        for key in ("content", "response", "text", "output"):
            value = body.get(key)
            if isinstance(value, str):
                return value
        if isinstance(body, dict):
            try:
                return json.dumps(body, ensure_ascii=False)
            except TypeError:
                return None
        return None
