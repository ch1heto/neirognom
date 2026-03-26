from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import requests

try:
    from websocket import WebSocketException, create_connection
except ImportError:  # pragma: no cover - depends on local install
    WebSocketException = Exception
    create_connection = None


REQUIRED_POLICY_KEYS = {
    "summary",
    "selected_strategy",
    "risk_level",
    "risks",
    "recommended_commands",
    "recheck_interval_sec",
    "stop_conditions",
    "escalation_conditions",
}


@dataclass
class OpenClawRoundtrip:
    ok: bool
    transport: str
    endpoint: str
    response_text: Optional[str] = None
    response_preview: str = ""
    status_code: Optional[int] = None
    error: str = ""


def resolve_transport(raw_url: str, transport_mode: str = "auto") -> tuple[str, str]:
    mode = (transport_mode or "auto").strip().lower()
    parsed = urlparse(raw_url.strip())
    scheme = (parsed.scheme or "http").lower()

    if mode in {"ws", "websocket"}:
        if scheme in {"http", "https"}:
            ws_scheme = "wss" if scheme == "https" else "ws"
            return "websocket", urlunparse(parsed._replace(scheme=ws_scheme))
        return "websocket", raw_url

    if mode == "http":
        return "http", raw_url

    if scheme in {"ws", "wss"}:
        return "websocket", raw_url

    if scheme in {"http", "https"} and (parsed.path or "/") in {"", "/"}:
        ws_scheme = "wss" if scheme == "https" else "ws"
        return "websocket", urlunparse(parsed._replace(scheme=ws_scheme))

    return "http", raw_url


def build_payload(model: str, system_prompt: str, user_message: str) -> dict[str, Any]:
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }


def _preview(text: str, limit: int = 240) -> str:
    return text.replace("\r", " ").replace("\n", " ")[:limit]


def _extract_content(data: Any) -> Optional[str]:
    if isinstance(data, str):
        return data
    if not isinstance(data, dict):
        return None

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] or {}
        if isinstance(first, dict):
            msg = first.get("message") or {}
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                return msg["content"]
            if isinstance(first.get("text"), str):
                return first["text"]

    for key in ("response", "content", "text", "output"):
        value = data.get(key)
        if isinstance(value, str):
            return value

    nested = data.get("data")
    if nested is not None:
        return _extract_content(nested)
    return None


def _ws_roundtrip(
    endpoint: str,
    payload: dict[str, Any],
    timeout_sec: int,
    logger: logging.Logger,
) -> OpenClawRoundtrip:
    if create_connection is None:
        return OpenClawRoundtrip(
            ok=False,
            transport="websocket",
            endpoint=endpoint,
            error="websocket-client dependency is not installed",
        )

    logger.info("OpenClaw request send attempt | transport=websocket endpoint=%s", endpoint)
    ws = None
    try:
        ws = create_connection(endpoint, timeout=timeout_sec)
        ws.send(json.dumps(payload, ensure_ascii=False))

        deadline = time.time() + timeout_sec
        chunks: list[str] = []
        last_preview = ""
        while time.time() < deadline:
            message = ws.recv()
            if message is None:
                continue
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            last_preview = _preview(str(message))
            logger.info("OpenClaw websocket response preview | %s", last_preview)
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                chunks.append(str(message))
                if chunks:
                    return OpenClawRoundtrip(
                        ok=True,
                        transport="websocket",
                        endpoint=endpoint,
                        response_text="".join(chunks).strip(),
                        response_preview=_preview("".join(chunks)),
                    )
                continue

            content = _extract_content(data)
            if content is not None:
                return OpenClawRoundtrip(
                    ok=True,
                    transport="websocket",
                    endpoint=endpoint,
                    response_text=content.strip(),
                    response_preview=_preview(content),
                )

            delta = data.get("delta") or data.get("token")
            if isinstance(delta, str):
                chunks.append(delta)

            done = bool(data.get("done")) or data.get("type") in {"final", "done", "response"}
            if done and chunks:
                text = "".join(chunks).strip()
                return OpenClawRoundtrip(
                    ok=True,
                    transport="websocket",
                    endpoint=endpoint,
                    response_text=text,
                    response_preview=_preview(text),
                )

        if chunks:
            text = "".join(chunks).strip()
            return OpenClawRoundtrip(
                ok=True,
                transport="websocket",
                endpoint=endpoint,
                response_text=text,
                response_preview=_preview(text),
            )
        return OpenClawRoundtrip(
            ok=False,
            transport="websocket",
            endpoint=endpoint,
            response_preview=last_preview,
            error="timeout waiting for websocket response",
        )
    except (OSError, WebSocketException) as exc:
        return OpenClawRoundtrip(
            ok=False,
            transport="websocket",
            endpoint=endpoint,
            error=str(exc),
        )
    finally:
        try:
            if ws is not None:
                ws.close()
        except Exception:
            pass


def _http_roundtrip(
    endpoint: str,
    payload: dict[str, Any],
    timeout_sec: int,
    logger: logging.Logger,
) -> OpenClawRoundtrip:
    logger.info("OpenClaw request send attempt | transport=http endpoint=%s", endpoint)
    try:
        resp = requests.post(endpoint, json=payload, timeout=timeout_sec)
        preview = _preview(resp.text)
        logger.info("OpenClaw HTTP response status=%s preview=%s", resp.status_code, preview)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError:
            return OpenClawRoundtrip(
                ok=True,
                transport="http",
                endpoint=endpoint,
                status_code=resp.status_code,
                response_text=resp.text.strip(),
                response_preview=preview,
            )

        content = _extract_content(data)
        if content is None:
            return OpenClawRoundtrip(
                ok=False,
                transport="http",
                endpoint=endpoint,
                status_code=resp.status_code,
                response_preview=preview,
                error="response JSON did not contain extractable content",
            )
        return OpenClawRoundtrip(
            ok=True,
            transport="http",
            endpoint=endpoint,
            status_code=resp.status_code,
            response_text=content.strip(),
            response_preview=_preview(content),
        )
    except requests.RequestException as exc:
        return OpenClawRoundtrip(
            ok=False,
            transport="http",
            endpoint=endpoint,
            error=str(exc),
        )


def send_policy_request(
    *,
    raw_url: str,
    model: str,
    system_prompt: str,
    user_message: str,
    timeout_sec: int,
    logger: logging.Logger,
    transport_mode: str = "auto",
) -> OpenClawRoundtrip:
    transport, endpoint = resolve_transport(raw_url, transport_mode=transport_mode)
    logger.info(
        "Resolved OpenClaw transport | requested_url=%s transport=%s endpoint=%s",
        raw_url,
        transport,
        endpoint,
    )
    payload = build_payload(model, system_prompt, user_message)
    if transport == "websocket":
        return _ws_roundtrip(endpoint, payload, timeout_sec, logger)
    return _http_roundtrip(endpoint, payload, timeout_sec, logger)


def healthcheck(
    *,
    raw_url: str,
    model: str,
    timeout_sec: int,
    logger: logging.Logger,
    transport_mode: str = "auto",
) -> dict[str, Any]:
    roundtrip = send_policy_request(
        raw_url=raw_url,
        model=model,
        system_prompt=(
            "Return exactly one JSON object with the required OpenClaw policy fields. "
            "Use no commands and set risk_level to low."
        ),
        user_message=json.dumps(
            {
                "request_type": "HEALTHCHECK",
                "instruction": "Return a minimal valid policy object with empty recommended_commands.",
            },
            ensure_ascii=False,
        ),
        timeout_sec=timeout_sec,
        logger=logger,
        transport_mode=transport_mode,
    )

    result = {
        "ok": False,
        "transport": roundtrip.transport,
        "endpoint": roundtrip.endpoint,
        "status_code": roundtrip.status_code,
        "response_preview": roundtrip.response_preview,
        "error": roundtrip.error,
    }
    if not roundtrip.ok or not roundtrip.response_text:
        return result

    try:
        data = json.loads(roundtrip.response_text)
    except json.JSONDecodeError as exc:
        result["error"] = f"roundtrip response is not valid JSON: {exc}"
        return result

    missing = sorted(REQUIRED_POLICY_KEYS - set(data.keys())) if isinstance(data, dict) else sorted(REQUIRED_POLICY_KEYS)
    if missing:
        result["error"] = f"roundtrip response missing required keys: {missing}"
        return result

    result["ok"] = True
    return result
