from __future__ import annotations

import json
import logging
import os
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

import requests


DEFAULT_OPENCLAW_URL = "http://127.0.0.1:18789/v1/chat/completions"
DEFAULT_OPENCLAW_MODEL = "llama3.1"
DEFAULT_AI_BACKEND = "openclaw"
VALID_TRANSPORT_MODES = {"", "auto", "http", "ws", "websocket"}
WEBSOCKET_PACKAGE_NAME = "websocket-client"
WEBSOCKET_UNSUPPORTED_ERROR = (
    "raw websocket transport is not implemented for this project's OpenAI-compatible OpenClaw client; "
    "use an http(s) OpenAI-compatible chat endpoint such as /v1/chat/completions, "
    "or add a dedicated gateway websocket protocol client with the required handshake/path/subprotocol/auth"
)

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


@dataclass(frozen=True)
class OpenClawRuntimeConfig:
    ai_backend: str
    requested_transport: str
    effective_transport: str
    raw_url: str
    effective_url: str
    model: str
    auth_token: str
    transport_source: str
    config_error: str = ""


@dataclass
class OpenClawRoundtrip:
    ok: bool
    transport: str
    endpoint: str
    response_text: Optional[str] = None
    response_preview: str = ""
    status_code: Optional[int] = None
    error: str = ""
    exception_type: str = ""


def websocket_dependency_status() -> dict[str, Any]:
    return {
        "package": WEBSOCKET_PACKAGE_NAME,
        "required": False,
        "installed": None,
        "import_error": "",
    }


def _normalize_transport_mode(transport_mode: str | None) -> str:
    mode = (transport_mode or "auto").strip().lower()
    if mode not in VALID_TRANSPORT_MODES:
        return "__invalid__"
    if mode in {"", "auto"}:
        return "auto"
    if mode in {"ws", "websocket"}:
        return "websocket"
    return "http"


def _normalize_url(raw_url: str | None) -> str:
    url = (raw_url or DEFAULT_OPENCLAW_URL).strip()
    if not url:
        return DEFAULT_OPENCLAW_URL
    parsed = urlparse(url)
    if (parsed.path or "/") == "/":
        return url.rstrip("/")
    return url


def load_runtime_config(env: Mapping[str, str] | None = None) -> OpenClawRuntimeConfig:
    source = env if env is not None else os.environ
    ai_backend = (source.get("AI_BACKEND", DEFAULT_AI_BACKEND) or DEFAULT_AI_BACKEND).strip().lower() or DEFAULT_AI_BACKEND
    raw_url = _normalize_url(source.get("OPENCLAW_URL", DEFAULT_OPENCLAW_URL))
    model = (source.get("OPENCLAW_MODEL", DEFAULT_OPENCLAW_MODEL) or DEFAULT_OPENCLAW_MODEL).strip() or DEFAULT_OPENCLAW_MODEL
    auth_token = (source.get("OPENCLAW_AUTH_TOKEN") or source.get("OPENCLAW_GATEWAY_TOKEN") or "").strip()
    requested_transport = _normalize_transport_mode(source.get("OPENCLAW_TRANSPORT", "auto"))
    parsed = urlparse(raw_url)
    scheme = (parsed.scheme or "").lower()

    effective_transport = "unknown"
    transport_source = "unresolved"
    config_error = ""

    if requested_transport == "__invalid__":
        config_error = (
            "OPENCLAW_TRANSPORT must be one of: auto, http, websocket"
        )
    elif requested_transport == "websocket":
        effective_transport = "websocket"
        transport_source = "OPENCLAW_TRANSPORT"
        if scheme not in {"ws", "wss"}:
            config_error = "OPENCLAW_TRANSPORT=websocket requires OPENCLAW_URL to use ws:// or wss://"
        else:
            config_error = WEBSOCKET_UNSUPPORTED_ERROR
    elif requested_transport == "http":
        effective_transport = "http"
        transport_source = "OPENCLAW_TRANSPORT"
        if scheme not in {"http", "https"}:
            config_error = (
                "OPENCLAW_TRANSPORT=http requires OPENCLAW_URL to use http:// or https://"
            )
    else:
        transport_source = "OPENCLAW_URL scheme"
        if scheme in {"ws", "wss"}:
            effective_transport = "websocket"
            config_error = WEBSOCKET_UNSUPPORTED_ERROR
        elif scheme in {"http", "https"}:
            effective_transport = "http"
        else:
            config_error = (
                "OPENCLAW_URL must start with ws://, wss://, http://, or https:// when OPENCLAW_TRANSPORT=auto"
            )

    return OpenClawRuntimeConfig(
        ai_backend=ai_backend,
        requested_transport=requested_transport,
        effective_transport=effective_transport,
        raw_url=raw_url,
        effective_url=raw_url,
        model=model,
        auth_token=auth_token,
        transport_source=transport_source,
        config_error=config_error,
    )


def resolve_transport(raw_url: str, transport_mode: str = "auto") -> tuple[str, str]:
    config = load_runtime_config(
        {
            "OPENCLAW_URL": raw_url,
            "OPENCLAW_TRANSPORT": transport_mode,
        }
    )
    return config.effective_transport, config.effective_url


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


def _configuration_failure(config: OpenClawRuntimeConfig) -> OpenClawRoundtrip:
    return OpenClawRoundtrip(
        ok=False,
        transport=config.effective_transport,
        endpoint=config.effective_url,
        error=config.config_error,
        exception_type="ConfigurationError",
    )


def _ws_roundtrip(
    endpoint: str,
    payload: dict[str, Any],
    timeout_sec: int,
    logger: logging.Logger,
) -> OpenClawRoundtrip:
    return OpenClawRoundtrip(
        ok=False,
        transport="websocket",
        endpoint=endpoint,
        error=WEBSOCKET_UNSUPPORTED_ERROR,
        exception_type="UnsupportedTransportError",
    )


def _http_roundtrip(
    endpoint: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_sec: int,
    logger: logging.Logger,
) -> OpenClawRoundtrip:
    logger.info("OpenClaw request send attempt | transport=http endpoint=%s", endpoint)
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=timeout_sec)
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
                exception_type="ResponseFormatError",
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
            exception_type=type(exc).__name__,
        )


class OpenClawProvider:
    def __init__(self, config: OpenClawRuntimeConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    @classmethod
    def from_env(
        cls,
        logger: logging.Logger,
        env: Mapping[str, str] | None = None,
    ) -> "OpenClawProvider":
        return cls(load_runtime_config(env), logger)

    def log_startup(self) -> None:
        dependency = websocket_dependency_status()
        dependency_label = "n/a"
        if dependency.get("required"):
            dependency_label = "yes" if dependency["installed"] else "no"
        self.logger.info(
            "OpenClaw provider selected | backend=%s requested_transport=%s effective_transport=%s endpoint=%s source=%s model=%s auth_configured=%s websocket_client_installed=%s",
            self.config.ai_backend,
            self.config.requested_transport,
            self.config.effective_transport,
            self.config.effective_url,
            self.config.transport_source,
            self.config.model,
            "yes" if self.config.auth_token else "no",
            dependency_label,
        )
        if self.config.config_error:
            self.logger.error(
                "OpenClaw configuration error | transport=%s endpoint=%s error=%s",
                self.config.requested_transport,
                self.config.raw_url,
                self.config.config_error,
            )

    def request(
        self,
        *,
        system_prompt: str,
        user_message: str,
        timeout_sec: int,
        model: str | None = None,
    ) -> OpenClawRoundtrip:
        if self.config.config_error:
            roundtrip = _configuration_failure(self.config)
        else:
            payload = build_payload(model or self.config.model, system_prompt, user_message)
            headers = {"Content-Type": "application/json"}
            if self.config.auth_token:
                headers["Authorization"] = f"Bearer {self.config.auth_token}"
            if self.config.effective_transport == "websocket":
                roundtrip = _ws_roundtrip(self.config.effective_url, payload, timeout_sec, self.logger)
            else:
                roundtrip = _http_roundtrip(self.config.effective_url, payload, headers, timeout_sec, self.logger)

        if not roundtrip.ok:
            self.logger.error(
                "OpenClaw roundtrip failed | backend=%s transport=%s endpoint=%s exception_type=%s error=%s",
                self.config.ai_backend,
                roundtrip.transport,
                roundtrip.endpoint,
                roundtrip.exception_type or "-",
                roundtrip.error or "-",
            )
        return roundtrip


def send_policy_request(
    *,
    raw_url: str,
    model: str,
    system_prompt: str,
    user_message: str,
    timeout_sec: int,
    logger: logging.Logger,
    transport_mode: str = "auto",
    ai_backend: str = DEFAULT_AI_BACKEND,
) -> OpenClawRoundtrip:
    provider = OpenClawProvider(
        load_runtime_config(
            {
                "AI_BACKEND": ai_backend,
                "OPENCLAW_URL": raw_url,
                "OPENCLAW_TRANSPORT": transport_mode,
                "OPENCLAW_MODEL": model,
            }
        ),
        logger,
    )
    return provider.request(
        system_prompt=system_prompt,
        user_message=user_message,
        timeout_sec=timeout_sec,
        model=model,
    )


def healthcheck(
    *,
    raw_url: str | None = None,
    model: str | None = None,
    timeout_sec: int,
    logger: logging.Logger,
    transport_mode: str = "auto",
    ai_backend: str = DEFAULT_AI_BACKEND,
    provider: OpenClawProvider | None = None,
) -> dict[str, Any]:
    active_provider = provider or OpenClawProvider(
        load_runtime_config(
            {
                "AI_BACKEND": ai_backend,
                "OPENCLAW_URL": raw_url or DEFAULT_OPENCLAW_URL,
                "OPENCLAW_TRANSPORT": transport_mode,
                "OPENCLAW_MODEL": model or DEFAULT_OPENCLAW_MODEL,
            }
        ),
        logger,
    )

    roundtrip = active_provider.request(
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
        model=model or active_provider.config.model,
    )

    dependency = websocket_dependency_status()
    result = {
        "ok": False,
        "effective_backend": active_provider.config.ai_backend,
        "transport": roundtrip.transport,
        "effective_transport": active_provider.config.effective_transport,
        "requested_transport": active_provider.config.requested_transport,
        "transport_source": active_provider.config.transport_source,
        "endpoint": roundtrip.endpoint,
        "effective_url": active_provider.config.effective_url,
        "status_code": roundtrip.status_code,
        "response_preview": roundtrip.response_preview,
        "error": roundtrip.error,
        "last_exception_text": roundtrip.error,
        "last_exception_type": roundtrip.exception_type,
        "config_error": active_provider.config.config_error,
        "websocket_client_installed": dependency["installed"],
        "websocket_client_import_error": dependency["import_error"],
    }
    if not roundtrip.ok or not roundtrip.response_text:
        if not result["error"]:
            result["error"] = "roundtrip completed without an extractable response body"
            result["last_exception_text"] = result["error"]
            result["last_exception_type"] = "EmptyResponseError"
        return result

    try:
        data = json.loads(roundtrip.response_text)
    except json.JSONDecodeError as exc:
        result["error"] = f"roundtrip response is not valid JSON: {exc}"
        result["last_exception_text"] = result["error"]
        result["last_exception_type"] = "JSONDecodeError"
        return result

    missing = sorted(REQUIRED_POLICY_KEYS - set(data.keys())) if isinstance(data, dict) else sorted(REQUIRED_POLICY_KEYS)
    if missing:
        result["error"] = f"roundtrip response missing required keys: {missing}"
        result["last_exception_text"] = result["error"]
        result["last_exception_type"] = "SchemaValidationError"
        return result

    result["ok"] = True
    result["error"] = ""
    result["last_exception_text"] = ""
    result["last_exception_type"] = ""
    return result


def collect_diagnostics(
    *,
    timeout_sec: int,
    logger: logging.Logger,
    provider: OpenClawProvider | None = None,
    raw_url: str | None = None,
    model: str | None = None,
    transport_mode: str = "auto",
    ai_backend: str = DEFAULT_AI_BACKEND,
) -> dict[str, Any]:
    active_provider = provider or OpenClawProvider(
        load_runtime_config(
            {
                "AI_BACKEND": ai_backend,
                "OPENCLAW_URL": raw_url or DEFAULT_OPENCLAW_URL,
                "OPENCLAW_TRANSPORT": transport_mode,
                "OPENCLAW_MODEL": model or DEFAULT_OPENCLAW_MODEL,
            }
        ),
        logger,
    )
    config = active_provider.config
    dependency = websocket_dependency_status()
    parsed = urlparse(config.effective_url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme in {"https", "wss"} else 80)
    result = {
        "ai_backend": config.ai_backend,
        "effective_backend": config.ai_backend,
        "requested_transport": config.requested_transport,
        "effective_transport": config.effective_transport,
        "effective_url": config.effective_url,
        "resolved_openclaw_url": config.raw_url,
        "resolved_transport": config.effective_transport,
        "resolved_endpoint": config.effective_url,
        "transport_source": config.transport_source,
        "model": config.model,
        "auth_configured": bool(config.auth_token),
        "scheme": parsed.scheme or "",
        "host": host,
        "port": port,
        "path": parsed.path or "/",
        "config_error": config.config_error,
        "dependency_status": {
            "websocket_client_installed": dependency["installed"],
            "websocket_client_import_error": dependency["import_error"],
        },
        "websocket_client_installed": dependency["installed"],
        "tcp_connect_ok": False,
        "tcp_error": "",
        "roundtrip_ok": False,
        "roundtrip_error": "",
        "roundtrip_response_preview": "",
        "roundtrip_status_code": None,
        "last_exception_text": "",
        "last_exception_type": "",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    if not host:
        result["tcp_error"] = "missing host in effective OpenClaw URL"
    else:
        try:
            with socket.create_connection((host, port), timeout=1.5):
                result["tcp_connect_ok"] = True
        except OSError as exc:
            result["tcp_error"] = str(exc)

    health = healthcheck(
        timeout_sec=timeout_sec,
        logger=logger,
        provider=active_provider,
    )
    result["roundtrip_ok"] = bool(health.get("ok"))
    result["roundtrip_error"] = str(health.get("error") or "")
    result["roundtrip_response_preview"] = str(health.get("response_preview") or "")
    result["roundtrip_status_code"] = health.get("status_code")
    result["last_exception_text"] = str(health.get("last_exception_text") or "")
    result["last_exception_type"] = str(health.get("last_exception_type") or "")
    if not result["config_error"]:
        result["config_error"] = str(health.get("config_error") or "")
    return result
