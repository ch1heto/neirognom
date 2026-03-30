from __future__ import annotations

import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from backend.operator.service import OperatorControlService


log = logging.getLogger("backend.operator.web")
ROOT = Path(__file__).resolve().parents[2]
UI_PATH = ROOT / "operator_ui.html"


class OperatorUiServer:
    def __init__(self, host: str, port: int, service: OperatorControlService) -> None:
        self._host = host
        self._port = port
        self._service = service
        self._server = ThreadingHTTPServer((host, port), self._build_handler())
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._server.serve_forever, name="operator-ui", daemon=True)
        self._thread.start()
        log.info("operator ui listening url=%s", self.url)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _build_handler(self):
        service = self._service

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                try:
                    if parsed.path in {"/", "/operator"}:
                        self._send_file(UI_PATH)
                        return
                    if parsed.path == "/api/operator/overview":
                        self._send_json(service.overview())
                        return
                    if parsed.path == "/api/operator/devices-zones":
                        self._send_json(service.list_devices_zones())
                        return
                    if parsed.path == "/api/operator/state":
                        self._send_json(service.get_control_safety_state())
                        return
                    if parsed.path == "/api/operator/system-mode":
                        self._send_json(service.get_system_mode_state())
                        return
                    if parsed.path == "/api/operator/event-log":
                        params = parse_qs(parsed.query)
                        limit = self._bounded_int(params.get("limit", ["200"])[0], default=200, minimum=1, maximum=200)
                        self._send_json(service.get_event_log(limit=limit))
                        return
                    if parsed.path == "/api/operator/commands":
                        params = parse_qs(parsed.query)
                        limit = self._bounded_int(params.get("limit", ["50"])[0], default=50, minimum=1, maximum=200)
                        self._send_json(service.command_history(limit=limit))
                        return
                    self.send_error(HTTPStatus.NOT_FOUND, "not_found")
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                except Exception:
                    log.exception("operator GET failed path=%s", parsed.path)
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal_error")

            def do_POST(self) -> None:
                parsed = urlparse(self.path)
                try:
                    body = self._read_json()
                    if parsed.path in {"/api/operator/command", "/api/operator/manual-command"}:
                        self._send_json(service.submit_manual_command(body))
                        return
                    if parsed.path == "/api/operator/system-mode":
                        self._send_json(service.set_system_mode(body))
                        return
                    if parsed.path == "/api/operator/emergency-stop":
                        self._send_json(service.emergency_stop(body))
                        return
                    self.send_error(HTTPStatus.NOT_FOUND, "not_found")
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                except Exception:
                    log.exception("operator POST failed path=%s", parsed.path)
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal_error")

            def _read_json(self) -> dict:
                size = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(size) if size > 0 else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise ValueError("invalid_json") from exc
                if not isinstance(payload, dict):
                    raise ValueError("payload_must_be_object")
                return payload

            @staticmethod
            def _bounded_int(value: str, *, default: int, minimum: int, maximum: int) -> int:
                try:
                    parsed = int(value)
                except (TypeError, ValueError):
                    return default
                return max(minimum, min(parsed, maximum))

            def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _send_file(self, path: Path) -> None:
                body = path.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args) -> None:
                return

        return Handler
