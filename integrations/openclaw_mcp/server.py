from __future__ import annotations

import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from integrations.openclaw_mcp.tools import McpAuthError, OpenClawMcpAdapter


log = logging.getLogger("integrations.openclaw_mcp.server")


class OpenClawMcpServer:
    def __init__(self, host: str, port: int, adapter: OpenClawMcpAdapter, *, server_name: str, server_version: str, lazy_bind: bool = False) -> None:
        self._host = host
        self._port = port
        self._adapter = adapter
        self._server_name = server_name
        self._server_version = server_version
        self._server: ThreadingHTTPServer | None = None if lazy_bind else ThreadingHTTPServer((host, port), self._build_handler())
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}/mcp"

    def start(self) -> None:
        if self._thread is not None:
            return
        if self._server is None:
            self._server = ThreadingHTTPServer((self._host, self._port), self._build_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, name="openclaw-mcp", daemon=True)
        self._thread.start()
        log.info("openclaw mcp listening url=%s", self.url)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def handle_rpc(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = request.get("id")
        method = str(request.get("method") or "")
        params = dict(request.get("params") or {})
        try:
            if method == "initialize":
                return self._result(
                    request_id,
                    {
                        "protocolVersion": "2025-03-26",
                        "serverInfo": {"name": self._server_name, "version": self._server_version},
                        "capabilities": {"tools": {"listChanged": False}},
                    },
                )
            if method == "ping":
                return self._result(request_id, {"ok": True})
            if method == "tools/list":
                return self._result(request_id, {"tools": self._adapter.tool_definitions()})
            if method == "tools/call":
                tool_name = str(params.get("name") or "")
                arguments = dict(params.get("arguments") or {})
                result = self._adapter.call_tool(tool_name, arguments)
                return self._result(request_id, {"content": [{"type": "json", "json": result}]})
            return self._error(request_id, -32601, f"Method not found: {method}")
        except McpAuthError as exc:
            return self._error(request_id, -32001, str(exc))
        except KeyError as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:
            log.exception("openclaw mcp request failed method=%s", method)
            return self._error(request_id, -32000, f"server_error:{type(exc).__name__}:{exc}")

    def _build_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/mcp":
                    self.send_error(HTTPStatus.NOT_FOUND, "not_found")
                    return
                size = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(size) if size > 0 else b"{}"
                try:
                    request = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json(server._error(None, -32700, "parse_error"), HTTPStatus.BAD_REQUEST)
                    return
                if not isinstance(request, dict):
                    self._send_json(server._error(None, -32600, "invalid_request"), HTTPStatus.BAD_REQUEST)
                    return
                response = server.handle_rpc(request)
                self._send_json(response, HTTPStatus.OK)

            def do_GET(self) -> None:
                if self.path == "/mcp":
                    self._send_json(
                        {
                            "server": server._server_name,
                            "version": server._server_version,
                            "endpoint": server.url,
                            "methods": ["initialize", "ping", "tools/list", "tools/call"],
                        },
                        HTTPStatus.OK,
                    )
                    return
                self.send_error(HTTPStatus.NOT_FOUND, "not_found")

            def _send_json(self, payload: dict[str, Any], status: int) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args) -> None:
                return

        return Handler

    @staticmethod
    def _result(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
