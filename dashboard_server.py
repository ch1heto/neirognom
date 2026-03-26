from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

import database as db

load_dotenv()


HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("DASHBOARD_PORT", "8765"))
ROOT = Path(__file__).resolve().parent
INDEX_PATH = ROOT / "dashboard.html"


def _json_response(handler: SimpleHTTPRequestHandler, payload: dict, status: int = HTTPStatus.OK) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _load_status_payload() -> dict:
    latest = db.get_latest_control_event("bridge_status", source="smart_bridge")
    base = {
        "updated_at": None,
        "status_source": None,
        "profile": None,
        "dry_run": None,
        "readiness": {},
        "telemetry_state": {},
        "telemetry": {},
        "deviations": [],
        "risks": [],
    }
    if not latest:
        return base

    context = {}
    raw_context = latest.get("context_json") or ""
    if raw_context:
        try:
            context = json.loads(raw_context)
        except json.JSONDecodeError:
            context = {}

    result = dict(base)
    result.update(context if isinstance(context, dict) else {})
    result["updated_at"] = latest.get("ts")
    result["status_source"] = latest.get("status")
    return result


def _load_events_payload(limit: int) -> dict:
    rows = db.get_recent_control_events(limit=max(1, min(limit, 250)))
    events = []
    for row in rows:
        item = dict(row)
        if item.get("event_type") == "bridge_status":
            continue
        raw_context = item.get("context_json") or ""
        if raw_context:
            try:
                item["context"] = json.loads(raw_context)
            except json.JSONDecodeError:
                item["context"] = None
        else:
            item["context"] = None
        events.append(item)
    return {"events": events}


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            _json_response(self, _load_status_payload())
            return
        if parsed.path == "/api/events":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["80"])[0])
            _json_response(self, _load_events_payload(limit))
            return
        if parsed.path == "/":
            self.path = str(INDEX_PATH.name)
        return super().do_GET()

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, format: str, *args) -> None:
        return


def main() -> int:
    db.init_db()
    os.chdir(ROOT)
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"[DASHBOARD] http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
