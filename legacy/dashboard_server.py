"""Deprecated legacy dashboard for historical bridge/OpenClaw diagnostics only.

It is not the operational operator UI and must not be treated as the active
control surface for the backend-centric runtime.
"""

from __future__ import annotations

import io
import json
import logging
import os
import zipfile
import warnings
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from legacy import database as db
from legacy.openclaw_client import collect_diagnostics, load_runtime_config

load_dotenv()


HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("DASHBOARD_PORT", "8765"))
LEGACY_ROOT = Path(__file__).resolve().parent
ROOT = LEGACY_ROOT.parent
INDEX_PATH = LEGACY_ROOT / "dashboard.html"
OPENCLAW_CONFIG = load_runtime_config()


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


def _db_rows(query: str, params: tuple = ()) -> list[dict]:
    conn = db.get_conn()
    try:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _text_bytes(text: str) -> bytes:
    return text.encode("utf-8", errors="replace")


def _relative_name(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.name


def _recent_bridge_statuses(limit: int = 20) -> list[dict]:
    rows = _db_rows(
        "SELECT ts,event_type,source,metric,status,details,context_json"
        " FROM control_events WHERE event_type='bridge_status' ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    result = []
    for row in rows:
        item = dict(row)
        raw_context = item.get("context_json") or ""
        if raw_context:
            try:
                item["context"] = json.loads(raw_context)
            except json.JSONDecodeError:
                item["context"] = None
        else:
            item["context"] = None
        result.append(item)
    return result


def _openclaw_diagnostics() -> dict:
    return collect_diagnostics(
        timeout_sec=8,
        logger=logging.getLogger("dashboard_openclaw"),
        raw_url=OPENCLAW_CONFIG.raw_url,
        model=OPENCLAW_CONFIG.model,
        transport_mode=OPENCLAW_CONFIG.requested_transport,
        ai_backend=OPENCLAW_CONFIG.ai_backend,
    )


def _build_diagnostic_bundle() -> tuple[bytes, str]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_name = f"cityfarm_diagnostic_bundle_{ts}.zip"
    status_payload = _load_status_payload()
    events_payload = _load_events_payload(120)
    alerts = db.get_recent_alerts(hours=24)
    escalation_state = db.get_current_escalation_state()
    actuator_states = db.get_actuator_states()
    bridge_statuses = _recent_bridge_statuses(20)
    llm_requests = _db_rows(
        "SELECT id,ts,request_type,crop,stage,growth_day,prompt_len,response_raw,analysis,status"
        " FROM llm_requests ORDER BY id DESC LIMIT 20"
    )
    commands = _db_rows(
        "SELECT ts,llm_req_id,command_id,actuator,action,duration_sec,reason,mqtt_topic,exec_status,ack_state"
        " FROM commands ORDER BY id DESC LIMIT 50"
    )
    openclaw_events = _db_rows(
        "SELECT ts,event_type,source,metric,status,details,context_json"
        " FROM control_events WHERE event_type IN ('llm_context','llm_result','policy_result')"
        " ORDER BY id DESC LIMIT 30"
    )
    command_ack_events = _db_rows(
        "SELECT ts,event_type,source,metric,status,details,context_json"
        " FROM control_events WHERE event_type IN ('command_dispatch','command_execution','command_ack','command_batch','command_accepted','command_rejected','fallback','effect_check','escalation','operator_reset')"
        " ORDER BY id DESC LIMIT 80"
    )

    buffer = io.BytesIO()
    included: list[str] = []
    missing: list[str] = []

    config_files = [
        ROOT / ".env",
        ROOT / ".env.example",
        ROOT / "runtime_profiles.json",
        ROOT / "openclaw_policy_schema.json",
        ROOT / "system_prompt_core.txt",
        ROOT / "system_prompt_schema.txt",
        ROOT / "knowledge_base" / "alerts" / "control_safety.json",
        ROOT / "knowledge_base" / "alerts" / "recovery_protocols.json",
        ROOT / "knowledge_base" / "alerts" / "critical_thresholds.json",
    ]
    log_files = [
        ROOT / "audit.log",
        ROOT / "bridge_dryrun_test.out.log",
        ROOT / "bridge_dryrun_test.err.log",
        ROOT / "sim_fault_test.out.log",
        ROOT / "sim_fault_test.err.log",
    ]

    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in log_files:
            if path.exists():
                bundle.write(path, arcname=f"logs/{path.name}")
                included.append(f"logs/{path.name}")
            else:
                missing.append(f"logs/{path.name}")

        for path in config_files:
            if path.exists():
                relative_name = _relative_name(path)
                bundle.write(path, arcname=f"config/{relative_name}")
                included.append(f"config/{relative_name}")
            else:
                relative_name = _relative_name(path)
                missing.append(f"config/{relative_name}")

        bundle.writestr("telemetry/current_status.json", _json_bytes(status_payload))
        included.append("telemetry/current_status.json")
        bundle.writestr("telemetry/recent_bridge_statuses.json", _json_bytes(bridge_statuses))
        included.append("telemetry/recent_bridge_statuses.json")
        bundle.writestr(
            "telemetry/health_readiness_summary.json",
            _json_bytes(
                {
                    "updated_at": status_payload.get("updated_at"),
                    "profile": status_payload.get("profile"),
                    "dry_run": status_payload.get("dry_run"),
                    "readiness": status_payload.get("readiness", {}),
                    "telemetry_state": status_payload.get("telemetry_state", {}),
                    "deviations": status_payload.get("deviations", []),
                    "risks": status_payload.get("risks", []),
                }
            ),
        )
        included.append("telemetry/health_readiness_summary.json")

        bundle.writestr("runtime/recent_control_events.json", _json_bytes(events_payload))
        included.append("runtime/recent_control_events.json")
        bundle.writestr("runtime/recent_commands.json", _json_bytes(commands))
        included.append("runtime/recent_commands.json")
        bundle.writestr("runtime/actuator_states.json", _json_bytes(actuator_states))
        included.append("runtime/actuator_states.json")
        bundle.writestr("runtime/escalation_state.json", _json_bytes(escalation_state))
        included.append("runtime/escalation_state.json")
        bundle.writestr("runtime/recent_alerts.json", _json_bytes(alerts))
        included.append("runtime/recent_alerts.json")
        bundle.writestr(
            "runtime/active_alerts_and_safety_blocks.json",
            _json_bytes(
                {
                    "alerts_last_24h": alerts,
                    "current_risks": status_payload.get("risks", []),
                    "current_deviations": status_payload.get("deviations", []),
                    "telemetry_state": status_payload.get("telemetry_state", {}),
                }
            ),
        )
        included.append("runtime/active_alerts_and_safety_blocks.json")

        bundle.writestr("openclaw/connection_diagnostics.json", _json_bytes(_openclaw_diagnostics()))
        included.append("openclaw/connection_diagnostics.json")
        bundle.writestr("openclaw/recent_llm_requests.json", _json_bytes(llm_requests))
        included.append("openclaw/recent_llm_requests.json")
        bundle.writestr("openclaw/recent_policy_events.json", _json_bytes(openclaw_events))
        included.append("openclaw/recent_policy_events.json")

        bundle.writestr(
            "runtime/bundle_manifest.json",
            _json_bytes(
                {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "archive_name": archive_name,
                    "included_files": included,
                    "missing_files": missing,
                }
            ),
        )
        included.append("runtime/bundle_manifest.json")
        bundle.writestr(
            "runtime/recent_command_ack_and_audit.json",
            _json_bytes(command_ack_events),
        )
        included.append("runtime/recent_command_ack_and_audit.json")
        bundle.writestr(
            "runtime/health_readiness_summary.txt",
            _text_bytes(
                "\n".join(
                    [
                        f"profile={status_payload.get('profile')}",
                        f"dry_run={status_payload.get('dry_run')}",
                        f"updated_at={status_payload.get('updated_at')}",
                        f"readiness={json.dumps(status_payload.get('readiness', {}), ensure_ascii=False)}",
                        f"risks={json.dumps(status_payload.get('risks', []), ensure_ascii=False)}",
                        f"deviations={json.dumps(status_payload.get('deviations', []), ensure_ascii=False)}",
                    ]
                )
            ),
        )
        included.append("runtime/health_readiness_summary.txt")

    return buffer.getvalue(), archive_name


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
        if parsed.path == "/api/openclaw/diagnostics":
            _json_response(self, _openclaw_diagnostics())
            return
        if parsed.path == "/download/diagnostic-bundle":
            body, archive_name = _build_diagnostic_bundle()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{archive_name}"')
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
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
    warnings.warn(
        "dashboard_server.py is deprecated. Use backend_server.py and operator_ui.html for the active backend-centric surfaces.",
        DeprecationWarning,
        stacklevel=2,
    )
    db.init_db()
    os.chdir(LEGACY_ROOT)
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
