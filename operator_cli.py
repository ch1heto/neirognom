from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

import paho.mqtt.publish as mqtt_publish

import database as db


DEFAULT_HOST = os.getenv("MQTT_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("MQTT_PORT", "1883"))
DEFAULT_TRAY = os.getenv("TRAY_ID", "tray01")


def _fmt_ts(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _print_status() -> int:
    state = db.get_current_escalation_state()
    print("Current escalation state")
    for metric in ("ph", "ec"):
        item = state.get(metric, {"active": False, "state": "unknown", "since": None, "reason": ""})
        line = f"- {metric}: {item['state']}"
        if item.get("active"):
            line += f" since={_fmt_ts(item.get('since'))}"
            if item.get("reason"):
                line += f" reason={item['reason']}"
        elif item.get("last_reset_ts"):
            line += f" last_reset={_fmt_ts(item.get('last_reset_ts'))}"
        print(line)
    return 0


def _print_events(limit: int) -> int:
    rows = db.get_recent_control_events(limit=limit)
    print(f"Recent control events (limit={limit})")
    if not rows:
        print("- none")
        return 0

    for row in rows:
        metric = row.get("metric") or "-"
        status = row.get("status") or "-"
        details = (row.get("details") or "").replace("\r", " ").replace("\n", " ")
        print(
            f"- {_fmt_ts(row.get('ts'))} | {row.get('event_type')} | "
            f"source={row.get('source')} | metric={metric} | status={status} | {details}"
        )
    return 0


def _publish_reset(metric: str, operator: str, host: str, port: int, tray: str) -> int:
    topic = f"farm/{tray}/operator/reset"
    payload = json.dumps({"metric": metric, "operator": operator}, ensure_ascii=False)
    mqtt_publish.single(topic, payload=payload, hostname=host, port=port)
    print(f"Reset request published to {topic}: {payload}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operator inspection/reset helper for the hybrid control bridge")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show current escalation state derived from control events")

    events = sub.add_parser("events", help="Show recent control_events rows")
    events.add_argument("--limit", type=int, default=20, help="Number of recent events to show")

    reset = sub.add_parser("reset", help="Publish an operator reset request over MQTT")
    reset.add_argument("metric", choices=["ph", "ec", "all"], help="Metric to reset")
    reset.add_argument("--operator", default="operator_cli", help="Operator name to include in the reset event")
    reset.add_argument("--host", default=DEFAULT_HOST, help="MQTT host")
    reset.add_argument("--port", type=int, default=DEFAULT_PORT, help="MQTT port")
    reset.add_argument("--tray", default=DEFAULT_TRAY, help="Tray id used in the reset topic")

    return parser


def main() -> int:
    db.init_db()
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "status":
        return _print_status()
    if args.command == "events":
        return _print_events(args.limit)
    if args.command == "reset":
        return _publish_reset(args.metric, args.operator, args.host, args.port, args.tray)
    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
