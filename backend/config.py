from __future__ import annotations

import os
import re
from dataclasses import dataclass, field


def _csv(value: str, fallback: list[str]) -> list[str]:
    if not value.strip():
        return fallback
    return [item.strip() for item in value.split(",") if item.strip()]


def _default_zone_device(zone_id: str) -> str | None:
    match = re.search(r"(\d+)$", zone_id)
    if not match:
        return None
    return f"esp32-{match.group(1)}"


def _default_zone_line(zone_id: str) -> str:
    return f"line_{zone_id}"


@dataclass(frozen=True)
class MqttConfig:
    host: str
    port: int
    username: str
    password: str
    client_id: str
    qos_default: int = 1


@dataclass(frozen=True)
class SqliteConfig:
    path: str


@dataclass(frozen=True)
class InfluxConfig:
    enabled: bool
    url: str
    org: str
    bucket: str
    token: str
    timeout_ms: int = 5000


@dataclass(frozen=True)
class LlamaConfig:
    api_url: str
    model: str
    timeout_sec: int
    api_key: str


@dataclass(frozen=True)
class OperatorUiConfig:
    enabled: bool
    host: str
    port: int


@dataclass(frozen=True)
class OpenClawMcpConfig:
    enabled: bool
    host: str
    port: int
    server_name: str
    server_version: str
    action_auth_token: str
    require_action_token: bool = True


@dataclass(frozen=True)
class IngestionPolicyConfig:
    allowed_clock_skew_sec: int = 30
    allowed_counter_reorder_window: int = 3
    stale_message_policy: str = "history_only"


@dataclass(frozen=True)
class ZoneSafetyConfig:
    zone_id: str
    device_id: str | None = None
    pump_id: str = "pump_main"
    line_id: str = ""
    mutually_exclusive_zones: list[str] = field(default_factory=list)
    shared_line_restricted: bool = False
    cooldown_sec: int = 300
    max_duration_per_run_sec: int = 30
    max_open_duration_sec: int = 30
    max_runs_per_hour: int = 6
    max_total_water_per_day_ml: int = 3000
    settle_delay_ms: int = 1500
    flow_confirm_timeout_ms: int = 5000
    min_flow_ml_per_min: float = 50.0
    blocked: bool = False
    maintenance_mode: bool = False


@dataclass(frozen=True)
class GlobalSafetyConfig:
    max_simultaneous_zones: int = 1
    max_active_lines: int = 1
    emergency_stop: bool = False
    master_pump_timeout_sec: int = 60
    pump_cooldown_sec: int = 0
    total_flow_limit_per_window_ml: int = 5000
    flow_window_sec: int = 3600
    leak_shutdown_enabled: bool = True
    command_ttl_sec: int = 20
    device_heartbeat_timeout_sec: int = 90
    broker_reconnect_lock_sec: int = 30
    anomaly_lookback_sec: int = 900
    min_pressure_kpa: float = 80.0
    max_pressure_kpa: float = 400.0
    tank_depletion_drop_threshold: float = 15.0
    stale_sensor_window_sec: int = 300
    noisy_sensor_delta_threshold: float = 35.0


@dataclass(frozen=True)
class BackendConfig:
    mqtt: MqttConfig
    sqlite: SqliteConfig
    influx: InfluxConfig
    llama: LlamaConfig
    operator_ui: OperatorUiConfig
    openclaw_mcp: OpenClawMcpConfig
    ingestion: IngestionPolicyConfig
    global_safety: GlobalSafetyConfig
    state_store_backend: str = "sqlite"
    telemetry_history_backend: str = "influx"
    zone_ids: list[str] = field(default_factory=lambda: ["tray_1", "tray_2", "tray_3", "tray_4"])

    def zone_configs(self) -> list[ZoneSafetyConfig]:
        zone_configs: list[ZoneSafetyConfig] = []
        for zone_id in self.zone_ids:
            env_zone = zone_id.upper()
            default_device_id = _default_zone_device(zone_id)
            default_line_id = _default_zone_line(zone_id)
            zone_configs.append(
                ZoneSafetyConfig(
                    zone_id=zone_id,
                    device_id=os.getenv(f"ZONE_{env_zone}_DEVICE_ID", default_device_id or "").strip() or default_device_id,
                    pump_id=os.getenv(f"ZONE_{env_zone}_PUMP_ID", "pump_main").strip() or "pump_main",
                    line_id=os.getenv(f"ZONE_{env_zone}_LINE_ID", default_line_id).strip() or default_line_id,
                    mutually_exclusive_zones=_csv(os.getenv(f"ZONE_{env_zone}_MUTUALLY_EXCLUSIVE_ZONES", ""), []),
                    shared_line_restricted=os.getenv(f"ZONE_{env_zone}_SHARED_LINE_RESTRICTED", "0").strip().lower() in {"1", "true", "yes", "on"},
                    cooldown_sec=int(os.getenv(f"ZONE_{env_zone}_COOLDOWN_SEC", "300")),
                    max_duration_per_run_sec=int(os.getenv(f"ZONE_{env_zone}_MAX_DURATION_SEC", "30")),
                    max_open_duration_sec=int(os.getenv(f"ZONE_{env_zone}_MAX_OPEN_DURATION_SEC", os.getenv(f"ZONE_{env_zone}_MAX_DURATION_SEC", "30"))),
                    max_runs_per_hour=int(os.getenv(f"ZONE_{env_zone}_MAX_RUNS_PER_HOUR", "6")),
                    max_total_water_per_day_ml=int(os.getenv(f"ZONE_{env_zone}_MAX_WATER_ML", "3000")),
                    settle_delay_ms=int(os.getenv(f"ZONE_{env_zone}_SETTLE_DELAY_MS", "1500")),
                    flow_confirm_timeout_ms=int(os.getenv(f"ZONE_{env_zone}_FLOW_CONFIRM_TIMEOUT_MS", "5000")),
                    min_flow_ml_per_min=float(os.getenv(f"ZONE_{env_zone}_MIN_FLOW_ML_PER_MIN", "50")),
                    blocked=os.getenv(f"ZONE_{env_zone}_BLOCKED", "0").strip().lower() in {"1", "true", "yes", "on"},
                    maintenance_mode=os.getenv(f"ZONE_{env_zone}_MAINTENANCE_MODE", "0").strip().lower() in {"1", "true", "yes", "on"},
                )
            )
        return zone_configs


def load_backend_config() -> BackendConfig:
    zone_ids = _csv(os.getenv("ZONE_IDS", ""), ["tray_1", "tray_2", "tray_3", "tray_4"])
    return BackendConfig(
        mqtt=MqttConfig(
            host=os.getenv("MQTT_HOST", "localhost").strip() or "localhost",
            port=int(os.getenv("MQTT_PORT", "1883")),
            username=os.getenv("MQTT_USERNAME", "").strip(),
            password=os.getenv("MQTT_PASSWORD", "").strip(),
            client_id=os.getenv("BACKEND_MQTT_CLIENT_ID", "greenhouse-backend").strip() or "greenhouse-backend",
        ),
        sqlite=SqliteConfig(
            path=os.getenv("SQLITE_PATH", "neuroagronom.db").strip() or "neuroagronom.db",
        ),
        influx=InfluxConfig(
            enabled=os.getenv("INFLUX_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            url=os.getenv("INFLUX_URL", "http://127.0.0.1:8086").strip(),
            org=os.getenv("INFLUX_ORG", "greenhouse").strip() or "greenhouse",
            bucket=os.getenv("INFLUX_BUCKET", "telemetry").strip() or "telemetry",
            token=os.getenv("INFLUX_TOKEN", "").strip(),
            timeout_ms=int(os.getenv("INFLUX_TIMEOUT_MS", "5000")),
        ),
        llama=LlamaConfig(
            api_url=os.getenv("LLAMA_API_URL", "http://127.0.0.1:11434/v1/chat/completions").strip(),
            model=os.getenv("LLAMA_MODEL", "llama3.1").strip() or "llama3.1",
            timeout_sec=int(os.getenv("LLAMA_TIMEOUT_SEC", os.getenv("LLM_TIMEOUT_SEC", "60"))),
            api_key=os.getenv("LLAMA_API_KEY", "").strip(),
        ),
        operator_ui=OperatorUiConfig(
            enabled=os.getenv("OPERATOR_UI_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            host=os.getenv("OPERATOR_UI_HOST", "127.0.0.1").strip() or "127.0.0.1",
            port=int(os.getenv("OPERATOR_UI_PORT", "8780")),
        ),
        openclaw_mcp=OpenClawMcpConfig(
            enabled=os.getenv("OPENCLAW_MCP_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            host=os.getenv("OPENCLAW_MCP_HOST", "127.0.0.1").strip() or "127.0.0.1",
            port=int(os.getenv("OPENCLAW_MCP_PORT", "8790")),
            server_name=os.getenv("OPENCLAW_MCP_SERVER_NAME", "greenhouse-backend-mcp").strip() or "greenhouse-backend-mcp",
            server_version=os.getenv("OPENCLAW_MCP_SERVER_VERSION", "1.0.0").strip() or "1.0.0",
            action_auth_token=os.getenv("OPENCLAW_MCP_ACTION_TOKEN", "").strip(),
            require_action_token=os.getenv("OPENCLAW_MCP_REQUIRE_ACTION_TOKEN", "1").strip().lower() in {"1", "true", "yes", "on"},
        ),
        ingestion=IngestionPolicyConfig(
            allowed_clock_skew_sec=int(os.getenv("INGESTION_ALLOWED_CLOCK_SKEW_SEC", "30")),
            allowed_counter_reorder_window=int(os.getenv("INGESTION_ALLOWED_COUNTER_REORDER_WINDOW", "3")),
            stale_message_policy=(os.getenv("INGESTION_STALE_MESSAGE_POLICY", "history_only").strip().lower() or "history_only"),
        ),
        global_safety=GlobalSafetyConfig(
            max_simultaneous_zones=int(os.getenv("GLOBAL_MAX_SIMULTANEOUS_ZONES", "1")),
            max_active_lines=int(os.getenv("GLOBAL_MAX_ACTIVE_LINES", os.getenv("GLOBAL_MAX_SIMULTANEOUS_ZONES", "1"))),
            emergency_stop=os.getenv("GLOBAL_EMERGENCY_STOP", "0").strip().lower() in {"1", "true", "yes", "on"},
            master_pump_timeout_sec=int(os.getenv("MASTER_PUMP_TIMEOUT_SEC", "60")),
            pump_cooldown_sec=int(os.getenv("PUMP_COOLDOWN_SEC", "0")),
            total_flow_limit_per_window_ml=int(os.getenv("TOTAL_FLOW_LIMIT_PER_WINDOW_ML", "5000")),
            flow_window_sec=int(os.getenv("FLOW_WINDOW_SEC", "3600")),
            leak_shutdown_enabled=os.getenv("LEAK_SHUTDOWN_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            command_ttl_sec=int(os.getenv("COMMAND_TTL_SEC", "20")),
            device_heartbeat_timeout_sec=int(os.getenv("DEVICE_HEARTBEAT_TIMEOUT_SEC", "90")),
            broker_reconnect_lock_sec=int(os.getenv("BROKER_RECONNECT_LOCK_SEC", "30")),
            anomaly_lookback_sec=int(os.getenv("ANOMALY_LOOKBACK_SEC", "900")),
            min_pressure_kpa=float(os.getenv("MIN_PRESSURE_KPA", "80")),
            max_pressure_kpa=float(os.getenv("MAX_PRESSURE_KPA", "400")),
            tank_depletion_drop_threshold=float(os.getenv("TANK_DEPLETION_DROP_THRESHOLD", "15")),
            stale_sensor_window_sec=int(os.getenv("STALE_SENSOR_WINDOW_SEC", "300")),
            noisy_sensor_delta_threshold=float(os.getenv("NOISY_SENSOR_DELTA_THRESHOLD", "35")),
        ),
        state_store_backend=os.getenv("STATE_STORE_BACKEND", "sqlite").strip().lower() or "sqlite",
        telemetry_history_backend=os.getenv("TELEMETRY_HISTORY_BACKEND", "influx").strip().lower() or "influx",
        zone_ids=zone_ids,
    )
