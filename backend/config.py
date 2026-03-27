from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv(value: str, fallback: list[str]) -> list[str]:
    if not value.strip():
        return fallback
    return [item.strip() for item in value.split(",") if item.strip()]


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
class OpenClawOperatorConfig:
    enabled: bool
    base_url: str
    api_key: str


@dataclass(frozen=True)
class ZoneSafetyConfig:
    zone_id: str
    cooldown_sec: int = 300
    max_duration_per_run_sec: int = 30
    max_runs_per_hour: int = 6
    max_total_water_per_day_ml: int = 3000
    settle_delay_ms: int = 1500
    flow_confirm_timeout_ms: int = 5000
    min_flow_ml_per_min: float = 50.0
    blocked: bool = False


@dataclass(frozen=True)
class GlobalSafetyConfig:
    max_simultaneous_zones: int = 1
    emergency_stop: bool = False
    master_pump_timeout_sec: int = 60
    total_flow_limit_per_window_ml: int = 5000
    flow_window_sec: int = 3600
    leak_shutdown_enabled: bool = True
    command_ttl_sec: int = 20
    device_heartbeat_timeout_sec: int = 90


@dataclass(frozen=True)
class BackendConfig:
    mqtt: MqttConfig
    sqlite: SqliteConfig
    influx: InfluxConfig
    llama: LlamaConfig
    openclaw: OpenClawOperatorConfig
    global_safety: GlobalSafetyConfig
    state_store_backend: str = "sqlite"
    telemetry_history_backend: str = "influx"
    zone_ids: list[str] = field(default_factory=lambda: ["tray_1", "tray_2", "tray_3", "tray_4"])

    def zone_configs(self) -> list[ZoneSafetyConfig]:
        zone_configs: list[ZoneSafetyConfig] = []
        for zone_id in self.zone_ids:
            env_zone = zone_id.upper()
            zone_configs.append(
                ZoneSafetyConfig(
                    zone_id=zone_id,
                    cooldown_sec=int(os.getenv(f"ZONE_{env_zone}_COOLDOWN_SEC", "300")),
                    max_duration_per_run_sec=int(os.getenv(f"ZONE_{env_zone}_MAX_DURATION_SEC", "30")),
                    max_runs_per_hour=int(os.getenv(f"ZONE_{env_zone}_MAX_RUNS_PER_HOUR", "6")),
                    max_total_water_per_day_ml=int(os.getenv(f"ZONE_{env_zone}_MAX_WATER_ML", "3000")),
                    settle_delay_ms=int(os.getenv(f"ZONE_{env_zone}_SETTLE_DELAY_MS", "1500")),
                    flow_confirm_timeout_ms=int(os.getenv(f"ZONE_{env_zone}_FLOW_CONFIRM_TIMEOUT_MS", "5000")),
                    min_flow_ml_per_min=float(os.getenv(f"ZONE_{env_zone}_MIN_FLOW_ML_PER_MIN", "50")),
                    blocked=os.getenv(f"ZONE_{env_zone}_BLOCKED", "0").strip().lower() in {"1", "true", "yes", "on"},
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
        openclaw=OpenClawOperatorConfig(
            enabled=os.getenv("OPENCLAW_OPERATOR_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            base_url=os.getenv("OPENCLAW_OPERATOR_URL", "").strip(),
            api_key=os.getenv("OPENCLAW_OPERATOR_KEY", "").strip(),
        ),
        global_safety=GlobalSafetyConfig(
            max_simultaneous_zones=int(os.getenv("GLOBAL_MAX_SIMULTANEOUS_ZONES", "1")),
            emergency_stop=os.getenv("GLOBAL_EMERGENCY_STOP", "0").strip().lower() in {"1", "true", "yes", "on"},
            master_pump_timeout_sec=int(os.getenv("MASTER_PUMP_TIMEOUT_SEC", "60")),
            total_flow_limit_per_window_ml=int(os.getenv("TOTAL_FLOW_LIMIT_PER_WINDOW_ML", "5000")),
            flow_window_sec=int(os.getenv("FLOW_WINDOW_SEC", "3600")),
            leak_shutdown_enabled=os.getenv("LEAK_SHUTDOWN_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            command_ttl_sec=int(os.getenv("COMMAND_TTL_SEC", "20")),
            device_heartbeat_timeout_sec=int(os.getenv("DEVICE_HEARTBEAT_TIMEOUT_SEC", "90")),
        ),
        state_store_backend=os.getenv("STATE_STORE_BACKEND", "sqlite").strip().lower() or "sqlite",
        telemetry_history_backend=os.getenv("TELEMETRY_HISTORY_BACKEND", "influx").strip().lower() or "influx",
        zone_ids=zone_ids,
    )
