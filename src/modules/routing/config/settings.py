from enum import StrEnum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent


class RedisMetricsSettings(BaseSettings):
    url: str = "redis://localhost:6379/0"
    history_limit: int = 32
    latency_window: int = 100


class MetricsBackend(StrEnum):
    MEMORY = "memory"
    REDIS = "redis"


class MetricsSettings(BaseSettings):
    backend: MetricsBackend = MetricsBackend.MEMORY
    redis: RedisMetricsSettings = Field(default_factory=RedisMetricsSettings)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_prefix="",
        extra="forbid",
        env_nested_delimiter="__",
    )

    collector_interval: float = 0.25
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)


settings = Settings()
