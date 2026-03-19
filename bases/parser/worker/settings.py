from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    message_monitoring_time_limit_hours: int = 168
    channel_messages_stat_time_limit_hours: int = 24
    # Seconds to wait between sequential Telegram API calls within a single task.
    # A small random delay in [min, max] reduces burst patterns that trigger bans.
    request_delay_min_seconds: float = 1.0
    request_delay_max_seconds: float = 3.0
