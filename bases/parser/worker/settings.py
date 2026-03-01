from typing import ClassVar
from uuid import uuid4

from pydantic import Field
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
    worker_id: str = Field(default_factory=lambda: str(uuid4()))
