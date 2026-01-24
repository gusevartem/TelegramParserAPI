from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class TelegramSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    session_storage_queue_name: str = "telegram.session_storage.queue"
    session_storage_delayed_exchange_name: str = (
        "telegram.session_storage.delayed_exchange"
    )
