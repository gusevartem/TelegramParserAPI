from typing import ClassVar
from uuid import uuid4

from parser.dto import TelegramCredentials
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TelegramSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    worker_id: str = Field(default_factory=lambda: str(uuid4()))
    account_lock_hours: int = 20
    save_telegram_responses: bool = False

    default_api_id: int
    default_api_hash: str
    default_device_model: str
    default_system_version: str
    default_app_version: str
    default_lang_code: str
    default_system_lang_code: str

    @property
    def default_credentials(self) -> TelegramCredentials:
        return TelegramCredentials(
            api_id=self.default_api_id,
            api_hash=self.default_api_hash,
            device_model=self.default_device_model,
            system_version=self.default_system_version,
            app_version=self.default_app_version,
            lang_code=self.default_lang_code,
            system_lang_code=self.default_system_lang_code,
        )
