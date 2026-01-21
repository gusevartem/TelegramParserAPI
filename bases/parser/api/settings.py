from typing import ClassVar

from dishka import Provider, Scope, provide
from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Telegram Parser API"
    api_prefix: str = "/api"
    api_port: int = 8080

    @property
    def methods_prefix(self) -> str:
        return f"{self.api_prefix}/v2"

    secret_key: str


class APISettingsProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> APISettings:
        return APISettings()  # type: ignore # pyright: ignore
