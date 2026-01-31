from typing import ClassVar

from dishka import Provider, Scope, provide
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProjectSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    debug: bool = False
    trace_exporter_endpoint: str | None = None


class ProjectSettingsProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> ProjectSettings:
        return ProjectSettings()
