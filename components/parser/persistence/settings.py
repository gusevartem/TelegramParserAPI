from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class PersistenceSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    mysql_user: str | None = None
    mysql_password: str | None = None
    mysql_host: str | None = None
    mysql_port: int | None = None
    mysql_database: str | None = None

    mysql_pool_size: int = 10
    mysql_max_overflow: int = 20
