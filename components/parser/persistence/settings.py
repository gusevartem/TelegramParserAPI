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

    mysql_user: str
    mysql_password: str
    mysql_host: str
    mysql_port: int
    mysql_database: str

    mysql_pool_size: int = 10
    mysql_max_overflow: int = 20
