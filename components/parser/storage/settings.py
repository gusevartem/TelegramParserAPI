from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    s3_access_key: str
    s3_secret_key: str
    s3_bucket_name: str
    s3_region_name: str
    s3_endpoint_url: str

    max_file_size_bytes: int = 10 * 1024 * 1024
