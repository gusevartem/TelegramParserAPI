from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class MessageBrokerSettings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    parsing_tasks_queue_name: str = "parsing.tasks.queue"

    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_login: str
    rabbitmq_password: str
