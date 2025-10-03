import os
from dotenv import load_dotenv
from typing import Any, Dict, Union
from arq.connections import RedisSettings
import uuid

load_dotenv()


class ApiServiceConfig:
    BASE_PREFIX = "/api/v1"
    DEFAULT_RESPONSE: Dict[Union[int, str], Dict[str, Any]] = {
        500: {"description": "Internal Error"},
        504: {"description": "Timeout Error"},
        400: {"description": "Something went wrong while processing request"},
        200: {"description": "Success"},
        403: {"description": "Invalid key"},
    }
    PROJECT_NAME = "Telegram Parser API"
    VERSION = "1.0.0"
    SECRET_KEY = os.getenv("SECRET_KEY", uuid.uuid4().hex)


class RedisConfig:
    PARSER_QUEUE_NAME = os.getenv("PARSER_QUEUE_NAME", "parser")
    TELEGRAM_QUEUE_NAME = os.getenv("TELEGRAM_QUEUE_NAME", "telegram")
    DATABASE_QUEUE_NAME = os.getenv("DATABASE_QUEUE_NAME", "database")
    SCHEDULER_QUEUE_NAME = os.getenv("SCHEDULER_QUEUE_NAME", "scheduler")
    STORAGE_QUEUE_NAME = os.getenv("STORAGE_QUEUE_NAME", "storage")
    REDIS_SETTINGS = RedisSettings(
        os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
    )
    DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", "10"))
    PARSER_TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "100"))
    DEFAULT_POLL_DELAY = float(os.getenv("DEFAULT_POLL_DELAY", "0.01"))
