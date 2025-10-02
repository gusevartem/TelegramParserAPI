import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SESSION_DIR = os.path.join(os.path.dirname(__file__), "..", "telegram", "sessions")
    TDATA_PATH = os.path.join(os.path.dirname(__file__), "..", "telegram", "tdata")


class TelegramClientConfig:
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    DEVICE_MODEL = "PC"
    SYSTEM_VERSION = "Windows:7"
    APP_VERSION = "1.0.1"
    LANG_CODE = "ru"
    SYSTEM_LANG_CODE = "ru"
    LANG_PACK = "ru"


TORTOISE_ORM = {
    "connections": {"default": os.getenv("MYSQL_URL")},
    "apps": {
        "models": {
            "models": ["src.telegram.models"],
            "default_connection": "default",
        }
    },
}
