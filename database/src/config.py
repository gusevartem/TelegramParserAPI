import os
from dotenv import load_dotenv

load_dotenv()


TORTOISE_ORM = {
    "connections": {"default": os.getenv("MYSQL_URL")},
    "apps": {
        "models": {
            "models": ["src.models"],
            "default_connection": "default",
        }
    },
}
