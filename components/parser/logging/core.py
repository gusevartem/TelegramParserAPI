import logging
import sys
from typing import override

from parser.settings import Settings

COLORED_LEVELS = {
    logging.DEBUG: "\033[36mDEBUG\033[0m",
    logging.INFO: "\033[32mINFO\033[0m",
    logging.WARNING: "\033[33mWARNING\033[0m",
    logging.ERROR: "\033[31mERROR\033[0m",
    logging.CRITICAL: "\033[91mCRITICAL\033[0m",
}


class ColorFormatter(logging.Formatter):
    @override
    def formatMessage(self, record: logging.LogRecord) -> str:
        field = COLORED_LEVELS.get(record.levelno, record.levelname)
        record.levelname = field + (" " * (17 - len(field)))

        return super().formatMessage(record)


def setup_logging(settings: Settings):
    log_level = logging.DEBUG if settings.debug else logging.INFO

    log_format = (
        "%(asctime)s | %(levelname)-8s | %(name)s | "
        "%(funcName)s:%(lineno)d | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter(log_format))

    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(logging.Formatter(log_format))

    logging.basicConfig(
        level=log_level,
        handlers=[
            console_handler,
            file_handler if not settings.debug else logging.NullHandler(),
        ],
    )

    sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
    sqlalchemy_logger.propagate = False
    sqlalchemy_logger.setLevel(logging.INFO if settings.debug else logging.WARNING)

    logging.getLogger("aiosqlite").disabled = True
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    logging.getLogger("dishka").setLevel(logging.INFO)

    logging.getLogger("aio_pika").setLevel(logging.INFO)
    logging.getLogger("aiormq").setLevel(logging.INFO)

    app_logger = logging.getLogger("app")

    app_logger.info(f"🔧  Logging configured: level={logging.getLevelName(log_level)}")
