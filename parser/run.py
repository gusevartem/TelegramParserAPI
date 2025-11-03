import os
import logging.config
from arq import Worker
from arq.connections import RedisSettings
from arq.logs import default_log_config
import logging
from src.parser import Parser
from src.telegram import Telegram
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv


load_dotenv()

import sentry_sdk

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=os.getenv("SENTRY_ENV"),
    send_default_pii=True,
    traces_sample_rate=1.0,
    # To collect profiles for all profile sessions,
    # set `profile_session_sample_rate` to 1.0.
    profile_session_sample_rate=1.0,
    # Profiles will be automatically collected while
    # there is an active span.
    profile_lifecycle="trace",
    attach_stacktrace=True,
)

REDIS_SETTINGS = RedisSettings(
    os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
)
PARSER_FUNCTIONS = [Parser.get_channel_info]
TELEGRAM_FUNCTIONS = [Telegram.add_client]
DEFAULT_POLL_DELAY = float(os.getenv("DEFAULT_POLL_DELAY", "0.01"))


async def startup(ctx):
    telegram = Telegram(
        os.getenv("REDIS_HOST", "localhost"),
        int(os.getenv("REDIS_PORT", "6379")),
        int(os.getenv("TELEGRAM_CLIENTS_REDIS_DB", "5")),
    )
    await telegram.init_database()
    parser = Parser(telegram)
    ctx["Parser_instance"] = parser
    ctx["Telegram_instance"] = telegram
    logging.getLogger("arq").info(
        f"Startup done {ctx['worker_id']} / {ctx['workers_count']}"
    )


async def shutdown(ctx):
    logging.getLogger("arq").info("Shutting down...")
    await ctx["Telegram_instance"].close()


def start_worker(worker_id: int, workers_count: int, functions, queue_name):
    verbose = os.getenv("VERBOSE", "1") == "1"
    log_level = "DEBUG" if verbose else "INFO"
    logging_config = default_log_config(verbose=verbose)
    logging_config["loggers"]["telegram"] = {
        "level": log_level,
        "handlers": ["arq.standard"],
    }
    logging_config["loggers"]["parser"] = {
        "level": log_level,
        "handlers": ["arq.standard"],
    }

    logging.config.dictConfig(logging_config)
    worker = Worker(
        functions=functions,
        on_startup=startup,
        on_shutdown=shutdown,
        redis_settings=REDIS_SETTINGS,
        queue_name=queue_name,
        job_timeout=100,
        max_jobs=int(os.getenv("MAX_JOBS", "10")),
        poll_delay=DEFAULT_POLL_DELAY,
    )
    worker.ctx["worker_id"] = worker_id
    worker.ctx["workers_count"] = workers_count
    worker.run()


def main(max_workers: int):
    with ProcessPoolExecutor(max_workers=max_workers + 1) as executor:
        for i in range(max_workers):
            executor.submit(
                start_worker,
                i + 1,
                max_workers,
                PARSER_FUNCTIONS,
                os.getenv("PARSER_QUEUE_NAME", "parser"),
            )
        executor.submit(
            start_worker,
            1,
            1,
            TELEGRAM_FUNCTIONS,
            os.getenv("TELEGRAM_QUEUE_NAME", "telegram"),
        )


if __name__ == "__main__":
    main(int(os.getenv("WORKERS_COUNT", "1")))
