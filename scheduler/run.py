import logging.config
import logging
import os
from arq import Worker
from arq.connections import RedisSettings
from arq.logs import default_log_config
from src.scheduler import Scheduler
from dotenv import load_dotenv
from arq.cron import cron

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
)

REDIS_SETTINGS = RedisSettings(
    os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
)
FUNCTIONS = [Scheduler.add_channel]
CRON = [cron(Scheduler.run_iteration, minute={*list(range(0, 59))})]
DEFAULT_POLL_DELAY = float(os.getenv("DEFAULT_POLL_DELAY", "0.01"))


async def startup(ctx):
    scheduler = Scheduler(
        144,
        10,
        REDIS_SETTINGS,
        os.getenv("PARSER_QUEUE_NAME", "parser"),
        REDIS_SETTINGS,
        os.getenv("DATABASE_QUEUE_NAME", "database"),
        REDIS_SETTINGS,
        os.getenv("STORAGE_QUEUE_NAME", "storage"),
    )
    await scheduler.init()
    ctx["Scheduler_instance"] = scheduler
    logging.getLogger("arq").info("Startup done")


async def shutdown(ctx):
    logging.getLogger("arq").info("Shutting down...")


def main():
    verbose = os.getenv("VERBOSE", "1") == "1"
    log_level = "DEBUG" if verbose else "INFO"
    logging_config = default_log_config(verbose=verbose)
    logging_config["loggers"]["scheduler"] = {
        "level": log_level,
        "handlers": ["arq.standard"],
    }

    logging.config.dictConfig(logging_config)
    worker = Worker(
        functions=FUNCTIONS,
        on_startup=startup,
        on_shutdown=shutdown,
        redis_settings=REDIS_SETTINGS,
        queue_name=os.getenv("SCHEDULER_QUEUE_NAME", "scheduler"),
        job_timeout=100,
        cron_jobs=CRON,
        max_jobs=int(os.getenv("MAX_JOBS", "10")),
        poll_delay=DEFAULT_POLL_DELAY,
    )
    worker.run()


if __name__ == "__main__":
    main()
