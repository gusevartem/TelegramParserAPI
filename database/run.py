import logging
import logging.config
import os
from arq import Worker
from arq.connections import RedisSettings
from src import Database
from arq.logs import default_log_config
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

load_dotenv()


REDIS_SETTINGS = RedisSettings(
    os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
)
FUNCTIONS = [
    Database.update_or_create_channel,
    Database.get_channel,
    Database.get_channel_by_link,
    Database.get_channels_ids,
    Database.get_24h_statistics,
    Database.get_messages,
    Database.update_or_create_message,
    Database.get_media
]
DEFAULT_POLL_DELAY = float(os.getenv("DEFAULT_POLL_DELAY", "0.01"))


async def startup(ctx):
    database = Database()
    await database.connect()
    ctx["Database_instance"] = database
    logging.getLogger("arq").info(
        f"Startup done {ctx['worker_id']} / {ctx['workers_count']}"
    )


async def shutdown(ctx):
    logging.getLogger("arq").info("Shutting down...")
    await ctx["Telegram_instance"].close()


def start_worker(worker_id: int, workers_count: int):
    verbose = os.getenv("VERBOSE", "1") == "1"
    log_level = "DEBUG" if verbose else "INFO"
    logging_config = default_log_config(verbose=verbose)
    logging_config["loggers"]["database"] = {
        "level": log_level,
        "handlers": ["arq.standard"],
    }

    logging.config.dictConfig(logging_config)
    worker = Worker(
        functions=FUNCTIONS,
        on_startup=startup,
        on_shutdown=shutdown,
        queue_name=os.getenv("DATABASE_QUEUE_NAME", "database"),
        redis_settings=REDIS_SETTINGS,
        job_timeout=100,
        max_jobs=int(os.getenv("MAX_JOBS", "10")),
        poll_delay=DEFAULT_POLL_DELAY,
    )
    worker.ctx["worker_id"] = worker_id
    worker.ctx["workers_count"] = workers_count
    worker.run()


def main(max_workers: int):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for i in range(max_workers):
            executor.submit(start_worker, i + 1, max_workers)


if __name__ == "__main__":
    main(int(os.getenv("WORKERS_COUNT", "1")))
