import os
import logging.config
from arq import Worker
from arq.connections import RedisSettings
from arq.logs import default_log_config
import logging
from src.storage import Storage
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

load_dotenv()

import sentry_sdk

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=os.getenv("SENTRY_ENV"),
    send_default_pii=True,
)

REDIS_SETTINGS = RedisSettings(
    os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
)
FUNCTIONS = [Storage.get_logo, Storage.save_logo, Storage.get_media, Storage.save_media]
DEFAULT_POLL_DELAY = float(os.getenv("DEFAULT_POLL_DELAY", "0.01"))

ACCESS_KEY = os.getenv("YC_KEY_ID", "")
SECRET_KEY = os.getenv("YC_SECRET_KEY", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
REGION_NAME = os.getenv("REGION_NAME", "")
ENDPOINT = os.getenv("ENDPOINT", "")


async def startup(ctx):
    ctx["Storage_instance"] = Storage(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        bucket_name=BUCKET_NAME,
        region_name=REGION_NAME,
        endpoint_url=ENDPOINT,
    )
    logging.getLogger("arq").info(
        f"Startup done {ctx['worker_id']} / {ctx['workers_count']}"
    )


async def shutdown(ctx):
    logging.getLogger("arq").info("Shutting down...")


def start_worker(worker_id: int, workers_count: int, functions, queue_name):
    verbose = os.getenv("VERBOSE", "1") == "1"
    log_level = "DEBUG" if verbose else "INFO"
    logging_config = default_log_config(verbose=verbose)
    logging_config["loggers"]["storage"] = {
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
                FUNCTIONS,
                os.getenv("STORAGE_QUEUE_NAME", "storage"),
            )


if __name__ == "__main__":
    main(int(os.getenv("WORKERS_COUNT", "1")))
