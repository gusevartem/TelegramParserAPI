import asyncio

import structlog
from dishka import Provider, Scope, make_async_container, provide
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from parser.logging import LoggingSettings, LoggingSettingsProvider, setup_logging
from parser.persistence import PersistenceProvider
from parser.scheduler import SchedulerProvider
from parser.storage import StorageProvider
from parser.telegram import TelegramProvider
from parser.telegram.settings import TelegramSettings

from .settings import WorkerSettings
from .worker import Worker


class WorkerProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> WorkerSettings:
        return WorkerSettings()

    worker: CompositeDependencySource = provide(
        Worker,
        scope=Scope.APP,
        provides=Worker,
    )


async def run_worker() -> None:
    container = make_async_container(
        WorkerProvider(),
        PersistenceProvider(),
        LoggingSettingsProvider(),
        StorageProvider(),
        TelegramProvider(),
        SchedulerProvider(),
    )
    logging_settings = await container.get(LoggingSettings)
    telegram_settings = await container.get(TelegramSettings)
    setup_logging(logging_settings, "worker", telegram_settings.worker_id)

    logger: structlog.BoundLogger = structlog.get_logger("worker")
    tracer = trace.get_tracer("worker")

    try:
        worker = await container.get(Worker)
        while True:
            try:
                await worker.start()
            except asyncio.CancelledError:
                break
            except Exception as e:
                with tracer.start_as_current_span("worker.iteration_failed") as span:
                    logger.error("worker_loop_failed", exc_info=True)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)
            await asyncio.sleep(60)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
