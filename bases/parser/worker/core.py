import asyncio
from importlib.metadata import version

import structlog
from dishka import Provider, Scope, make_async_container, provide
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from parser.logging import setup_logging
from parser.message_broker import MessageBrokerProvider
from parser.persistence import PersistenceProvider
from parser.settings import ProjectSettings, ProjectSettingsProvider
from parser.storage import StorageProvider
from parser.telegram import TelegramProvider

from .worker import Worker


class WorkerProvider(Provider):
    worker: CompositeDependencySource = provide(
        Worker,
        scope=Scope.APP,
        provides=Worker,
    )


async def run_worker() -> None:
    container = make_async_container(
        WorkerProvider(),
        PersistenceProvider(),
        ProjectSettingsProvider(),
        StorageProvider(),
        TelegramProvider(),
        MessageBrokerProvider(),
    )
    project_settings = await container.get(ProjectSettings)
    setup_logging(project_settings, "worker", version("worker"))

    logger: structlog.BoundLogger = structlog.get_logger("worker")
    tracer = trace.get_tracer("worker")

    try:
        worker = await container.get(Worker)
        while True:
            with tracer.start_as_current_span("worker.iteration") as span:
                try:
                    logger.info("worker_loop_started")

                    await worker.start()

                    logger.info("worker_loop_completed")

                except asyncio.CancelledError:
                    logger.info("worker_loop_cancelled")
                    break
                except Exception as e:
                    logger.error("worker_loop_failed", exc_info=True)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)

            logger.info("sleeping", duration_seconds=60)
            await asyncio.sleep(60)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
