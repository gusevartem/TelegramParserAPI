import asyncio
from logging import getLogger

from dishka import Provider, Scope, make_async_container, provide
from dishka.dependency_source import CompositeDependencySource
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
    setup_logging(project_settings)

    logger = getLogger("app")

    try:
        worker = await container.get(Worker)
        while True:
            try:
                logger.info("🚀 Starting worker loop...")
                await worker.start()
                logger.info("🏁 Worker loop stopped")
                logger.info("⏳ Restarting in 60 seconds...")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                logger.info("🛑 Worker stopping...")
                break
            except Exception as e:
                logger.fatal(
                    "❗❗❗ WORKER CRASHED ❗❗❗"
                    + "\nCAUSE OF CURRENT SETTINGS "
                    + "THIS EXCEPTION WILL BE IGNORED."
                    + f"\nName: {type(e).__name__}. Exception: {e}",
                    exc_info=True,
                )
                logger.info("⏳ Restarting in 60 seconds...")
                await asyncio.sleep(60)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
