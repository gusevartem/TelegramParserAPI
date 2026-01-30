import asyncio
from logging import getLogger

from dishka import make_async_container
from parser.logging import setup_logging
from parser.message_broker import IMessageBroker, MessageBrokerProvider
from parser.persistence import PersistenceProvider
from parser.scheduler import GetTasks, SchedulerProvider
from parser.settings import ProjectSettings, ProjectSettingsProvider


async def run_scheduler() -> None:
    container = make_async_container(
        SchedulerProvider(),
        MessageBrokerProvider(),
        PersistenceProvider(),
        ProjectSettingsProvider(),
    )
    settings = await container.get(ProjectSettings)
    setup_logging(settings)
    message_broker = await container.get(IMessageBroker)
    logger = getLogger(__name__)

    try:
        while True:
            try:
                logger.info("⌛ Running iteration")

                async with container() as request_container:
                    get_tasks = await request_container.get(GetTasks)
                    async for task in get_tasks(set_processing=True):
                        await message_broker.publish_task(task)

                logger.info("✅ Iteration completed")

            except Exception:
                logger.error("❗❗❗ SCHEDULER CRASHED ❗❗❗", exc_info=True)

            logger.info("⏳ Restarting in 60 seconds...")
            await asyncio.sleep(60)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_scheduler())
    except KeyboardInterrupt:
        pass
