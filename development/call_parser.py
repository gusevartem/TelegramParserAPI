import asyncio
import sys

from dishka import make_async_container
from parser.message_broker import IMessageBroker, MessageBrokerProvider
from parser.persistence import PersistenceProvider
from parser.scheduler import AddTask, SchedulerProvider
from parser.settings import ProjectSettingsProvider


async def main(channel_url: str):
    container = make_async_container(
        PersistenceProvider(),
        MessageBrokerProvider(),
        SchedulerProvider(),
        ProjectSettingsProvider(),
    )
    message_broker = await container.get(IMessageBroker)
    async with container() as request_container:
        add_task = await request_container.get(AddTask)
        task = await add_task(channel_url)
    await message_broker.publish_task(task)


def run():
    if len(sys.argv) != 2:
        raise ValueError("Usage: poetry run call_parser <channel_url>")
    asyncio.run(main(sys.argv[1]))
