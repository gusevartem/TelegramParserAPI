from collections.abc import AsyncIterator

import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from parser.persistence.core import PersistenceProvider
from parser.telegram import TelegramProvider
from parser.telegram.client import ITelegramClientFactory

from .mock_client import MockTelegramClientFactory


class TestTelegramProvider(Provider):
    @provide(scope=Scope.APP)
    def telegram_client_factory(self) -> ITelegramClientFactory:
        return MockTelegramClientFactory()


@pytest_asyncio.fixture(scope="class")
async def app_container(
    test_persistence_provider: Provider,
) -> AsyncIterator[AsyncContainer]:
    container = make_async_container(
        PersistenceProvider(),
        TelegramProvider(),
        test_persistence_provider,
        TestTelegramProvider(),
    )
    yield container
    await container.close()
