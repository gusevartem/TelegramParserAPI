from collections.abc import AsyncIterator

import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from parser.persistence.core import PersistenceProvider
from parser.telegram import TelegramProvider
from parser.telegram.client import ITelegramClientFactory
from parser.telegram.settings import TelegramSettings

from .mock_client import MockTelegramClientFactory


class TestTelegramProvider(Provider):
    @provide(scope=Scope.APP)
    def telegram_settings(self) -> TelegramSettings:
        return TelegramSettings.model_construct(
            default_api_id=1,
            default_api_hash="test_hash",
            default_device_model="test_device",
            default_system_version="test_system",
            default_app_version="test_app",
            default_lang_code="en",
            default_system_lang_code="en",
        )

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
