from collections.abc import AsyncIterator

import pytest_asyncio
from dishka import AsyncContainer, Provider, make_async_container
from parser.persistence.core import PersistenceProvider


@pytest_asyncio.fixture(scope="class")
async def app_container(
    test_persistence_provider: Provider,
) -> AsyncIterator[AsyncContainer]:
    container = make_async_container(PersistenceProvider(), test_persistence_provider)
    yield container
    await container.close()
