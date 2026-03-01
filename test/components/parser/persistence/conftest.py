from collections.abc import AsyncIterator

import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from parser.persistence.core import PersistenceProvider, register_model
from parser.persistence.models._base import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class TestPersistenceProvider(Provider):
    @provide(scope=Scope.APP)
    async def engine(self) -> AsyncIterator[AsyncEngine]:
        register_model()
        engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            echo=False,
        )
        async with engine.begin() as conn:
            await conn.run_sync(BaseModel.metadata.create_all)
        yield engine
        await engine.dispose()


@pytest_asyncio.fixture(scope="class")
async def app_container() -> AsyncIterator[AsyncContainer]:
    container = make_async_container(PersistenceProvider(), TestPersistenceProvider())
    yield container
    await container.close()


@pytest_asyncio.fixture
async def request_container(
    app_container: AsyncContainer,
) -> AsyncIterator[AsyncContainer]:
    async with app_container() as request_container:
        yield request_container
