from collections.abc import AsyncIterable
from logging import getLogger
from typing import NewType

from dishka import Provider, Scope, provide, provide_all
from dishka.dependency_source import CompositeDependencySource
from parser.settings import Settings
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .models import ChannelDAO, ChannelMessageDAO, ChannelStatisticDAO, MediaDAO
from .models._base import BaseModel

DatabaseUrl = NewType("DatabaseUrl", str)


def register_model() -> list[type]:
    from .models import (
        Channel,
        ChannelMessage,
        ChannelStatistic,
        Media,
        MessageMediaLink,
    )

    return [Channel, ChannelMessage, MessageMediaLink, ChannelStatistic, Media]


class PersistenceProvider(Provider):
    @provide(scope=Scope.APP)
    def database_url(self, settings: Settings) -> DatabaseUrl:
        if settings.debug:
            url = "sqlite+aiosqlite:///:memory:"
            logger = getLogger(__name__)
            logger.info("⚠️  DEBUG MODE: Using in-memory SQLite database")
        else:
            if any(
                parameter is None
                for parameter in (
                    settings.postgres_user,
                    settings.postgres_password,
                    settings.postgres_host,
                    settings.postgres_port,
                    settings.postgres_db,
                )
            ):
                raise ValueError(
                    "PostgreSQL settings must be provided in non-debug mode"
                )
            url = (
                f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}"
                + f"@{settings.postgres_host}:{settings.postgres_port}"
                + f"/{settings.postgres_db}"
            )
        return DatabaseUrl(url)

    @provide(scope=Scope.APP)
    async def engine(
        self, database_url: DatabaseUrl, settings: Settings
    ) -> AsyncIterable[AsyncEngine]:
        register_model()
        logger = getLogger(__name__)
        engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=settings.postgres_pool_size,
            max_overflow=settings.postgres_max_overflow,
            pool_pre_ping=True,
        )
        logger.info("Database engine created")
        if settings.debug:
            logger.info("⚠️  DEBUG MODE: Creating all tables in the database")
            async with engine.begin() as conn:
                await conn.run_sync(BaseModel.metadata.create_all)
        try:
            yield engine
        finally:
            logger.info("Disposing database engine")
            await engine.dispose()

    @provide(scope=Scope.APP)
    def session_maker(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

    @provide(scope=Scope.REQUEST)
    async def session(
        self, session_maker: async_sessionmaker[AsyncSession]
    ) -> AsyncIterable[AsyncSession]:
        async with session_maker() as session:
            yield session

    data_access_objects: CompositeDependencySource = provide_all(
        ChannelDAO,
        ChannelMessageDAO,
        ChannelStatisticDAO,
        MediaDAO,
        scope=Scope.REQUEST,
    )
