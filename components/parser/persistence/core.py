from collections.abc import AsyncIterable
from logging import getLogger
from typing import NewType

from dishka import Provider, Scope, provide, provide_all
from dishka.dependency_source import CompositeDependencySource
from parser.settings import ProjectSettings
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .models import (
    ChannelDAO,
    ChannelMessageDAO,
    ChannelMessageStatisticDAO,
    ChannelStatisticDAO,
    MediaDAO,
    TelegramClientDAO,
)
from .models._base import BaseModel
from .settings import PersistenceSettings

DatabaseUrl = NewType("DatabaseUrl", str)


def register_model() -> list[type]:
    from .models import (
        Channel,
        ChannelMessage,
        ChannelMessageStatistic,
        ChannelStatistic,
        Media,
        MessageMediaLink,
        TelegramClient,
        TelegramClientProxy,
    )

    return [
        Channel,
        ChannelMessage,
        MessageMediaLink,
        ChannelStatistic,
        Media,
        ChannelMessageStatistic,
        TelegramClient,
        TelegramClientProxy,
    ]


class PersistenceProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> PersistenceSettings:
        return PersistenceSettings()

    @provide(scope=Scope.APP)
    def database_url(
        self,
        project_settings: ProjectSettings,
        persistence_settings: PersistenceSettings,
    ) -> DatabaseUrl:
        if project_settings.debug:
            url = "sqlite+aiosqlite:///:memory:"
            logger = getLogger(__name__)
            logger.info("⚠️  DEBUG MODE: Using in-memory SQLite database")
        else:
            if any(
                parameter is None
                for parameter in (
                    persistence_settings.mysql_user,
                    persistence_settings.mysql_password,
                    persistence_settings.mysql_host,
                    persistence_settings.mysql_port,
                    persistence_settings.mysql_database,
                )
            ):
                raise ValueError("MySQL settings must be provided in non-debug mode")
            url = (
                f"mysql+asyncmy://{persistence_settings.mysql_user}"
                + f":{persistence_settings.mysql_password}"
                + f"@{persistence_settings.mysql_host}"
                + f":{persistence_settings.mysql_port}"
                + f"/{persistence_settings.mysql_database}"
            )
        return DatabaseUrl(url)

    @provide(scope=Scope.APP)
    async def engine(
        self,
        database_url: DatabaseUrl,
        persistence_settings: PersistenceSettings,
        project_settings: ProjectSettings,
    ) -> AsyncIterable[AsyncEngine]:
        register_model()
        logger = getLogger(__name__)
        engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=persistence_settings.mysql_pool_size,
            max_overflow=persistence_settings.mysql_max_overflow,
            pool_pre_ping=True,
        )
        logger.info("Database engine created")
        if project_settings.debug:
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
        ChannelMessageStatisticDAO,
        TelegramClientDAO,
        scope=Scope.REQUEST,
    )
