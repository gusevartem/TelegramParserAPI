from collections.abc import AsyncIterable
from typing import NewType

import structlog
from dishka import Provider, Scope, provide, provide_all
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .models import (
    ChannelDAO,
    ChannelDAOFactory,
    ChannelMessageDAO,
    ChannelMessageDAOFactory,
    ChannelMessageStatisticDAO,
    ChannelMessageStatisticDAOFactory,
    ChannelStatisticDAO,
    ChannelStatisticDAOFactory,
    MediaDAO,
    MediaDAOFactory,
    MultipleDAOFactory,
    ParsingTaskDAO,
    ParsingTaskDAOFactory,
    TaskClaimHistoryDAO,
    TaskClaimHistoryDAOFactory,
    TelegramClientDAO,
    TelegramClientDAOFactory,
    WorkerAccountUsageDAO,
    WorkerAccountUsageDAOFactory,
)
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
        ParsingTask,
        TaskClaimHistory,
        TelegramClient,
        TelegramClientProxy,
        WorkerAccountUsage,
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
        ParsingTask,
        WorkerAccountUsage,
        TaskClaimHistory,
    ]


class PersistenceProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> PersistenceSettings:
        return PersistenceSettings()  # type: ignore  # pyright: ignore[reportCallIssue]

    @provide(scope=Scope.APP)
    def database_url(
        self,
        persistence_settings: PersistenceSettings,
    ) -> DatabaseUrl:
        url = (
            f"postgresql+asyncpg://{persistence_settings.postgres_user}"
            + f":{persistence_settings.postgres_password}"
            + f"@{persistence_settings.postgres_host}"
            + f":{persistence_settings.postgres_port}"
            + f"/{persistence_settings.postgres_database}"
        )
        return DatabaseUrl(url)

    @provide(scope=Scope.APP)
    async def engine(
        self,
        database_url: DatabaseUrl,
        persistence_settings: PersistenceSettings,
    ) -> AsyncIterable[AsyncEngine]:
        register_model()
        logger = structlog.get_logger("persistence")
        engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=persistence_settings.postgres_pool_size,
            max_overflow=persistence_settings.postgres_max_overflow,
            pool_pre_ping=True,
            isolation_level="READ COMMITTED",
        )
        SQLAlchemyInstrumentor().instrument(
            engine=engine.sync_engine,
            tracer_provider=trace.get_tracer_provider(),
            enable_commenter=True,
            commenter_options={"trace_id": True, "span_id": True},
        )
        logger.info("database_engine_created")
        try:
            yield engine
        finally:
            logger.info("disposing_database_engine")
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
        ParsingTaskDAO,
        WorkerAccountUsageDAO,
        TaskClaimHistoryDAO,
        scope=Scope.REQUEST,
    )

    data_access_object_factories: CompositeDependencySource = provide_all(
        ChannelDAOFactory,
        ChannelMessageDAOFactory,
        ChannelStatisticDAOFactory,
        MediaDAOFactory,
        ChannelMessageStatisticDAOFactory,
        TelegramClientDAOFactory,
        ParsingTaskDAOFactory,
        WorkerAccountUsageDAOFactory,
        TaskClaimHistoryDAOFactory,
        scope=Scope.APP,
    )

    multiple_dao_factory: CompositeDependencySource = provide(
        MultipleDAOFactory, scope=Scope.APP
    )
