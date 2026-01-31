from collections.abc import AsyncIterable
from typing import NewType

import structlog
from dishka import Provider, Scope, provide, provide_all
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from parser.settings import ProjectSettings
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
    TelegramClientDAO,
    TelegramClientDAOFactory,
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
        ParsingTask,
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
        ParsingTask,
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
        logger = structlog.get_logger("persistence")
        if project_settings.debug:
            url = "sqlite+aiosqlite:///:memory:"
            logger.info("using_database", mode="debug", database_type="sqlite_memory")
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
        logger = structlog.get_logger("persistence")
        engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=persistence_settings.mysql_pool_size,
            max_overflow=persistence_settings.mysql_max_overflow,
            pool_pre_ping=True,
            connect_args={"init_command": "SET time_zone = '+00:00'"},
            isolation_level="READ COMMITTED",
        )
        SQLAlchemyInstrumentor().instrument(
            engine=engine.sync_engine,
            tracer_provider=trace.get_tracer_provider(),
            enable_commenter=True,
            commenter_options={"trace_id": True, "span_id": True},
        )
        logger.info("database_engine_created")
        if project_settings.debug:
            logger.info("creating_all_tables", mode="debug")
            async with engine.begin() as conn:
                await conn.run_sync(BaseModel.metadata.create_all)
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
        scope=Scope.APP,
    )

    multiple_dao_factory: CompositeDependencySource = provide(
        MultipleDAOFactory, scope=Scope.APP
    )
