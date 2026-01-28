from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, override
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, ForeignKey, String, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseDAOFactory, BaseModel

if TYPE_CHECKING:
    from .channel import Channel


class ParsingTaskStatus(StrEnum):
    IDLE = "idle"
    PROCESSING = "processing"
    EXISTS = "exists"
    ERROR = "error"


class ParsingTask(BaseModel):
    __tablename__: str = "parsing_task"

    __table_args__: tuple[Any, ...] = (
        CheckConstraint("bucket >= 0 AND bucket < 1440", name="check_bucket_range"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    url: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey("channel.id", ondelete="CASCADE"), index=True, default=None
    )
    status: Mapped[ParsingTaskStatus] = mapped_column(
        String(20), default=ParsingTaskStatus.IDLE
    )
    bucket: Mapped[int] = mapped_column()
    last_parsed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), default=None
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    channel: Mapped[Channel | None] = relationship(back_populates="tasks")


class ParsingTaskDAO(BaseDAO[ParsingTask, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ParsingTask)

    @override
    async def create(
        self,
        url: str,
        bucket: int,
    ) -> ParsingTask:
        new_task = ParsingTask(url=url, bucket=bucket)
        await self.save(new_task)
        return new_task

    async def find_by_url(self, url: str) -> ParsingTask | None:
        stmt = select(ParsingTask).where(ParsingTask.url == url)

        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def get_buckets_loading(self) -> dict[int, int]:
        """Получение загруженности бакетов

        Returns:
            dict[int, int]: ключ - номер бакета, значение - количество задач
        """
        stmt = (
            select(ParsingTask.bucket, func.count(ParsingTask.id))
            .where(
                ParsingTask.status.in_(
                    [ParsingTaskStatus.IDLE, ParsingTaskStatus.PROCESSING]
                )
            )
            .group_by(ParsingTask.bucket)
        )

        result = await self._session.execute(stmt)

        return {bucket: count for bucket, count in result.all() if count > 0}

    async def get_scheduled_tasks(
        self, current_minute_of_day: int, limit: int = 1000
    ) -> Sequence[ParsingTask]:
        """Получение задач, которые пора парсить

        Args:
            current_minute_of_day (int): текущая минута

        Returns:
            Sequence[ParsingTask]: список задач
        """
        stmt = (
            select(ParsingTask)
            .where(ParsingTask.status == ParsingTaskStatus.IDLE)
            .where(ParsingTask.bucket <= current_minute_of_day)
            .where(
                (
                    (ParsingTask.last_parsed_at.is_not(None))
                    & (ParsingTask.last_parsed_at < func.current_date())
                )
                | (
                    (ParsingTask.last_parsed_at.is_(None))
                    & (ParsingTask.created_at < func.current_date())
                )
            )
            .order_by(ParsingTask.bucket.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(stmt)
        return result.scalars().all()


class ParsingTaskDAOFactory(BaseDAOFactory[ParsingTaskDAO]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(session_maker, ParsingTaskDAO)
