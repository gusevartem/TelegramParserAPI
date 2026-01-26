from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, override
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, String, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ._base import BaseDAO, BaseModel

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
    link: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    channel_id: Mapped[BigInteger | None] = mapped_column(
        ForeignKey("channel.id", ondelete="CASCADE"), index=True, default=None
    )
    status: Mapped[ParsingTaskStatus] = mapped_column(
        String(20), default=ParsingTaskStatus.IDLE
    )
    bucket: Mapped[int] = mapped_column()
    last_parsed_at: Mapped[datetime | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    channel: Mapped[Channel | None] = relationship(back_populates="tasks")


class ParsingTaskDAO(BaseDAO[ParsingTask, int]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ParsingTask)

    @override
    async def create(
        self,
        link: str,
        bucket: int,
    ) -> ParsingTask:
        new_task = ParsingTask(link=link, bucket=bucket)
        await self.save(new_task)
        return new_task
