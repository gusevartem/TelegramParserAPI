from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, override
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, ForeignKey, String, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseDAOFactory, BaseModel

if TYPE_CHECKING:
    from .telegram_client import TelegramClient


class WorkerAccountUsage(BaseModel):
    __tablename__: str = "worker_account_usage"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    telegram_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("telegram_client.telegram_id", ondelete="CASCADE"),
    )
    worker_id: Mapped[str] = mapped_column(String(255))
    locked_until: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    reason: Mapped[str] = mapped_column(String(255), default="usage")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    telegram_client: Mapped[TelegramClient] = relationship()


class WorkerAccountUsageDAO(BaseDAO[WorkerAccountUsage, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, WorkerAccountUsage)

    @override
    async def create(
        self,
        telegram_id: int,
        worker_id: str,
        locked_until: datetime,
        reason: str = "usage",
    ) -> WorkerAccountUsage:
        new_usage = WorkerAccountUsage(
            telegram_id=telegram_id,
            worker_id=worker_id,
            locked_until=locked_until,
            reason=reason,
        )
        await self.save(new_usage)
        return new_usage


class WorkerAccountUsageDAOFactory(BaseDAOFactory[WorkerAccountUsageDAO]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(session_maker, WorkerAccountUsageDAO)
