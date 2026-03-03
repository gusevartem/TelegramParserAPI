from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, override
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, String, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseDAOFactory, BaseModel

if TYPE_CHECKING:
    from .parsing_task import ParsingTask


class TaskClaimHistory(BaseModel):
    __tablename__: str = "task_claim_history"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("parsing_task.id", ondelete="CASCADE"),
        index=True,
    )
    worker_id: Mapped[str] = mapped_column(String(255))
    claimed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    task: Mapped[ParsingTask] = relationship()


class TaskClaimHistoryDAO(BaseDAO[TaskClaimHistory, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, TaskClaimHistory)

    @override
    async def create(
        self,
        task_id: UUID,
        worker_id: str,
    ) -> TaskClaimHistory:
        new_claim = TaskClaimHistory(
            task_id=task_id,
            worker_id=worker_id,
        )
        await self.save(new_claim)
        return new_claim

    async def is_claimed_this_hour(self, task_id: UUID) -> bool:
        """Проверяет, забирал ли кто-то задачу в текущий час."""
        now = datetime.now(timezone.utc)
        this_hour_start = now.replace(minute=0, second=0, microsecond=0)

        stmt = (
            select(TaskClaimHistory.id)
            .where(TaskClaimHistory.task_id == task_id)
            .where(TaskClaimHistory.claimed_at >= this_hour_start)
            .limit(1)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first() is not None


class TaskClaimHistoryDAOFactory(BaseDAOFactory[TaskClaimHistoryDAO]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(session_maker, TaskClaimHistoryDAO)
