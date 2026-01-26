from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, override
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, event, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel import Channel


class ChannelStatistic(BaseModel):
    __tablename__: str = "channel_statistic"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channel.id", ondelete="CASCADE"), index=True
    )

    subscribers_count: Mapped[int] = mapped_column()
    views: Mapped[int] = mapped_column()
    posts_count: Mapped[int] = mapped_column()

    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    channel: Mapped[Channel] = relationship(back_populates="statistics")


class ChannelStatisticDAO(BaseDAO[ChannelStatistic, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ChannelStatistic)

    @override
    async def create(
        self, channel: Channel, subscribers_count: int, views: int, posts_count: int
    ) -> ChannelStatistic:
        new_statistic = ChannelStatistic(
            channel=channel,
            subscribers_count=subscribers_count,
            views=views,
            posts_count=posts_count,
        )
        await self.save(new_statistic)
        return new_statistic

    async def get_latest_by_channel_id(
        self, channel_id: int
    ) -> ChannelStatistic | None:
        stmt = (
            select(ChannelStatistic)
            .where(ChannelStatistic.channel_id == channel_id)
            .order_by(ChannelStatistic.recorded_at.desc())
            .limit(1)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def get_channel_statistics(
        self,
        channel_id: int,
        sorting: Literal["newest", "oldest"],
        skip: int,
        limit: int | None,
    ) -> list[ChannelStatistic]:
        stmt = (
            select(ChannelStatistic)
            .where(ChannelStatistic.channel_id == channel_id)
            .offset(skip)
        )

        if sorting == "newest":
            stmt = stmt.order_by(ChannelStatistic.recorded_at.desc())
        else:
            stmt = stmt.order_by(ChannelStatistic.recorded_at.asc())

        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())


@event.listens_for(ChannelStatistic, "before_update")
def receive_before_update(_mapper: Any, _connection: Any, _target: Any) -> None:
    raise RuntimeError("ChannelStatistic records are immutable and cannot be updated.")
