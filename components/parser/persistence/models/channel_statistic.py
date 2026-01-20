from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, override
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, event, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel import Channel


class ChannelStatistic(BaseModel):
    __tablename__: str = "channel_statistic"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey(Channel.id, ondelete="CASCADE"), index=True
    )

    subscribers_count: Mapped[int] = mapped_column()
    views: Mapped[int] = mapped_column()
    posts_count: Mapped[int] = mapped_column()

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())

    channel: Mapped[Channel] = relationship(back_populates="statistics")


class ChannelStatisticDAO(BaseDAO[ChannelStatistic, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ChannelStatistic)

    @override
    async def create(
        self, channel: Channel, subscribers_count: int, views: int, posts_count: int
    ) -> ChannelStatistic:
        new_statistic = ChannelStatistic(
            channel_id=channel.id,
            subscribers_count=subscribers_count,
            views=views,
            posts_count=posts_count,
        )
        await self.save(new_statistic)
        return new_statistic


@event.listens_for(ChannelStatistic, "before_update")
def receive_before_update(_mapper: Any, _connection: Any, _target: Any) -> None:
    raise RuntimeError("ChannelStatistic records are immutable and cannot be updated.")
