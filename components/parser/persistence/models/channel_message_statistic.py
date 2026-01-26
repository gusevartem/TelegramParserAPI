from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, override
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, event, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel_message import ChannelMessage


class ChannelMessageStatistic(BaseModel):
    __tablename__: str = "channel_message_statistic"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    message_id: Mapped[int] = mapped_column(
        ForeignKey("channel_message.id", ondelete="CASCADE"), index=True
    )
    views: Mapped[int] = mapped_column()

    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    message: Mapped[ChannelMessage] = relationship(back_populates="statistics")


class ChannelMessageStatisticDAO(BaseDAO[ChannelMessageStatistic, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ChannelMessageStatistic)

    @override
    async def create(
        self, message: ChannelMessage, views: int
    ) -> ChannelMessageStatistic:
        new_statistic = ChannelMessageStatistic(
            message=message,
            views=views,
        )
        await self.save(new_statistic)
        return new_statistic


@event.listens_for(ChannelMessageStatistic, "before_update")
def receive_before_update(_mapper: Any, _connection: Any, _target: Any) -> None:
    raise RuntimeError(
        "ChannelMessageStatistic records are immutable and cannot be updated."
    )
