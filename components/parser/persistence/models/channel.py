from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, override
from uuid import UUID

from sqlalchemy import BigInteger, ForeignKey, String, Text, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, joinedload, mapped_column, relationship

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel_message import ChannelMessage
    from .channel_statistic import ChannelStatistic
    from .media import Media


class Channel(BaseModel):
    __tablename__: str = "channel"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)

    link: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, default=None)
    logo_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("media.id", ondelete="SET NULL"), default=None
    )

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    statistics: Mapped[list[ChannelStatistic]] = relationship(
        back_populates="channel", cascade="all, delete-orphan", passive_deletes=True
    )
    messages: Mapped[list[ChannelMessage]] = relationship(
        back_populates="channel", cascade="all, delete-orphan", passive_deletes=True
    )
    logo: Mapped[Media | None] = relationship()


class ChannelDAO(BaseDAO[Channel, int]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Channel)

    @override
    async def create(
        self,
        id: int,
        link: str,
        name: str,
        description: str | None = None,
        logo: Media | None = None,
    ) -> Channel:
        new_channel = Channel(
            id=id, link=link, name=name, description=description, logo=logo
        )
        await self.save(new_channel)
        return new_channel

    async def find_by_id_with_loaded_logo(self, channel_id: int) -> Channel | None:
        stmt = (
            select(Channel)
            .options(joinedload(Channel.logo))
            .where(Channel.id == channel_id)
        )

        result = await self._session.execute(stmt)
        return result.unique().scalars().first()

    async def get_ids(self) -> list[int]:
        stmt = select(Channel.id)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())
