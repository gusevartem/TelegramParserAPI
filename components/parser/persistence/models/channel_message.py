from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, override
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, ForeignKey, Text, UniqueConstraint, func, select
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseDAOFactory, BaseModel

if TYPE_CHECKING:
    from .channel import Channel
    from .channel_message_statistic import ChannelMessageStatistic
    from .media import Media


class ChannelMessage(BaseModel):
    __tablename__: str = "channel_message"

    __table_args__: tuple[Any, ...] = (
        UniqueConstraint(
            "channel_message_id", "channel_id", name="uq_channel_message_channel"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    channel_message_id: Mapped[int] = mapped_column(BigInteger, index=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    text: Mapped[str] = mapped_column(Text)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channel.id", ondelete="CASCADE"), index=True
    )

    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    channel: Mapped[Channel] = relationship(back_populates="messages")
    media_links: Mapped[list[MessageMediaLink]] = relationship(
        back_populates="message", cascade="all, delete-orphan", passive_deletes=True
    )
    media: AssociationProxy[list[Media]] = association_proxy(
        "media_links",
        "media_item",
        creator=lambda media_obj: MessageMediaLink(media_item=media_obj),
    )
    statistics: Mapped[list[ChannelMessageStatistic]] = relationship(
        back_populates="message", cascade="all, delete-orphan", passive_deletes=True
    )


class ChannelMessageDAO(BaseDAO[ChannelMessage, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ChannelMessage)

    @override
    async def create(
        self, channel: Channel, channel_message_id: int, created_at: datetime, text: str
    ) -> ChannelMessage:
        new_message = ChannelMessage(
            channel_message_id=channel_message_id,
            created_at=created_at,
            text=text,
            channel=channel,
        )
        await self.save(new_message)
        return new_message

    async def get_channel_messages(
        self,
        channel_id: int,
        sorting: Literal["newest", "oldest"],
        skip: int,
        limit: int | None,
    ) -> list[ChannelMessage]:
        stmt = (
            select(ChannelMessage)
            .where(ChannelMessage.channel_id == channel_id)
            .options(
                selectinload(ChannelMessage.statistics),
                selectinload(ChannelMessage.media_links).joinedload(
                    MessageMediaLink.media_item
                ),
            )
            .offset(skip)
        )

        if sorting == "newest":
            stmt = stmt.order_by(ChannelMessage.created_at.desc())
        else:
            stmt = stmt.order_by(ChannelMessage.created_at.asc())

        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def find_with_loaded_statistics_and_media(
        self, message_id: UUID
    ) -> ChannelMessage | None:
        stmt = (
            select(ChannelMessage)
            .options(
                selectinload(ChannelMessage.statistics),
                selectinload(ChannelMessage.media_links).joinedload(
                    MessageMediaLink.media_item
                ),
            )
            .where(ChannelMessage.id == message_id)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def find_by_channel_id_and_message_id(
        self, channel_id: int, message_id: int
    ) -> ChannelMessage | None:
        stmt = (
            select(ChannelMessage)
            .where(ChannelMessage.channel_id == channel_id)
            .where(ChannelMessage.channel_message_id == message_id)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()


class ChannelMessageDAOFactory(BaseDAOFactory[ChannelMessageDAO]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(session_maker, ChannelMessageDAO)


class MessageMediaLink(BaseModel):
    __tablename__: str = "message_media_link"

    media_id: Mapped[UUID] = mapped_column(
        ForeignKey("media.id", ondelete="CASCADE"), primary_key=True
    )
    message_id: Mapped[UUID] = mapped_column(
        ForeignKey("channel_message.id", ondelete="CASCADE"),
    )

    message: Mapped[ChannelMessage] = relationship(back_populates="media_links")
    media_item: Mapped[Media] = relationship()
