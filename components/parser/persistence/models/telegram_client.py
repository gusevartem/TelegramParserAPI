from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import override
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, ForeignKey, String, Text, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, joinedload, mapped_column, relationship
from sqlalchemy.types import TIMESTAMP

from ._base import BaseDAO, BaseDAOFactory, BaseModel


class TelegramClient(BaseModel):
    __tablename__: str = "telegram_client"

    telegram_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    phone: Mapped[str] = mapped_column(String(255))
    banned: Mapped[bool] = mapped_column(default=False)

    api_id: Mapped[int] = mapped_column(BigInteger)
    api_hash: Mapped[str] = mapped_column(String(255))
    session_string: Mapped[str | None] = mapped_column(Text, default=None)
    device_model: Mapped[str] = mapped_column(String(255))
    system_version: Mapped[str] = mapped_column(String(50))
    app_version: Mapped[str] = mapped_column(String(50))
    lang_code: Mapped[str] = mapped_column(String(10))
    system_lang_code: Mapped[str] = mapped_column(String(10))

    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    proxy: Mapped[TelegramClientProxy | None] = relationship(
        back_populates="telegram_client"
    )


class TelegramClientDAO(BaseDAO[TelegramClient, int]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, TelegramClient)

    @override
    async def create(
        self,
        telegram_id: int,
        phone: str,
        api_id: int,
        api_hash: str,
        device_model: str,
        system_version: str,
        app_version: str,
        lang_code: str,
        system_lang_code: str,
        session_string: str | None = None,
        proxy: TelegramClientProxy | None = None,
    ) -> TelegramClient:
        new_client = TelegramClient(
            telegram_id=telegram_id,
            phone=phone,
            api_id=api_id,
            api_hash=api_hash,
            device_model=device_model,
            system_version=system_version,
            app_version=app_version,
            lang_code=lang_code,
            system_lang_code=system_lang_code,
            session_string=session_string,
            proxy=proxy,
        )
        await self.save(new_client)
        return new_client

    async def is_working_clients_exists(self) -> bool:
        stmt = (
            select(TelegramClient)
            .where(TelegramClient.banned == False)  # noqa: E712
            .where(TelegramClient.session_string.is_not(None))
        )

        result = await self._session.execute(stmt)
        return result.scalars().first() is not None

    async def find_with_proxy(self, telegram_id: int) -> TelegramClient | None:
        stmt = (
            select(TelegramClient)
            .where(TelegramClient.telegram_id == telegram_id)
            .options(joinedload(TelegramClient.proxy))
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def find_available_account(self) -> TelegramClient | None:
        from .worker_account_usage import WorkerAccountUsage

        now = datetime.now(timezone.utc)

        latest_locked_until = (
            select(WorkerAccountUsage.locked_until)
            .where(WorkerAccountUsage.telegram_id == TelegramClient.telegram_id)
            .order_by(WorkerAccountUsage.created_at.desc())
            .limit(1)
            .scalar_subquery()
        )

        stmt = (
            select(TelegramClient)
            .where(TelegramClient.banned == False)  # noqa: E712
            .where(TelegramClient.session_string.is_not(None))
            .where((latest_locked_until.is_(None)) | (latest_locked_until <= now))
            .limit(1)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()


class TelegramClientDAOFactory(BaseDAOFactory[TelegramClientDAO]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(session_maker, TelegramClientDAO)


class ProxyType(StrEnum):
    HTTP = "http"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"


class TelegramClientProxy(BaseModel):
    __tablename__: str = "telegram_client_proxy"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    telegram_client_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("telegram_client.telegram_id", ondelete="CASCADE")
    )
    proxy_type: Mapped[ProxyType] = mapped_column(String(10))
    host: Mapped[str] = mapped_column(String(255))
    port: Mapped[int] = mapped_column()
    username: Mapped[str | None] = mapped_column(String(255), default=None)
    password: Mapped[str | None] = mapped_column(String(255), default=None)

    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    telegram_client: Mapped[TelegramClient] = relationship(back_populates="proxy")
