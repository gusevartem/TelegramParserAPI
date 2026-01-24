from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import override
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, ForeignKey, String, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ._base import BaseDAO, BaseModel


class TelegramClient(BaseModel):
    __tablename__: str = "telegram_client"

    telegram_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    phone: Mapped[str] = mapped_column(String(255))
    banned: Mapped[bool] = mapped_column(default=False)

    api_id: Mapped[int] = mapped_column(BigInteger)
    api_hash: Mapped[str] = mapped_column(String(255))
    device_model: Mapped[str] = mapped_column(String(255))
    system_version: Mapped[str] = mapped_column(String(50))
    app_version: Mapped[str] = mapped_column(String(50))
    lang_code: Mapped[str] = mapped_column(String(10))
    system_lang_code: Mapped[str] = mapped_column(String(10))

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
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
        lang_pack: str,
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
            lang_pack=lang_pack,
            proxy=proxy,
        )
        await self.save(new_client)
        return new_client


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

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    telegram_client: Mapped[TelegramClient] = relationship(back_populates="proxy")
