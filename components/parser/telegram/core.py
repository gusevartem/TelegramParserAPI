from contextlib import AbstractAsyncContextManager
from typing import Protocol

from parser.dto import ProxySettings, TelegramCredentials

from .client import ITelegramClient


class ITelegram(Protocol):
    async def add_client(
        self,
        session_data: bytes,
        credentials: TelegramCredentials | None,
        proxy: ProxySettings | None,
    ) -> None: ...

    def get_client(self) -> AbstractAsyncContextManager[ITelegramClient]: ...
