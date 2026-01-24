import time
from logging import Logger, getLogger
from types import TracebackType
from typing import Any, Protocol, Self, override

from parser.dto import ProxySettings, TelegramCredentials
from parser.persistence import ProxyType as PersistenceProxyType
from python_socks import ProxyType as SocksProxyType
from telethon import TelegramClient as TelethonTelegramClient
from telethon.hints import (
    DateLike,
    EntitiesLike,
    Entity,
    EntityLike,
    FileLike,
    MessageLike,
)
from telethon.requestiter import RequestIter
from telethon.sessions.abstract import Session
from telethon.tl import TLObject
from telethon.types import TypePhotoSize, User


class ITelegramClient(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    async def __call__(self, request: TLObject) -> Any | list[Any]: ...
    async def get_me(self) -> User: ...
    async def get_entity(self, entity: EntitiesLike) -> Entity | list[Entity]: ...
    def iter_messages(
        self,
        entity: EntityLike,
        limit: float | None = None,
        *,
        offset_date: DateLike = None,
        search: str | None = None,
    ) -> RequestIter: ...
    async def download_media(
        self,
        message: MessageLike,
        file: FileLike | None = None,
        *,
        thumb: TypePhotoSize | int | None = None,
    ) -> str | bytes | None: ...
    async def download_profile_photo(
        self,
        entity: EntityLike,
        file: FileLike | None = None,
        *,
        download_big: bool = True,
    ) -> str | None: ...


class TelegramClient(ITelegramClient):
    def __init__(
        self,
        credentials: TelegramCredentials,
        proxy: ProxySettings | None,
        session: Session,
    ) -> None:
        self.logger: Logger = getLogger(__name__)

        self.logger.info("Creating telegram client")
        telethon_kwargs: dict[str, Any] = {
            "session": session,
            "api_id": credentials.api_id,
            "api_hash": credentials.api_hash,
            "device_model": credentials.device_model,
            "system_version": credentials.system_version,
            "app_version": credentials.app_version,
            "lang_code": credentials.lang_code,
            "system_lang_code": credentials.system_lang_code,
        }

        if proxy is not None:
            self.logger.info("Proxy provided, using it")
            proxy_type_mapper = {
                PersistenceProxyType.HTTP: SocksProxyType.HTTP,
                PersistenceProxyType.SOCKS4: SocksProxyType.SOCKS4,
                PersistenceProxyType.SOCKS5: SocksProxyType.SOCKS5,
            }
            proxy_kwargs = {
                "proxy_type": proxy_type_mapper[proxy.proxy_type],
                "addr": proxy.host,
                "port": proxy.port,
            }
            if proxy.username is not None:
                proxy_kwargs["username"] = proxy.username
            if proxy.password is not None:
                proxy_kwargs["password"] = proxy.password
            telethon_kwargs["proxy"] = proxy_kwargs

        self._telethon_client: TelethonTelegramClient = TelethonTelegramClient(
            **telethon_kwargs
        )

    @override
    async def __aenter__(self) -> Self:
        self.logger.info("⌛ Starting telegram client")

        start_time = time.perf_counter()
        client = await self._telethon_client.start()  # pyright: ignore
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(f"✅ Telegram client started. Duration: {duration:.0f}ms")
        return client

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.logger.info("⌛ Disconnecting telegram client")

        start_time = time.perf_counter()
        await self._telethon_client.disconnect()  # pyright: ignore
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(f"✅ Telegram client disconnected. Duration: {duration:.0f}ms")

    @override
    async def __call__(self, request: TLObject) -> Any | list[Any]:
        self.logger.info(f"⌛ Sending tl request: {type(request).__name__}")

        start_time = time.perf_counter()
        result = await self._telethon_client(request)
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(
            f"✅ Sent tl request: {type(request).__name__}. "
            + f"Response class: {type(result).__name__}. Duration: {duration:.0f}ms"
        )
        return result

    @override
    async def get_me(self) -> User:
        self.logger.info("⌛ Getting me")

        start_time = time.perf_counter()
        user = await self._telethon_client.get_me()
        duration = (time.perf_counter() - start_time) * 1000

        if not isinstance(user, User):
            raise ValueError(
                f"Unexpected user type. Got {type(user).__name__}, expected User"
            )

        self.logger.info(
            f"✅ Got me. Response class: {type(user).__name__}. "
            + f"Duration: {duration:.0f}ms"
        )
        return user

    @override
    async def get_entity(self, entity: EntitiesLike) -> Entity | list[Entity]:
        self.logger.info(f"⌛ Getting entity: {entity} ({type(entity).__name__})")

        start_time = time.perf_counter()
        result = await self._telethon_client.get_entity(entity)
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(
            f"✅ Got entity: {entity} ({type(entity).__name__}). "
            + f"Response class: {type(result).__name__}. Duration: {duration:.0f}ms"
        )
        return result

    @override
    def iter_messages(
        self,
        entity: EntityLike,
        limit: float | None = None,
        *,
        offset_date: DateLike = None,
        search: str | None = None,
    ) -> RequestIter:
        self.logger.info(f"⌛ Iterating messages for entity: {type(entity).__name__}")
        return self._telethon_client.iter_messages(
            entity,
            limit,  # pyright: ignore[reportArgumentType]
            offset_date=offset_date,
            search=search,  # pyright: ignore[reportArgumentType]
        )

    @override
    async def download_media(
        self,
        message: MessageLike,
        file: FileLike | None = None,
        *,
        thumb: TypePhotoSize | int | None = None,
    ) -> str | bytes | None:
        self.logger.info(f"⌛ Downloading media for message: {type(message).__name__}")

        start_time = time.perf_counter()
        result = await self._telethon_client.download_media(
            message,
            file,  # pyright: ignore[reportArgumentType]
            thumb=thumb,  # pyright: ignore[reportArgumentType]
        )
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(
            f"✅ Downloaded media for message: {type(message).__name__}. "
            + f"Duration: {duration:.0f}ms"
        )
        return result

    @override
    async def download_profile_photo(
        self,
        entity: EntityLike,
        file: FileLike | None = None,
        *,
        download_big: bool = True,
    ) -> str | None:
        self.logger.info(
            f"⌛ Downloading profile photo for entity: {type(entity).__name__}"
        )

        start_time = time.perf_counter()
        result = await self._telethon_client.download_profile_photo(
            entity,
            file,  # pyright: ignore[reportArgumentType]
            download_big=download_big,
        )
        duration = (time.perf_counter() - start_time) * 1000

        self.logger.info(
            f"✅ Downloaded profile photo for entity: {type(entity).__name__}. "
            + f"Duration: {duration:.0f}ms"
        )
        return result
