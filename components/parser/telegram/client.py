import json
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import contextmanager
from functools import wraps as functools_wraps
from types import TracebackType
from typing import Any, ParamSpec, Protocol, Self, TypeVar, override

import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.dto import ProxySettings, TelegramCredentials
from parser.persistence import ProxyType as PersistenceProxyType
from python_socks import ProxyType as SocksProxyType
from telethon import TelegramClient as TelethonTelegramClient
from telethon import errors
from telethon.hints import (
    DateLike,
    EntitiesLike,
    Entity,
    EntityLike,
    FileLike,
    MessageLike,
)
from telethon.sessions.abstract import Session
from telethon.tl import TLObject
from telethon.types import Message, TypePhotoSize, User

from .exceptions import ClientBanned, FloodWait
from .settings import TelegramSettings

P = ParamSpec("P")
T = TypeVar("T")


@contextmanager
def _telethon_exception_handler():
    try:
        yield
    except errors.FloodWaitError as e:
        raise FloodWait(seconds=e.seconds, message=str(e)) from e
    except (
        errors.UserDeactivatedError,
        errors.UserBannedInChannelError,
        errors.AuthKeyDuplicatedError,
        errors.SessionRevokedError,
        errors.AuthKeyUnregisteredError,
        errors.UserDeactivatedBanError,
        errors.PeerFloodError,
        errors.UserRestrictedError,
        errors.FrozenMethodInvalidError,
    ) as e:
        raise ClientBanned(f"Client banned or invalid: {str(e)}") from e
    except (ConnectionError, ConnectionResetError, OSError) as e:
        raise FloodWait(seconds=5, message=str(e)) from e


def handle_telethon_errors(
    func: Callable[P, Coroutine[Any, Any, T]],
) -> Callable[P, Any]:
    @functools_wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        with _telethon_exception_handler():
            return await func(*args, **kwargs)

    return wrapper  # type: ignore


def handle_telethon_errors_generator(
    func: Callable[P, AsyncIterator[T]],
) -> Callable[P, AsyncIterator[T]]:
    @functools_wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> AsyncIterator[T]:
        with _telethon_exception_handler():
            async for item in func(*args, **kwargs):
                yield item

    return wrapper


class ITelegramClient(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    @property
    def current_session(self) -> Session: ...

    async def __call__(self, request: TLObject) -> Any | list[Any]: ...
    async def get_me(self) -> User: ...
    async def get_entity(self, entity: EntitiesLike) -> Entity | list[Entity]: ...
    def iter_messages(
        self,
        entity: EntityLike,
        limit: int | None = None,
        *,
        offset_date: DateLike = None,
        search: str | None = None,
    ) -> AsyncIterator[Message]: ...
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


class ITelegramClientFactory(Protocol):
    def __call__(
        self,
        session: Session,
        credentials: TelegramCredentials,
        settings: TelegramSettings,
        proxy: ProxySettings | None,
        requests_timeout: int = 10,
    ) -> ITelegramClient: ...


class TelegramClient(ITelegramClient):
    def __init__(
        self,
        session: Session,
        credentials: TelegramCredentials,
        settings: TelegramSettings,
        proxy: ProxySettings | None,
        requests_timeout: int = 10,
    ) -> None:
        self.logger: structlog.BoundLogger = structlog.get_logger("telegram_client")
        self.tracer: trace.Tracer = trace.get_tracer("telegram_client")
        self.settings: TelegramSettings = settings

        self.logger.info(
            "creating_telegram_client",
            proxy_provided="Yes" if proxy is not None else "No",
            device_model=credentials.device_model,
            system_version=credentials.system_version,
            app_version=credentials.app_version,
            lang_code=credentials.lang_code,
            system_lang_code=credentials.system_lang_code,
            requests_timeout=requests_timeout,
        )

        telethon_kwargs: dict[str, Any] = {
            "session": session,
            "api_id": credentials.api_id,
            "api_hash": credentials.api_hash,
            "device_model": credentials.device_model,
            "system_version": credentials.system_version,
            "app_version": credentials.app_version,
            "lang_code": credentials.lang_code,
            "system_lang_code": credentials.system_lang_code,
            "timeout": requests_timeout,
        }

        if proxy is not None:
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

    @property
    @override
    def current_session(self) -> Session:
        if not isinstance(self._telethon_client.session, Session):  # pyright: ignore
            raise RuntimeError("Unexpected session type")
        return self._telethon_client.session

    @override
    @handle_telethon_errors
    async def __aenter__(self) -> Self:
        with self.tracer.start_as_current_span("telegram.connect") as span:
            self.logger.info("connecting_telegram_client", stage="start")

            await self._telethon_client.connect()

            if not await self._telethon_client.is_user_authorized():
                self.logger.error("client_not_authorized")
                span.set_status(Status(StatusCode.ERROR, "Client not authorized"))
                raise ClientBanned(
                    "Client is not authorized (session invalid or revoked)"
                )

            self.logger.info("telegram_client_connected", stage="complete")
            me = await self.get_me()
            span.set_attribute("telegram.user_id", me.id)

            return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        with self.tracer.start_as_current_span("telegram.disconnect"):
            self.logger.info("disconnecting_telegram_client", stage="start")

            await self._telethon_client.disconnect()  # pyright: ignore

            self.logger.info("telegram_client_disconnected", stage="complete")

    @override
    @handle_telethon_errors
    async def __call__(self, request: TLObject) -> Any | list[Any]:
        with self.tracer.start_as_current_span("telegram.raw_request") as span:
            span.set_attribute("telegram.request_type", type(request).__name__)
            self.logger.info("sending_raw_request", request_type=type(request).__name__)

            result = await self._telethon_client(request)

            if self.settings.save_telegram_responses and hasattr(result, "to_dict"):
                try:
                    response_dict = result.to_dict()
                    self.logger.info(
                        "telegram_raw_response",
                        response=json.dumps(
                            response_dict, ensure_ascii=False, default=str
                        ),
                    )
                except Exception:
                    self.logger.info(
                        "telegram_raw_response_serialization_failed", exc_info=True
                    )

            self.logger.info(
                "raw_request_completed",
                response_type=type(result).__name__,
                stage="complete",
            )
            return result

    @override
    @handle_telethon_errors
    async def get_me(self) -> User:
        with self.tracer.start_as_current_span("telegram.get_me"):
            self.logger.info("getting_me", stage="start")

            user = await self._telethon_client.get_me()

            if not isinstance(user, User):
                self.logger.error("unexpected_user_type", user_type=type(user).__name__)
                raise ValueError(
                    f"Unexpected user type. Got {type(user).__name__}, expected User"
                )
            if user.restricted:
                self.logger.warning(
                    "user_is_restricted",
                    restriction_reason=str(user.restriction_reason),
                )
                raise ClientBanned(
                    f"User is restricted/frozen. Reasons: {user.restriction_reason}"
                )

            if self.settings.save_telegram_responses:
                try:
                    self.logger.info(
                        "telegram_response_get_me",
                        response=json.dumps(
                            user.to_dict(), ensure_ascii=False, default=str
                        ),
                    )
                except Exception:
                    self.logger.info("response_serialization_failed", exc_info=True)

            self.logger.info(
                "got_me",
                stage="success",
                user_id=user.id,
                username=user.username or "none",
                user_first_name=user.first_name or "none",
                user_last_name=user.last_name or "none",
            )
            return user

    @override
    @handle_telethon_errors
    async def get_entity(self, entity: EntitiesLike) -> Entity | list[Entity]:
        with self.tracer.start_as_current_span("telegram.get_entity") as span:
            span.set_attribute("telegram.entity", str(entity))

            self.logger.info("getting_entity", entity=str(entity), stage="start")

            result = await self._telethon_client.get_entity(entity)

            if self.settings.save_telegram_responses and hasattr(result, "to_dict"):
                try:
                    response_dict = (
                        result.to_dict()
                        if not isinstance(result, list)
                        else [r.to_dict() for r in result]
                    )
                    self.logger.info(
                        "telegram_response_get_entity",
                        entity=str(entity),
                        response=json.dumps(
                            response_dict, ensure_ascii=False, default=str
                        ),
                    )
                except Exception:
                    self.logger.info("response_serialization_failed", exc_info=True)

            self.logger.info("got_entity", entity=str(entity), stage="complete")
            return result

    @override
    @handle_telethon_errors_generator
    async def iter_messages(
        self,
        entity: EntityLike,
        limit: int | None = None,
        *,
        offset_date: DateLike = None,
        search: str | None = None,
    ) -> AsyncIterator[Message]:
        with self.tracer.start_as_current_span("telegram.iter_messages") as span:
            span.set_attribute("telegram.entity", str(entity))
            span.set_attribute("telegram.messages_limit", limit or "none")
            span.set_attribute("telegram.offset_date", str(offset_date or "none"))
            span.set_attribute("telegram.search_query", search or "none")

            self.logger.info(
                "iterating_messages",
                entity=str(entity),
                limit=limit or "none",
                offset_date=str(offset_date) or "none",
                search=search or "none",
                stage="start",
            )
            message_count = 0

            async for message in self._telethon_client.iter_messages(
                entity,
                limit,  # pyright: ignore[reportArgumentType] Ошибка в telethon, очевидно, что limit - это int, а не float
                offset_date=offset_date,
                search=search,  # pyright: ignore[reportArgumentType]
            ):
                yield message
                message_count += 1

            span.set_attribute("telegram.messages_yielded", message_count)
            self.logger.info(
                "iter_messages_completed",
                entity=str(entity),
                yielded_count=message_count,
                stage="complete",
            )

    @override
    @handle_telethon_errors
    async def download_media(
        self,
        message: MessageLike,
        file: FileLike | None = None,
        *,
        thumb: TypePhotoSize | int | None = None,
    ) -> str | bytes | None:
        with self.tracer.start_as_current_span("telegram.download_media") as span:
            if hasattr(message, "id"):
                span.set_attribute("telegram.message_id", getattr(message, "id"))
            self.logger.info(
                "downloading_media",
                message_id=getattr(message, "id", None),
                stage="start",
            )

            result = await self._telethon_client.download_media(
                message,
                file,  # pyright: ignore[reportArgumentType]
                thumb=thumb,  # pyright: ignore[reportArgumentType]
            )

            result_size = (
                len(result) if isinstance(result, (bytes, bytearray)) else "path"
            )
            self.logger.info(
                "media_downloaded", result_size=result_size, stage="complete"
            )
            return result

    @override
    @handle_telethon_errors
    async def download_profile_photo(
        self,
        entity: EntityLike,
        file: FileLike | None = None,
        *,
        download_big: bool = True,
    ) -> str | None:
        with self.tracer.start_as_current_span(
            "telegram.download_profile_photo"
        ) as span:
            span.set_attribute("telegram.entity", str(entity))
            span.set_attribute("telegram.download_big", download_big)
            self.logger.info(
                "downloading_profile_photo", entity=str(entity), stage="start"
            )

            result = await self._telethon_client.download_profile_photo(
                entity,
                file,  # pyright: ignore[reportArgumentType]
                download_big=download_big,
            )

            self.logger.info(
                "profile_photo_downloaded",
                stage="success",
            )

            return result
