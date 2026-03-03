from collections.abc import AsyncIterator, Callable
from types import TracebackType
from typing import Any, Self, override

from parser.dto import ProxySettings, TelegramCredentials
from parser.telegram.client import ITelegramClient, ITelegramClientFactory
from parser.telegram.settings import TelegramSettings
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
from telethon.types import Message, PeerChannel, TypePhotoSize, User


class MockTelegramClient(ITelegramClient):
    """A mock implementation of ITelegramClient for testing."""

    def __init__(
        self,
        session: Session,
        credentials: TelegramCredentials,
        settings: TelegramSettings,
        proxy: ProxySettings | None,
        requests_timeout: int = 10,
    ) -> None:
        self._session: Session = session
        self.credentials: TelegramCredentials = credentials
        self.settings: TelegramSettings = settings
        self.proxy: ProxySettings | None = proxy
        self.requests_timeout: int = requests_timeout

        # Mock behavior controls
        self.connected: bool = False
        self.mock_user: User = User(
            id=123456789,
            is_self=True,
            contact=False,
            mutual_contact=False,
            deleted=False,
            bot=False,
            bot_chat_history=False,
            bot_nochats=False,
            verified=False,
            restricted=False,
            min=False,
            bot_inline_geo=False,
            support=False,
            scam=False,
            apply_min_photo=False,
            fake=False,
            bot_attach_menu=False,
            premium=False,
            attach_menu_enabled=False,
            bot_can_edit=False,
            close_friend=False,
            stories_hidden=False,
            stories_unavailable=False,
            contact_require_premium=False,
            bot_business=False,
            access_hash=87654321,
            first_name="Mock",
            last_name="User",
            username="mock_user",
            phone="+1234567890",
            photo=None,
            status=None,
            bot_info_version=None,
            restriction_reason=None,
            bot_inline_placeholder=None,
            lang_code="en",
            emoji_status=None,
            usernames=None,
            stories_max_id=None,
            color=None,
            profile_color=None,
        )

        self.on_connect: Callable[[], None] | None = None
        self.on_get_me: Callable[[], User] | None = None
        self.on_call: Callable[[TLObject], Any] | None = None
        self.on_get_entity: Callable[[EntitiesLike], Entity | list[Entity]] | None = (
            None
        )
        self.on_iter_messages: (
            Callable[
                [EntityLike, int | None, DateLike, str | None], AsyncIterator[Message]
            ]
            | None
        ) = None
        self.on_download_media: Callable[[MessageLike], str | bytes | None] | None = (
            None
        )
        self.on_download_profile_photo: Callable[[EntityLike], str | None] | None = None

    @property
    @override
    def current_session(self) -> Session:
        return self._session

    @override
    async def __aenter__(self) -> Self:
        if self.on_connect is not None:
            self.on_connect()
        self.connected = True
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.connected = False

    @override
    async def __call__(self, request: TLObject) -> Any | list[Any]:
        if self.on_call is not None:
            return self.on_call(request)
        return None

    @override
    async def get_me(self) -> User:
        if self.on_get_me is not None:
            return self.on_get_me()
        return self.mock_user

    @override
    async def get_entity(self, entity: EntitiesLike) -> Entity | list[Entity]:
        if self.on_get_entity is not None:
            return self.on_get_entity(entity)
        return User(id=1, first_name="Mock Entity")

    @override
    async def iter_messages(
        self,
        entity: EntityLike,
        limit: int | None = None,
        *,
        offset_date: DateLike = None,
        search: str | None = None,
    ) -> AsyncIterator[Message]:
        if self.on_iter_messages is not None:
            async for message in self.on_iter_messages(
                entity, limit, offset_date, search
            ):
                yield message
        else:
            for _ in range(0):
                yield Message(
                    id=1, peer_id=PeerChannel(channel_id=1), date=None, message=""
                )

    @override
    async def download_media(
        self,
        message: MessageLike,
        file: FileLike | None = None,
        *,
        thumb: TypePhotoSize | int | None = None,
    ) -> str | bytes | None:
        if self.on_download_media is not None:
            return self.on_download_media(message)
        return b"mock_media_data"

    @override
    async def download_profile_photo(
        self,
        entity: EntityLike,
        file: FileLike | None = None,
        *,
        download_big: bool = True,
    ) -> str | None:
        if self.on_download_profile_photo is not None:
            return self.on_download_profile_photo(entity)
        return "mock_photo_path"


class MockTelegramClientFactory(ITelegramClientFactory):
    def __init__(self):
        self.created_clients: list[MockTelegramClient] = []
        self.next_client_configurator: Callable[[MockTelegramClient], None] | None = (
            None
        )

    @override
    def __call__(
        self,
        session: Session,
        credentials: TelegramCredentials,
        settings: TelegramSettings,
        proxy: ProxySettings | None,
        requests_timeout: int = 10,
    ) -> ITelegramClient:
        client = MockTelegramClient(
            session=session,
            credentials=credentials,
            settings=settings,
            proxy=proxy,
            requests_timeout=requests_timeout,
        )
        if self.next_client_configurator:
            self.next_client_configurator(client)
            self.next_client_configurator = None

        self.created_clients.append(client)
        return client
