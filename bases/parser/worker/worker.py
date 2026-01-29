import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from uuid import uuid4

import filetype
from parser.dto import ParsingTask
from parser.message_broker import IMessageBroker, InvalidMessageError, InvalidTask
from parser.persistence import (
    ChannelDAO,
    ChannelMessageDAO,
    ChannelMessageStatisticDAO,
    ChannelStatisticDAO,
    Media,
    MediaDAO,
    MultipleDAOFactory,
    ParsingTaskDAO,
    ParsingTaskStatus,
)
from parser.storage import IStorage
from parser.telegram import (
    FloodWait,
    InvalidClient,
    ITelegram,
    ITelegramClient,
    NoWorkingClientsFoundError,
    TelegramException,
)
from pydantic import HttpUrl, TypeAdapter
from telethon import errors, functions, types
from telethon.tl.types import Channel

from .exceptions import TaskError, TaskExists, TemporaryCannotProcessTask


@dataclass
class _ParsedMessage:
    id: int
    views: int
    text: str
    created_at: datetime
    original_messages: list[types.Message]


class Worker:
    def __init__(
        self,
        telegram: ITelegram,
        message_broker: IMessageBroker,
        storage: IStorage,
        dao_factory: MultipleDAOFactory,
    ):
        self._telegram: ITelegram = telegram
        self._message_broker: IMessageBroker = message_broker
        self._storage: IStorage = storage
        self._dao_factory: MultipleDAOFactory = dao_factory
        self.logger: Logger = getLogger(__name__)

    async def start(self, tasks_timeout_sleep: int = 5) -> None:
        try:
            async with self._telegram.get_client() as client:
                while True:
                    try:
                        async with self._message_broker.get_task() as task:
                            async with self._dao_factory() as dao_factory:
                                parsing_task_dao = dao_factory(ParsingTaskDAO)
                                persistence_task = await parsing_task_dao.find_by_id(
                                    task.id
                                )
                                if persistence_task is None:
                                    self.logger.warning(
                                        "⚠️ Task from message broker "
                                        + f"with id {task.id} not found in database"
                                    )
                                    raise InvalidTask("Task not found in database")

                                if persistence_task.status in (
                                    ParsingTaskStatus.ERROR,
                                    ParsingTaskStatus.EXISTS,
                                ):
                                    self.logger.warning(
                                        "⚠️ Task from message broker "
                                        + f"with id {task.id} "
                                        + f"is in {persistence_task.status} status"
                                    )
                                    raise InvalidTask(
                                        "Expect status "
                                        + f"{ParsingTaskStatus.PROCESSING}, "
                                        + f"got {persistence_task.status}"
                                    )

                                if persistence_task.status == ParsingTaskStatus.IDLE:
                                    self.logger.warning(
                                        "⚠️ Task is idle, but expect processing. "
                                        + "Changing status to processing."
                                    )
                                    persistence_task.status = (
                                        ParsingTaskStatus.PROCESSING
                                    )
                                    await parsing_task_dao.save(persistence_task)
                                    await parsing_task_dao.commit()

                            self.logger.info(f"⌛ Processing task with id {task.id}")

                            async with self._handle_task_processing_errors(task):
                                await self._process_task(client, task)
                                self.logger.info(
                                    f"✅ Task with id {task.id} "
                                    + "processed successfully"
                                )
                            async with self._dao_factory() as dao_factory:
                                parsing_task_dao = dao_factory(ParsingTaskDAO)
                                persistence_task = await parsing_task_dao.find_by_id(
                                    task.id
                                )
                                if (
                                    persistence_task is not None
                                    and persistence_task.status
                                    == ParsingTaskStatus.PROCESSING
                                ):
                                    persistence_task.status = ParsingTaskStatus.IDLE
                                    persistence_task.last_parsed_at = datetime.now(
                                        timezone.utc
                                    )
                                    await parsing_task_dao.save(persistence_task)
                                    await parsing_task_dao.commit()

                    except InvalidMessageError:
                        self.logger.info("⚠️ Got invalid message, ignoring")
                        continue
                    except InvalidTask:
                        self.logger.info("⚠️ Got invalid task, ignoring")
                        continue
                    except TimeoutError:
                        self.logger.info(
                            "⚠️ Got timeout error while getting task, "
                            + f"sleeping for {tasks_timeout_sleep}s"
                        )
                        await asyncio.sleep(tasks_timeout_sleep)
                        continue
                    except TelegramException:
                        raise
                    except Exception as e:
                        self.logger.fatal(
                            "❗❗❗ UNKNOWN EXCEPTION OCCURRED ❗❗❗"
                            + "\nCAUSE OF CURRENT SETTINGS "
                            + "THIS EXCEPTION WILL BE IGNORED."
                            + f"\nName: {type(e).__name__}. Exception: {e}",
                            exc_info=True,
                        )
                        continue

        except (
            NoWorkingClientsFoundError,
            InvalidClient,
            TimeoutError,
            FloodWait,
        ) as e:
            self.logger.info(
                f"⚠️ Client exception occurred. Name: {type(e).__name__}. "
                + f"Exception: {e}"
            )

    @asynccontextmanager
    async def _handle_task_processing_errors(self, task: ParsingTask):
        async def change_task_status(
            new_status: ParsingTaskStatus,
            exception: Exception,
            channel_id: int | None = None,
        ):
            async with self._dao_factory() as dao_factory:
                parsing_task_dao = dao_factory(ParsingTaskDAO)
                persistence_task = await parsing_task_dao.find_by_id(task.id)
                if persistence_task is None:
                    self.logger.error(f"⚠️ Task {task.id} was deleted while processing")
                    raise InvalidTask(
                        "Task was deleted while processing"
                    ) from exception

                persistence_task.status = new_status
                persistence_task.channel_id = channel_id
                persistence_task.last_parsed_at = datetime.now(timezone.utc)

                await parsing_task_dao.save(persistence_task)
                await parsing_task_dao.commit()

        try:
            yield
        except TaskExists as e:
            await change_task_status(ParsingTaskStatus.EXISTS, e, e.channel_id)
            self.logger.warning(f"⚠️ Channel {task.url} already exists")
            raise InvalidTask("Task already exists") from e

        except TaskError as e:
            await change_task_status(ParsingTaskStatus.ERROR, e)
            self.logger.warning(f"⚠️ Task {task.id} is invalid and was marked as error")
            raise InvalidTask("Task error") from e

        except TemporaryCannotProcessTask as e:
            await change_task_status(ParsingTaskStatus.IDLE, e)
            self.logger.warning(
                f"⚠️ Task {task.id} was marked as idle, because "
                + "temporary cannot process it"
            )

    async def _process_task(self, client: ITelegramClient, task: ParsingTask) -> None:
        channel_entity = await self._get_channel(client, task.url)

        async with self._dao_factory() as dao_factory:
            parsing_task_dao = dao_factory(ParsingTaskDAO)
            tasks_with_same_channel_id = await parsing_task_dao.find_by_channel_id(
                channel_entity.id
            )

            for task_with_same_channel in tasks_with_same_channel_id:
                if (
                    task_with_same_channel.channel_id is not None
                    and task_with_same_channel.channel_id == channel_entity.id
                    and task_with_same_channel.id != task.id
                    and task_with_same_channel.status
                    in (
                        ParsingTaskStatus.PROCESSING,
                        ParsingTaskStatus.IDLE,
                    )
                ):
                    raise TaskExists(
                        f"Task {task_with_same_channel.id} and {task.id} "
                        + "are for the same channel",
                        task_with_same_channel.channel_id,
                    )

        full_channel = await client(
            functions.channels.GetFullChannelRequest(channel_entity)  # pyright: ignore[reportArgumentType]
        )
        if not isinstance(full_channel, types.messages.ChatFull):
            self.logger.error(
                "⚠️ Cannot get channel full info. "
                + f"Expected ChannelFull, got: {type(full_channel).__name__}"
            )
            raise TaskError("Cannot get channel full info")

        collected_messages = await self._collect_messages(client, channel_entity)

        async with self._dao_factory() as dao_factory:
            self.logger.info("⌛ Saving channel to database")
            channel_dao = dao_factory(ChannelDAO)
            media_dao = dao_factory(MediaDAO)
            channel_statistic_dao = dao_factory(ChannelStatisticDAO)

            persistence_channel = await channel_dao.find_by_id(channel_entity.id)
            if persistence_channel is None:
                self.logger.info(
                    "⌛ Channel entity does not exist in database. Creating new one"
                )
                self.logger.info("⌛ Downloading channel logo")
                logo = await client.download_profile_photo(channel_entity, file=bytes)  # pyright: ignore[reportArgumentType]
                logo_media: Media | None = None
                if logo is None or not isinstance(logo, bytes):
                    self.logger.info("⚠️ Cannot get channel logo")
                else:
                    self.logger.info("✅ Channel logo downloaded")
                    kind = filetype.guess(logo)
                    if kind is None:
                        media_extension = "jpg"
                        media_mime = "image/jpeg"
                    else:
                        media_extension = kind.extension
                        media_mime = kind.mime
                    media_id = uuid4()
                    file_name = f"{media_id}.{media_extension}"
                    logo_media = await media_dao.create(
                        id=media_id,
                        mime_type=media_mime,
                        size_bytes=len(logo),
                        file_name=file_name,
                    )
                    await self._storage.save_media(logo, file_name)

                persistence_channel = await channel_dao.create(
                    id=channel_entity.id,
                    name=channel_entity.title,
                    description=full_channel.full_chat.about,
                    logo=logo_media,
                )
                self.logger.info("✅ Channel saved to database")

            self.logger.info("⌛ Saving channel statistic to database")

            now_utc = datetime.now(timezone.utc)
            time_limit_24h = now_utc - timedelta(hours=72)

            messages_24h = [
                message
                for message in collected_messages
                if message.created_at > time_limit_24h
            ]

            await channel_statistic_dao.create(
                channel=persistence_channel,
                subscribers_count=full_channel.full_chat.participants_count or 0,  # pyright: ignore[reportAttributeAccessIssue]
                views=sum(message.views for message in messages_24h),
                posts_count=len(messages_24h),
            )

            await dao_factory.commit()
        self.logger.info("✅ Channel info saved to database successfully")

        await self._process_messages(client, collected_messages, channel_entity.id)

    async def _get_channel(self, client: ITelegramClient, url: str) -> Channel:
        self.logger.info("⌛ Getting channel entity")

        try:
            http_url = TypeAdapter(HttpUrl).validate_python(url)
            if not http_url.path or len(http_url.path) < 2:
                raise ValueError("Empty path")
        except Exception as e:
            self.logger.error(f"⚠️ Invalid URL format: {url}")
            raise TaskError(f"Invalid URL format: {url}") from e

        is_join_link = http_url.path.startswith(
            "/joinchat"
        ) or http_url.path.startswith("/+")

        if is_join_link:
            self.logger.info("⌛ Getting channel entity by invite link")

            hash_part = http_url.path.split("/")[-1].replace("+", "").strip()
            if not hash_part:
                raise TaskError("Empty hash in invite link")
            try:
                self.logger.info("⌛ Trying to get channel entity without join")
                check_res = await client(
                    functions.messages.CheckChatInviteRequest(hash_part)
                )

                if isinstance(check_res, types.ChatInviteAlready):
                    if not isinstance(check_res.chat, types.Channel):
                        self.logger.warning(
                            "⚠️ Unexpected chat type. "
                            + f"Got {type(check_res.chat).__name__}, expected Channel"
                        )
                        raise TaskError(
                            f"Unexpected chat type: {type(check_res.chat).__name__}"
                        )

                    self.logger.info("✅ Got channel without join")
                    return check_res.chat

                elif isinstance(check_res, types.ChatInvite):
                    if not check_res.channel and not check_res.broadcast:
                        raise TaskError("Link points to a Group/Chat, not a Channel")

                    self.logger.info("⌛ Trying to join to channel")
                    updates = await client(
                        functions.messages.ImportChatInviteRequest(hash_part)
                    )

                    if not isinstance(updates, types.Updates):
                        self.logger.warning(
                            "⚠️ Unexpected updates type. "
                            + f"Got {type(updates).__name__}, expected Updates"
                        )
                        raise TaskError(
                            f"Unexpected updates type: {type(updates).__name__}"
                        )

                    if len(updates.chats) != 0:
                        entity = updates.chats[0]
                        if not isinstance(entity, types.Channel):
                            self.logger.warning(
                                "⚠️ Unexpected chat type. "
                                + f"Got {type(entity).__name__}, expected Channel"
                            )
                            raise TaskError(
                                f"Unexpected chat type: {type(entity).__name__}"
                            )

                        self.logger.info("✅ Joined to channel")
                        return entity
                    else:
                        raise TaskError("Update did not contain any chats")

                self.logger.warning(
                    f"⚠️ Unknown response type: {type(check_res).__name__}. "
                    + "while checking chat invite request"
                )
                raise TaskError(
                    f"Unknown response type: {type(check_res).__name__} "
                    + "while checking chat invite request"
                )

            except errors.InviteRequestSentError:
                raise TemporaryCannotProcessTask(
                    "Join request sent. "
                    + "Cannot parse private channel pending approval."
                )

            except errors.InviteHashExpiredError:
                raise TaskError("Invite link expired")

            except errors.InviteHashInvalidError:
                raise TaskError("Invite link invalid")

        else:
            self.logger.info("⌛ Getting channel entity by username")
            username = http_url.path.lstrip("/")
            if "/" in username:
                username = username.split("/")[0]
            try:
                entity = await client.get_entity(username)
                if not isinstance(entity, types.Channel):
                    self.logger.warning(
                        "⚠️ Resolved entity is not a Channel. "
                        + f"It is {type(entity).__name__}"
                    )
                    raise TaskError(
                        "Resolved entity is not a Channel. "
                        + f"It is {type(entity).__name__}"
                    )
                return entity
            except ValueError:
                raise TaskError(f"Cannot resolve username: {username}")
            except errors.UsernameInvalidError:
                raise TaskError(f"Invalid username format: {username}")
            except errors.UsernameNotOccupiedError:
                raise TaskError(f"Username not found: {username}")

    async def _collect_messages(
        self, client: ITelegramClient, channel: Channel
    ) -> list[_ParsedMessage]:
        self.logger.info("⌛ Collecting messages")
        now_utc = datetime.now(timezone.utc)
        time_limit_72h = now_utc - timedelta(hours=72)

        grouped_messages: dict[int, _ParsedMessage] = {}

        async for message in client.iter_messages(channel, offset_date=now_utc):
            if not message.date:
                continue

            message_date = message.date.astimezone(timezone.utc)
            if message_date < time_limit_72h:
                break

            group_key = message.grouped_id if message.grouped_id else message.id
            if group_key not in grouped_messages:
                new_message = _ParsedMessage(
                    id=message.id,
                    views=message.views or 0,
                    created_at=message_date,
                    text=message.message or "",
                    original_messages=[message],
                )
                grouped_messages[group_key] = new_message
            else:
                existing_message = grouped_messages[group_key]

                if message.message and not existing_message.text:
                    existing_message.text = message.message
                if message.views and existing_message.views == 0:
                    existing_message.views = message.views
                if message.id < existing_message.id:
                    existing_message.id = message.id
                    existing_message.created_at = message_date

                existing_message.original_messages.append(message)

        result = list(grouped_messages.values())
        self.logger.info(f"✅ Collected {len(result)} messages")
        return result

    async def _process_messages(
        self,
        client: ITelegramClient,
        collected_messages: list[_ParsedMessage],
        channel_id: int,
    ) -> None:
        self.logger.info("⌛ Processing messages")

        async with self._dao_factory() as dao_factory:
            channel_message_dao = dao_factory(ChannelMessageDAO)
            channel_message_statistic_dao = dao_factory(ChannelMessageStatisticDAO)
            channel_dao = dao_factory(ChannelDAO)
            media_dao = dao_factory(MediaDAO)

            channel = await channel_dao.find_by_id(channel_id)
            if channel is None:
                self.logger.error(
                    f"⚠️ Channel with id {channel_id} not found "
                    + "while processing messages"
                )
                raise TemporaryCannotProcessTask(
                    f"Channel with id {channel_id} not found"
                )

            for message in collected_messages:
                persistence_message = await channel_message_dao.find_by_id(message.id)
                if persistence_message is not None:
                    persistence_message.text = message.text
                    await channel_message_dao.save(persistence_message)
                    await channel_message_statistic_dao.create(
                        persistence_message, message.views
                    )
                else:
                    persistence_message = await channel_message_dao.create(
                        channel, message.id, message.created_at, message.text
                    )
                    await persistence_message.awaitable_attrs.media
                    for original_message in message.original_messages:
                        media_content: bytes | None = None
                        if isinstance(original_message.media, types.MessageMediaPhoto):
                            data = await client.download_media(
                                original_message,
                                file=bytes,  # pyright: ignore[reportArgumentType]
                                thumb=1,
                            )
                            if data is None:
                                data = await client.download_media(
                                    original_message,
                                    file=bytes,  # pyright: ignore[reportArgumentType]
                                )
                            if data is None or not isinstance(data, bytes):
                                self.logger.warning(
                                    "⚠️ Failed to download media "
                                    + f"for message with id {original_message.id}"
                                )
                                continue
                            media_content = data
                        elif isinstance(
                            original_message.media, types.MessageMediaDocument
                        ):
                            data = await client.download_media(
                                original_message,
                                file=bytes,  # pyright: ignore[reportArgumentType]
                                thumb=0,
                            )
                            if data is None or not isinstance(data, bytes):
                                self.logger.warning(
                                    "⚠️ Failed to download media "
                                    + f"for message with id {original_message.id}"
                                )
                                continue
                            media_content = data

                        if media_content is not None:
                            kind = filetype.guess(media_content)
                            if kind is None:
                                media_extension = "bin"
                                media_mime = "application/octet-stream"
                            else:
                                media_extension = kind.extension
                                media_mime = kind.mime
                            media_id = uuid4()
                            file_name = f"{media_id}.{media_extension}"
                            persistence_media = await media_dao.create(
                                id=media_id,
                                mime_type=media_mime,
                                size_bytes=len(media_content),
                                file_name=file_name,
                            )
                            await self._storage.save_media(media_content, file_name)
                            persistence_message.media.append(persistence_media)

                    await channel_message_statistic_dao.create(
                        persistence_message, message.views
                    )

            await dao_factory.commit()
        self.logger.info(f"✅ Processed {len(collected_messages)} messages")
