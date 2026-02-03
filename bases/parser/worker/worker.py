import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import filetype
import structlog
from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode
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
from .settings import WorkerSettings


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
        settings: WorkerSettings,
    ):
        self._telegram: ITelegram = telegram
        self._message_broker: IMessageBroker = message_broker
        self._storage: IStorage = storage
        self._dao_factory: MultipleDAOFactory = dao_factory
        self.settings: WorkerSettings = settings

        self.logger: structlog.BoundLogger = structlog.get_logger()
        self.tracer: trace.Tracer = trace.get_tracer("worker")

    async def start(self, tasks_timeout_sleep: int = 5) -> None:
        try:
            async with self._telegram.get_client() as client:
                while True:
                    with self.tracer.start_as_current_span("worker.wait_task") as span:
                        wait_timeout = 600
                        span.set_attribute("wait_timeout", wait_timeout)
                        logger = self.logger.bind(wait_timeout=wait_timeout)
                        logger.info("waiting_for_task")
                        try:
                            async with self._message_broker.get_task(
                                wait_timeout
                            ) as task:
                                with self.tracer.start_as_current_span(
                                    "worker.process_task"
                                ) as task_span:
                                    task_span.set_attribute("task.id", str(task.id))
                                    task_span.set_attribute("task.url", task.url)
                                    task_span.set_attribute(
                                        "task.channel_id", task.channel_id or "none"
                                    )
                                    task_span.set_attribute(
                                        "task.next_run_at",
                                        task.next_run_at or "none",
                                    )
                                    task_span.set_attribute(
                                        "task.last_parsed_at",
                                        task.last_parsed_at or "none",
                                    )
                                    task_span.set_attribute(
                                        "task.created_at",
                                        task.created_at,
                                    )
                                    task_logger = logger.bind(
                                        task_id=str(task.id),
                                        task_url=task.url,
                                        channel_id=task.channel_id or "none",
                                        next_run_at=task.next_run_at or "none",
                                        last_parsed_at=task.last_parsed_at or "none",
                                        created_at=task.created_at,
                                    )
                                    task_logger.info("processing_task", stage="start")
                                    task_logger.info("finding_task")
                                    async with self._dao_factory() as dao_factory:
                                        parsing_task_dao = dao_factory(ParsingTaskDAO)
                                        persistence_task = (
                                            await parsing_task_dao.find_by_id(task.id)
                                        )
                                        if persistence_task is None:
                                            task_logger.warning(
                                                "task_not_found_in_database"
                                            )
                                            task_span.set_status(
                                                Status(
                                                    StatusCode.ERROR,
                                                    "Task not found in database",
                                                )
                                            )
                                            raise InvalidTask(
                                                "Task not found in database"
                                            )

                                        task_span.set_attribute(
                                            "task.status",
                                            str(persistence_task.status),
                                        )

                                        if persistence_task.status in (
                                            ParsingTaskStatus.ERROR,
                                            ParsingTaskStatus.EXISTS,
                                        ):
                                            task_logger.warning(
                                                "unexpected_task_status",
                                                current_task_status=str(
                                                    persistence_task.status
                                                ),
                                                expected_task_status=str(
                                                    ParsingTaskStatus.PROCESSING
                                                ),
                                                next_action="raise_exception",
                                            )
                                            task_span.set_status(
                                                Status(
                                                    StatusCode.ERROR,
                                                    "Unexpected task status",
                                                )
                                            )
                                            raise InvalidTask(
                                                "Expect status "
                                                + f"{ParsingTaskStatus.PROCESSING}, "
                                                + f"got {persistence_task.status}"
                                            )

                                        if (
                                            persistence_task.status
                                            == ParsingTaskStatus.IDLE
                                        ):
                                            task_logger.warning(
                                                "unexpected_task_status",
                                                current_task_status=str(
                                                    persistence_task.status
                                                ),
                                                expected_task_status=str(
                                                    ParsingTaskStatus.PROCESSING
                                                ),
                                                next_action="update_status",
                                            )
                                            persistence_task.status = (
                                                ParsingTaskStatus.PROCESSING
                                            )
                                            await parsing_task_dao.save(
                                                persistence_task
                                            )
                                            await parsing_task_dao.commit()

                                    task_logger.info("task_found_in_database")

                                    async with self._handle_task_processing_errors(
                                        task, task_logger, task_span
                                    ):
                                        await self._process_task(
                                            client, task, task_logger, task_span
                                        )

                                    task_logger.info("task_processed", stage="complete")

                                    async with self._dao_factory() as dao_factory:
                                        task_logger.info("updating_task_status")

                                        parsing_task_dao = dao_factory(ParsingTaskDAO)
                                        persistence_task = (
                                            await parsing_task_dao.find_by_id(task.id)
                                        )
                                        if (
                                            persistence_task is not None
                                            and persistence_task.status
                                            == ParsingTaskStatus.PROCESSING
                                        ):
                                            persistence_task.status = (
                                                ParsingTaskStatus.IDLE
                                            )
                                            persistence_task.last_parsed_at = (
                                                datetime.now(timezone.utc)
                                            )
                                            await parsing_task_dao.save(
                                                persistence_task
                                            )
                                            await parsing_task_dao.commit()

                                        task_logger.info("task_updated")

                        except InvalidMessageError:
                            logger.error("invalid_message", action="ignore")
                            continue
                        except InvalidTask:
                            logger.error("invalid_task", action="ignore")
                            continue
                        except TimeoutError:
                            logger.error(
                                "timeout_error",
                                action="sleep",
                                sleep_seconds=tasks_timeout_sleep,
                            )
                            await asyncio.sleep(tasks_timeout_sleep)
                            continue
                        except TelegramException:
                            raise
                        except Exception as e:
                            logger.critical("unknown_error", exc_info=True)
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.record_exception(e)
                            continue

        except (
            NoWorkingClientsFoundError,
            InvalidClient,
            TimeoutError,
            FloodWait,
        ):
            self.logger.warning("client_exception_occurred", exc_info=True)

    @asynccontextmanager
    async def _handle_task_processing_errors(
        self, task: ParsingTask, task_logger: structlog.BoundLogger, task_span: Span
    ):
        async def change_task_status(
            new_status: ParsingTaskStatus,
            exception: Exception,
            channel_id: int | None = None,
        ):
            async with self._dao_factory() as dao_factory:
                parsing_task_dao = dao_factory(ParsingTaskDAO)
                persistence_task = await parsing_task_dao.find_by_id(task.id)
                if persistence_task is None:
                    task_logger.error("task_deleted_while_processing")
                    task_span.set_status(
                        Status(StatusCode.ERROR, "Task was deleted while processing")
                    )
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
            task_logger.warning("channel_already_exists", action="mark_as_exists")
            raise InvalidTask("Task already exists") from e

        except TaskError as e:
            await change_task_status(ParsingTaskStatus.ERROR, e)
            task_span.set_status(Status(StatusCode.ERROR, str(e)))
            task_span.record_exception(e)
            task_logger.error("task_error", action="mark_as_error", exc_info=True)
            raise InvalidTask("Task error") from e

        except TemporaryCannotProcessTask as e:
            await change_task_status(ParsingTaskStatus.IDLE, e)
            task_logger.warning(
                "temporary_cannot_process_task", action="mark_as_idle", exc_info=True
            )

    async def _process_task(
        self,
        client: ITelegramClient,
        task: ParsingTask,
        task_logger: structlog.BoundLogger,
        task_span: Span,
    ) -> None:
        task_logger.info("getting_channel")
        channel_entity = await self._get_channel(client, task.url)
        task_logger.info("got_channel")
        task_logger = task_logger.bind(channel_id=channel_entity.id)
        task_span.set_attribute("channel_id", channel_entity.id)

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
                    task_logger.error("task_exists")
                    task_span.set_status(Status(StatusCode.ERROR, "Task exists"))
                    raise TaskExists(
                        f"Task {task_with_same_channel.id} and {task.id} "
                        + "are for the same channel",
                        task_with_same_channel.channel_id,
                    )

        full_channel = await client(
            functions.channels.GetFullChannelRequest(channel_entity)  # pyright: ignore[reportArgumentType]
        )
        if not isinstance(full_channel, types.messages.ChatFull):
            task_logger.error(
                "cannot_get_channel_full_info",
                response_type=type(full_channel).__name__,
                expect="ChatFull",
            )
            task_span.set_status(
                Status(
                    StatusCode.ERROR,
                    "Cannot get channel full info. "
                    + f"Expected ChatFull, got: {type(full_channel).__name__}",
                )
            )
            raise TaskError("Cannot get channel full info")

        collected_messages = await self._collect_messages(client, channel_entity)

        async with self._dao_factory() as dao_factory:
            task_logger.info("saving_channel_to_database")
            parsing_task_dao = dao_factory(ParsingTaskDAO)
            channel_dao = dao_factory(ChannelDAO)
            media_dao = dao_factory(MediaDAO)
            channel_statistic_dao = dao_factory(ChannelStatisticDAO)

            persistence_channel = await channel_dao.find_by_id(channel_entity.id)
            if persistence_channel is None:
                task_logger.info(
                    "channel_does_not_exist_in_database", action="create_new"
                )
                task_logger.info("downloading_channel_logo")
                logo = await client.download_profile_photo(channel_entity, file=bytes)  # pyright: ignore[reportArgumentType]
                logo_media: Media | None = None
                if logo is None or not isinstance(logo, bytes):
                    task_logger.error(
                        "cannot_download_channel_logo",
                        action="skip",
                        logo=type(logo).__name__,
                        expect="bytes",
                    )
                else:
                    task_logger.info("channel_logo_downloaded")
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
                task_logger.info("channel_saved_to_database")

            task_logger.info("saving_channel_statistic_to_database")

            now_utc = datetime.now(timezone.utc)
            stat_time_limit = now_utc - timedelta(
                hours=self.settings.channel_messages_stat_time_limit_hours
            )

            messages_stat = [
                message
                for message in collected_messages
                if message.created_at > stat_time_limit
            ]

            await channel_statistic_dao.create(
                channel=persistence_channel,
                subscribers_count=full_channel.full_chat.participants_count or 0,  # pyright: ignore[reportAttributeAccessIssue]
                views=sum(message.views for message in messages_stat),
                posts_count=len(messages_stat),
            )

            task_logger.info("channel_statistic_saved_to_database")

            task_logger.info("updating_current_task_channel_id")
            persistence_parsing_task = await parsing_task_dao.find_by_id(task.id)
            if persistence_parsing_task is None:
                task_logger.error("task_deleted_while_processing")
                task_span.set_status(
                    Status(StatusCode.ERROR, "Task was deleted while processing")
                )
                raise InvalidTask("Task was deleted while processing")

            persistence_parsing_task.channel_id = channel_entity.id
            await parsing_task_dao.save(persistence_parsing_task)
            task_logger.info("updated_current_task_channel_id")

            await dao_factory.commit()
        task_logger.info("channel_saved_to_database")

        await self._process_messages(client, collected_messages, channel_entity.id)

    async def _get_channel(self, client: ITelegramClient, url: str) -> Channel:
        with self.tracer.start_as_current_span("worker.get_channel_entity") as span:
            span.set_attribute("url", url)
            logger = self.logger.bind(url=url)
            logger.info("getting_channel_entity", status="start")
            span.set_attribute("channel.url", url)

            try:
                http_url = TypeAdapter(HttpUrl).validate_python(url)
                if not http_url.path or len(http_url.path) < 2:
                    logger.error("empty_path")
                    raise ValueError("Empty path")
            except Exception as e:
                logger.error("invalid_url_format", exc_info=True)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise TaskError(f"Invalid URL format: {url}") from e

            is_join_link = http_url.path.startswith(
                "/joinchat"
            ) or http_url.path.startswith("/+")

            if is_join_link:
                logger.info("getting_channel_entity_with_join_link")

                hash_part = http_url.path.split("/")[-1].replace("+", "").strip()
                if not hash_part:
                    logger.error("empty_hash")
                    span.set_status(
                        Status(StatusCode.ERROR, "Empty hash in invite link")
                    )
                    raise TaskError("Empty hash in invite link")
                try:
                    check_res = await client(
                        functions.messages.CheckChatInviteRequest(hash_part)
                    )

                    if isinstance(check_res, types.ChatInviteAlready):
                        if not isinstance(check_res.chat, types.Channel):
                            logger.error(
                                "unexpected_chat_type",
                                chat_type=type(check_res.chat).__name__,
                                expect="Channel",
                            )
                            span.set_status(
                                Status(
                                    StatusCode.ERROR,
                                    "Unexpected chat type: "
                                    + f"{type(check_res.chat).__name__}",
                                )
                            )
                            raise TaskError(
                                f"Unexpected chat type: {type(check_res.chat).__name__}"
                            )

                        logger.info("got_channel_entity")
                        return check_res.chat

                    elif isinstance(check_res, types.ChatInvite):
                        if not check_res.channel and not check_res.broadcast:
                            logger.error("link_points_to_group_or_chat")
                            span.set_status(
                                Status(
                                    StatusCode.ERROR,
                                    "Link points to a Group/Chat, not a Channel",
                                )
                            )
                            raise TaskError(
                                "Link points to a Group/Chat, not a Channel"
                            )

                        logger.info("joining_channel")
                        updates = await client(
                            functions.messages.ImportChatInviteRequest(hash_part)
                        )

                        if not isinstance(updates, types.Updates):
                            logger.warning(
                                "unexpected_updates_type",
                                updates_type=type(updates).__name__,
                                expect="Updates",
                            )
                            span.set_status(
                                Status(
                                    StatusCode.ERROR,
                                    "Unexpected updates type: "
                                    + f"{type(updates).__name__}",
                                )
                            )
                            raise TaskError(
                                f"Unexpected updates type: {type(updates).__name__}"
                            )

                        if len(updates.chats) != 0:
                            entity = updates.chats[0]
                            if not isinstance(entity, types.Channel):
                                logger.warning(
                                    "unexpected_chat_type",
                                    chat_type=type(entity).__name__,
                                    expect="Channel",
                                )
                                span.set_status(
                                    Status(
                                        StatusCode.ERROR,
                                        "Unexpected chat type: "
                                        + f"{type(entity).__name__}",
                                    )
                                )
                                raise TaskError(
                                    f"Unexpected chat type: {type(entity).__name__}"
                                )

                            logger.info("got_channel_entity")
                            return entity
                        else:
                            logger.error("update_did_not_contain_any_chats")
                            span.set_status(
                                Status(
                                    StatusCode.ERROR,
                                    "Update did not contain any chats",
                                )
                            )
                            raise TaskError("Update did not contain any chats")

                    logger.info(
                        "unknown_response_type", response_type=type(check_res).__name__
                    )
                    span.set_status(
                        Status(
                            StatusCode.ERROR,
                            "Unknown response type: " + f"{type(check_res).__name__}",
                        )
                    )
                    raise TaskError(
                        f"Unknown response type: {type(check_res).__name__} "
                        + "while checking chat invite request"
                    )

                except errors.InviteRequestSentError as e:
                    logger.error("invite_request_sent_error", exc_info=True)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise TemporaryCannotProcessTask(
                        "Join request sent. "
                        + "Cannot parse private channel pending approval."
                    )

                except errors.InviteHashExpiredError as e:
                    logger.error("invite_link_expired", exc_info=True)
                    span.set_status(Status(StatusCode.ERROR, "Invite link expired"))
                    span.record_exception(e)
                    raise TaskError("Invite link expired")

                except errors.InviteHashInvalidError as e:
                    logger.error("invite_link_invalid", exc_info=True)
                    span.set_status(Status(StatusCode.ERROR, "Invite link invalid"))
                    span.record_exception(e)
                    raise TaskError("Invite link invalid")

            else:
                logger.info("getting_channel_entity_by_username")

                username = http_url.path.lstrip("/")
                if "/" in username:
                    username = username.split("/")[0]
                try:
                    entity = await client.get_entity(username)
                    if not isinstance(entity, types.Channel):
                        logger.warning(
                            "resolved_entity_is_not_a_channel",
                            entity_type=type(entity).__name__,
                            expect="Channel",
                        )
                        span.set_status(
                            Status(
                                StatusCode.ERROR,
                                "Resolved entity is not a Channel. "
                                + f"It is {type(entity).__name__}",
                            )
                        )
                        raise TaskError(
                            "Resolved entity is not a Channel. "
                            + f"It is {type(entity).__name__}"
                        )
                    return entity
                except ValueError as e:
                    logger.error(
                        "cannot_resolve_username", username=username, exc_info=True
                    )
                    span.set_status(Status(StatusCode.ERROR, "Cannot resolve username"))
                    span.record_exception(e)
                    raise TaskError(f"Cannot resolve username: {username}")
                except errors.UsernameInvalidError as e:
                    logger.error(
                        "invalid_username_format", username=username, exc_info=True
                    )
                    span.set_status(Status(StatusCode.ERROR, "Invalid username format"))
                    span.record_exception(e)
                    raise TaskError(f"Invalid username format: {username}")
                except errors.UsernameNotOccupiedError as e:
                    logger.error("username_not_found", username=username, exc_info=True)
                    span.set_status(Status(StatusCode.ERROR, "Username not found"))
                    span.record_exception(e)
                    raise TaskError(f"Username not found: {username}")

    async def _collect_messages(
        self, client: ITelegramClient, channel: Channel
    ) -> list[_ParsedMessage]:
        with self.tracer.start_as_current_span("worker.collect_messages") as span:
            logger = self.logger.bind(channel_id=channel.id)
            span.set_attribute("channel.id", channel.id)
            now_utc = datetime.now(timezone.utc)
            time_limit = now_utc - timedelta(
                hours=self.settings.message_monitoring_time_limit_hours
            )

            grouped_messages: dict[int, _ParsedMessage] = {}

            async for message in client.iter_messages(channel, offset_date=now_utc):
                if not message.date:
                    continue

                message_date = message.date.astimezone(timezone.utc)
                if message_date < time_limit:
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
            span.set_attribute("messages_count", len(result))
            logger.info("collected_messages", count=len(result))
            return result

    async def _process_messages(
        self,
        client: ITelegramClient,
        collected_messages: list[_ParsedMessage],
        channel_id: int,
    ) -> None:
        with self.tracer.start_as_current_span("worker.process_messages") as span:
            span.set_attribute("messages_count", len(collected_messages))
            span.set_attribute("channel.id", channel_id)
            logger = self.logger.bind(
                channel_id=channel_id, count=len(collected_messages)
            )
            logger.info("processing_messages", stage="start")

            async with self._dao_factory() as dao_factory:
                channel_message_dao = dao_factory(ChannelMessageDAO)
                channel_message_statistic_dao = dao_factory(ChannelMessageStatisticDAO)
                channel_dao = dao_factory(ChannelDAO)
                media_dao = dao_factory(MediaDAO)

                channel = await channel_dao.find_by_id(channel_id)
                if channel is None:
                    logger.error("channel_not_found")
                    span.set_status(Status(StatusCode.ERROR, "Channel not found"))
                    raise TaskError(f"Channel with id {channel_id} not found")

                for message in collected_messages:
                    persistence_message = (
                        await channel_message_dao.find_by_channel_id_and_message_id(
                            channel_id, message.id
                        )
                    )
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
                            if isinstance(
                                original_message.media, types.MessageMediaPhoto
                            ):
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
                                    logger.warning(
                                        "failed_to_download_media",
                                        message_id=original_message.id,
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
                                    logger.warning(
                                        "failed_to_download_media",
                                        message_id=original_message.id,
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
            logger.info("processed_messages", stage="complete")
