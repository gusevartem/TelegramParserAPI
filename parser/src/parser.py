import asyncio
from datetime import datetime, timedelta
from pytz import UTC
import logging
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types.messages import ChatFull
from telethon.tl import types
from shared_models.parser.get_channel_info import GetChannelInfoRequest, GetChannelInfoResponse
from .telegram import Telegram
from telethon.errors.rpcerrorlist import UserAlreadyParticipantError, InviteRequestSentError, InviteHashExpiredError, UserDeactivatedBanError, FloodWaitError
from shared_models.parser.errors import FloodWait, InvalidChannelLink, UserBan, CannotGetChannelInfo
from telethon.tl.functions.messages import ImportChatInviteRequest
from shared_models import Channel as ChannelInfo
from telethon.errors.rpcbaseerrors import FloodError
from telethon.errors.rpcerrorlist import FloodWaitError


class Parser:
    def __init__(self, telegram: Telegram) -> None:
        self.logger = logging.getLogger('parser')
        self.telegram = telegram
    
    async def get_channel_entity(self, activated_client: TelegramClient, link) -> types.Channel:
        try:
            channel_entity = await activated_client.get_entity(link)
        except ValueError:
            try:
                channel_entity = await self.join_private_channel(activated_client, link) # type: ignore
            except InviteHashExpiredError as e:
                raise InvalidChannelLink(link, str(e))
            except FloodWaitError as e:
                raise FloodWait(e.seconds)
            except UserDeactivatedBanError as e:
                raise UserBan(str(e))
        except FloodWaitError as e:
            raise FloodWait(e.seconds)
        except Exception as e:
            raise InvalidChannelLink(link, str(e))
        if not channel_entity:
            raise CannotGetChannelInfo(link)
        return channel_entity # type: ignore
    
    async def join_private_channel(self, activated_client: TelegramClient, url: str):
        invite_hash = url.split('/')[-1]
        if invite_hash.startswith('+'):
            invite_hash = invite_hash[1:]
        try:
            await activated_client(ImportChatInviteRequest(invite_hash)) # type: ignore
        except UserAlreadyParticipantError:
            return await activated_client.get_entity(url)
        except InviteRequestSentError:
            for _ in range(3):
                await asyncio.sleep(10)
                try:
                    return await activated_client.get_entity(url)
                except ValueError:
                    continue
        return await activated_client.get_entity(url)
    
    async def get_channel(self, client, entity: types.Channel, url: str) -> ChannelInfo:
        channel_info : ChatFull = await client(GetFullChannelRequest(channel=entity))  # type: ignore
        if url.startswith('https://'):
            url = url.removeprefix('https://')
        elif url.startswith('http://'):
            url = url.removeprefix('http://')
        
        views, count = await self.__get_posts(client, entity)
        
        return ChannelInfo(
            channel_id=channel_info.full_chat.id,
            link=url,
            name=entity.title,
            description=channel_info.full_chat.about,
            subscribers=channel_info.full_chat.participants_count, # type: ignore
            views=views,
            posts_count=count,
        )
    
    async def __get_posts(self, client: TelegramClient, entity: types.Channel) -> tuple[int, int]:
        start_date = datetime.now(UTC)
        end_date = start_date - timedelta(hours=24)
        
        count = 0
        views = 0
        async for post in client.iter_messages(entity, offset_date=start_date):  # type: ignore
            post: types.Message
            post_date = post.date.replace(tzinfo=UTC) if post.date.tzinfo is None else post.date # type: ignore
            if post_date.timestamp() < end_date.timestamp(): # type: ignore
                break
            if post.views is not None:
                views = post.views
            count += 1
            
        return views, count
    
    # Cron
    @staticmethod
    async def update_client(ctx):
        self: Parser = ctx['Parser_instance']
    
    
    # Methods
    @staticmethod
    async def get_channel_info(ctx, request: GetChannelInfoRequest) -> GetChannelInfoResponse:
        self: Parser = ctx['Parser_instance']
        non_active_client = await self.telegram.get_client()

        async with non_active_client as client:
            try:
                return await asyncio.wait_for(
                    self._get_channel_info_internal(client, request), 
                    timeout=60
                )
            except asyncio.TimeoutError:
                await non_active_client.mark_as_ban()
                raise TimeoutError("Timeout while getting channel info. Client may be banned")
            except FloodWaitError as e:
                raise FloodWait(e.seconds)
            except FloodError as e:
                await non_active_client.mark_as_ban()
                raise UserBan("User is banned from the channel") from e
                    
    async def _get_channel_info_internal(self, client: TelegramClient, request: GetChannelInfoRequest) -> GetChannelInfoResponse:
        entity = await self.get_channel_entity(client, request.channel_link)
        if request.get_logo:
            try:
                logo = await client.download_profile_photo(entity, file=bytes)  # type: ignore
            except Exception:
                logo = None
        else:
            logo = None
        return GetChannelInfoResponse(
            channel=await self.get_channel(client, entity, request.channel_link),
            logo=logo  # type: ignore
        )