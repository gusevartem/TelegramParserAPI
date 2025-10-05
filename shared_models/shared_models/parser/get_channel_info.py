from pydantic import BaseModel
from ..channel import Channel
from ..message import Message
from typing import Optional


class GetChannelInfoRequest(BaseModel):
    channel_link: str
    get_logo: bool = False
    download_message_media: bool = False


class GetChannelInfoResponse(BaseModel):
    channel: Channel
    logo: Optional[bytes]
    messages: list[Message]
