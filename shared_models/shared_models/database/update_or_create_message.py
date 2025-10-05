from pydantic import BaseModel
from ..message import Message


class UpdateOrCreateMessageRequest(BaseModel):
    message: Message
    channel_id: int


class UpdateOrCreateMessageResponse(BaseModel):
    message: Message
    record_created: bool
