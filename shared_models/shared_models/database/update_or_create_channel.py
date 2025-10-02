from pydantic import BaseModel


class UpdateOrCreateChannelResponse(BaseModel):
    record_created: bool
