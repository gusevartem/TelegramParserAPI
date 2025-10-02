from pydantic import BaseModel


class SaveLogoRequest(BaseModel):
    channel_id: int
    logo: bytes
