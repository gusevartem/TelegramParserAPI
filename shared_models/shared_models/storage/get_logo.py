from pydantic import BaseModel


class GetLogoRequest(BaseModel):
    channel_id: int


class GetLogoResponse(BaseModel):
    logo: bytes
