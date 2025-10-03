import io
from app.config import ApiServiceConfig
from fastapi import APIRouter, HTTPException
from shared_models.database.get_channel import GetChannelRequest, GetChannelResponse
from shared_models.storage.get_logo import GetLogoRequest
from shared_models.database.get_channels_ids import GetChannelsIdsResponse
from shared_models.database.get_24h_statistics import (
    Get24hStatisticsRequest,
    Get24hStatisticsResponse,
    StatisticsSorting,
)
from fastapi import Depends
from fastapi.responses import StreamingResponse
from app.services import Database, Storage


database_service = Database()
storage_service = Storage()
router = APIRouter(prefix="/public", tags=["Public"])

get_logo_responses = ApiServiceConfig.DEFAULT_RESPONSE.copy()
get_logo_responses[200] = {"content": {"image/jpeg": {}}}


@router.get("/get_logo", responses=get_logo_responses, response_class=StreamingResponse)
async def get_logo(request: GetLogoRequest = Depends()):
    try:
        logo = (await storage_service.get_logo(request)).logo
        return StreamingResponse(io.BytesIO(logo), media_type="image/jpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/get_channel",
    responses=ApiServiceConfig.DEFAULT_RESPONSE,
    response_model=GetChannelResponse,
)
async def add_client(request: GetChannelRequest = Depends()):
    try:
        return await database_service.get_channel(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/get_channels_ids",
    responses=ApiServiceConfig.DEFAULT_RESPONSE,
    response_model=GetChannelsIdsResponse,
)
async def get_channels_ids():
    try:
        return await database_service.get_channels_ids()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/get_24h_statistics",
    responses=ApiServiceConfig.DEFAULT_RESPONSE,
    response_model=Get24hStatisticsResponse,
)
async def get_24h_statistics(channel_id: int):
    try:
        req = Get24hStatisticsRequest(
            channel_id=channel_id, sorting=StatisticsSorting.NEWEST
        )
        return await database_service.get_24h_statistics(req)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
