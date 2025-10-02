from app.config import ApiServiceConfig
from fastapi import APIRouter, HTTPException
from shared_models.scheduler.add_channel import AddChannelRequest, AddChannelResponse
from app.models import (
    AddChannelResponse as SchedulerAddChannelResponse,
)
from fastapi.responses import JSONResponse
from app.services import verify_api_key, Scheduler
from fastapi import Depends


scheduler_service = Scheduler()
router = APIRouter(prefix="/scheduler", tags=["Scheduler"])


@router.post(
    "/add_channel",
    responses={
        200: {
            "model": SchedulerAddChannelResponse,
            "description": "Successful response",
        },
        302: {
            "model": SchedulerAddChannelResponse,
            "description": "Channel already exists",
        },
        **ApiServiceConfig.DEFAULT_RESPONSE,
    },
    response_model=SchedulerAddChannelResponse,
)
async def add_channel(request: AddChannelRequest, _: None = Depends(verify_api_key)):
    """Add channel to the scheduler"""
    try:
        response: AddChannelResponse = await scheduler_service.add_channel(request)
        if response.success:
            return JSONResponse(
                status_code=200,
                content={
                    "channel": response.channel.model_dump(),
                },
            )
        else:
            return JSONResponse(
                status_code=302,
                content={
                    "channel": response.channel.model_dump(),
                },
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
