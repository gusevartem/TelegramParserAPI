from fastapi import APIRouter, Depends, HTTPException, File, UploadFile
from app.config import ApiServiceConfig
from shared_models.parser.get_channel_info import (
    GetChannelInfoResponse,
    GetChannelInfoRequest,
)
from shared_models.parser.errors import (
    SessionPasswordNeeded,
    InvalidChannelLink,
    FloodWait,
    CannotGetChannelInfo,
)
from app.services import verify_api_key, Parser, Telegram


parser_service = Parser()
telegram_service = Telegram()
router = APIRouter(prefix="/parser", tags=["Parser"])


@router.post("/add_client", responses=ApiServiceConfig.DEFAULT_RESPONSE)
async def add_client(
    tdata: UploadFile = File(...), api_key_verified: None = Depends(verify_api_key)
):
    """Add new telegram account"""
    try:
        await telegram_service.add_client(tdata.file.read())
        return {"message": "Client added successfully"}
    except SessionPasswordNeeded as e:
        raise HTTPException(
            status_code=400,
            detail={"error": "SessionPasswordNeeded", "message": str(e)},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": str(e)})


@router.post(
    "/get_channel_info",
    responses=ApiServiceConfig.DEFAULT_RESPONSE,
    response_model=GetChannelInfoResponse,
)
async def get_channel_info(
    request: GetChannelInfoRequest, api_key_verified: None = Depends(verify_api_key)
):
    """Get current channel data without writing to the database"""
    try:
        return await parser_service.get_channel_info(request)
    except InvalidChannelLink as e:
        raise HTTPException(
            status_code=400, detail={"error": "InvalidChannelLink", "message": str(e)}
        )
    except FloodWait as e:
        raise HTTPException(
            status_code=400, detail={"error": "FloodWait", "message": str(e)}
        )
    except CannotGetChannelInfo as e:
        raise HTTPException(
            status_code=500, detail={"error": "CannotGetChannelInfo", "message": str(e)}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
