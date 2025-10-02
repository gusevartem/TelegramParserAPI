from app.config import ApiServiceConfig
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader


api_key_header = APIKeyHeader(name="Authorization", auto_error=True)


def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify the API key from the request header."""
    if api_key != ApiServiceConfig.SECRET_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
