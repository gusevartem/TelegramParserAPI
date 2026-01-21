from hmac import compare_digest
from typing import Self

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import Depends, status
from fastapi.security import APIKeyHeader
from fastapi.security.utils import get_authorization_scheme_param
from pydantic import BaseModel, Field

from .settings import APISettings


class MessageResponse(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    error: str = Field(description="Тип ошибки")
    message: str = Field(description="Детали ошибки")


class CustomHTTPException(Exception):
    def __init__(
        self,
        error: str,
        message: str,
        status_code: int,
        headers: dict[str, str] | None = None,
    ):
        self.status_code: int = status_code
        self.error: str = error
        self.message: str = message
        self.headers: dict[str, str] | None = headers

        super().__init__(message)

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        status_code: int,
        message: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Self:
        return cls(
            status_code=status_code,
            error=exc.__class__.__name__,
            message=message if message else str(exc),
            headers=headers,
        )


security = APIKeyHeader(
    name="Authorization",
    scheme_name="Secret key authorization",
    description=(
        "Для использования метода "
        "необходимо передать токен в заголовке "
        "Authorization в формате 'SECRET <токен>'."
    ),
)


@inject
def secret_key_check(
    api_settings: FromDishka[APISettings], header_value: str = Depends(security)
) -> None:
    try:
        scheme, api_key = get_authorization_scheme_param(header_value)
        if not scheme or scheme.lower() != "secret":
            raise CustomHTTPException(
                error="InvalidScheme",
                status_code=status.HTTP_401_UNAUTHORIZED,
                message="Invalid authentication scheme",
                headers={"WWW-Authenticate": "SECRET"},
            )
        if not api_key:
            raise CustomHTTPException(
                error="InvalidCredentials",
                status_code=status.HTTP_401_UNAUTHORIZED,
                message="Missing authentication credentials",
                headers={"WWW-Authenticate": "SECRET"},
            )
        if not compare_digest(api_key, api_settings.secret_key):
            raise CustomHTTPException(
                error="InvalidCredentials",
                status_code=status.HTTP_401_UNAUTHORIZED,
                message="Invalid authentication credentials",
                headers={"WWW-Authenticate": "SECRET"},
            )
    except CustomHTTPException:
        raise
    except Exception as e:
        raise CustomHTTPException.from_exception(
            e,
            status_code=status.HTTP_401_UNAUTHORIZED,
            message="Invalid authentication credentials",
            headers={"WWW-Authenticate": "SECRET"},
        )
