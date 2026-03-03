import asyncio
from contextlib import asynccontextmanager

from dishka import make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import Status, StatusCode
from parser.logging import LoggingSettings, LoggingSettingsProvider, setup_logging
from parser.persistence import PersistenceProvider
from parser.scheduler import SchedulerProvider
from parser.storage import StorageProvider
from parser.telegram import TelegramProvider
from uvicorn import Config, Server

from .routers import parser_router, public_router
from .settings import APISettings, APISettingsProvider
from .utils import CustomHTTPException, ErrorResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await app.state.dishka_container.close()


async def build_app() -> FastAPI:
    container = make_async_container(
        APISettingsProvider(),
        PersistenceProvider(),
        LoggingSettingsProvider(),
        StorageProvider(),
        TelegramProvider(),
        SchedulerProvider(),
    )

    api_settings = await container.get(APISettings)
    logging_settings = await container.get(LoggingSettings)

    setup_logging(logging_settings, "api", "api")

    app = FastAPI(
        title=api_settings.app_name,
        docs_url=api_settings.api_prefix + "/docs",
        redoc_url=api_settings.api_prefix + "/redoc",
        openapi_url=api_settings.api_prefix + "/openapi.json",
        swagger_ui_oauth2_redirect_url=api_settings.api_prefix
        + "/docs/oauth2-redirect",
        lifespan=lifespan,
        responses={
            status.HTTP_401_UNAUTHORIZED: {
                "model": ErrorResponse,
                "description": "Токен не валиден",
            },
            status.HTTP_500_INTERNAL_SERVER_ERROR: {
                "model": ErrorResponse,
                "description": "Внутренняя ошибка сервера",
            },
        },
    )

    FastAPIInstrumentor.instrument_app(app)

    setup_dishka(container=container, app=app)

    @app.exception_handler(CustomHTTPException)
    async def custom_http_exception_handler(  # pyright: ignore[reportUnusedFunction]
        request: Request, exc: CustomHTTPException
    ) -> JSONResponse:
        error_response = ErrorResponse(error=exc.error, message=exc.message)
        span = trace.get_current_span()
        span.record_exception(exc)
        span.set_attribute("error", exc.error)
        span.set_attribute("message", exc.message)
        span.set_attribute("status_code", exc.status_code)
        span.set_attribute("request.path", request.url.path)
        span.set_attribute("request.method", request.method)
        span.set_status(status=Status(StatusCode.ERROR, exc.error))

        return JSONResponse(
            status_code=exc.status_code,
            content=error_response.model_dump(),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def all_exception_handler(  # pyright: ignore[reportUnusedFunction]
        request: Request, exc: Exception
    ) -> JSONResponse:
        error_response = ErrorResponse(error=exc.__class__.__name__, message=str(exc))
        span = trace.get_current_span()
        span.record_exception(exc)
        span.set_attribute("error", error_response.error)
        span.set_attribute("message", error_response.message)
        span.set_attribute("status_code", 500)
        span.set_attribute("request.path", request.url.path)
        span.set_attribute("request.method", request.method)
        span.set_status(status=Status(StatusCode.ERROR, error_response.message))

        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response.model_dump(),
        )

    app.include_router(parser_router, prefix=api_settings.methods_prefix)
    app.include_router(public_router, prefix=api_settings.api_prefix)

    return app


async def run_app() -> None:
    app = await build_app()
    api_settings: APISettings = await app.state.dishka_container.get(APISettings)

    config = Config(
        app=app,
        host="0.0.0.0",
        port=api_settings.api_port,
        reload=False,
    )
    server = Server(config)
    await server.serve()


def run() -> None:
    asyncio.run(run_app())
