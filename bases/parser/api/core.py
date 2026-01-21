import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import version
from logging import getLogger

from dishka import make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from parser.logging import setup_logging
from parser.persistence import PersistenceProvider
from parser.settings import ProjectSettings, ProjectSettingsProvider
from uvicorn import Config, Server

from .settings import APISettings, APISettingsProvider
from .utils import CustomHTTPException, ErrorResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await app.state.dishka_container.close()


async def build_app() -> FastAPI:
    container = make_async_container(
        APISettingsProvider(), PersistenceProvider(), ProjectSettingsProvider()
    )

    api_settings = await container.get(APISettings)
    project_settings = await container.get(ProjectSettings)
    logger = getLogger("app")

    setup_logging(project_settings)

    app = FastAPI(
        title=api_settings.app_name,
        version=version("api"),
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
        },
    )

    setup_dishka(container=container, app=app)

    @app.exception_handler(CustomHTTPException)
    async def custom_http_exception_handler(  # pyright: ignore[reportUnusedFunction]
        request: Request, exc: CustomHTTPException
    ) -> JSONResponse:
        error_response = ErrorResponse(error=exc.error, message=exc.message)

        logger.warning(
            f"Custom HTTP exception: {exc.error} - {exc.message}",
            extra={
                "exception_type": exc.error,
                "path": request.url.path,
                "method": request.method,
            },
        )

        return JSONResponse(
            status_code=exc.status_code, content=error_response.model_dump()
        )

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
