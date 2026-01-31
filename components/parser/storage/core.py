import asyncio
from io import BytesIO
from typing import Protocol, override

import structlog
from aioboto3 import Session
from botocore.exceptions import ClientError
from dishka import Provider, Scope, provide
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from . import exceptions
from .settings import StorageSettings


class IStorage(Protocol):
    async def save_media(
        self, data: bytes, file_name: str, max_retries: int = 3
    ) -> None: ...

    async def generate_presigned_url(
        self, file_name: str, expires_seconds: int = 3600, max_retries: int = 3
    ) -> str: ...


class Storage(IStorage):
    def __init__(self, storage_settings: StorageSettings) -> None:
        self.settings: StorageSettings = storage_settings
        self.s3: Session = Session(
            aws_access_key_id=storage_settings.s3_access_key,
            aws_secret_access_key=storage_settings.s3_secret_key,
            region_name=storage_settings.s3_region_name,
        )
        self.logger: structlog.BoundLogger = structlog.get_logger("storage")
        self.tracer: trace.Tracer = trace.get_tracer("storage")

    @override
    async def save_media(
        self, data: bytes, file_name: str, max_retries: int = 3
    ) -> None:
        with self.tracer.start_as_current_span("storage.upload_media") as span:
            span.set_attribute("storage.system", "s3")
            span.set_attribute(
                "storage.max_file_size_bytes", self.settings.max_file_size_bytes
            )
            span.set_attribute("storage.bucket", self.settings.s3_bucket_name)
            span.set_attribute("storage.operation", "upload")
            span.set_attribute("storage.file_name", file_name)
            span.set_attribute("storage.file_size_bytes", len(data))
            logger = self.logger.bind(
                file_name=file_name,
                file_size_bytes=len(data),
                bucket=self.settings.s3_bucket_name,
            )

            file_size = len(data)
            if file_size > self.settings.max_file_size_bytes:
                logger.error(
                    "media_too_large",
                    max_size_bytes=self.settings.max_file_size_bytes,
                    actual_size_bytes=file_size,
                )
                span.set_status(Status(StatusCode.ERROR, "Media too large"))
                raise exceptions.MediaTooLargeError(
                    file_name, self.settings.max_file_size_bytes, file_size
                )

            logger.info("uploading_media", stage="start")
            span.add_event("upload_started")

            for attempt in range(1, max_retries + 1):
                try:
                    async with self._get_client() as s3_client:
                        await s3_client.upload_fileobj(
                            BytesIO(data), self.settings.s3_bucket_name, file_name
                        )
                    logger.info(
                        "media_uploaded",
                        stage="success",
                        attempt=attempt,
                    )
                    span.add_event("upload_completed", {"attempt": attempt})
                    return

                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "Unknown")
                    if code in {"AccessDenied", "NoSuchBucket"}:
                        logger.error("s3_config_error", error_code=code, exc_info=True)
                        span.set_status(
                            Status(StatusCode.ERROR, f"S3 config error: {code}")
                        )
                        span.record_exception(e)
                        raise exceptions.ConfigError(f"S3 {code}: {e}") from e

                    wait = 2 ** (attempt - 1)
                    logger.warning(
                        "upload_retry",
                        attempt=attempt,
                        max_attempts=max_retries,
                        wait_seconds=wait,
                        error_code=code,
                    )
                    span.add_event(
                        "upload_retry",
                        {
                            "attempt": attempt,
                            "wait_seconds": wait,
                            "error_code": code,
                            "max_attempts": max_retries,
                        },
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)

                except Exception as e:
                    wait = 2 ** (attempt - 1)
                    logger.warning(
                        "upload_retry",
                        attempt=attempt,
                        max_attempts=max_retries,
                        wait_seconds=wait,
                        error_type=type(e).__name__,
                        exc_info=True,
                    )
                    span.add_event(
                        "upload_retry",
                        {
                            "attempt": attempt,
                            "wait_seconds": wait,
                            "error_type": type(e).__name__,
                            "max_attempts": max_retries,
                        },
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)

            logger.error("upload_max_retries_exceeded", max_attempts=max_retries)
            span.set_status(Status(StatusCode.ERROR, "Max retries exceeded"))
            raise exceptions.MaxRetriesExceededError(max_retries)

    @override
    async def generate_presigned_url(
        self, file_name: str, expires_seconds: int = 3600, max_retries: int = 3
    ) -> str:
        with self.tracer.start_as_current_span(
            "storage.generate_presigned_url"
        ) as span:
            span.set_attribute("storage.system", "s3")
            span.set_attribute("storage.bucket", self.settings.s3_bucket_name)
            span.set_attribute("storage.operation", "presign")
            span.set_attribute("storage.file_name", file_name)
            span.set_attribute("storage.url_expires_seconds", expires_seconds)

            logger = self.logger.bind(
                file_name=file_name,
                bucket=self.settings.s3_bucket_name,
                expires_seconds=expires_seconds,
            )

            logger.info("generating_presigned_url", stage="start")
            span.add_event("presign_started")

            for attempt in range(1, max_retries + 1):
                try:
                    async with self._get_client() as s3_client:
                        try:
                            await s3_client.head_object(
                                Bucket=self.settings.s3_bucket_name, Key=file_name
                            )
                        except ClientError as head_e:
                            code = head_e.response.get("Error", {}).get("Code")
                            if code in {"404", "NoSuchKey"}:
                                logger.error("media_not_found", file_name=file_name)
                                span.set_status(
                                    Status(StatusCode.ERROR, "Media not found")
                                )
                                raise exceptions.MediaNotFoundError(
                                    file_name
                                ) from head_e
                            raise head_e

                        url = await s3_client.generate_presigned_url(
                            ClientMethod="get_object",
                            Params={
                                "Bucket": self.settings.s3_bucket_name,
                                "Key": file_name,
                            },
                            ExpiresIn=expires_seconds,
                        )

                    logger.info(
                        "presigned_url_generated",
                        stage="success",
                        attempt=attempt,
                    )
                    span.add_event("presign_completed", {"attempt": attempt})
                    return url

                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "Unknown")
                    if code in {"AccessDenied", "NoSuchBucket"}:
                        logger.error("s3_config_error", error_code=code, exc_info=True)
                        span.set_status(
                            Status(StatusCode.ERROR, f"S3 config error: {code}")
                        )
                        span.record_exception(e)
                        raise exceptions.ConfigError(f"S3 {code}: {e}") from e
                    if code in {"404", "NoSuchKey"}:
                        logger.error("media_not_found", file_name=file_name)
                        span.set_status(Status(StatusCode.ERROR, "Media not found"))
                        raise exceptions.MediaNotFoundError(file_name) from e

                    wait = 2 ** (attempt - 1)
                    logger.warning(
                        "presign_retry",
                        attempt=attempt,
                        max_attempts=max_retries,
                        wait_seconds=wait,
                        error_code=code,
                    )
                    span.add_event(
                        "presign_retry",
                        {
                            "attempt": attempt,
                            "wait_seconds": wait,
                            "error_code": code,
                            "max_attempts": max_retries,
                        },
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)

                except Exception as e:
                    wait = 2 ** (attempt - 1)
                    logger.warning(
                        "presign_retry",
                        attempt=attempt,
                        max_attempts=max_retries,
                        wait_seconds=wait,
                        error_type=type(e).__name__,
                        exc_info=True,
                    )
                    span.add_event(
                        "presign_retry",
                        {
                            "attempt": attempt,
                            "wait_seconds": wait,
                            "error_type": type(e).__name__,
                            "max_attempts": max_retries,
                        },
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)

            logger.error("presign_max_retries_exceeded", max_attempts=max_retries)
            span.set_status(Status(StatusCode.ERROR, "Max retries exceeded"))
            raise exceptions.MaxRetriesExceededError(max_retries)

    def _get_client(self):
        return self.s3.client(
            service_name="s3", endpoint_url=self.settings.s3_endpoint_url
        )


class StorageProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> StorageSettings:
        return StorageSettings()  # type: ignore # pyright: ignore

    @provide(scope=Scope.APP)
    def storage(self, settings: StorageSettings) -> IStorage:
        return Storage(settings)
