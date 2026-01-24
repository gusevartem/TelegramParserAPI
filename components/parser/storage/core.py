import asyncio
import time
from io import BytesIO
from logging import Logger, getLogger
from typing import Protocol, override

from aioboto3 import Session
from botocore.exceptions import ClientError
from dishka import Provider, Scope, provide

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
        self.storage_settings: StorageSettings = storage_settings
        self.s3: Session = Session(
            aws_access_key_id=storage_settings.s3_access_key,
            aws_secret_access_key=storage_settings.s3_secret_key,
            region_name=storage_settings.s3_region_name,
        )
        self.logger: Logger = getLogger(__name__)

    @override
    async def save_media(
        self, data: bytes, file_name: str, max_retries: int = 3
    ) -> None:
        self.logger.info(f"⌛ Uploading file: {file_name} ({len(data)} bytes)")

        start_time = time.perf_counter()
        file_size = len(data)
        if file_size > self.storage_settings.max_file_size_bytes:
            raise exceptions.MediaTooLargeError(
                file_name, self.storage_settings.max_file_size_bytes, file_size
            )

        for attempt in range(max_retries):
            try:
                async with self._get_client() as s3_client:
                    await s3_client.upload_fileobj(
                        BytesIO(data), self.storage_settings.s3_bucket_name, file_name
                    )
                duration = (time.perf_counter() - start_time) * 1000

                self.logger.info(
                    f"✅ Uploaded file: {file_name} ({len(data)} bytes). "
                    + f"Duration: {duration:.0f}ms"
                )
                return

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code == "AccessDenied":
                    raise exceptions.ConfigError(f"S3 Access Denied: {e}")
                elif code == "NoSuchBucket":
                    raise exceptions.ConfigError(
                        f"Bucket {self.storage_settings.s3_bucket_name} not found"
                    )
                else:
                    wait = 2**attempt
                    self.logger.warning(
                        f"⚠️ S3 Error {code}. "
                        + f"Retry {attempt + 1}/{max_retries} after {wait}s"
                    )
                    await asyncio.sleep(wait)

            except Exception as e:
                wait = 2**attempt
                self.logger.warning(
                    f"⚠️ Error while trying to upload file ({type(e).__name__}). "
                    + f"Retry {attempt + 1}/{max_retries} after {wait}s. "
                    + f"Error: {e}"
                )
                await asyncio.sleep(wait)

        raise exceptions.MaxRetriesExceededError(max_retries)

    @override
    async def generate_presigned_url(
        self, file_name: str, expires_seconds: int = 3600, max_retries: int = 3
    ) -> str:
        self.logger.info(f"⌛ Generating presigned url for media {file_name}")

        start_time = time.perf_counter()
        for attempt in range(max_retries):
            try:
                async with self._get_client() as s3_client:
                    try:
                        await s3_client.head_object(
                            Bucket=self.storage_settings.s3_bucket_name, Key=file_name
                        )
                    except ClientError as e:
                        if e.response.get("Error", {}).get("Code") == "404":
                            raise exceptions.MediaNotFoundError(file_name)
                        raise e

                    url = await s3_client.generate_presigned_url(
                        ClientMethod="get_object",
                        Params={
                            "Bucket": self.storage_settings.s3_bucket_name,
                            "Key": file_name,
                        },
                        ExpiresIn=expires_seconds,
                    )

                duration = (time.perf_counter() - start_time) * 1000
                self.logger.info(
                    f"✅ Generated presigned url for media {file_name}. "
                    + f"Duration: {duration:.0f}ms"
                )
                return url

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")

                if code == "AccessDenied":
                    raise exceptions.ConfigError(f"S3 Access Denied: {e}")
                elif code == "NoSuchBucket":
                    raise exceptions.ConfigError(
                        f"Bucket {self.storage_settings.s3_bucket_name} not found"
                    )
                elif code == "NoSuchKey" or code == "404":
                    raise exceptions.MediaNotFoundError(file_name)
                else:
                    wait = 2**attempt
                    self.logger.warning(
                        f"⚠️ S3 Error {code} during URL gen. "
                        + f"Retry {attempt + 1}/{max_retries} after {wait}s"
                    )
                    await asyncio.sleep(wait)

            except Exception as e:
                wait = 2**attempt
                self.logger.warning(
                    f"⚠️ Error generating URL ({type(e).__name__}). "
                    + f"Retry {attempt + 1}/{max_retries} after {wait}s. "
                    + f"Error: {e}"
                )
                await asyncio.sleep(wait)

        raise exceptions.MaxRetriesExceededError(max_retries)

    def _get_client(self):
        return self.s3.client(
            service_name="s3", endpoint_url=self.storage_settings.s3_endpoint_url
        )


class StorageProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> StorageSettings:
        return StorageSettings()  # type: ignore # pyright: ignore

    @provide(scope=Scope.APP)
    def storage(self, settings: StorageSettings) -> IStorage:
        return Storage(settings)
