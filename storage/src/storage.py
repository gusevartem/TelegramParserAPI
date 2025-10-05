import io
from aioboto3 import Session
from shared_models.storage.save_logo import SaveLogoRequest
from shared_models.storage.get_logo import GetLogoRequest, GetLogoResponse
from shared_models.storage.save_media import SaveMediaRequest
from shared_models.storage.get_media import GetMediaRequest, GetMediaResponse
from shared_models.storage.errors import LogoNotFoundError, MediaNotFoundError
import logging


class Storage:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region_name: str,
        endpoint_url: str,
    ):
        self.session = Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.logger = logging.getLogger("storage")

    def get_client(self):
        return self.session.client(service_name="s3", endpoint_url=self.endpoint_url)

    # Methods
    @staticmethod
    async def save_logo(ctx, request: SaveLogoRequest):
        self: Storage = ctx["Storage_instance"]
        async with self.get_client() as s3_client:  # type: ignore
            file_obj = io.BytesIO(request.logo)
            await s3_client.upload_fileobj(
                file_obj, self.bucket_name, f"{request.channel_id}.jpg"
            )

    @staticmethod
    async def get_logo(ctx, request: GetLogoRequest) -> GetLogoResponse:
        self: Storage = ctx["Storage_instance"]
        async with self.get_client() as s3_client:  # type: ignore
            file_obj = io.BytesIO()
            try:
                await s3_client.download_fileobj(
                    self.bucket_name, f"{request.channel_id}.jpg", file_obj
                )
                file_obj.seek(0)
                return GetLogoResponse(logo=file_obj.read())
            except Exception as e:
                self.logger.error(f"Error while downloading file: {e}")
                raise LogoNotFoundError(request.channel_id)

    @staticmethod
    async def save_media(ctx, request: SaveMediaRequest):
        self: Storage = ctx["Storage_instance"]
        async with self.get_client() as s3_client:  # type: ignore
            file_obj = io.BytesIO(request.media)
            await s3_client.upload_fileobj(
                file_obj, self.bucket_name, f"media/{request.media_id}"
            )

    @staticmethod
    async def get_media(ctx, request: GetMediaRequest) -> GetMediaResponse:
        self: Storage = ctx["Storage_instance"]
        async with self.get_client() as s3_client:  # type: ignore
            file_obj = io.BytesIO()
            try:
                await s3_client.download_fileobj(
                    self.bucket_name, f"media/{request.media_id}", file_obj
                )
                file_obj.seek(0)
                return GetMediaResponse(media=file_obj.read())
            except Exception as e:
                self.logger.error(f"Error while downloading file: {e}")
                raise MediaNotFoundError(request.media_id)
