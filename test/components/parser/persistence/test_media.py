import uuid

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import MediaDAO
from sqlalchemy.exc import IntegrityError


@pytest_asyncio.fixture
async def media_dao(request_container: AsyncContainer) -> MediaDAO:
    return await request_container.get(MediaDAO)


@pytest.mark.asyncio
class TestMediaCreateRead:
    """Tests basic CRUD operations, isolated in its own class scope DB."""

    async def test_create_and_read_media(
        self,
        media_dao: MediaDAO,
    ):
        # 1. Create media
        media_id = uuid.uuid4()
        media = await media_dao.create(
            id=media_id,
            mime_type="image/jpeg",
            size_bytes=1024,
            file_name="test.jpg",
        )
        await media_dao.commit()
        await media_dao.refresh(media)

        assert media.id == media_id
        assert media.mime_type == "image/jpeg"
        assert media.size_bytes == 1024
        assert media.file_name == "test.jpg"
        assert media.recorded_at is not None

        # 2. Read back the media
        fetched = await media_dao.find_by_id(media.id)
        assert fetched is not None
        assert fetched.id == media_id
        assert fetched.mime_type == "image/jpeg"
        assert fetched.size_bytes == 1024
        assert fetched.file_name == "test.jpg"
        assert fetched.recorded_at == media.recorded_at

        # 3. Read back non-existent media
        not_found = await media_dao.find_by_id(uuid.uuid4())
        assert not_found is None

    async def test_exists_by_id(self, media_dao: MediaDAO):
        media_id = uuid.uuid4()
        exists_before = await media_dao.exists_by_id(media_id)
        assert exists_before is False

        await media_dao.create(
            id=media_id,
            mime_type="video/mp4",
            size_bytes=2048,
            file_name="video.mp4",
        )
        await media_dao.commit()

        exists_after = await media_dao.exists_by_id(media_id)
        assert exists_after is True


@pytest.mark.asyncio
class TestMediaConstraints:
    """Tests constraints of media records."""

    async def test_unique_media_uuid_constraint(
        self,
        media_dao: MediaDAO,
    ):
        media_id = uuid.uuid4()
        await media_dao.create(
            id=media_id,
            mime_type="image/gif",
            size_bytes=512,
            file_name="test1.gif",
        )
        await media_dao.commit()

        # Try to insert another media with same ID
        with pytest.raises(IntegrityError):
            await media_dao.create(
                id=media_id,
                mime_type="image/gif",
                size_bytes=1024,
                file_name="test2.gif",
            )
            # Depending on DB / config error can happen on flush via create or commit
            await media_dao.commit()


@pytest.mark.asyncio
class TestMediaImmutability:
    """Tests the immutability constraint of media records."""

    async def test_media_immutability(
        self,
        media_dao: MediaDAO,
    ):
        media_id = uuid.uuid4()
        media = await media_dao.create(
            id=media_id,
            mime_type="image/png",
            size_bytes=2048,
            file_name="test.png",
        )
        await media_dao.commit()

        # Try to modify
        media.size_bytes = 4096
        with pytest.raises(Exception, match="Media records are immutable"):
            await media_dao.commit()


@pytest.mark.asyncio
class TestMediaList:
    """Tests the listing and bulk reading functionalities."""

    async def test_list_media(
        self,
        media_dao: MediaDAO,
    ):
        # Create multiple medias
        for i in range(5):
            await media_dao.create(
                id=uuid.uuid4(),
                mime_type=f"image/jpeg{i}",
                size_bytes=100 * (i + 1),
                file_name=f"test{i}.jpg",
            )
        await media_dao.commit()

        # List them
        medias = await media_dao.list()
        assert len(medias) == 5

        # Test offset and limit
        limited_medias = await media_dao.list(skip=2, limit=2)
        assert len(limited_medias) == 2
