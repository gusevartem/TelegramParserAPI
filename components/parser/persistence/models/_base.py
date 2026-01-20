from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy.orm import DeclarativeBase


class BaseModel(AsyncAttrs, DeclarativeBase):
    pass


class BaseDAO[Model: BaseModel, Id](ABC):
    def __init__(self, session: AsyncSession, model: type[Model]):
        self._session: AsyncSession = session
        self.model: type[Model] = model

    async def save(self, obj: Model) -> Model:
        self._session.add(obj)
        await self._session.flush()
        return obj

    async def find_by_id(self, id: Id) -> Model | None:
        return await self._session.get(self.model, id)

    async def list(self, skip: int = 0, limit: int | None = None) -> Sequence[Model]:
        stmt = select(self.model).offset(skip)
        if limit is not None:
            stmt = stmt.limit(limit)
        result = (await self._session.execute(stmt)).scalars().all()
        return result

    async def delete(self, id: Id) -> bool:
        obj = await self.find_by_id(id)
        if obj is None:
            return False
        await self._session.delete(obj)
        await self._session.flush()
        return True

    async def exists_by_id(self, id: Id) -> bool:
        return await self.find_by_id(id) is not None

    async def commit(self) -> None:
        await self._session.commit()

    @abstractmethod
    async def create(self, *args: Any, **kwargs: Any) -> Model:
        pass
