from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Any, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_sessionmaker
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

    async def refresh(self, obj: Model) -> Model:
        await self._session.refresh(obj)
        return obj

    async def find_by_id(self, id: Id) -> Model | None:
        return await self._session.get(self.model, id)

    async def list(self, skip: int = 0, limit: int | None = None) -> Sequence[Model]:
        stmt = select(self.model).offset(skip)
        if limit is not None:
            stmt = stmt.limit(limit)
        result = (await self._session.execute(stmt)).scalars().all()
        return result

    async def exists_by_id(self, id: Id) -> bool:
        return await self.find_by_id(id) is not None

    async def commit(self) -> None:
        await self._session.commit()

    async def flush(self) -> None:
        await self._session.flush()

    async def rollback(self) -> None:
        await self._session.rollback()

    @abstractmethod
    async def create(self, *args: Any, **kwargs: Any) -> Model:
        pass


class BaseDAOFactory[DAO: BaseDAO[Any, Any]]:
    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        dao_cls: Callable[[AsyncSession], DAO],
    ) -> None:
        self._session_maker: async_sessionmaker[AsyncSession] = session_maker
        self.dao_cls: Callable[[AsyncSession], DAO] = dao_cls

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[DAO]:
        async with self._session_maker() as session:
            yield self.dao_cls(session)

    def __new__(cls, *args: Any, **kwargs: Any):
        if cls is BaseDAOFactory:
            raise TypeError(f"Only subclasses of {cls.__name__} can be instantiated")
        return super().__new__(cls)


T_DAO = TypeVar("T_DAO", bound=BaseDAO[Any, Any])


class _DAOFactory:
    def __init__(self, session: AsyncSession) -> None:
        self._session: AsyncSession = session

    def __call__(self, dao_cls: Callable[[AsyncSession], T_DAO]) -> T_DAO:
        return dao_cls(self._session)

    async def commit(self) -> None:
        await self._session.commit()


class MultipleDAOFactory:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self._session_maker: async_sessionmaker[AsyncSession] = session_maker

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[_DAOFactory]:
        async with self._session_maker() as session:
            yield _DAOFactory(session)
