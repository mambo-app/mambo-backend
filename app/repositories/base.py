from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

class BaseRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def fetch_one(self, query: str, params: dict) -> dict | None:
        result = await self.db.execute(text(query), params)
        row = result.mappings().first()
        return dict(row) if row else None

    async def fetch_many(self, query: str, params: dict) -> list[dict]:
        result = await self.db.execute(text(query), params)
        return [dict(row) for row in result.mappings()]

    async def execute(self, query: str, params: dict) -> None:
        await self.db.execute(text(query), params)
        await self.db.commit()

    async def execute_returning(self, query: str, params: dict) -> dict:
        result = await self.db.execute(text(query), params)
        await self.db.commit()
        return dict(result.mappings().one())