from app.core.database import engine
from sqlalchemy import text
import asyncio

async def run():
    print("Creating error_logs table...")
    async with engine.begin() as conn:
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS error_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_name TEXT,
                message TEXT,
                stack_trace TEXT,
                request_id TEXT,
                path TEXT,
                method TEXT,
                status_code INTEGER DEFAULT 500,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc', now()),
                metadata JSONB
            );
        '''))
    print("Table created successfully.")

if __name__ == "__main__":
    asyncio.run(run())
