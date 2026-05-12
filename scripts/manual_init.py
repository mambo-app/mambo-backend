import asyncio
import logging
from app.core.database import AsyncSessionLocal
from app.core.init_db import init_db

# Set up basic logging to see output
logging.basicConfig(level=logging.INFO)

async def run():
    print("Starting manual database initialization...")
    async with AsyncSessionLocal() as db:
        await init_db(db)
    print("Database initialization finished.")

if __name__ == "__main__":
    asyncio.run(run())
