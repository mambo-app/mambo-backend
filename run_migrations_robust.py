import asyncio
import sys
import os
from sqlalchemy import text
from app.core.database import engine

async def run_migration(filename: str):
    print(f"Reading migration file: {filename}")
    if not os.path.exists(filename):
        print(f"Error: Migration file {filename} not found.")
        return
        
    with open(filename, 'r') as f:
        sql = f.read()
        
    async with engine.begin() as conn:
        print("Executing migration statements...")
        # Simple split by semicolon. For more complex scripts, this might need refinement.
        statements = sql.split(';')
        for stmt in statements:
            s = stmt.strip()
            if s:
                print(f"Running statement starting with: {s[:50]}...")
                await conn.execute(text(s))
        print(f"Migration {filename} completed successfully.")

async def main():
    try:
        await run_migration('migrations/20260401_create_persons_table.sql')
        await run_migration('migrations/20260401_add_person_favorites.sql')
    except Exception as e:
        print(f"MIGRATION_ERROR: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
