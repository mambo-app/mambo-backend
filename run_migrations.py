import asyncio
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
        # Split by semicolon but be careful with functions/triggers if any (MAMBO's script uses simple CREATE TABLE)
        statements = sql.split(';')
        for stmt in statements:
            s = stmt.strip()
            if s:
                print(f"Running: {s[:50]}...")
                await conn.execute(text(s))
        print("Migration completed successfully.")

if __name__ == "__main__":
    # Run the persons table migration
    asyncio.run(run_migration('migrations/20260401_create_persons_table.sql'))
    # Also run the favorites table migration just in case
    asyncio.run(run_migration('migrations/20260401_add_person_favorites.sql'))
