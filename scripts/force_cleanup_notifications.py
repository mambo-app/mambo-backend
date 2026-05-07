
import asyncio
import sys
import os

# Add the parent directory to sys.path to allow importing from 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.notification_service import NotificationService

async def force_cleanup():
    print("Starting immediate notification cleanup (30-day threshold)...")
    async with AsyncSessionLocal() as db:
        service = NotificationService(db)
        deleted_count = await service.cleanup_old_notifications(days=30)
        print(f"Cleanup complete! Removed {deleted_count} stale notifications.")

if __name__ == "__main__":
    asyncio.run(force_cleanup())
