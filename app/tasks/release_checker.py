import asyncio
import logging
from datetime import date
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.services.push_service import PushService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mambo.release_checker')

async def check_and_notify_releases():
    """
    Checks for content released TODAY and notifies:
    1. Users who have it in their Watchlist.
    2. Users who clicked "Notify Me" (calendar_alerts).
    """
    logger.info(f"Starting release check for {date.today()}...")
    
    async with AsyncSessionLocal() as db:
        # 1. Get all content released TODAY
        # We check content where release_date is today
        res = await db.execute(text('''
            SELECT id, title, poster_url, content_type 
            FROM content 
            WHERE release_date = CURRENT_DATE
        '''))
        releasing_today = res.mappings().all()
        
        if not releasing_today:
            logger.info("No content releasing today.")
            return

        push_svc = PushService(db)
        
        for item in releasing_today:
            content_id = item['id']
            title = item['title']
            poster = item['poster_url']
            
            # 2. Find explicitly notified users
            # We ONLY notify users who explicitly clicked "Notify Me" (calendar_alerts)
            user_res = await db.execute(text('''
                SELECT user_id FROM calendar_alerts WHERE content_id = :cid
            '''), {'cid': content_id})

            
            users = user_res.scalars().all()
            
            logger.info(f"Notifying {len(users)} users about '{title}'")
            
            for uid in users:
                await push_svc.send_to_user(
                    str(uid),
                    title="🎉 Out Now!",
                    body=f"'{title}' has been released today. Check it out on Mambo!",
                    image_url=poster,
                    data={
                        "type": "release_alert",
                        "content_id": str(content_id)
                    }
                )

    logger.info("Release check completed.")

if __name__ == "__main__":
    asyncio.run(check_and_notify_releases())
