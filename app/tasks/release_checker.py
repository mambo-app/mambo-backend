import asyncio
import logging
import uuid
from datetime import date, datetime, timezone
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.services.notification_service import NotificationService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mambo.release_checker')

async def check_and_notify_releases():
    """
    Checks for content released TODAY and notifies:
    1. Users who clicked "Notify Me" (calendar_alerts).
    """
    logger.info(f"Starting release check for {date.today()}...")
    
    async with AsyncSessionLocal() as db:
        # 1. Get all content released TODAY (either premiere release_date or next_episode_to_air air_date)
        res = await db.execute(text('''
            SELECT id, title, poster_url, content_type, release_date, next_episode_to_air
            FROM content 
            WHERE release_date = CURRENT_DATE
               OR (next_episode_to_air IS NOT NULL AND (next_episode_to_air->>'air_date')::date = CURRENT_DATE)
        '''))
        releasing_today = res.mappings().all()
        
        if not releasing_today:
            logger.info("No content releasing today.")
            return

        notif_svc = NotificationService(db)
        
        for item in releasing_today:
            content_id = item['id']
            title = item['title']
            poster = item['poster_url']
            next_ep = item.get('next_episode_to_air')
            
            is_ep_drop = isinstance(next_ep, dict) and next_ep.get('air_date') and str(next_ep.get('air_date'))[:10] == str(date.today())
            s_num = next_ep.get('season_number') if is_ep_drop else None
            ep_num = next_ep.get('episode_number') if is_ep_drop else None

            if is_ep_drop and s_num and ep_num:
                if ep_num == 1:
                    notif_title = f"{title} Season {s_num} Out Now"
                    notif_msg = f"{title} Season {s_num} has officially premiered today!"
                else:
                    notif_title = f"{title} S{s_num} Ep {ep_num} Released"
                    notif_msg = f"{title} Season {s_num} Episode {ep_num} is out now!"
            elif is_ep_drop and ep_num:
                notif_title = f"{title} Ep {ep_num} Released"
                notif_msg = f"Episode {ep_num} of {title} is out now!"
            else:
                notif_title = f"{title} Out Now"
                notif_msg = f"{title} has officially been released today!"

            # 2. Find explicitly notified users (calendar_alerts)
            user_res = await db.execute(text('''
                SELECT user_id FROM calendar_alerts WHERE content_id = :cid
            '''), {'cid': content_id})

            users = user_res.scalars().all()
            
            logger.info(f"Notifying {len(users)} users about '{title}'")
            
            for uid in users:
                # Check if notification already created for this release today to avoid duplicates
                check_existing = await db.execute(text('''
                    SELECT id FROM notifications 
                    WHERE user_id = CAST(:uid AS UUID) 
                      AND related_id = CAST(:cid AS UUID) 
                      AND type = 'release_alert'
                      AND created_at >= CURRENT_DATE
                '''), {'uid': uid, 'cid': content_id})
                if check_existing.scalar():
                    continue

                notif_data = {
                    "id": uuid.uuid4(),
                    "user_id": uid,
                    "type": "release_alert",
                    "title": notif_title,
                    "message": notif_msg,
                    "related_id": content_id,
                    "poster_url": poster,
                    "is_read": False,
                    "created_at": datetime.now(timezone.utc)
                }
                try:
                    await notif_svc.create_notification(notif_data)
                except Exception as e:
                    logger.error(f"Failed to create release notification for user {uid}: {e}")

    logger.info("Release check completed.")

if __name__ == "__main__":
    asyncio.run(check_and_notify_releases())
