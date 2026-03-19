import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any
from app.core.websocket import ws_manager
from app.services.push_service import PushService

logger = logging.getLogger('mambo.notifications')

class NotificationService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def init_schema(self) -> None:
        """Ensure the notifications table has required columns."""
        try:
            # actor_id might exist but just in case
            await self.db.execute(text("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS actor_id UUID"))
            # related_id is newer
            await self.db.execute(text("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS related_id UUID"))
            # title might be NOT NULL, make it nullable
            await self.db.execute(text("ALTER TABLE notifications ALTER COLUMN title DROP NOT NULL"))
            await self.db.execute(text("ALTER TABLE notifications ALTER COLUMN type TYPE VARCHAR(50)"))
            await self.db.commit()
            logger.info("Notification schema initialized successfully")
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to initialize notification schema: {e}")

    async def get_notifications(self, user_id: str, page: int = 1, limit: int = 20) -> tuple[List[Dict], int]:
        offset = (page - 1) * limit
        # Get total
        count_res = await self.db.execute(text('''
            SELECT COUNT(*) FROM notifications 
            WHERE user_id = CAST(:user_id AS UUID) AND is_deleted = false
        '''), {'user_id': user_id})
        total = count_res.scalar() or 0

        # Get items
        res = await self.db.execute(text('''
            SELECT n.*, 
                   p.username as actor_username, 
                   p.display_name as actor_display_name, 
                   p.avatar_url as actor_avatar_url
            FROM notifications n
            LEFT JOIN profiles p ON p.id = n.actor_id
            WHERE n.user_id = CAST(:user_id AS UUID) AND n.is_deleted = false
            ORDER BY COALESCE(n.last_updated_at, n.created_at) DESC
            LIMIT :limit OFFSET :offset
        '''), {'user_id': user_id, 'limit': limit, 'offset': offset})
        
        items = []
        for row in res.mappings():
            item = dict(row)
            # Nest actor details if actor_id exists
            if item.get('actor_id'):
                item['actor'] = {
                    'id': str(item['actor_id']),
                    'username': item.get('actor_username'),
                    'display_name': item.get('actor_display_name'),
                    'avatar_url': item.get('actor_avatar_url')
                }
            items.append(item)
        return items, total

    async def get_unread_count(self, user_id: str) -> int:
        res = await self.db.execute(text('''
            SELECT COUNT(*) FROM notifications
            WHERE user_id = CAST(:uid AS UUID) AND is_read = false AND is_deleted = false
        '''), {'uid': user_id})
        return res.scalar() or 0

    async def mark_as_read(self, user_id: str, notification_id: str = None) -> bool:
        if notification_id:
            query = "UPDATE notifications SET is_read = true, read_at = now() WHERE user_id = CAST(:uid AS UUID) AND id = CAST(:nid AS UUID) AND is_read = false"
            params = {'uid': user_id, 'nid': notification_id}
        else:
            query = "UPDATE notifications SET is_read = true, read_at = now() WHERE user_id = CAST(:uid AS UUID) AND is_read = false"
            params = {'uid': user_id}
            
        res = await self.db.execute(text(query), params)
        await self.db.commit()
        return res.rowcount > 0

    async def create_notification(self, data: dict) -> None:
        """Helper to create notification and broadcast WS message."""
        # Filter data to only include valid columns (simple approach) or just trust the caller
        cols = ", ".join(data.keys())
        vals = ", ".join([f":{k}" for k in data.keys()])
        query = f"INSERT INTO notifications ({cols}) VALUES ({vals}) RETURNING id"
        
        logger.debug(f"Creating notification: {data}")
        try:
            res = await self.db.execute(text(query), data)
            new_id = res.scalar()
            await self.db.commit()
            
            # Broadcast to WS
            if new_id:
                try:
                    await ws_manager.send_personal_message(
                        f'{{"type": "new_notification", "notification_id": "{new_id}"}}', 
                        str(data['user_id'])
                    )
                except Exception as wse:
                    logger.error(f"WS broadcast failed: {wse}")
            
                # Send Push Notification
                try:
                    push_svc = PushService(self.db)
                    title = data.get('title') or "New Notification"
                    body = data.get('message') or "You have a new activity on Mambo"
                    await push_svc.send_to_user(str(data['user_id']), title, body, {
                        "type": str(data.get('type', 'general')),
                        "notification_id": str(new_id)
                    })
                except Exception as pe:
                    logger.error(f"Push notification failed: {pe}")
                    
        except Exception as e:
            await self.db.rollback()
            logger.error(f"CRITICAL: Failed to create notification: {e}. Data provided: {data}")
            raise e