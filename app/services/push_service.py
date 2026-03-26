import firebase_admin
from firebase_admin import credentials, messaging
from app.core.config import settings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

logger = logging.getLogger('mambo.push')

# Init Firebase once
try:
    cred = credentials.Certificate(settings.firebase_credentials_path)
    firebase_admin.initialize_app(cred)
except Exception as e:
    logger.error(f'Firebase init failed: {e}')

class PushService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def send_to_user(self, user_id: str, title: str, body: str,
                            data: dict = None) -> None:
        try:
            result = await self.db.execute(text('''
                SELECT token FROM push_tokens
                WHERE user_id = :user_id
            '''), {'user_id': user_id})
            row = result.fetchone()
            if not row:
                return

            message = messaging.Message(
                notification=messaging.Notification(title=title, body=body),
                data=data or {},
                token=row[0],
            )
            messaging.send(message)
        except Exception as e:
            logger.error(f'Push failed for user {user_id}: {e}')

    async def save_token(self, user_id: str, token: str, platform: str) -> None:
        await self.db.execute(text('''
            INSERT INTO push_tokens (user_id, token, platform)
            VALUES (:user_id, :token, :platform)
            ON CONFLICT (token) DO UPDATE
            SET user_id = :user_id, 
                platform = :platform,
                updated_at = now()
        '''), {'user_id': user_id, 'token': token, 'platform': platform})
        await self.db.commit()