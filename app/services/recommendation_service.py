import logging
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.services.action_service import ActionService
from app.services.notification_service import NotificationService
from app.services.chat_service import ChatService
from app.core.websocket import ws_manager
import json

logger = logging.getLogger('mambo.recommendations')

class RecommendationService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.action_service = ActionService(db)
        self.notif_service = NotificationService(db)
        self.chat_service = ChatService(db)

    async def create_recommendation(self, sender_id: UUID, content_id: UUID, recipient_id: UUID, message: str = None) -> Dict:
        try:
            # 1. Insert into recommendations
            # We can now use 'user' because we updated the constraint!
            rec_stmt = text('''
                INSERT INTO recommendations (sender_id, content_id, message, recipient_type)
                VALUES (:sid, :cid, :msg, 'user')
                RETURNING id, sent_at
            ''')
            res = await self.db.execute(rec_stmt, {
                'sid': sender_id,
                'cid': content_id,
                'msg': message
            })
            
            mapping = res.mappings().first()
            if not mapping:
                raise Exception("Failed to create recommendation record: no row returned")
                
            rec = dict(mapping)
            rec_id = rec['id']
            sent_at = rec['sent_at']

            # 2. Add recipient
            await self.db.execute(text('''
                INSERT INTO recommendation_recipients (recommendation_id, recipient_id)
                VALUES (:rid, :rcid)
            '''), {'rid': rec_id, 'rcid': recipient_id})

            # 3. Get sender info and content info for notification
            sender_res = await self.db.execute(text("SELECT username, display_name FROM profiles WHERE id = :sid"), {'sid': sender_id})
            sender_mapping = sender_res.mappings().first()
            if not sender_mapping:
                raise Exception(f"Sender {sender_id} not found")
            sender_dict = dict(sender_mapping)
            sender_name = sender_dict.get('display_name') or sender_dict.get('username')

            content_res = await self.db.execute(text("SELECT title FROM content WHERE id = :cid"), {'cid': content_id})
            content_mapping = content_res.mappings().first()
            content_dict = dict(content_mapping) if content_mapping else {}
            content_title = content_dict.get('title', "Content")

            # 4. Create Notification
            notif_data = {
                'user_id': recipient_id,
                'actor_id': sender_id,
                'type': 'recommendation',
                'title': 'New Recommendation',
                'message': f"{sender_name} recommended you {content_title}",
                'related_id': content_id
            }
            await self.notif_service.create_notification(notif_data)
            
            # 5. Send Chat Message
            try:
                # Find or create a direct conversation (bypass friendship check for recommendations)
                logger.info(f"Creating conversation between {sender_id} and {recipient_id}")
                conv_id = await self.chat_service.get_or_create_direct_conversation(
                    str(sender_id), str(recipient_id), bypass_friendship_check=True
                )
                logger.info(f"Conversation created/found: {conv_id}")
                
                chat_body = f"I'm recommending this to you: {content_title}"
                if message:
                    chat_body += f"\n\n'{message}'"
                    
                await self.chat_service.send_message(
                    user_id=str(sender_id),
                    conversation_id=conv_id,
                    body=chat_body,
                    receiver_id=str(recipient_id),
                    shared_content_id=content_id,
                    bypass_friendship_check=True
                )
                logger.info(f"Chat message sent in conversation {conv_id}")
            except Exception as ce:
                import traceback
                logger.error(f"Failed to send recommendation chat message: {ce}\n{traceback.format_exc()}")
                # Don't fail the whole recommendation if chat fails
                pass

            # 6. Log activities
            # We can now use 'receive_recommendation' because we updated the constraint!
            await self.action_service._log_activity(
                user_id=recipient_id,
                activity_type='receive_recommendation',
                content_id=content_id,
                related_user_id=sender_id,
                details={'recommendation_id': str(rec_id)}
            )
            
            await self.action_service._log_activity(
                user_id=sender_id,
                activity_type='send_recommendation',
                content_id=content_id,
                related_user_id=recipient_id,
                details={'recommendation_id': str(rec_id)}
            )

            await self.db.commit()

            return {
                "id": str(rec_id),
                "sender_id": str(sender_id),
                "content_id": str(content_id),
                "sent_at": sent_at.isoformat(),
                "message": message,
                "recipient_id": str(recipient_id),
                "is_viewed": False
            }
        except Exception as e:
            await self.db.rollback()
            logger.exception("create_recommendation_failed")
            raise e

    async def get_received_recommendations(self, user_id: UUID) -> List[Dict]:
        query = text('''
            WITH RankedRecs AS (
                SELECT r.*, rr.recipient_id, rr.is_viewed, rr.viewed_at,
                        p.username as actor_username, p.display_name as actor_display_name,
                        p.avatar_url as actor_avatar_url,
                        c.title as content_title, c.poster_url as content_poster_url, c.content_type,
                        ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY r.sent_at DESC) as rn
                FROM recommendations r
                JOIN recommendation_recipients rr ON r.id = rr.recommendation_id
                JOIN profiles p ON r.sender_id = p.id
                JOIN content c ON r.content_id = c.id
                WHERE rr.recipient_id = :uid
            )
            SELECT * FROM RankedRecs WHERE rn = 1
            ORDER BY sent_at DESC
        ''')
        res = await self.db.execute(query, {'uid': user_id})
        return [dict(r) for r in res.mappings()]
