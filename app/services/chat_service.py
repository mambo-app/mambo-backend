import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from uuid import UUID
from datetime import datetime
from typing import List, Dict, Any, Optional
from app.core.websocket import ws_manager

logger = logging.getLogger('mambo.chat')

class ChatService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def init_schema(self):
        """Ensure the conversations table has the direct_pair_key and unique constraint."""
        # Check if column exists
        res = await self.db.execute(text('''
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'conversations' AND column_name = 'direct_pair_key'
        '''))
        if not res.fetchone():
            logger.info("Adding direct_pair_key to conversations table")
            await self.db.execute(text("ALTER TABLE conversations ADD COLUMN direct_pair_key TEXT"))
            await self.db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_direct_pair ON conversations(direct_pair_key)"))
            await self.db.commit()
        
        # Add shared_content_id to messages
        res = await self.db.execute(text('''
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'messages' AND column_name = 'shared_content_id'
        '''))
        if not res.fetchone():
            logger.info("Adding shared_content_id to messages table")
            await self.db.execute(text("ALTER TABLE messages ADD COLUMN shared_content_id UUID"))
            await self.db.commit()

    async def get_conversations(self, user_id: str) -> List[Dict]:
        res = await self.db.execute(text('''
            SELECT c.*, 
                   (SELECT COUNT(*) FROM messages m 
                    WHERE m.conversation_id = c.id 
                    AND m.sender_id != :uid 
                    AND m.is_read = false) as unread_count
            FROM conversations c
            JOIN conversation_members cm ON cm.conversation_id = c.id
            WHERE cm.user_id = :uid
            ORDER BY c.last_message_at DESC NULLS LAST
        '''), {'uid': user_id})
        
        conversations = [dict(row) for row in res.mappings()]
        
        for c in conversations:
            # 1. Fetch last message if exists
            if c.get("last_message_id"):
                msg_res = await self.db.execute(text("SELECT * FROM messages WHERE id = :mid"), {'mid': c["last_message_id"]})
                row = msg_res.mappings().first()
                c["last_message"] = dict(row) if row else None
            else:
                c["last_message"] = None
                
            # 2. For direct conversations, fetch the OTHER user's profile
            if c.get("conversation_type") == 'direct':
                other_res = await self.db.execute(text('''
                    SELECT p.* 
                    FROM conversation_members cm
                    JOIN profiles p ON p.id = cm.user_id
                    WHERE cm.conversation_id = :cid AND cm.user_id != :uid
                    LIMIT 1
                '''), {'cid': c["id"], 'uid': user_id})
                other_row = other_res.mappings().first()
                if other_row:
                    other_user = dict(other_row)
                    # Convert UUIDs/dates to strings for JSON
                    for k, v in other_user.items():
                        if not isinstance(v, (str, int, float, bool, type(None))):
                            other_user[k] = str(v)
                    c["other_user"] = other_user
                else:
                    c["other_user"] = None
            else:
                c["other_user"] = None
                
        return conversations

    async def get_messages(self, conversation_id: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        res = await self.db.execute(text('''
            SELECT m.*, 
                   p.title as post_title,
                   r.text_review as review_text, r.rating as review_stars,
                   c.title as content_title, c.poster_url as content_poster, c.content_type
            FROM messages m
            LEFT JOIN posts p ON p.id = m.shared_post_id
            LEFT JOIN reviews r ON r.id = m.shared_review_id
            LEFT JOIN content c ON c.id = m.shared_content_id
            WHERE m.conversation_id = :cid
            ORDER BY m.sent_at DESC
            LIMIT :limit OFFSET :offset
        '''), {'cid': conversation_id, 'limit': limit, 'offset': offset})
        
        messages = [dict(row) for row in res.mappings()]
        logger.info(f"get_messages for {conversation_id}: found {len(messages)} items")
        for m in messages:
            try:
                # Enrich with a unified 'shared_meta' for the frontend
                if m.get('shared_post_id'):
                    m['shared_meta'] = {
                        'title': m.get('post_title'),
                        'image_url': m.get('image_url'), # Note: we still have image_url in messages table?
                        'type': 'post',
                        'id': str(m['shared_post_id'])
                    }
                elif m.get('shared_review_id'):
                    m['shared_meta'] = {
                        'title': f"Review for {m.get('post_title') or 'Content'}",
                        'text': m.get('review_text'),
                        'stars': m.get('review_stars'),
                        'type': 'review',
                        'id': str(m['shared_review_id'])
                    }
                elif m.get('shared_content_id'):
                    m['shared_meta'] = {
                        'title': m.get('content_title'),
                        'image_url': m.get('content_poster'),
                        'type': m.get('content_type', 'movie'),
                        'id': str(m['shared_content_id'])
                    }
            except Exception as e:
                logger.error(f"Error enriching message {m.get('id')}: {e}")
        
        return messages

    async def mark_as_read(self, user_id: str, conversation_id: str) -> bool:
        await self.db.execute(text('''
            UPDATE messages 
            SET is_read = true, read_at = now()
            WHERE conversation_id = :cid 
            AND sender_id != :uid 
            AND is_read = false
        '''), {'cid': conversation_id, 'uid': user_id})
        
        await self.db.commit()
        return True

    async def search_messages(self, user_id: str, conversation_id: str, query: str) -> list:
        res = await self.db.execute(text('''
            SELECT * FROM messages 
            WHERE conversation_id = :cid 
            AND (body ILIKE :q)
            ORDER BY sent_at DESC
        '''), {'cid': conversation_id, 'q': f'%{query}%'})
        return [dict(r) for r in res.mappings()]

    async def send_message(self, user_id: str, conversation_id: str, body: str, receiver_id: str = None, 
                           shared_post_id: UUID = None, shared_review_id: UUID = None, 
                           shared_content_id: UUID = None, bypass_friendship_check: bool = False) -> dict:
        query = """
            INSERT INTO messages (conversation_id, sender_id, receiver_id, body, message_type, 
                               shared_post_id, shared_review_id, shared_content_id)
            VALUES (:cid, :sid, :rid, :body, :mtype, :spid, :srid, :scid)
            RETURNING *
        """
        
        if shared_post_id:
            message_type = 'post_share'
        elif shared_review_id:
            message_type = 'review_share'
        elif shared_content_id:
            message_type = 'content_share'
        else:
            message_type = 'text'
        
        # Check friendship for direct messages
        if receiver_id and str(user_id) != str(receiver_id) and not bypass_friendship_check:
            u1, u2 = sorted([str(user_id), str(receiver_id)])
            res = await self.db.execute(text('SELECT 1 FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2'), {'u1': u1, 'u2': u2})
            if not res.fetchone():
                raise ValueError("Messaging is only allowed between friends")
        
        # Avoid self-messaging if there's a DB constraint (usually sender != receiver)
        if receiver_id and str(user_id) == str(receiver_id):
            # We skip the message insertion but return a dummy result
            return {"id": None, "conversation_id": conversation_id, "sent_at": datetime.utcnow()}

        res = await self.db.execute(text(query), {
            'cid': conversation_id,
            'sid': user_id,
            'rid': receiver_id,
            'body': body,
            'mtype': message_type,
            'spid': shared_post_id,
            'srid': shared_review_id,
            'scid': shared_content_id
        })
        msg = dict(res.mappings().first())
        
        await self.db.execute(text("""
            UPDATE conversations 
            SET last_message_id = :mid, last_message_at = :sent_at, updated_at = now()
            WHERE id = :cid
        """), {'mid': msg['id'], 'sent_at': msg['sent_at'], 'cid': conversation_id})
        
        await self.db.commit()
        
        # Websocket notification (reusing logic but ensuring UUIDs are strings)
        import json
        msg_str = msg.copy()
        for k, v in msg_str.items():
            if hasattr(v, '__str__') and not isinstance(v, (str, int, float, bool, type(None))):
                msg_str[k] = str(v)
        if msg_str.get('sent_at') and hasattr(msg['sent_at'], 'isoformat'): 
            msg_str['sent_at'] = msg['sent_at'].isoformat()
        
        payload = json.dumps({"type": "new_message", "message": msg_str})
        
        if receiver_id:
            await ws_manager.send_personal_message(payload, str(receiver_id))
        await ws_manager.send_personal_message(payload, str(user_id))
            
        return msg

    async def get_or_create_direct_conversation(self, user_id1: str, user_id2: str, bypass_friendship_check: bool = False) -> str:
        """Find an existing 1:1 conversation or create a new one, handling race conditions."""
        u1, u2 = sorted([str(user_id1), str(user_id2)])
        # 0. Check friendship (skip if bypass_friendship_check=True, e.g. for recommendations)
        if not bypass_friendship_check and u1 != u2:
            res = await self.db.execute(text('SELECT 1 FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2'), {'u1': u1, 'u2': u2})
            if not res.fetchone():
                # Check if conversation already exists (allow if already exists)
                res = await self.db.execute(text('SELECT id FROM conversations WHERE direct_pair_key = :key'), {'key': f"{u1}:{u2}"})
                if not res.fetchone():
                    raise ValueError("Messaging is only allowed between friends")

        # 1. Try to find existing by key
        key = f"{u1}:{u2}"
        res = await self.db.execute(text('SELECT id FROM conversations WHERE direct_pair_key = :key'), {'key': key})
        cid = res.scalar()
        if cid:
            return str(cid)
            
        # 2. Try to insert with ON CONFLICT (idempotent creation)
        try:
            # We use a subquery or just check after insert to be sure we get the ID
            res = await self.db.execute(text('''
                INSERT INTO conversations (conversation_type, direct_pair_key, created_by, updated_at) 
                VALUES ('direct', :key, :uid, now())
                ON CONFLICT (direct_pair_key) DO UPDATE SET updated_at = now()
                RETURNING id
            '''), {'key': key, 'uid': user_id1})
            new_id = res.scalar()
            
            if not new_id:
                # Fallback: fetch again
                res = await self.db.execute(text('SELECT id FROM conversations WHERE direct_pair_key = :key'), {'key': key})
                new_id = res.scalar()
            
            if not new_id:
                raise ValueError("Failed to create or find conversation")

            # 3. Add members
            # If u1 == u2, we only add one member record
            member_ids = [u1] if u1 == u2 else [u1, u2]
            for uid in member_ids:
                await self.db.execute(text('''
                    INSERT INTO conversation_members (conversation_id, user_id)
                    VALUES (:cid, :uid)
                    ON CONFLICT DO NOTHING
                '''), {'cid': new_id, 'uid': uid})
            
            await self.db.commit()
            return str(new_id)
        except Exception as e:
            await self.db.rollback()
            # If fail, try one last fetch
            res = await self.db.execute(text('SELECT id FROM conversations WHERE direct_pair_key = :key'), {'key': key})
            final_id = res.scalar()
            if not final_id:
                raise e # Re-raise if we really can't find it
            return str(final_id)
