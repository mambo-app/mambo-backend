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
        uid_obj = UUID(user_id) if isinstance(user_id, str) else user_id
        res = await self.db.execute(text('''
            SELECT c.*, 
                   (SELECT COUNT(*) FROM messages m 
                    WHERE m.conversation_id = c.id 
                    AND m.sender_id != :uid 
                    AND m.is_read = false) as unread_count,
                   m.id as msg_id, m.sender_id as msg_sender_id, m.body as msg_content,
                   m.message_type as msg_type, m.sent_at as msg_created_at, m.shared_meta as msg_shared_meta,
                   p.id as other_id, p.username as other_username, p.display_name as other_display_name,
                   p.avatar_url as other_avatar_url, p.bio as other_bio
            FROM conversations c
            JOIN conversation_members cm ON cm.conversation_id = c.id
            LEFT JOIN messages m ON m.id = c.last_message_id
            LEFT JOIN conversation_members cm_other ON cm_other.conversation_id = c.id AND cm_other.user_id != :uid AND c.conversation_type = 'direct'
            LEFT JOIN profiles p ON p.id = cm_other.user_id
            WHERE cm.user_id = :uid
            ORDER BY c.last_message_at DESC NULLS LAST
        '''), {'uid': uid_obj})
        
        conversations = []
        for row in res.mappings():
            c = dict(row)
            # Extract last_message sub-object
            if c.get("msg_id"):
                iso_time = c["msg_created_at"].isoformat() if hasattr(c.get("msg_created_at"), "isoformat") else c.get("msg_created_at")
                c["last_message"] = {
                    "id": str(c["msg_id"]),
                    "conversation_id": str(c["id"]),
                    "sender_id": str(c["msg_sender_id"]) if c.get("msg_sender_id") else None,
                    "body": c.get("msg_content"),
                    "content": c.get("msg_content"),
                    "message_type": c.get("msg_type", "text"),
                    "type": c.get("msg_type", "text"),
                    "sent_at": iso_time,
                    "created_at": iso_time,
                    "shared_meta": c.get("msg_shared_meta"),
                }
            else:
                c["last_message"] = None

            # Extract other_user sub-object
            if c.get("conversation_type") == "direct" and c.get("other_id"):
                c["other_user"] = {
                    "id": str(c["other_id"]),
                    "username": c.get("other_username"),
                    "display_name": c.get("other_display_name"),
                    "avatar_url": c.get("other_avatar_url"),
                    "bio": c.get("other_bio"),
                }
            else:
                c["other_user"] = None

            # Cleanup flat joined fields
            for k in ["msg_id", "msg_sender_id", "msg_content", "msg_type", "msg_created_at", "msg_shared_meta",
                      "other_id", "other_username", "other_display_name", "other_avatar_url", "other_bio"]:
                c.pop(k, None)

            conversations.append(c)

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
                raw_sm = m.get('shared_meta')
                meta = {}
                if isinstance(raw_sm, str):
                    import json
                    try: meta = json.loads(raw_sm)
                    except Exception: meta = {}
                elif isinstance(raw_sm, dict):
                    meta = dict(raw_sm)

                if m.get('shared_post_id'):
                    meta.update({
                        'title': meta.get('title') or m.get('post_title'),
                        'image_url': meta.get('image_url') or m.get('image_url'),
                        'type': 'post',
                        'id': str(m['shared_post_id'])
                    })
                elif m.get('shared_review_id'):
                    meta.update({
                        'title': meta.get('title') or f"Review for {m.get('post_title') or 'Content'}",
                        'text': meta.get('text') or m.get('review_text'),
                        'stars': meta.get('stars') or m.get('review_stars'),
                        'type': 'review',
                        'id': str(m['shared_review_id'])
                    })
                elif m.get('shared_content_id') or meta:
                    title_val = meta.get('title') or m.get('content_title')
                    if not title_val or title_val == 'None' or title_val == m.get('body'):
                        title_val = m.get('content_title') or meta.get('title')
                    if not title_val or title_val == 'None':
                        title_val = 'Shared Content'
                    
                    poster_val = meta.get('image_url') or m.get('content_poster') or ''
                    
                    body_txt = m.get('body') or ''
                    is_rec = m.get('message_type') == 'recommendation' or "I think you'd love" in body_txt
                    is_attach = meta.get('is_attachment', not is_rec)
                    
                    meta.update({
                        'title': title_val,
                        'image_url': poster_val,
                        'type': meta.get('type') or m.get('content_type') or 'series' if meta.get('season_number') else 'movie',
                        'id': meta.get('id') or str(m.get('shared_content_id', '')),
                        'is_attachment': is_attach
                    })

                if meta:
                    m['shared_meta'] = meta
            except Exception as e:
                logger.error(f"Error enriching message {m.get('id')}: {e}")
        
        return messages

    async def mark_as_read(self, user_id: str, conversation_id: str) -> bool:
        await self.db.execute(text('''
            UPDATE messages 
            SET is_read = true, read_at = now()
            WHERE conversation_id = :cid 
              AND receiver_id = :uid 
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
                           shared_content_id: UUID = None, shared_meta: dict = None,
                           bypass_friendship_check: bool = False) -> dict:
        query = """
            INSERT INTO messages (conversation_id, sender_id, receiver_id, body, message_type, 
                               shared_post_id, shared_review_id, shared_content_id, shared_meta)
            VALUES (:cid, :sid, :rid, :body, :mtype, :spid, :srid, :scid, CAST(:sm AS JSONB))
            RETURNING *
        """
        import json
        sm_json = json.dumps(shared_meta) if shared_meta else None

        def _safe_uuid(val):
            if not val: return None
            try: return str(UUID(str(val)))
            except Exception: return None

        valid_spid = _safe_uuid(shared_post_id)
        valid_srid = _safe_uuid(shared_review_id)
        valid_scid = _safe_uuid(shared_content_id)

        if valid_spid:
            message_type = 'post_share'
        elif valid_srid:
            message_type = 'review_share'
        elif shared_content_id:
            message_type = 'content_share'
        elif shared_meta and (shared_meta.get('type') == 'collection' or 'collection_id' in shared_meta):
            message_type = 'collection'
        else:
            message_type = 'text'
        
        # Auto lookup receiver_id from conversation if not supplied
        if not receiver_id and conversation_id:
            try:
                uid_obj = UUID(str(user_id))
                cid_obj = UUID(str(conversation_id))
                c_res = await self.db.execute(
                    text("SELECT user_id FROM conversation_members WHERE conversation_id = :cid AND user_id != :uid"),
                    {"cid": cid_obj, "uid": uid_obj}
                )
                c_row = c_res.mappings().first()
                if c_row:
                    receiver_id = str(c_row['user_id'])
            except Exception as ex:
                logger.warning(f"Failed to lookup receiver_id for conversation {conversation_id}: {ex}")

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

        try:
            async with self.db.begin_nested():
                res = await self.db.execute(text(query), {
                    'cid': conversation_id,
                    'sid': user_id,
                    'rid': receiver_id,
                    'body': body,
                    'mtype': message_type,
                    'spid': valid_spid,
                    'srid': valid_srid,
                    'scid': valid_scid,
                    'sm': sm_json
                })
        except Exception as e:
            logger.warning(f"Failed inserting message with shared_meta: {e}. Fixing table schema/constraints and retrying.")
            try:
                await self.db.execute(text("ALTER TABLE public.messages DROP CONSTRAINT IF EXISTS messages_message_type_check"))
                await self.db.execute(text("ALTER TABLE public.messages ADD COLUMN IF NOT EXISTS shared_meta JSONB"))
            except Exception as alter_err:
                logger.warning(f"Could not update messages table schema/constraint: {alter_err}")
            
            res = await self.db.execute(text(query), {
                'cid': conversation_id,
                'sid': user_id,
                'rid': receiver_id,
                'body': body,
                'mtype': message_type,
                'spid': valid_spid,
                'srid': valid_srid,
                'scid': valid_scid,
                'sm': sm_json
            })
        msg = dict(res.mappings().first())
        
        await self.db.execute(text("""
            UPDATE conversations 
            SET last_message_id = :mid, last_message_at = :sent_at, updated_at = now()
            WHERE id = :cid
        """), {'mid': msg['id'], 'sent_at': msg['sent_at'], 'cid': conversation_id})
        
        await self.db.commit()
        
        # Fetch sender details for WS & Push
        sender_name = "Mambo"
        sender_pfp = None
        try:
            sender_res = await self.db.execute(text(
                "SELECT username, display_name, avatar_url FROM profiles WHERE id = :id"
            ), {'id': user_id})
            sender = sender_res.mappings().one_or_none()
            if sender:
                sender_name = sender.get('display_name') or sender.get('username') or "Mambo"
                sender_pfp = sender.get('avatar_url')
        except Exception:
            pass

        # Websocket notification (reusing logic but ensuring UUIDs are strings)
        import json
        msg_str = msg.copy()
        for k, v in msg_str.items():
            if hasattr(v, '__str__') and not isinstance(v, (str, int, float, bool, type(None))):
                msg_str[k] = str(v)
        if msg_str.get('sent_at') and hasattr(msg['sent_at'], 'isoformat'): 
            msg_str['sent_at'] = msg['sent_at'].isoformat()
        
        ws_data = {
            "type": "new_message", 
            "message": msg_str,
            "sender_name": sender_name,
            "sender_avatar": sender_pfp
        }
        payload = json.dumps(ws_data)
        
        if receiver_id:
            await ws_manager.send_personal_message(payload, str(receiver_id))
        await ws_manager.send_personal_message(payload, str(user_id))

        # Push Notification
        if receiver_id and str(user_id) != str(receiver_id):
            try:
                # Fetch sender details for PFP/Name
                sender_res = await self.db.execute(text(
                    "SELECT username, avatar_url FROM profiles WHERE id = :id"
                ), {'id': user_id})
                sender = sender_res.mappings().one_or_none()
                sender_name = sender.get('username') if sender else "Mambo"
                sender_pfp = sender.get('avatar_url') if sender else None

                from app.services.push_service import PushService
                push_svc = PushService(self.db)
                
                # Truncate body for notification
                preview = body[:100] + ('...' if len(body) > 100 else '')
                
                await push_svc.send_to_user(
                    str(receiver_id),
                    title=f"New Message from {sender_name}",
                    body=preview,
                    image_url=sender_pfp,
                    data={
                        "type": "chat_message",
                        "conversation_id": str(conversation_id),
                        "sender_id": str(user_id)
                    }
                )
            except Exception as pe:
                logger.error(f"Chat push failed: {pe}")

        return msg

    async def delete_message(self, user_id: str, message_id: str) -> bool:
        """Deletes a message if the sender matches the user_id."""
        res = await self.db.execute(text('''
            DELETE FROM messages 
            WHERE id = :mid AND sender_id = :uid
        '''), {'mid': message_id, 'uid': user_id})
        
        if res.rowcount == 0:
            raise ValueError("Message not found or you don't have permission to delete it")
            
        await self.db.commit()
        return True

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
