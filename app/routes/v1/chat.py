from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any
from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_ws_user_id
from app.models.common import ok
from app.services.chat_service import ChatService
from app.core.websocket import ws_manager
from uuid import UUID
import json
import logging

logger = logging.getLogger('mambo.chat_ws')

router = APIRouter()

@router.get('/conversations', response_model=Dict[str, Any])
async def get_conversations(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ChatService(db)
    items = await service.get_conversations(user_id)
    return ok({"items": items})

@router.post('/start', response_model=Dict[str, Any])
async def start_direct_chat(
    data: Dict[str, str],
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    target_user_id = data.get("user_id")
    if not target_user_id:
        return ok({"error": "Missing user_id"}, status="error")
    
    service = ChatService(db)
    try:
        cid = await service.get_or_create_direct_conversation(user_id, target_user_id)
        return ok({"conversation_id": cid})
    except ValueError as e:
        return ok({"error": str(e)}, status="error")

@router.get('/{conversation_id}/messages', response_model=Dict[str, Any])
async def get_messages(
    conversation_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ChatService(db)
    offset = (page - 1) * limit
    items = await service.get_messages(str(conversation_id), limit, offset)
    return ok({"items": items})

@router.post('/{conversation_id}/messages', response_model=Dict[str, Any])
async def send_message(
    conversation_id: UUID,
    data: Dict[str, Any],
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    body = data.get("body") or ""
    receiver_id = data.get("receiver_id")
    shared_meta = data.get("shared_meta") or {}
    shared_content_id = data.get("shared_content_id") or shared_meta.get("content_id")
    shared_post_id = data.get("shared_post_id") or shared_meta.get("post_id")
    shared_review_id = data.get("shared_review_id") or shared_meta.get("review_id")

    service = ChatService(db)
    msg = await service.send_message(
        user_id, 
        str(conversation_id), 
        body, 
        receiver_id=receiver_id,
        shared_post_id=shared_post_id,
        shared_review_id=shared_review_id,
        shared_content_id=shared_content_id,
        shared_meta=shared_meta
    )
    return ok(msg)

@router.post('/{conversation_id}/read', response_model=Dict[str, Any])
async def mark_as_read(
    conversation_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ChatService(db)
    await service.mark_as_read(user_id, str(conversation_id))
    return ok({"success": True})

@router.delete('/messages/{message_id}', response_model=Dict[str, Any])
async def delete_message(
    message_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ChatService(db)
    try:
        await service.delete_message(user_id, str(message_id))
        return ok({"success": True})
    except ValueError as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=str(e))

@router.get('/{conversation_id}/search', response_model=Dict[str, Any])
async def search_messages(
    conversation_id: UUID,
    query: str = Query(...),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ChatService(db)
    items = await service.search_messages(user_id, str(conversation_id), query)
    return ok({"items": items})

@router.post('/send-test')
async def send_test_chat(
    data: Dict[str, Any] = None,
    db: AsyncSession = Depends(get_db),
):
    krrish_id = '32a2fbc0-cd5c-4b3e-a801-e85c033ea674' # krrish04
    laksh_id = 'c5131ed0-b5e5-4278-8211-1cbfc6ba6910'  # lakshhh
    
    service = ChatService(db)
    conv_id = await service.get_or_create_direct_conversation(krrish_id, laksh_id, bypass_friendship_check=True)
    
    msg_text = (data or {}).get("message") or "Hey Laksh! Testing the new Instagram-style in-app DM toast 🚀"
    
    msg = await service.send_message(
        user_id=krrish_id,
        conversation_id=str(conv_id),
        body=msg_text,
        receiver_id=laksh_id,
        bypass_friendship_check=True
    )
    return ok({"status": "sent", "message": msg})

@router.post('/send-typing-test')
async def send_typing_test_chat(
    data: Dict[str, Any] = None,
    db: AsyncSession = Depends(get_db),
):
    krrish_id = '32a2fbc0-cd5c-4b3e-a801-e85c033ea674' # krrish04
    laksh_id = 'c5131ed0-b5e5-4278-8211-1cbfc6ba6910'  # lakshhh
    
    service = ChatService(db)
    conv_id = await service.get_or_create_direct_conversation(krrish_id, laksh_id, bypass_friendship_check=True)
    is_typing = (data or {}).get("is_typing", True)
    
    payload = json.dumps({
        "type": "typing_status",
        "conversation_id": str(conv_id),
        "sender_id": krrish_id,
        "is_typing": is_typing
    })
    await ws_manager.send_personal_message(payload, laksh_id)
    return ok({"status": "typing_dispatched", "is_typing": is_typing})

@router.websocket('/ws')
async def chat_websocket(
    websocket: WebSocket,
    user_id: str = Depends(get_ws_user_id),
    db: AsyncSession = Depends(get_db)
):
    await ws_manager.connect(user_id, websocket)
    service = ChatService(db)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                js = json.loads(data)
                
                if js.get("type") == "send_message":
                    cid = js.get("conversation_id")
                    rid = js.get("receiver_id")
                    body = js.get("body")
                    if cid and body:
                        await service.send_message(user_id, cid, body, rid)
                elif js.get("type") == "typing":
                    cid = js.get("conversation_id")
                    rid = js.get("receiver_id")
                    is_typing = js.get("is_typing", False)
                    if cid and rid:
                        payload = json.dumps({
                            "type": "typing_status",
                            "conversation_id": cid,
                            "sender_id": user_id,
                            "is_typing": is_typing
                        })
                        await ws_manager.send_personal_message(payload, str(rid))
                elif js.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"WS error: {e}")
    except WebSocketDisconnect:
        ws_manager.disconnect(user_id, websocket)
