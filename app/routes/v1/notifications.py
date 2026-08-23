from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any
from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_ws_user_id
from app.models.common import ok
from app.models.notification import NotificationListResponse
from app.services.notification_service import NotificationService
from app.core.websocket import ws_manager
from uuid import UUID
import json

from pydantic import BaseModel

router = APIRouter()

class PushTokenRequest(BaseModel):
    token: str
    device_type: str = "android"

@router.post('/push-token', response_model=Dict[str, Any])
async def register_push_token(
    req: PushTokenRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.push_service import PushService
    from app.repositories.user_repo import UserRepository # might be needed by PushService
    svc = PushService(db)
    try:
        await svc.save_token(user_id, req.token, req.device_type)
        return ok({"success": True})
    except Exception:
        pass
    return ok({"success": True})

@router.get('/', response_model=Dict[str, Any])
async def get_notifications(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    items, total = await service.get_notifications(user_id, page, limit)
    has_more = (page * limit) < total
    return ok({
        "items": items,
        "total": total,
        "has_more": has_more
    })

@router.get('/unread-count', response_model=Dict[str, Any])
async def get_unread_count(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    count = await service.get_unread_count(user_id)
    return ok({"count": count})

@router.patch('/read-all')
async def mark_all_read(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    await service.mark_as_read(user_id)
    return ok({"success": True})

@router.patch('/{id}/read')
async def mark_read(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    await service.mark_as_read(user_id, str(id))
    return ok({"success": True})

@router.delete('/{id}')
async def delete_notification(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    await service.delete_notification(user_id, str(id))
    return ok({"success": True})

@router.websocket('/ws')
async def notification_websocket(
    websocket: WebSocket,
    user_id: str = Depends(get_ws_user_id)
):
    await ws_manager.connect(user_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                js = json.loads(data)
                if js.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(user_id, websocket)

class TestTriggerRequest(BaseModel):
    user_id: UUID
    message: str

@router.post('/test-trigger')
async def test_trigger(
    req: TestTriggerRequest,
    db: AsyncSession = Depends(get_db),
):
    from app.services.chat_service import ChatService
    from app.services.notification_service import NotificationService
    
    chat_service = ChatService(db)
    notif_service = NotificationService(db)
    
    krrish_id = '32a2fbc0-cd5c-4b3e-a801-e85c033ea674'
    
    # 1. Direct conversation
    conv_id = await chat_service.get_or_create_direct_conversation(
        user_id1=krrish_id,
        user_id2=str(req.user_id),
        bypass_friendship_check=True
    )
    
    # 2. Send message
    await chat_service.send_message(
        user_id=krrish_id,
        conversation_id=conv_id,
        body=req.message,
        receiver_id=str(req.user_id),
        bypass_friendship_check=True
    )
    
    # 3. Send notification
    await notif_service.create_notification({
        'user_id': str(req.user_id),
        'actor_id': krrish_id,
        'title': 'krrish04',
        'message': f"sent you a new message: {req.message[:30]}",
        'type': 'recommendation',
        'is_read': False
    })
    
    return ok({"success": True})

@router.delete('/{id}')
async def delete_notification(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = NotificationService(db)
    success = await service.delete_notification(user_id, str(id))
    return ok({'success': success})
