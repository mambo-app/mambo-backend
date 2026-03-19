import asyncio
import logging
from typing import Dict, Set
from fastapi import WebSocket
from app.core.redis import redis_client

logger = logging.getLogger('mambo.websocket')

class ConnectionManager:
    def __init__(self):
        # Maps user_id -> set of active WebSockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Tasks for Redis subscribers
        self.subscriber_tasks: Dict[str, asyncio.Task] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
            await self._subscribe_redis(user_id)
        self.active_connections[user_id].add(websocket)
        logger.info(f"User {user_id} connected. Total WS for user: {len(self.active_connections[user_id])}")

    def disconnect(self, user_id: str, websocket: WebSocket):
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                self.active_connections.pop(user_id, None)
                self._unsubscribe_redis(user_id)
        logger.info(f"User {user_id} disconnected.")

    async def send_personal_message(self, message: str, user_id: str):
        # Local delivery
        if user_id in self.active_connections:
            closed_socks = set()
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_text(message)
                except Exception:
                    closed_socks.add(connection)
            
            # Cleanup dead sockets
            for connection in closed_socks:
                self.disconnect(user_id, connection)
                
        # Redis Pub/Sub delivery (multi-instance)
        redis = redis_client.get_client()
        if redis:
            try:
                await redis.publish(f"channel:user:{user_id}", message)
            except Exception as e:
                logger.error(f"Redis publish error: {e}")

    async def _subscribe_redis(self, user_id: str):
        redis = redis_client.get_client()
        if not redis:
            return
            
        async def reader(pubsub):
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        data = message["data"]
                        # We send exclusively to local connections here.
                        if user_id in self.active_connections:
                            closed_socks = set()
                            for ws in self.active_connections[user_id]:
                                try:
                                    await ws.send_text(data)
                                except Exception:
                                    closed_socks.add(ws)
                            for ws in closed_socks:
                                self.disconnect(user_id, ws)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Redis listener error for user {user_id}: {e}")
                
        pubsub = redis.pubsub()
        await pubsub.subscribe(f"channel:user:{user_id}")
        task = asyncio.create_task(reader(pubsub))
        self.subscriber_tasks[user_id] = task

    def _unsubscribe_redis(self, user_id: str):
        if user_id in self.subscriber_tasks:
            self.subscriber_tasks[user_id].cancel()
            self.subscriber_tasks.pop(user_id, None)

ws_manager = ConnectionManager()
