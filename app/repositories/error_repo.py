from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import json
import logging

logger = logging.getLogger('mambo.errors')

class ErrorRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def log_error(
        self,
        event_name: str,
        message: str,
        stack_trace: str | None = None,
        request_id: str | None = None,
        path: str | None = None,
        method: str | None = None,
        status_code: int = 500,
        metadata: dict | None = None
    ):
        try:
            await self.db.execute(text('''
                INSERT INTO error_logs (
                    event_name, message, stack_trace, request_id, 
                    path, method, status_code, metadata
                )
                VALUES (
                    :event, :msg, :stack, :rid, :path, :method, :status, :meta
                )
            '''), {
                'event': event_name,
                'msg': message,
                'stack': stack_trace,
                'rid': request_id,
                'path': path,
                'method': method,
                'status': status_code,
                'meta': json.dumps(metadata) if metadata else None
            })
            await self.db.commit()
        except Exception as e:
            logger.error(f"Failed to log error to database: {e}")
            await self.db.rollback()
