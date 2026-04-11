"""
Structured logging utilities for MAMBO backend.

Uses Python's standard logging with a structured JSON formatter.
All log calls should pass structured data via `extra={...}` so
production logs are queryable by field (Sentry, Datadog, etc.).

Usage:
    from app.core.logger import get_logger
    logger = get_logger('mambo.content')
    logger.error("tmdb_upsert_failed", extra={"title": "Dune", "error": str(e)})
"""
import logging
import json
import sys
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    """
    Emits each log record as a single-line JSON object.
    Includes standard fields plus any extra fields passed by the caller.
    """
    _SKIP_ATTRS = frozenset({
        'args', 'created', 'exc_info', 'exc_text', 'filename',
        'funcName', 'id', 'levelname', 'levelno', 'lineno',
        'message', 'module', 'msecs', 'msg', 'name', 'pathname',
        'process', 'processName', 'relativeCreated', 'stack_info',
        'taskName', 'thread', 'threadName'
    })

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        log: dict = {
            'ts':     datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level':  record.levelname,
            'logger': record.name,
            'event':  record.message,
            'module': record.module,
            'line':   record.lineno,
        }

        # Attach extra fields — skip anything that can't be safely serialized
        for key, val in record.__dict__.items():
            if key in self._SKIP_ATTRS or key.startswith('_'):
                continue
            try:
                json.dumps(val, default=str)
                log[key] = val
            except Exception:
                log[key] = repr(val)

        if record.exc_info:
            log['exc'] = self.formatException(record.exc_info)

        try:
            return json.dumps(log, default=str, ensure_ascii=False)
        except Exception:
            # Ultimate fallback — should never happen with default=str
            return json.dumps({'event': str(record.message), 'level': record.levelname}, ensure_ascii=False)


def configure_logging(level: str = 'INFO') -> None:
    """
    Call once at startup.  Replaces the root handler with a structured JSON handler.
    In development, falls back to a readable format.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    if root.handlers:
        root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())
    root.addHandler(handler)

    # Silence noisy third-party loggers
    for noisy in ('httpx', 'httpcore', 'hpack', 'hpack.hpack', 'hpack.table', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    
    # Keep uvicorn.access at INFO level unless in production
    logging.getLogger('uvicorn.access').setLevel(logging.INFO if level == 'DEBUG' else logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger — use this everywhere instead of logging.getLogger()."""
    return logging.getLogger(name)
