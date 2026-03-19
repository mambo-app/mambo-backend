from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import ORJSONResponse

class MamboException(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code

class NotFoundError(MamboException):
    def __init__(self, resource: str):
        super().__init__('NOT_FOUND', f'{resource} not found.', 404)

class AlreadyExistsError(MamboException):
    def __init__(self, resource: str):
        super().__init__('ALREADY_EXISTS', f'{resource} already exists.', 409)

class ForbiddenError(MamboException):
    def __init__(self, message: str = 'You cannot do this.'):
        super().__init__('FORBIDDEN', message, 403)

class RateLimitError(MamboException):
    def __init__(self):
        super().__init__('RATE_LIMITED', 'Too many requests. Slow down.', 429)

def register_exception_handlers(app: FastAPI):
    @app.exception_handler(MamboException)
    async def mambo_exception_handler(request: Request, exc: MamboException):
        return ORJSONResponse(
            status_code=exc.status_code,
            content={'success': False, 'error': {'code': exc.code, 'message': exc.message}}
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return ORJSONResponse(
            status_code=exc.status_code,
            content={'success': False, 'error': {'code': 'HTTP_ERROR', 'message': exc.detail}}
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        import logging
        logging.exception('Unhandled exception')
        return ORJSONResponse(
            status_code=500,
            content={'success': False, 'error': {'code': 'INTERNAL_ERROR', 'message': str(exc)}}
        )