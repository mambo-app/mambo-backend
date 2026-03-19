from pydantic import BaseModel
from typing import TypeVar, Generic, Any

T = TypeVar('T')

class Meta(BaseModel):
    page: int = 1
    limit: int = 20
    total: int | None = None
    has_more: bool = False

class ApiResponse(BaseModel, Generic[T]):
    success: bool = True
    data: T | None = None
    meta: Meta | None = None

def ok(data: Any = None) -> dict:
    return {'success': True, 'data': data}

def paginated(data: list, page: int, limit: int, total: int) -> dict:
    return {
        'success': True,
        'data': data,
        'meta': {
            'page': page,
            'limit': limit,
            'total': total,
            'has_more': (page * limit) < total
        }
    }