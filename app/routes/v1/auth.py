from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field, EmailStr
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok

router = APIRouter()

class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=6, max_length=30, pattern=r"^[a-zA-Z0-9_]+$")
    email: EmailStr
    phone: str = Field(..., pattern=r"^\d{10}$")
    password: str = Field(..., min_length=8)
    invite_key: str

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class PasswordChangeRequest(BaseModel):
    new_password: str = Field(..., min_length=8)

@router.post('/register')
async def register(
    body: RegisterRequest,
    db: AsyncSession = Depends(get_db),
):
    from app.services.auth_service import AuthService
    service = AuthService(db)
    
    import logging
    logger = logging.getLogger('mambo.auth')
    logger.debug(f"Registering user: {body.username} ({body.email})")
    
    result = await service.register(
        username=body.username,
        email=body.email,
        phone=body.phone,
        password=body.password,
        invite_key=body.invite_key
    )
    return ok(result)

@router.post('/login')
async def login(
    body: LoginRequest,
    db: AsyncSession = Depends(get_db),
):
    from app.services.auth_service import AuthService
    service = AuthService(db)
    import logging
    logger = logging.getLogger('mambo.auth')
    logger.debug(f"Login attempt for: {body.email}")
    result = await service.login(body.email, body.password)
    return ok(result)

class RefreshRequest(BaseModel):
    refresh_token: str

@router.post('/refresh')
async def refresh(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
):
    from app.services.auth_service import AuthService
    service = AuthService(db)
    result = await service.refresh_token(body.refresh_token)
    return ok(result)

@router.post('/change-password')
async def change_password(
    body: PasswordChangeRequest,
    user_id: str = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    from app.services.auth_service import AuthService
    service = AuthService(db)
    await service.change_password(user_id, body.new_password)
    return ok({"message": "Password updated successfully"})

@router.get('/me')
async def get_me(
    user_id: str = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    from app.services.auth_service import AuthService
    service = AuthService(db)
    profile = await service.check_verified(user_id)
    return ok({'profile': profile})