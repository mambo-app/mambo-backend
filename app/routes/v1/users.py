from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_current_user_id_optional
from app.models.common import ok

from pydantic import BaseModel
from typing import Optional, Any
from app.models.social import PersonFavoriteRequest, FavoritePersonResponse

router = APIRouter()

class UpdateProfileRequest(BaseModel):
    display_name: Optional[str] = None
    username: Optional[str] = None
    bio: Optional[str] = None
    gender: Optional[str] = None
    birthday: Optional[str] = None
    avatar_url: Optional[str] = None

class TopFavoritesRequest(BaseModel):
    content_ids: list[str]

@router.get('/me')
async def get_current_user_profile(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    profile = await service.get_by_id(user_id)
    return ok(profile)

@router.get('/me/stats')
async def get_current_user_stats(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    stats = await service.get_stats(user_id)
    return ok(stats)

@router.get('/me/friends')
async def get_current_user_friends(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.social_service import SocialService
    service = SocialService(db)
    friends = await service.get_friends(UUID(user_id))
    return ok({"items": friends})


@router.get('/search', response_model=dict[str, Any])
async def search_users(
    query: str,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    items = await service.search_users(query, limit, viewer_id=user_id)
    return ok({"items": items})

@router.post('/favorites/person')
async def toggle_person_favorite(
    request: PersonFavoriteRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from app.services.user_service import UserService
    service = UserService(db)
    is_fav = await service.toggle_person_favorite(
        user_id,
        request.person_id,
        request.name,
        request.profile_url,
        request.is_actor
    )
    return ok({"is_favorite": is_fav})

@router.get('/favorites/person/check')
async def check_person_favorite(
    person_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from app.services.user_service import UserService
    service = UserService(db)
    is_fav = await service.is_person_favorite(user_id, person_id)
    return ok({"is_favorite": is_fav})

@router.get('/favorites/person')
async def get_favorite_persons(
    is_actor: bool = True,
    username: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from app.services.user_service import UserService
    service = UserService(db)
    
    target_user_id = user_id
    if username:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(db)
        user = await repo.get_by_username(username)
        if user:
            target_user_id = str(user['id'])
            
    items = await service.get_favorite_persons(target_user_id, is_actor)
    return ok({"items": items})

@router.post('/me/favorites/content')
async def set_top_favorites(
    body: TopFavoritesRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    await service.set_top_favorites(user_id, body.content_ids)
    return ok({"message": "Top favorites updated successfully"})


@router.post('/{username}/follow')
async def follow_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.repositories.user_repo import UserRepository
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    repo = UserRepository(db)
    await repo.follow(user_id, str(target['id']))
    
    # Invalidate caches for both
    await u_svc.invalidate_profile_cache(user_id)
    await u_svc.invalidate_profile_cache(str(target['id']))
    
    return ok({"message": f"Successfully followed {username}"})

@router.delete('/{username}/follow')
async def unfollow_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.repositories.user_repo import UserRepository
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    repo = UserRepository(db)
    await repo.unfollow(user_id, str(target['id']))
    
    # Invalidate caches for both
    await u_svc.invalidate_profile_cache(user_id)
    await u_svc.invalidate_profile_cache(str(target['id']))
    
    return ok({"message": f"Successfully unfollowed {username}"})


@router.patch('/me')
async def update_current_profile(
    body: UpdateProfileRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    updated_profile = await service.update_profile(user_id, body.model_dump(exclude_unset=True))
    return ok(updated_profile)

@router.post('/me/avatar')
async def upload_avatar(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    
    content = await file.read()
    avatar_url = await service.upload_avatar(user_id, content, file.filename)
    return ok({"avatar_url": avatar_url})

class PushTokenRequest(BaseModel):
    token: str
    platform: str # 'android' or 'ios'

@router.post('/me/push-token')
async def save_push_token(
    body: PushTokenRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.push_service import PushService
    service = PushService(db)
    await service.save_token(user_id, body.token, body.platform)
    return ok({"message": "Token saved successfully"})

@router.get('/{username}/activity')
async def get_activity_feed(
    username: str,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional),
):
    from app.services.user_service import UserService
    service = UserService(db)
    return ok(await service.get_activity(username, viewer_id))

@router.get('/{username}/collections')
async def get_collections(
    username: str,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional),
):
    from app.services.user_service import UserService
    service = UserService(db)
    return ok(await service.get_collections(username, viewer_id))

@router.get('/{username}/liked')
async def get_liked_content(
    username: str,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional),
):
    from app.services.user_service import UserService
    service = UserService(db)
    return ok(await service.get_liked_content(username, viewer_id))

@router.get('/{username}/recommendations')
async def get_received_recommendations(
    username: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.user_service import UserService
    service = UserService(db)
    return ok(await service.get_received_recommendations(username))

@router.get('/{username}/reviews')
async def get_user_reviews(
    username: str,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional),
):
    from app.services.user_service import UserService
    from app.services.social_service import SocialService
    from uuid import UUID
    
    u_svc = UserService(db)
    user = await u_svc.get_by_username(username, viewer_id=viewer_id)
    
    s_svc = SocialService(db)
    reviews = await s_svc.get_user_reviews(UUID(str(user['id'])), viewer_id=viewer_id)
    return ok(reviews)

@router.put('/me/privacy')
async def update_privacy_settings(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    updated = await service.update_privacy(user_id, body)
    return ok(updated)

@router.post('/me/genres')
async def update_genre_preferences(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    genres = body.get('genres', [])
    updated_genres = await service.update_genres(user_id, genres)
    return ok({"genres": updated_genres})

class UpdateSettingsRequest(BaseModel):
    push_notifications_enabled: Optional[bool] = None

@router.patch('/me/settings')
async def update_settings(
    body: UpdateSettingsRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        return ok({"message": "No changes"})
    updated = await service.update_profile(user_id, updates)
    return ok(updated)

@router.delete('/me')
async def delete_account(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    await service.delete_account(user_id)
    return ok({"message": "Account successfully deleted"})

class UpdateSocialLinksRequest(BaseModel):
    links: list[dict]

@router.put('/me/social-links')
async def update_social_links(
    body: UpdateSocialLinksRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    service = UserService(db)
    updated_links = await service.update_social_links(user_id, body.links)
    return ok({"social_links": updated_links})

# --- Mute and Block ---
@router.post('/{username}/mute')
async def mute_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.services.social_service import SocialService
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    s_svc = SocialService(db)
    return ok(await s_svc.mute_user(UUID(user_id), UUID(str(target['id']))))

@router.delete('/{username}/mute')
async def unmute_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.services.social_service import SocialService
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    s_svc = SocialService(db)
    return ok(await s_svc.unmute_user(UUID(user_id), UUID(str(target['id']))))

@router.post('/{username}/block')
async def block_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.services.social_service import SocialService
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    s_svc = SocialService(db)
    return ok(await s_svc.block_user(UUID(user_id), UUID(str(target['id']))))

@router.delete('/{username}/block')
async def unblock_user(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    from app.services.user_service import UserService
    from app.services.social_service import SocialService
    u_svc = UserService(db)
    target = await u_svc.get_by_username(username, viewer_id=user_id)
    
    s_svc = SocialService(db)
    return ok(await s_svc.unblock_user(UUID(user_id), UUID(str(target['id']))))

@router.get('/{username}')
async def get_profile(
    username: str,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional),
):
    from app.services.user_service import UserService
    service = UserService(db)
    profile = await service.get_by_username(username, viewer_id)
    return ok(profile)

@router.get('/{username}/followers')
async def get_user_followers(
    username: str,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    from app.services.user_service import UserService
    service = UserService(db)
    followers = await service.get_followers(username, limit, offset)
    return ok({"items": followers})

@router.get('/{username}/following')
async def get_user_following(
    username: str,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    from app.services.user_service import UserService
    service = UserService(db)
    following = await service.get_following(username, limit, offset)
    return ok({"items": following})



@router.get('/{username}/friends')
async def get_user_friends(
    username: str,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    from app.services.user_service import UserService
    service = UserService(db)
    friends = await service.get_friends(username, limit, offset)
    return ok({"items": friends})

