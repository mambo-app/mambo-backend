from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from app.core.config import settings
from app.core.supabase import supabase_admin
import logging

logger = logging.getLogger('mambo.auth')

class AuthService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def register(self, username: str, email: str, phone: str | None, password: str, invite_key: str) -> dict:
        # 1. Check invite key
        if invite_key != settings.invite_key:
            raise HTTPException(
                status_code=403,
                detail='Invalid verification key. Please sign up again.'
            )

        # 2. Validate password strength
        if len(password) < 8:
            raise HTTPException(status_code=400, detail='Password must be at least 8 characters.')

        # 3. Check username not taken in Neon
        existing = await self.db.execute(
            text('SELECT id FROM profiles WHERE username = :u'),
            {'u': username}
        )
        if existing.fetchone():
            raise HTTPException(status_code=409, detail='Username already taken.')

        # 4. Create user in Supabase Admin
        try:
            res = supabase_admin.auth.admin.create_user({
                "email": email,
                "phone": phone,
                "password": password,
                "email_confirm": True,
                "phone_confirm": True,
                "user_metadata": {
                    "username": username,
                    "display_name": username
                }
            })
            user_id = res.user.id
        except Exception as e:
            err_msg = str(e)
            if 'already been registered' in err_msg or 'already exists' in err_msg or 'already registered' in err_msg.lower():
                raise HTTPException(status_code=409, detail='Email already registered.')
            raise HTTPException(status_code=400, detail=err_msg)

        # 5. Create profile in Neon — identity fields only
        phone_clean = phone.strip() if phone else None
        try:
            result = await self.db.execute(text('''
                INSERT INTO profiles (
                    id, username, display_name, email, phone_number, is_verified
                )
                VALUES (
                    :id, :username, :username, :email, :phone, true
                )
                ON CONFLICT (id) DO UPDATE SET 
                    username = :username,
                    display_name = :username,
                    email = :email,
                    phone_number = :phone,
                    is_verified = true
                RETURNING *
            '''), {
                'id': user_id, 
                'username': username,
                'email': email,
                'phone': phone_clean
            })
            await self.db.commit()
        except IntegrityError as e:
            await self.db.rollback()
            await self._delete_supabase_user(user_id)
            err_str = str(e)
            if 'profiles_username_key' in err_str:
                raise HTTPException(status_code=409, detail='Username already taken.')
            if 'idx_profiles_email' in err_str:
                raise HTTPException(status_code=409, detail='Email already registered in profile.')
            raise HTTPException(status_code=409, detail='Registration failed due to profile conflict.')

        # 6. Create default related rows
        try:
            # Stats
            await self.db.execute(text('''
                INSERT INTO user_stats (user_id) VALUES (:id)
                ON CONFLICT (user_id) DO NOTHING
            '''), {'id': user_id})

            # Privacy
            await self.db.execute(text('''
                INSERT INTO privacy_settings (user_id) VALUES (:id)
                ON CONFLICT (user_id) DO NOTHING
            '''), {'id': user_id})

            # Default Collections
            default_collections = [
                # name, desc, is_public, is_default, is_pinned, pin_order
                ('Watchlist', 'My watchlist of movies and shows', False, True, True, 1),
                ('Dropped', 'Content I stopped watching', False, True, True, 2),
                ('Watched', 'All content I have watched', False, True, True, 3),
            ]
            for name, desc, is_public, is_def, is_pin, pin_ord in default_collections:
                await self.db.execute(text('''
                    INSERT INTO collections (
                        user_id, name, description, is_public, 
                        collection_type, is_default, is_deletable,
                        is_pinned, pin_order
                    )
                    VALUES (
                        :uid, :name, :desc, :public, 
                        :type, :is_def, false,
                        :is_pin, :pin_ord
                    )
                    ON CONFLICT DO NOTHING
                '''), {
                    'uid': user_id,
                    'name': name,
                    'desc': desc,
                    'public': is_public,
                    'type': name.lower(),
                    'is_def': is_def,
                    'is_pin': is_pin,
                    'pin_ord': pin_ord
                })

            await self.db.commit()
        except Exception as e:
            logger.error(f"Failed to create related tables for {user_id}: {e}")
            await self.db.rollback()

        # 7. Return token + full profile
        return await self.login(email, password)

    async def login(self, email: str, password: str) -> dict:
        from app.core.supabase import get_supabase_client
        client = get_supabase_client()
        try:
            auth_response = client.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            if not auth_response.user or not auth_response.session:
                 raise Exception("Invalid credentials")
                 
            user_id = auth_response.user.id
            access_token = auth_response.session.access_token
        except Exception as e:
            logger.error(f"Login failed for {email}: {e}")
            raise HTTPException(status_code=401, detail="Invalid email or password")

        # Now get the profile
        result = await self.db.execute(
            text('SELECT * FROM profiles WHERE id = :id AND is_deleted = false'),
            {'id': user_id}
        )
        profile = result.mappings().first()

        if not profile:
            # Profile missing in Neon (e.g., after a DB wipe) but user exists in Supabase.
            # Auto-heal: recreate the profile from Supabase metadata.
            logger.warning(f"Profile not found for authenticated user {user_id}. Attempting auto-heal.")
            try:
                supabase_user = auth_response.user
                metadata = supabase_user.user_metadata or {}
                username = metadata.get('username') or email.split('@')[0]
                display_name = metadata.get('display_name') or username
                phone = getattr(supabase_user, 'phone', None)

                await self.db.execute(text('''
                    INSERT INTO profiles (id, username, display_name, email, phone_number, is_verified)
                    VALUES (:id, :username, :display_name, :email, :phone, true)
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = :display_name,
                        email = :email,
                        is_verified = true
                    RETURNING *
                '''), {
                    'id': user_id,
                    'username': username,
                    'display_name': display_name,
                    'email': email,
                    'phone': phone
                })
                await self.db.execute(text('''
                    INSERT INTO user_stats (user_id) VALUES (:id)
                    ON CONFLICT (user_id) DO NOTHING
                '''), {'id': user_id})
                await self.db.commit()
                logger.info(f"Auto-healed profile for user {user_id}")

                # Re-fetch the rebuilt profile
                result = await self.db.execute(
                    text('SELECT * FROM profiles WHERE id = :id'),
                    {'id': user_id}
                )
                profile = result.mappings().first()
            except Exception as heal_err:
                await self.db.rollback()
                logger.error(f"Profile auto-heal failed for {user_id}: {heal_err}")
                raise HTTPException(
                    status_code=404,
                    detail='Profile not found. Please contact support or re-register.'
                )

        return {
            "access_token": access_token,
            "refresh_token": auth_response.session.refresh_token,
            "profile": dict(profile)
        }

    async def check_verified(self, user_id: str) -> dict:
        result = await self.db.execute(
            text('SELECT * FROM profiles WHERE id = :id AND is_deleted = false'),
            {'id': user_id}
        )
        profile = result.mappings().first()

        if not profile:
            raise HTTPException(
                status_code=401,
                detail='Account not found. Please sign up.'
            )

        if not profile['is_verified']:
            raise HTTPException(
                status_code=403,
                detail='Account not verified. Please sign up again.'
            )

        return dict(profile)

    async def change_password(self, user_id: str, new_password: str):
        try:
            supabase_admin.auth.admin.update_user_by_id(
                user_id,
                {"password": new_password}
            )
        except Exception as e:
            logger.error(f"Failed to change password for user {user_id}: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def refresh_token(self, refresh_token: str) -> dict:
        import httpx
        url = f"{settings.supabase_url}/auth/v1/token?grant_type=refresh_token"
        headers = {
            "apikey": settings.supabase_anon_key,
            "Content-Type": "application/json",
        }
        data = {"refresh_token": refresh_token}
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=data)
                if response.status_code != 200:
                    logger.error(f"Supabase refresh failed: {response.status_code} {response.text}")
                    raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
                
                res_data = response.json()
                return {
                    "access_token": res_data["access_token"],
                    "refresh_token": res_data["refresh_token"],
                }
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    async def _delete_supabase_user(self, user_id: str):
        try:
            supabase_admin.auth.admin.delete_user(user_id)
        except Exception as e:
            logger.error(f'Failed to delete Supabase user {user_id}: {e}')