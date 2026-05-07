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

    async def register(self, username: str, email: str, phone: str | None, password: str) -> dict:
        # 1. Validate password strength
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
        from fastapi import HTTPException as _HTTPException
        url = f"{settings.supabase_url}/auth/v1/token?grant_type=refresh_token"
        headers = {
            "apikey": settings.supabase_anon_key,
            "Content-Type": "application/json",
        }
        data = {"refresh_token": refresh_token}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, headers=headers, json=data)
                if response.status_code != 200:
                    # Log the real Supabase error body for debugging
                    logger.error(f"Supabase refresh failed: {response.status_code} — {response.text}")
                    raise _HTTPException(status_code=401, detail="Invalid or expired refresh token")

                res_data = response.json()
                return {
                    "access_token": res_data["access_token"],
                    "refresh_token": res_data["refresh_token"],
                }
        except _HTTPException:
            # Re-raise cleanly — don't swallow the 401 in the generic except below
            raise
        except httpx.TimeoutException:
            logger.error("Token refresh timed out calling Supabase (>10s)")
            raise _HTTPException(status_code=503, detail="Auth service timeout. Please retry.")
        except Exception as e:
            logger.error(f"Token refresh unexpected error: {type(e).__name__}: {e}")
            raise _HTTPException(status_code=401, detail="Invalid or expired refresh token")

    async def login_with_google(self, id_token: str) -> dict:
        from google.oauth2 import id_token as google_id_token
        from google.auth.transport import requests as google_requests
        
        # This is your Web Client ID from the screenshot
        GOOGLE_CLIENT_ID = "350334884442-f5epk0tkbceh89h09qv7urifn6ffivkm.apps.googleusercontent.com"
        
        try:
            # 1. Verify Google Token with Audience check
            id_info = google_id_token.verify_oauth2_token(
                id_token, google_requests.Request(), GOOGLE_CLIENT_ID
            )
            
            email = id_info.get('email')
            if not email:
                raise HTTPException(status_code=400, detail="Email not provided by Google")
            
            # 2. Check if user exists in Supabase
            try:
                # We use the admin client to check by email
                from app.core.supabase import supabase_admin
                user_res = supabase_admin.auth.admin.list_users() # Not ideal but get_user_by_email is sometimes flaky in old SDKs
                # Efficiently check by filtering in our Neon profiles first!
                result = await self.db.execute(text("SELECT id FROM profiles WHERE email = :e"), {'e': email})
                profile = result.mappings().first()
                
                if profile:
                    # Existing user: We need a Supabase session.
                    # This is tricky: Backend can't "sudo login" as a user and get a session token
                    # UNLESS we use a custom JWT or allow the mobile app to sign in with ID token.
                    # BEST WAY: If user exists, tell mobile app to "sign in with Supabase ID Token" 
                    # OR we handle it here if Supabase supports admin-level session generation.
                    
                    user_id = profile['id']
                    # We can't easily generate a SESSION here without a password.
                    # But wait, Supabase has a "link" or we can use our service role to bypass.
                    # Actually, the easiest way for mobile is to return a special flag
                    # to tell the mobile app to use its Supabase client to sign in with the token.
                    
                    return {
                        "is_new_user": False,
                        "profile": dict(await self.check_verified(user_id))
                    }
                else:
                    # New user: Provisioning state
                    return {
                        "is_new_user": True,
                        "email": email,
                        "display_name": id_info.get('name', ''),
                        "provisioning_token": id_token # We use the ID token as the proof for the final step
                    }
                    
            except Exception as e:
                logger.error(f"Google discovery failed: {e}")
                raise HTTPException(status_code=500, detail="Auth service synchronization failed")

        except ValueError as e:
            raise HTTPException(status_code=401, detail=f"Invalid Google token: {e}")

    async def finalize_google_signup(self, username: str, provisioning_token: str, password: str) -> dict:
        from google.oauth2 import id_token as google_id_token
        from google.auth.transport import requests as google_requests
        
        GOOGLE_CLIENT_ID = "350334884442-f5epk0tkbceh89h09qv7urifn6ffivkm.apps.googleusercontent.com"
        
        # 1. Re-verify the provisioning token with Audience check
        id_info = google_id_token.verify_oauth2_token(
            provisioning_token, google_requests.Request(), GOOGLE_CLIENT_ID
        )
        email = id_info.get('email')
        
        # 2. Check username
        existing = await self.db.execute(text('SELECT id FROM profiles WHERE username = :u'), {'u': username})
        if existing.fetchone():
            raise HTTPException(status_code=409, detail='Username already taken.')
            
        # 3. Create user in Supabase with the confirmed email
        # We also set the password so they have a backup manual login!
        try:
            res = supabase_admin.auth.admin.create_user({
                "email": email,
                "password": password,
                "email_confirm": True,
                "user_metadata": {
                    "username": username,
                    "display_name": id_info.get('name', username)
                }
            })
            user_id = res.user.id
        except Exception as e:
             raise HTTPException(status_code=400, detail=str(e))

        # 4. Create Neon Profile
        # ... logic similar to register() ...
        # I'll extract it to a helper if I had one, but I'll just write it for now.
        await self.db.execute(text('''
            INSERT INTO profiles (id, username, display_name, email, is_verified)
            VALUES (:id, :username, :dname, :email, true)
        '''), {
            'id': user_id, 
            'username': username, 
            'dname': id_info.get('name', username),
            'email': email
        })
        
        # 5. Create default related rows
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
            logger.error(f"Failed to create related tables for Google user {user_id}: {e}")
            await self.db.rollback()
        
        return await self.login(email, password)

    async def _delete_supabase_user(self, user_id: str):
        try:
            supabase_admin.auth.admin.delete_user(user_id)
        except Exception as e:
            logger.error(f'Failed to delete Supabase user {user_id}: {e}')