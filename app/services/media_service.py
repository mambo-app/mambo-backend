from app.core.supabase import supabase_admin
from fastapi import HTTPException
import uuid
import logging

logger = logging.getLogger('mambo.media')

class MediaService:
    @staticmethod
    async def upload_post_media(user_id: str, file_data: bytes, filename: str) -> str:
        file_ext = filename.split('.')[-1] if '.' in filename else 'jpg'
        storage_path = f"posts/{user_id}/{uuid.uuid4()}.{file_ext}"
        
        try:
            # Upload to Supabase Storage "post-media" bucket
            supabase_admin.storage.from_("post-media").upload(
                path=storage_path,
                file=file_data,
                file_options={
                    "content-type": f"image/{file_ext}",
                    "upsert": "true"
                }
            )
            
            # Get public URL
            public_url = supabase_admin.storage.from_("post-media").get_public_url(storage_path)
            return public_url
        except Exception as e:
            logger.error(f"Post media upload failed for user {user_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to upload media: {str(e)}")
