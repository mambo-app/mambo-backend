from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.core.config import settings
from app.models.common import ok
from uuid import uuid4

router = APIRouter()

# Allowed buckets for upload — do NOT expose buckets that users shouldn't access
ALLOWED_BUCKETS = {'avatars', 'post-media', 'review-media', 'message-media'}

@router.post('/upload-url')
async def get_upload_url(
    bucket: str = Query(..., description="Supabase bucket name"),
    file_name: str = Query(..., description="Original filename"),
    content_type: str = Query('application/octet-stream', description="MIME type"),
    user_id: str = Depends(get_current_user_id)
):
    """
    Generate a signed upload URL for direct Supabase Storage upload.
    The client uses this URL to PUT the file directly to Supabase.
    """
    if bucket not in ALLOWED_BUCKETS:
        raise HTTPException(status_code=400, detail=f"Invalid bucket. Allowed: {sorted(ALLOWED_BUCKETS)}")

    # Sanitize the file extension only — use a server-generated UUID path
    ext = file_name.rsplit('.', 1)[-1].lower() if '.' in file_name else ''
    allowed_extensions = {
        'jpg', 'jpeg', 'png', 'gif', 'webp', 'heic',  # images
        'mp4', 'mov', 'avi', 'webm',                    # videos
        'pdf'                                            # documents
    }
    if ext not in allowed_extensions:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: .{ext}")

    # Build a safe path that prevents path traversal and collisions
    path = f"{user_id}/{uuid4()}.{ext}"

    try:
        from supabase import create_client
        supabase = create_client(settings.supabase_url, settings.supabase_service_role_key)
        result = supabase.storage.from_(bucket).create_signed_upload_url(path)
        signed_url = result.get('signed_url') or result.get('signedUrl') or result.get('signedURL')
        if not signed_url:
            raise ValueError(f"Unexpected response from Supabase: {result}")
    except ImportError:
        raise HTTPException(status_code=500, detail="Supabase client not installed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate upload URL: {str(e)}")

    return ok({
        "upload_url": signed_url,
        "path": path,
        "bucket": bucket,
        "public_url": f"{settings.supabase_url}/storage/v1/object/public/{bucket}/{path}"
    })
