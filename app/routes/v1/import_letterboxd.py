from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Optional
from uuid import UUID
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.services.letterboxd_service import LetterboxdService, sync_progress
from sqlalchemy import text
import logging

logger = logging.getLogger("mambo.import_letterboxd")

router = APIRouter(tags=['import_letterboxd'])

@router.get('/profile/{username}', response_model=Dict[str, Any])
async def get_letterboxd_profile(
    username: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Fetches Letterboxd profile info for validation before starting sync."""
    service = LetterboxdService(db)
    result = await service.fetch_profile(username)
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message", "Could not fetch profile"))
    return ok(result)

@router.post('/sync', response_model=Dict[str, Any])
async def start_letterboxd_sync(
    username: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Dispatches asynchronous scraping and import for a username."""
    # Check if already running
    if sync_progress.get(user_id, {}).get("status") == "running":
        raise HTTPException(status_code=400, detail="Import already in progress")

    service = LetterboxdService(db)
    background_tasks.add_task(service.run_sync_in_background, user_id, username)
    return ok({"message": "Background sync started successfully"})

@router.post('/upload-zip', response_model=Dict[str, Any])
async def upload_letterboxd_zip(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Handles uploading a Letterboxd exported zip file and starts sync."""
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Invalid file format. Must be a .zip file.")

    if sync_progress.get(user_id, {}).get("status") == "running":
        raise HTTPException(status_code=400, detail="Import already in progress")

    zip_bytes = await file.read()
    service = LetterboxdService(db)
    background_tasks.add_task(service.run_zip_in_background, user_id, zip_bytes)
    return ok({"message": "Background ZIP import started successfully"})

@router.get('/status', response_model=Dict[str, Any])
async def get_import_status(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Retrieves current Letterboxd import status, progress, live feed, and stats."""
    # 1. Fetch from Database
    res = await db.execute(text("""
        SELECT p.letterboxd_username, p.letterboxd_import_status,
               (SELECT COUNT(*) FROM public.watch_history WHERE user_id = p.id) AS imported_films,
               (SELECT COUNT(*) FROM public.reviews WHERE user_id = p.id AND is_deleted = false) AS imported_reviews,
               p.updated_at
        FROM public.profiles p
        WHERE p.id = :uid
    """), {"uid": UUID(user_id)})
    row = res.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User profile not found")

    letterboxd_username, db_status, db_films, db_reviews, last_synced = row

    # If user has imported items and username, effective status is completed!
    if letterboxd_username and (int(db_films or 0) > 0 or int(db_reviews or 0) > 0 or db_status == "completed"):
        if db_status not in ["running", "failed"]:
            db_status = "completed"
            if row[1] != "completed":
                try:
                    await db.execute(text(
                        "UPDATE public.profiles SET letterboxd_import_status = 'completed', updated_at = now() WHERE id = :uid"
                    ), {"uid": UUID(user_id)})
                    await db.commit()
                except Exception:
                    await db.rollback()

    # 2. Match with in-memory active progress & Recovery Guard
    progress_info = sync_progress.get(user_id)
    if progress_info:
        progress_status = progress_info.get("status", "running")
        if progress_status != db_status and db_status == "running":
            db_status = progress_status
    else:
        # Stuck Recovery Guard: DB status is 'running' but no active task exists in RAM
        if db_status == "running":
            # Auto-recover stuck imports (e.g. after server restart)
            try:
                await db.execute(text(
                    "UPDATE public.profiles SET letterboxd_import_status = 'failed', updated_at = now() WHERE id = :uid"
                ), {"uid": UUID(user_id)})
                await db.commit()
                db_status = "failed"
                logger.info(f"Auto-recovered stuck import for user {user_id} to failed state.")
            except Exception as recovery_err:
                await db.rollback()
                logger.error(f"Error executing recovery guard for user {user_id}: {recovery_err}")

        progress_info = {
            "processed": 0,
            "total": 0,
            "status": db_status,
            "current_item": None,
            "recent_items": [],
            "imported_films": int(db_films or 0),
            "imported_reviews": int(db_reviews or 0),
            "unresolved_count": 0,
            "skipped_count": 0,
        }

    total_count = progress_info.get("total", 0)
    unresolved_count = progress_info.get("unresolved_count", 0)
    skipped_count = progress_info.get("skipped_count", 0)
    imported_count = progress_info.get("imported_films", int(db_films or 0))

    resp_data = {
        "letterboxd_username": letterboxd_username,
        "status": db_status,
        "last_synced_at": last_synced.isoformat() if last_synced else None,
        "progress": {
            "processed": progress_info.get("processed", 0),
            "total": total_count,
        },
        "current_item": progress_info.get("current_item"),
        "recent_items": progress_info.get("recent_items", []),
        "imported_films": imported_count,
        "imported_reviews": progress_info.get("imported_reviews", int(db_reviews or 0)),
        "summary": {
            "total_found": total_count,
            "imported": imported_count,
            "unresolved": unresolved_count,
            "skipped": skipped_count,
        }
    }
    return ok(resp_data)

@router.delete('/data', response_model=Dict[str, Any])
async def delete_letterboxd_data(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Deletes all Letterboxd-imported data for the current user and resets status."""
    uid = UUID(user_id)

    # Prevent deletion while import is running
    if sync_progress.get(user_id, {}).get("status") == "running":
        raise HTTPException(status_code=400, detail="Cannot delete data while import is in progress")

    # Delete reviews → watch_history → user_content_status (cascades as needed)
    await db.execute(text(
        "DELETE FROM public.reviews WHERE user_id = :uid AND imported_from = 'letterboxd'"
    ), {"uid": uid})

    await db.execute(text(
        "DELETE FROM public.watch_history WHERE user_id = :uid AND imported_from = 'letterboxd'"
    ), {"uid": uid})

    await db.execute(text(
        "DELETE FROM public.user_content_status WHERE user_id = :uid AND imported_from = 'letterboxd'"
    ), {"uid": uid})

    await db.execute(text(
        "DELETE FROM public.collection_items WHERE added_by = :uid AND imported_from = 'letterboxd'"
    ), {"uid": uid})

    # Delete custom collections created from Letterboxd that are now empty
    await db.execute(text("""
        DELETE FROM public.collections 
        WHERE user_id = :uid 
          AND collection_type = 'custom' 
          AND (SELECT COUNT(*) FROM public.collection_items WHERE collection_id = public.collections.id) = 0
    """), {"uid": uid})

    # Recalculate user_stats
    await db.execute(text("""
        UPDATE public.user_stats
        SET total_watched = (SELECT COUNT(DISTINCT content_id) FROM public.watch_history WHERE user_id = :uid),
            total_reviews = (SELECT COUNT(*) FROM public.reviews WHERE user_id = :uid AND is_deleted = false),
            total_posts = (SELECT COUNT(*) FROM public.reviews WHERE user_id = :uid AND is_deleted = false) + COALESCE((SELECT COUNT(*) FROM public.posts WHERE user_id = :uid), 0),
            updated_at = now()
        WHERE user_id = :uid
    """), {"uid": uid})

    # Recalculate collections item_count
    await db.execute(text("""
        UPDATE public.collections c
        SET item_count = (SELECT COUNT(*) FROM public.collection_items WHERE collection_id = c.id),
            updated_at = now()
        WHERE c.user_id = :uid
    """), {"uid": uid})

    # Reset letterboxd import status to idle so they can re-import
    await db.execute(text("""
        UPDATE public.profiles
        SET letterboxd_import_status = 'idle', updated_at = now()
        WHERE id = :uid
    """), {"uid": uid})

    await db.commit()

    # Clear in-memory progress
    sync_progress.pop(user_id, None)

    return ok({"message": "All imported data deleted successfully"})

@router.post('/skip', response_model=Dict[str, Any])
async def skip_onboarding_import(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Sets status to 'skipped' so onboarding flow does not prompt user again."""
    await db.execute(text(
        "UPDATE public.profiles SET letterboxd_import_status = 'skipped', updated_at = now() WHERE id = :uid"
    ), {"uid": UUID(user_id)})
    await db.commit()
    return ok({"message": "Onboarding skipped successfully"})

@router.post('/cancel', response_model=Dict[str, Any])
async def cancel_letterboxd_import(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Cancels an in-progress Letterboxd import and marks it as failed."""
    # Ensure there is a progress dict in memory to receive the cancellation signal
    if user_id not in sync_progress:
        sync_progress[user_id] = {
            "processed": 0,
            "total": 0,
            "status": "failed",
            "current_item": None,
            "recent_items": [],
            "imported_films": 0,
            "imported_reviews": 0,
        }

    # Signal the background task to stop at its next check
    sync_progress[user_id]["cancelled"] = True
    sync_progress[user_id]["status"] = "failed"

    # Update DB immediately so the UI reflects cancellation right away
    try:
        await db.execute(text(
            "UPDATE public.profiles SET letterboxd_import_status = 'failed', updated_at = now() WHERE id = :uid"
        ), {"uid": UUID(user_id)})
        await db.commit()
    except Exception as db_err:
        await db.rollback()
        logger.error(f"Failed to update profile to failed during cancel: {db_err}")

    return ok({"message": "Import cancelled"})
