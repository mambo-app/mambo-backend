import asyncio
import logging
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.services.tmdb_client import TMDBClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mambo.healing')

async def heal_favorites():
    tmdb = TMDBClient()
    
    async with AsyncSessionLocal() as db:
        print("--- Scanning Orphaned Favorites ---")
        
        # 1. Identity orphans in user_person_favorites
        # Orphans are entries where person_id is a UUID NOT found in the persons table
        res = await db.execute(text("""
            SELECT id, person_id, name, user_id
            FROM user_person_favorites
            WHERE person_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        """))
        potential_orphans = [dict(row) for row in res.mappings()]
        
        healed_count = 0
        for orphan in potential_orphans:
            pid = orphan['person_id']
            name = orphan['name']
            
            # Check if this UUID exists in persons table
            check = await db.execute(text("SELECT 1 FROM persons WHERE id::text = :pid"), {"pid": pid})
            if check.scalar():
                continue # Not an orphan
            
            print(f"Found orphaned favorite: {name} (ID: {pid})")
            
            # Search TMDB for this person
            search_results = await tmdb.search_people(name)
            if not search_results:
                print(f"  FAILED: Could not find '{name}' on TMDB.")
                continue
                
            # Match by name (exact or very close)
            match = None
            for r in search_results[:3]:
                if r['name'].lower() == name.lower():
                    match = r
                    break
            
            if not match:
                match = search_results[0] # Fallback to first result
                print(f"  WARNING: No exact name match for '{name}'. Using best guess: '{match['name']}'")
            
            new_tmdb_id_int = match['tmdb_id']
            print(f"  HEALING: Resolving {name} (TMDB: {new_tmdb_id_int})")
            
            # --- UPSERT into persons table ---
            # This ensures we have a valid local UUID for preference tables
            from app.services.content_service import ContentService
            service = ContentService(db)
            
            # Prepare minimal person dict for upsert
            person_dict = {
                "id": new_tmdb_id_int,
                "name": name,
                "profile_path": match.get('profile_url', '').replace(tmdb.IMAGE_BASE, '')
            }
            
            new_uuid_str = await service._upsert_person(person_dict)
            if not new_uuid_str:
                print(f"  FAILED: Could not upsert {name}")
                continue
                
            print(f"  SUCCESS: Mapped to new local UUID: {new_uuid_str}")
            
            # --- Update all tables with the NEW UUID ---
            # Update user_person_favorites
            await db.execute(text("""
                UPDATE user_person_favorites
                SET person_id = :new_id
                WHERE id = :entry_id
            """), {"new_id": new_uuid_str, "entry_id": orphan['id']})
            
            # Update user_actor_preferences
            await db.execute(text("""
                UPDATE user_actor_preferences
                SET person_id = :new_id
                WHERE person_id = :old_id AND user_id = :uid
            """), {"new_id": new_uuid_str, "old_id": pid, "uid": orphan['user_id']})
            
            # Update user_director_preferences
            await db.execute(text("""
                UPDATE user_director_preferences
                SET person_id = :new_id
                WHERE person_id = :old_id AND user_id = :uid
            """), {"new_id": new_uuid_str, "old_id": pid, "uid": orphan['user_id']})
            
            healed_count += 1
            # Commit after each person to avoid massive rollbacks if one fails
            await db.commit()
            
        print(f"\nHEALING COMPLETE. Resolved {healed_count} orphaned records.")

if __name__ == "__main__":
    asyncio.run(heal_favorites())
