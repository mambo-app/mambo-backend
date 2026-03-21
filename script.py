import asyncio
from app.core.database import AsyncSessionLocal
from app.repositories.social_repo import SocialRepository
from uuid import UUID

async def main():
    async with AsyncSessionLocal() as db:
        repo = SocialRepository(db)
        
        result = await repo.fetch_one('''
            SELECT DISTINCT r.user_id 
            FROM reviews r
            LIMIT 1
        ''', {})
        
        if not result:
            print("No reviews found in DB")
            return
            
        user_id = result['user_id']
        print(f"Fetching reviews for user {user_id}")
        
        reviews = await repo.get_reviews_by_user(user_id, limit=5)
        
        for r in reviews:
            print(f"Review ID: {r.get('id')}")
            print(f"Content ID: {r.get('content_id')}")
            print(f"Title: {r.get('content_title')}")
            print(f"Poster: {r.get('content_poster')}")
            print(f"Text: {r.get('text_review')}")
            print("---")

asyncio.run(main())
