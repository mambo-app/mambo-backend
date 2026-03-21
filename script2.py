import asyncio
from app.core.database import AsyncSessionLocal
from app.repositories.social_repo import SocialRepository
from sqlalchemy import text

async def main():
    async with AsyncSessionLocal() as db:
        res = await db.execute(text("SELECT id, user_id, content_id, text_review FROM reviews WHERE text_review LIKE '%Damnn bhaiii%'"))
        review = res.mappings().first()
        if not review:
            print("Review not found")
            return
            
        print(f"Review Content ID: {review['content_id']}")
        content_res = await db.execute(text("SELECT id, title FROM content WHERE id = :cid"), {'cid': review['content_id']})
        content_row = content_res.mappings().first()
        print(f"Content: {content_row}")
        
        repo = SocialRepository(db)
        fetched = await repo.get_reviews_by_user(review['user_id'])
        for f in fetched:
            if f['id'] == review['id']:
                print(f"Fetched via repo:")
                print(f"Title: {f.get('content_title')}")
                print(f"Poster: {f.get('content_poster')}")

asyncio.run(main())
