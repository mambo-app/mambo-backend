import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Dict, Any
from fastapi import HTTPException

logger = logging.getLogger('mambo.reports')

class ReportService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_report(self, reporter_id: str, data: dict) -> Dict:
        cols = ["reported_by", "report_type", "reason"]
        vals = [":reported_by", ":report_type", ":reason"]
        
        insert_data = {
            "reported_by": reporter_id,
            "report_type": data["report_type"],
            "reason": data["reason"]
        }
        
        for k in ["description", "reported_user_id", "post_id", "review_id", "message_id", "news_id"]:
            if data.get(k):
                # Self-report check
                if k == "post_id":
                    res = await self.db.execute(text("SELECT user_id FROM posts WHERE id = :id"), {"id": data[k]})
                    post = res.mappings().one_or_none()
                    if post and str(post["user_id"]) == str(reporter_id):
                        raise HTTPException(status_code=400, detail="You cannot report your own post.")
                elif k == "review_id":
                    res = await self.db.execute(text("SELECT user_id FROM reviews WHERE id = :id"), {"id": data[k]})
                    review = res.mappings().one_or_none()
                    if review and str(review["user_id"]) == str(reporter_id):
                        raise HTTPException(status_code=400, detail="You cannot report your own review.")
                elif k == "reported_user_id" and str(data[k]) == str(reporter_id):
                    raise HTTPException(status_code=400, detail="You cannot report yourself.")

                cols.append(k)
                vals.append(f":{k}")
                insert_data[k] = str(data[k])
                
        query = f"INSERT INTO reported_content ({', '.join(cols)}) VALUES ({', '.join(vals)}) RETURNING id"
        
        res = await self.db.execute(text(query), insert_data)
        await self.db.commit()
        return {"id": str(res.scalar())}
        
    async def get_reports(self, limit: int = 50, offset: int = 0) -> list[Dict]:
        res = await self.db.execute(text("SELECT * FROM reported_content ORDER BY reported_at DESC LIMIT :l OFFSET :o"), {"l": limit, "o": offset})
        return [dict(row) for row in res.mappings()]
