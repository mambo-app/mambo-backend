import asyncio
import httpx

async def main():
    async with httpx.AsyncClient() as c:
        res = await c.get('http://localhost:8000/v1/users/lakshhh03/reviews')
        data = res.json().get('data', [])
        for d in data:
            print(f"Review: {d.get('text_review')[:20] if d.get('text_review') else 'None'} | Title: {d.get('content_title')}")

asyncio.run(main())
