import asyncio
import httpx

async def main():
    async with httpx.AsyncClient() as client:
        # /v1/users/lakshhh03/reviews
        res = await client.get("http://localhost:8000/v1/users/lakshhh03/reviews")
        print("Status code:", res.status_code)
        
        # Print keys of the first item
        data = res.json().get('data', [])
        if data:
            print("Response fields:", data[0].keys())
            for d in data:
                print(f"Text: {d.get('text_review')}")
                print(f"Title: {d.get('content_title')} | {d.get('title')}")
                print(f"Poster: {d.get('content_poster')} | {d.get('poster_url')}")
                print('---')
        else:
            print("No data", res.json())

asyncio.run(main())
