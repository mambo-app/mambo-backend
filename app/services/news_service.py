import httpx
import html
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.config import settings
from app.core.logging import get_logger
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

logger = get_logger('mambo.news')

class NewsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def init_table(self):
        # Create table if not exists for temporary news storage
        await self.db.execute(text('''
            CREATE TABLE IF NOT EXISTS news_articles (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT NOT NULL,
                description TEXT,
                url TEXT UNIQUE NOT NULL,
                external_url TEXT NOT NULL,
                image_url TEXT,
                source_name TEXT,
                category TEXT DEFAULT 'all',
                is_active BOOLEAN DEFAULT true,
                published_at TIMESTAMP WITH TIME ZONE,
                content TEXT,
                fetched_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        '''))
        await self.db.commit()

    async def fetch_and_store_news(self, limit: int = 20):
        # Ensure table exists
        await self.init_table()
        
        # Check when we last fetched
        res = await self.db.execute(text('SELECT MAX(fetched_at) FROM news_articles'))
        last_fetched = res.scalar()
        
        # If fetched within last 6 hours, return
        if last_fetched:
            # handle naive or aware datetime fallback
            now = datetime.now(timezone.utc)
            if last_fetched.tzinfo is None:
                last_fetched = last_fetched.replace(tzinfo=timezone.utc)
            if (now - last_fetched) < timedelta(hours=6):
                return

        api_key = settings.news_api
        if not api_key:
            logger.error("No News API key configured")
            return

        # Fetch news related to movies, anime, series, pop culture
        query = '("movies" OR "anime" OR "tv series" OR "netflix" OR "pop culture")'
        url = f"https://newsapi.org/v2/everything?q={query}&language=en&sortBy=publishedAt&apiKey={api_key}&pageSize={limit}"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                articles = data.get('articles', [])
                
                async def extract_full_text(article_url: str) -> str:
                    try:
                        from bs4 import BeautifulSoup
                        async with httpx.AsyncClient(timeout=3.0, follow_redirects=True) as scraper:
                            s_resp = await scraper.get(article_url, headers={'User-Agent': 'Mozilla/5.0'})
                            if s_resp.status_code == 200:
                                soup = BeautifulSoup(s_resp.text, 'html.parser')

                                # 1. Aggressively remove all noise-generating tags
                                for tag in soup.find_all(['script', 'style', 'nav', 'header',
                                                         'footer', 'aside', 'figure', 'noscript',
                                                         'iframe', 'form', 'button', 'svg']):
                                    tag.decompose()

                                # 2. Remove ad/signup blocks by common class/id names
                                noise_selectors = [
                                    '[class*="ad"]', '[class*="promo"]', '[class*="banner"]',
                                    '[class*="subscribe"]', '[class*="newsletter"]',
                                    '[class*="related"]', '[class*="read-next"]',
                                    '[class*="sidebar"]', '[class*="menu"]',
                                    '[class*="breadcrumb"]', '[id*="ad"]',
                                ]
                                for selector in noise_selectors:
                                    for el in soup.select(selector):
                                        el.decompose()

                                # 3. Extract text from remaining <p> tags
                                paragraphs = soup.find_all('p')
                                clean_text = []
                                for p in paragraphs:
                                    chunk = p.get_text(separator=' ', strip=True)
                                    # Decode HTML entities and filter out short noise
                                    chunk = html.unescape(chunk)
                                    if len(chunk) > 40:
                                        clean_text.append(chunk)

                                if clean_text:
                                    return "\n\n".join(clean_text)
                    except Exception as scrape_err:
                        logger.debug(f"Scraping failed for {article_url}: {scrape_err}")
                    return ""

                # Insert new articles
                for article in articles:
                    if article.get('title') and article['title'] != '[Removed]':
                        try:
                            # Handle different datetime formats NewsAPI might return
                            pub_str = article.get('publishedAt')
                            pub_date = None
                            if pub_str:
                                try:
                                    pub_date = datetime.strptime(pub_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                                except ValueError:
                                    pub_date = datetime.now(timezone.utc)

                            # Try to get fuller content
                            content = article.get('content') or ""
                            # Remove the "[+xxxx chars]" suffix often added by NewsAPI
                            import re
                            content = re.sub(r'\s*\[\+\d+ chars\]\s*$', '', content)
                            
                            # Disable scraping for now to save server memory
                            # scraped = await extract_full_text(article['url'])
                            # if len(scraped) > len(content):
                            #     content = scraped

                            async with self.db.begin_nested():
                                await self.db.execute(text('''
                                    INSERT INTO news_articles (title, description, content, url, external_url, image_url, source_name, published_at, fetched_at)
                                    VALUES (:title, :description, :content, :url, :url, :image_url, :source_name, :published_at, now())
                                    ON CONFLICT (url) DO UPDATE SET
                                        title = EXCLUDED.title,
                                        description = EXCLUDED.description,
                                        content = EXCLUDED.content,
                                        external_url = EXCLUDED.external_url,
                                        image_url = EXCLUDED.image_url,
                                        published_at = EXCLUDED.published_at,
                                        fetched_at = now()
                                '''), {
                                    'title': html.unescape(article['title'])[:250],
                                    'description': html.unescape(article.get('description') or ''),
                                    'content': content,
                                    'url': article['url'],
                                    'image_url': article.get('urlToImage'),
                                    'source_name': article['source']['name'] if article.get('source') else None,
                                    'published_at': pub_date
                                })
                        except Exception as e:
                            import traceback
                            traceback.print_exc()
                            logger.error(f"Failed to insert news article: {e}")
                
                # Clean up old temporary news (older than 7 days)
                await self.db.execute(text("DELETE FROM news_articles WHERE fetched_at < now() - interval '7 days'"))
                
                await self.db.commit()
        except Exception as e:
            logger.error(f"Failed to fetch news from NewsAPI: {e}")

    async def get_latest_news(self, category: str = 'all', limit: int = 20) -> List[Dict[str, Any]]:
        query = '''
            SELECT id, title, description, content, url, image_url, source_name, category, published_at
            FROM news_articles
            WHERE is_active = true
        '''
        params: Dict[str, Any] = {'limit': limit}
        if category != 'all':
            query += " AND category = :category"
            params['category'] = category
            
        query += " ORDER BY published_at DESC NULLS LAST LIMIT :limit"
        
        res = await self.db.execute(text(query), params)
        return [dict(row) for row in res.mappings()]
