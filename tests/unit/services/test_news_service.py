import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from fastapi import HTTPException
from app.services.news_service import NewsService

class TestNewsService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_db = AsyncMock()
        # begin_nested is a synchronous call that returns an async context manager
        self.mock_db.begin_nested = MagicMock()
        self.mock_db.begin_nested.return_value.__aenter__ = AsyncMock()
        self.mock_db.begin_nested.return_value.__aexit__ = AsyncMock()
        self.service = NewsService(self.mock_db)

    @patch('app.services.news_service.httpx.AsyncClient')
    @patch('app.services.news_service.settings')
    async def test_fetch_removed_articles_filtered(self, mock_settings, mock_client_class):
        """
        Behavior: Articles with title '[Removed]' are filtered out.
        Bug Protection: Prevents broken or retracted articles from NewsAPI from entering the database.
        """
        mock_settings.news_api = "test_key"
        
        # Mock last_fetched to be old enough
        mock_result = MagicMock()
        mock_result.scalar.return_value = datetime.now(timezone.utc) - timedelta(hours=7)
        self.mock_db.execute.return_value = mock_result

        # Mock NewsAPI response
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            'articles': [
                {'title': 'Valid Article', 'url': 'http://valid.com', 'source': {'name': 'Source'}, 'publishedAt': '2024-01-01T00:00:00Z'},
                {'title': '[Removed]', 'url': 'http://removed.com', 'source': {'name': 'Source'}}
            ]
        }
        mock_client.get.return_value = mock_resp

        await self.service.fetch_and_store_news()

        # Should only try to insert 'Valid Article'
        insert_calls = []
        for call in self.mock_db.execute.call_args_list:
            arg = call[0][0]
            # Handle case where arg is a text object or a string
            sql = str(arg.text) if hasattr(arg, 'text') else str(arg)
            if "INSERT INTO news_articles" in sql:
                insert_calls.append(call)

        self.assertEqual(len(insert_calls), 1)
        # Check title in params (second arg of execute)
        self.assertEqual(insert_calls[0][0][1]['title'], 'Valid Article')

    @patch('app.services.news_service.httpx.AsyncClient')
    @patch('app.services.news_service.settings')
    async def test_fetch_debounce_6h(self, mock_settings, mock_client_class):
        """
        Behavior: Fetch is skipped if last fetch was within 6 hours.
        Bug Protection: Prevents excessive API calls and database writes, respecting rate limits.
        """
        mock_settings.news_api = "test_key"
        
        # Mock last_fetched to be only 3 hours ago
        mock_result = MagicMock()
        # Use a timezone-aware datetime
        mock_result.scalar.return_value = datetime.now(timezone.utc) - timedelta(hours=3)
        self.mock_db.execute.return_value = mock_result

        await self.service.fetch_and_store_news()

        # httpx.AsyncClient should NOT be instantiated/called
        self.assertFalse(mock_client_class.called)

    @patch('app.services.news_service.settings')
    @patch('app.services.news_service.logger')
    async def test_fetch_missing_api_key(self, mock_logger, mock_settings):
        """
        Behavior: Fetch returns early and logs error if News API key is missing.
        Bug Protection: Ensures application doesn't crash or make invalid requests when misconfigured.
        """
        mock_settings.news_api = None
        
        # Mock last_fetched to be old
        mock_result = MagicMock()
        mock_result.scalar.return_value = datetime.now(timezone.utc) - timedelta(hours=10)
        self.mock_db.execute.return_value = mock_result

        await self.service.fetch_and_store_news()

        mock_logger.error.assert_called_with("No News API key configured")
        # init_table calls execute, debounce checks scalar execute. Total 2.
        self.assertEqual(self.mock_db.execute.call_count, 2)

    @patch('app.services.news_service.httpx.AsyncClient')
    @patch('app.services.news_service.settings')
    async def test_fetch_cleanup_old_articles(self, mock_settings, mock_client_class):
        """
        Behavior: Articles older than 7 days are deleted after fetching.
        Bug Protection: Prevents database bloat by removing stale temporary news data.
        """
        mock_settings.news_api = "test_key"
        
        # Mock last_fetched to be old
        mock_result = MagicMock()
        mock_result.scalar.return_value = datetime.now(timezone.utc) - timedelta(hours=10)
        self.mock_db.execute.return_value = mock_result

        # Mock successful empty fetch
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'articles': []}
        mock_client.get.return_value = mock_resp

        await self.service.fetch_and_store_news()

        # Check for cleanup query
        cleanup_called = False
        for call in self.mock_db.execute.call_args_list:
            arg = call[0][0]
            sql = str(arg.text) if hasattr(arg, 'text') else str(arg)
            if "DELETE FROM news_articles WHERE fetched_at < now() - interval '7 days'" in sql:
                cleanup_called = True
                break
        self.assertTrue(cleanup_called)

if __name__ == '__main__':
    unittest.main()
