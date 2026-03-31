import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from app.services.push_service import PushService
from firebase_admin import messaging

class TestPushService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_db = AsyncMock()
        self.service = PushService(self.mock_db)

    @patch('app.services.push_service.messaging.send')
    async def test_send_to_user_cleans_up_stale_tokens(self, mock_send):
        """
        Behavior: If FCM returns UnregisteredError, the token should be deleted from DB.
        """
        user_id = "test-user-id"
        stale_token = "stale-token"
        valid_token = "valid-token"
        
        # Mock DB returning two tokens
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(stale_token,), (valid_token,)]
        self.mock_db.execute.return_value = mock_result

        # Mock messaging.send: first call fails, second succeeds
        mock_send.side_effect = [messaging.UnregisteredError("Stale"), None]

        await self.service.send_to_user(user_id, "Title", "Body")

        # Verify messaging.send was called twice
        self.assertEqual(mock_send.call_count, 2)
        
        # Verify DELETE was called for stale_token
        delete_called = False
        for call in self.mock_db.execute.call_args_list:
            sql = str(call[0][0])
            params = call[0][1] if len(call[0]) > 1 else {}
            if "DELETE FROM push_tokens" in sql and params.get('token') == stale_token:
                delete_called = True
        
        self.assertTrue(delete_called, "Stale token should have been deleted")
        self.assertEqual(self.mock_db.commit.call_count, 1)

    @patch('app.services.push_service.messaging.send')
    async def test_send_to_user_handles_other_errors(self, mock_send):
        """
        Behavior: Other errors should be logged but not delete the token.
        """
        user_id = "test-user-id"
        token = "some-token"
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(token,)]
        self.mock_db.execute.return_value = mock_result

        mock_send.side_effect = Exception("General failure")

        await self.service.send_to_user(user_id, "Title", "Body")

        # Verify DELETE was NOT called
        for call in self.mock_db.execute.call_args_list:
            sql = str(call[0][0])
            self.assertNotIn("DELETE FROM push_tokens", sql)

if __name__ == '__main__':
    unittest.main()
