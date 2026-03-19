import unittest
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
from fastapi import HTTPException
from app.services.social_service import SocialService

# Decorators apply bottom-up. 
# MockChat corresponds to the bottom patch, MockRepo to the top patch.
@patch('app.services.social_service.SocialRepository') # TOP
@patch('app.services.social_service.NotificationService') # MID
@patch('app.services.social_service.ChatService') # BOTTOM
class TestSocialService(unittest.IsolatedAsyncioTestCase):
    
    async def asyncSetUp(self):
        self.mock_db = AsyncMock()

    def get_service(self, MockChat, MockNotif, MockRepo):
        # NOTE: Bottom decorator is the first argument after self
        MockRepo.return_value = AsyncMock()
        MockNotif.return_value = AsyncMock()
        MockChat.return_value = AsyncMock()
        
        s = SocialService(self.mock_db)
        # Link internal instances to mocks for assertion access
        # s.repo corresponds to SocialRepository patch (MockRepo)
        # s.notif_service corresponds to NotificationService patch (MockNotif)
        s.repo_mock = s.repo
        s.notif_mock = s.notif_service
        s.chat_mock_class = MockChat
        return s

    async def test_debug_mock_order(self, MockChat, MockNotif, MockRepo):
        # Requested by user to verify mock order
        # MockChat should be ChatService
        self.assertIn('ChatService', str(MockChat))
        # MockRepo should be SocialRepository
        self.assertIn('SocialRepository', str(MockRepo))

    # --- Friend Request Tests ---

    async def test_send_friend_request_to_self(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        user_id = uuid4()
        with self.assertRaises(HTTPException) as cm:
            await service.send_friend_request(user_id, user_id)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Cannot send friend request to yourself", cm.exception.detail)

    async def test_send_friend_request_already_friends(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        sender_id, receiver_id = uuid4(), uuid4()
        service.repo_mock.check_request_exists.return_value = {'status': 'accepted'}
        with self.assertRaises(HTTPException) as cm:
            await service.send_friend_request(sender_id, receiver_id)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Already friends", cm.exception.detail)

    async def test_send_friend_request_success(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        sender_id, receiver_id = uuid4(), uuid4()
        service.repo_mock.check_request_exists.return_value = None
        service.repo_mock.create_friend_request.return_value = {'id': uuid4()}
        await service.send_friend_request(sender_id, receiver_id)
        service.repo_mock.create_friend_request.assert_called_once_with(sender_id, receiver_id)
        service.notif_mock.create_notification.assert_called_once()

    async def test_respond_to_request_invalid_status(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Validate status input
        service = self.get_service(MockChat, MockNotif, MockRepo)
        with self.assertRaises(HTTPException) as cm:
            await service.respond_to_request(uuid4(), uuid4(), 'invalid_status')
        self.assertEqual(cm.exception.status_code, 400)

    async def test_respond_to_request_not_found(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: 404 for missing requests
        service = self.get_service(MockChat, MockNotif, MockRepo)
        service.repo_mock.get_friend_request.return_value = None
        with self.assertRaises(HTTPException) as cm:
            await service.respond_to_request(uuid4(), uuid4(), 'accepted')
        self.assertEqual(cm.exception.status_code, 404)

    async def test_respond_to_request_unauthorized(self, MockChat, MockNotif, MockRepo):
        # SECURITY PROTECTION: Only recipient can respond
        service = self.get_service(MockChat, MockNotif, MockRepo)
        user_id = uuid4()
        service.repo_mock.get_friend_request.return_value = {'receiver_id': uuid4(), 'status': 'pending'}
        with self.assertRaises(HTTPException) as cm:
            await service.respond_to_request(user_id, uuid4(), 'accepted')
        self.assertEqual(cm.exception.status_code, 403)

    async def test_respond_to_request_already_processed(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Prevent duplicate processing
        service = self.get_service(MockChat, MockNotif, MockRepo)
        user_id = uuid4()
        service.repo_mock.get_friend_request.return_value = {'receiver_id': user_id, 'status': 'accepted'}
        with self.assertRaises(HTTPException) as cm:
            await service.respond_to_request(user_id, uuid4(), 'accepted')
        self.assertEqual(cm.exception.status_code, 400)

    async def test_respond_to_request_accept_success(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        user_id, sender_id = uuid4(), uuid4()
        req = {'sender_id': sender_id, 'receiver_id': user_id, 'status': 'pending'}
        service.repo_mock.get_friend_request.return_value = req
        service.repo_mock.update_request_status.return_value = {**req, 'status': 'accepted'}
        result = await service.respond_to_request(user_id, uuid4(), 'accepted')
        service.repo_mock.add_friend.assert_called_once_with(sender_id, user_id)
        self.assertEqual(service.repo_mock.increment_friends_count.call_count, 2)
        service.notif_mock.create_notification.assert_called_once()

    # --- Community & Interaction ---

    async def test_create_post(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        await service.create_post(uuid4(), "Title", "Body")
        service.repo_mock.create_post.assert_called_once()

    async def test_create_comment_invalid(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Prevent orphan comments
        service = self.get_service(MockChat, MockNotif, MockRepo)
        with self.assertRaises(HTTPException) as cm:
            await service.create_comment(uuid4(), "Hi", post_id=None, review_id=None)
        self.assertEqual(cm.exception.status_code, 400)

    async def test_save_post(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Manual SQL commit verification
        service = self.get_service(MockChat, MockNotif, MockRepo)
        await service.save_post(uuid4(), uuid4())
        self.assertTrue(self.mock_db.execute.called)
        self.assertTrue(self.mock_db.commit.called)

    # --- Sharing BUG protection ---

    async def test_get_share_metadata_missing_identifiers(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Raise 400 before calling ChatService
        service = self.get_service(MockChat, MockNotif, MockRepo)
        with self.assertRaises(HTTPException) as cm:
            await service.get_share_metadata(uuid4(), uuid4(), 'post', None, None)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertFalse(MockChat.called)

    async def test_get_share_metadata_success_post(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        post_id = uuid4()
        service.repo_mock.get_post.return_value = {'title': 'Post'}
        mock_chat_inst = MockChat.return_value
        await service.get_share_metadata(uuid4(), post_id, 'post', conversation_id=uuid4())
        mock_chat_inst.send_message.assert_called_once()
        self.assertTrue(self.mock_db.execute.called)

    async def test_share_post_delegation(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        post_id = uuid4()
        service.repo_mock.get_post.return_value = {'title': 'Post'}
        mock_chat_inst = MockChat.return_value
        await service.share_post(uuid4(), post_id, conversation_id=uuid4())
        service.repo_mock.get_post.assert_called_with(post_id)
        mock_chat_inst.send_message.assert_called_once()

    # --- Moderation ---

    async def test_mute_user_self(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        u_id = uuid4()
        with self.assertRaises(HTTPException) as cm:
            await service.mute_user(u_id, u_id)
        self.assertEqual(cm.exception.status_code, 400)

    async def test_block_user_self(self, MockChat, MockNotif, MockRepo):
        # BUG PROTECTION: Self-block check
        service = self.get_service(MockChat, MockNotif, MockRepo)
        u_id = uuid4()
        with self.assertRaises(HTTPException) as cm:
            await service.block_user(u_id, u_id)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Cannot block yourself", cm.exception.detail)

    async def test_unblock_user_success(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        t_id = uuid4()
        await service.unblock_user(uuid4(), t_id)
        service.repo_mock.unblock_user.assert_called_once_with(unittest.mock.ANY, t_id)

    async def test_share_review_delegation(self, MockChat, MockNotif, MockRepo):
        service = self.get_service(MockChat, MockNotif, MockRepo)
        review_id = uuid4()
        service.repo_mock.get_review.return_value = {'text_review': 'Review'}
        mock_chat_inst = MockChat.return_value
        await service.share_review(uuid4(), review_id, conversation_id=uuid4())
        service.repo_mock.get_review.assert_called_with(review_id)
        mock_chat_inst.send_message.assert_called_once()
