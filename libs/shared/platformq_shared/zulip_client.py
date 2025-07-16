import requests
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
import json
import time
from functools import wraps
import mimetypes
import os

from .resilience import (
    with_resilience, 
    get_connection_pool_manager,
    get_metrics_collector,
    CircuitBreakerError,
    RateLimitExceeded
)

logger = logging.getLogger(__name__)


class ZulipError(Exception):
    """Base exception for Zulip client errors"""
    pass


class ZulipAuthError(ZulipError):
    """Authentication related errors"""
    pass


class ZulipNotFoundError(ZulipError):
    """Resource not found errors"""
    pass


class ZulipRateLimitError(ZulipError):
    """Rate limit exceeded errors"""
    pass


def retry_on_failure(max_retries: int = 3, backoff_factor: float = 1.0):
    """Decorator for retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.ConnectionError, requests.Timeout) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = backoff_factor * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed, retrying in {sleep_time}s: {e}")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"All {max_retries} attempts failed")
            raise last_exception
        return wrapper
    return decorator


class ZulipClient:
    """
    Comprehensive Zulip client with support for:
    - Messaging (stream and private)
    - User management
    - Stream/channel management
    - Subscriptions
    - File uploads
    - Webhooks
    - Presence tracking
    """
    
    def __init__(self, zulip_site: str, zulip_email: str, zulip_api_key: str,
                 timeout: int = 30, verify_ssl: bool = True,
                 use_connection_pool: bool = True, rate_limit: float = 20.0):
        self.base_url = f"{zulip_site.rstrip('/')}/api/v1"
        self.auth = (zulip_email, zulip_api_key)
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.headers = {"User-Agent": "PlatformQ-Zulip-Client/1.0"}
        
        # Connection pooling
        self.use_connection_pool = use_connection_pool
        self.rate_limit = rate_limit
        if use_connection_pool:
            self.session = get_connection_pool_manager().get_session(
                self.base_url,
                headers=self.headers,
                auth=self.auth,
                timeout=self.timeout
            )
        else:
            self.session = None
        
    def _handle_response(self, response: requests.Response) -> Dict:
        """Handle API response and raise appropriate exceptions"""
        if response.status_code == 401:
            raise ZulipAuthError("Authentication failed")
        elif response.status_code == 404:
            raise ZulipNotFoundError("Resource not found")
        elif response.status_code == 429:
            raise ZulipRateLimitError("Rate limit exceeded")
        elif response.status_code >= 400:
            try:
                error_data = response.json()
                msg = error_data.get('msg', 'Unknown error')
            except:
                msg = response.text
            raise ZulipError(f"API Error {response.status_code}: {msg}")
            
        try:
            return response.json()
        except json.JSONDecodeError:
            return {}
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request using session if available"""
        if self.session:
            return self.session.request(method, url, verify=self.verify_ssl, **kwargs)
        else:
            kwargs.setdefault('auth', self.auth)
            kwargs.setdefault('timeout', self.timeout)
            kwargs.setdefault('verify', self.verify_ssl)
            return requests.request(method, url, **kwargs)

    # Messaging
    @with_resilience("zulip", rate_limit=20.0)
    def send_message(self, content: str, message_type: str = "stream", 
                     to: Union[str, List[str]] = None, topic: str = None,
                     queue_id: str = None, local_id: str = None) -> Dict:
        """
        Send a message to a stream or private message
        
        Args:
            content: Message content
            message_type: 'stream' or 'private'
            to: Stream name for stream messages, list of user emails for private
            topic: Topic for stream messages (required for streams)
            queue_id: Optional queue ID for message sending optimization
            local_id: Optional local ID for idempotency
        """
        url = f"{self.base_url}/messages"
        data = {
            "type": message_type,
            "content": content
        }
        
        if message_type == "stream":
            if not to or not topic:
                raise ValueError("Stream name and topic are required for stream messages")
            data["to"] = to
            data["topic"] = topic
        else:  # private
            if not to:
                raise ValueError("Recipient list is required for private messages")
            data["to"] = json.dumps(to) if isinstance(to, list) else to
            
        if queue_id:
            data["queue_id"] = queue_id
        if local_id:
            data["local_id"] = local_id
            
        response = self._request("post".upper(), url, data=data,
                                )
        result = self._handle_response(response)
        logger.info(f"Successfully sent message (ID: {result.get('id')})")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_messages(self, num_before: int = 0, num_after: int = 0,
                     anchor: Union[int, str] = "newest", narrow: List[Dict] = None,
                     client_gravatar: bool = False, apply_markdown: bool = True) -> Dict:
        """
        Fetch messages from Zulip
        
        Args:
            num_before: Number of messages before anchor
            num_after: Number of messages after anchor
            anchor: Message ID, 'newest', 'oldest', or 'first_unread'
            narrow: List of filters (e.g., [{"operator": "stream", "operand": "general"}])
            client_gravatar: Whether to include gravatar URLs
            apply_markdown: Whether to apply markdown rendering
        """
        url = f"{self.base_url}/messages"
        params = {
            "num_before": num_before,
            "num_after": num_after,
            "anchor": anchor,
            "client_gravatar": json.dumps(client_gravatar),
            "apply_markdown": json.dumps(apply_markdown)
        }
        
        if narrow:
            params["narrow"] = json.dumps(narrow)
            
        response = self._request("get".upper(), url, params=params,
                               )
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def update_message(self, message_id: int, content: str = None, 
                       topic: str = None, propagate_mode: str = "change_one",
                       send_notification_to_old_thread: bool = True) -> Dict:
        """
        Edit a message
        
        Args:
            message_id: ID of the message to edit
            content: New content (optional)
            topic: New topic for stream messages (optional)
            propagate_mode: 'change_one', 'change_later', or 'change_all'
            send_notification_to_old_thread: Notify users in old thread
        """
        url = f"{self.base_url}/messages/{message_id}"
        data = {}
        
        if content is not None:
            data["content"] = content
        if topic is not None:
            data["topic"] = topic
            data["propagate_mode"] = propagate_mode
            data["send_notification_to_old_thread"] = json.dumps(send_notification_to_old_thread)
            
        if not data:
            raise ValueError("Either content or topic must be provided")
            
        response = self._request("patch".upper(), url, data=data,
                                 )
        result = self._handle_response(response)
        logger.info(f"Successfully updated message {message_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def delete_message(self, message_id: int) -> Dict:
        """Delete a message"""
        url = f"{self.base_url}/messages/{message_id}"
        response = self._request("delete".upper(), url,
                                  )
        result = self._handle_response(response)
        logger.info(f"Successfully deleted message {message_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def add_reaction(self, message_id: int, emoji_name: str, 
                     emoji_code: str = None, reaction_type: str = "unicode_emoji") -> Dict:
        """Add an emoji reaction to a message"""
        url = f"{self.base_url}/messages/{message_id}/reactions"
        data = {
            "emoji_name": emoji_name,
            "reaction_type": reaction_type
        }
        
        if emoji_code:
            data["emoji_code"] = emoji_code
            
        response = self._request("post".upper(), url, data=data,
                                )
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def remove_reaction(self, message_id: int, emoji_name: str,
                        emoji_code: str = None, reaction_type: str = "unicode_emoji") -> Dict:
        """Remove an emoji reaction from a message"""
        url = f"{self.base_url}/messages/{message_id}/reactions"
        params = {
            "emoji_name": emoji_name,
            "reaction_type": reaction_type
        }
        
        if emoji_code:
            params["emoji_code"] = emoji_code
            
        response = self._request("delete".upper(), url, params=params,
                                  )
        return self._handle_response(response)
    
    # Stream/Channel Management
    @with_resilience("zulip", rate_limit=20.0)
    def create_stream(self, name: str, description: str = "", 
                      invite_only: bool = False, announce: bool = False,
                      history_public_to_subscribers: bool = True,
                      stream_post_policy: int = 1, message_retention_days: int = None) -> Dict:
        """
        Create a new stream
        
        Args:
            name: Stream name
            description: Stream description
            invite_only: Whether stream is private
            announce: Whether to announce stream creation
            history_public_to_subscribers: Whether history is visible to new subscribers
            stream_post_policy: Who can post (1=any, 2=admins, 3=full members, 4=moderators)
            message_retention_days: Message retention period
        """
        url = f"{self.base_url}/users/me/subscriptions"
        stream_data = {
            "name": name,
            "description": description,
            "invite_only": json.dumps(invite_only),
            "history_public_to_subscribers": json.dumps(history_public_to_subscribers),
            "stream_post_policy": stream_post_policy
        }
        
        if message_retention_days is not None:
            stream_data["message_retention_days"] = message_retention_days
            
        data = {
            "subscriptions": json.dumps([stream_data]),
            "announce": json.dumps(announce)
        }
        
        response = self._request("post".upper(), url, data=data,
                                )
        result = self._handle_response(response)
        logger.info(f"Successfully created stream: {name}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_stream_id(self, stream: str) -> int:
        """Get stream ID by name"""
        url = f"{self.base_url}/get_stream_id"
        params = {"stream": stream}
        
        response = self._request("get".upper(), url, params=params,
                               )
        result = self._handle_response(response)
        return result.get("stream_id")
    
    @with_resilience("zulip", rate_limit=20.0)
    def update_stream(self, stream_id: int, description: str = None,
                      new_name: str = None, invite_only: bool = None,
                      stream_post_policy: int = None, message_retention_days: int = None,
                      history_public_to_subscribers: bool = None) -> Dict:
        """Update stream properties"""
        url = f"{self.base_url}/streams/{stream_id}"
        data = {}
        
        if description is not None:
            data["description"] = json.dumps(description)
        if new_name is not None:
            data["new_name"] = json.dumps(new_name)
        if invite_only is not None:
            data["is_private"] = json.dumps(invite_only)
        if stream_post_policy is not None:
            data["stream_post_policy"] = json.dumps(stream_post_policy)
        if message_retention_days is not None:
            data["message_retention_days"] = json.dumps(message_retention_days)
        if history_public_to_subscribers is not None:
            data["history_public_to_subscribers"] = json.dumps(history_public_to_subscribers)
            
        response = self._request("patch".upper(), url, data=data,
                                 )
        result = self._handle_response(response)
        logger.info(f"Successfully updated stream {stream_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def delete_stream(self, stream_id: int) -> Dict:
        """Delete/archive a stream"""
        url = f"{self.base_url}/streams/{stream_id}"
        response = self._request("delete".upper(), url,
                                  )
        result = self._handle_response(response)
        logger.info(f"Successfully deleted stream {stream_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_streams(self, include_public: bool = True, include_subscribed: bool = True,
                    include_all_active: bool = False, include_default: bool = False,
                    include_owner_subscribed: bool = False) -> List[Dict]:
        """Get all streams"""
        url = f"{self.base_url}/streams"
        params = {
            "include_public": json.dumps(include_public),
            "include_subscribed": json.dumps(include_subscribed),
            "include_all_active": json.dumps(include_all_active),
            "include_default": json.dumps(include_default),
            "include_owner_subscribed": json.dumps(include_owner_subscribed)
        }
        
        response = self._request("get".upper(), url, params=params,
                               )
        result = self._handle_response(response)
        return result.get("streams", [])
    
    # Subscriptions
    @with_resilience("zulip", rate_limit=20.0)
    def subscribe_users(self, streams: List[str], principals: List[str] = None,
                        authorization_errors_fatal: bool = False, announce: bool = False,
                        invite_only: bool = False) -> Dict:
        """
        Subscribe users to streams
        
        Args:
            streams: List of stream names
            principals: List of user emails to subscribe (None = self)
            authorization_errors_fatal: Whether auth errors should be fatal
            announce: Whether to announce new subscriptions
            invite_only: Whether streams should be invite-only
        """
        url = f"{self.base_url}/users/me/subscriptions"
        
        # Format streams data
        stream_data = [{"name": stream} for stream in streams]
        
        data = {
            "subscriptions": json.dumps(stream_data),
            "authorization_errors_fatal": json.dumps(authorization_errors_fatal),
            "announce": json.dumps(announce),
            "invite_only": json.dumps(invite_only)
        }
        
        if principals:
            data["principals"] = json.dumps(principals)
            
        response = self._request("post".upper(), url, data=data,
                                )
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def unsubscribe_users(self, streams: List[str], principals: List[str] = None) -> Dict:
        """Unsubscribe users from streams"""
        url = f"{self.base_url}/users/me/subscriptions"
        
        params = {
            "subscriptions": json.dumps(streams)
        }
        
        if principals:
            params["principals"] = json.dumps(principals)
            
        response = self._request("delete".upper(), url, params=params,
                                  )
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_subscriptions(self, include_subscribers: bool = False) -> List[Dict]:
        """Get user's subscriptions"""
        url = f"{self.base_url}/users/me/subscriptions"
        params = {
            "include_subscribers": json.dumps(include_subscribers)
        }
        
        response = self._request("get".upper(), url, params=params,
                               )
        result = self._handle_response(response)
        return result.get("subscriptions", [])
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_subscribers(self, stream: str) -> List[str]:
        """Get subscribers of a stream"""
        stream_id = self.get_stream_id(stream)
        url = f"{self.base_url}/streams/{stream_id}/members"
        
        response = self._request("get".upper(), url,
                               )
        result = self._handle_response(response)
        return result.get("subscribers", [])
    
    # User Management
    @with_resilience("zulip", rate_limit=20.0)
    def create_user(self, email: str, password: str, full_name: str) -> Dict:
        """Create a new user"""
        url = f"{self.base_url}/users"
        data = {
            "email": email,
            "password": password,
            "full_name": full_name
        }
        
        response = self._request("post".upper(), url, data=data,
                                )
        result = self._handle_response(response)
        logger.info(f"Successfully created user: {email}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def update_user(self, user_id: int, full_name: str = None, 
                    role: int = None, profile_data: List[Dict] = None) -> Dict:
        """
        Update user information
        
        Args:
            user_id: User ID to update
            full_name: New full name
            role: New role (100=owner, 200=admin, 300=moderator, 400=member, 600=guest)
            profile_data: Custom profile fields
        """
        url = f"{self.base_url}/users/{user_id}"
        data = {}
        
        if full_name is not None:
            data["full_name"] = json.dumps(full_name)
        if role is not None:
            data["role"] = json.dumps(role)
        if profile_data is not None:
            data["profile_data"] = json.dumps(profile_data)
            
        response = self._request("patch".upper(), url, data=data,
                                 )
        result = self._handle_response(response)
        logger.info(f"Successfully updated user {user_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def deactivate_user(self, user_id: int) -> Dict:
        """Deactivate a user"""
        url = f"{self.base_url}/users/{user_id}"
        response = self._request("delete".upper(), url,
                                  )
        result = self._handle_response(response)
        logger.info(f"Successfully deactivated user {user_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def reactivate_user(self, user_id: int) -> Dict:
        """Reactivate a deactivated user"""
        url = f"{self.base_url}/users/{user_id}/reactivate"
        response = self._request("post".upper(), url,
                                )
        result = self._handle_response(response)
        logger.info(f"Successfully reactivated user {user_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_users(self, client_gravatar: bool = False, include_custom_profile_fields: bool = False) -> List[Dict]:
        """Get all users in the organization"""
        url = f"{self.base_url}/users"
        params = {
            "client_gravatar": json.dumps(client_gravatar),
            "include_custom_profile_fields": json.dumps(include_custom_profile_fields)
        }
        
        response = self._request("get".upper(), url, params=params,
                               )
        result = self._handle_response(response)
        return result.get("members", [])
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_user(self, user_id: int, client_gravatar: bool = False,
                 include_custom_profile_fields: bool = False) -> Dict:
        """Get a specific user"""
        url = f"{self.base_url}/users/{user_id}"
        params = {
            "client_gravatar": json.dumps(client_gravatar),
            "include_custom_profile_fields": json.dumps(include_custom_profile_fields)
        }
        
        response = self._request("get".upper(), url, params=params,
                               )
        result = self._handle_response(response)
        return result.get("user", {})
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_user_presence(self, user_id_or_email: Union[int, str]) -> Dict:
        """Get user presence information"""
        url = f"{self.base_url}/users/{user_id_or_email}/presence"
        response = self._request("get".upper(), url,
                               )
        return self._handle_response(response)
    
    # File Uploads
    @with_resilience("zulip", rate_limit=20.0)
    def upload_file(self, file_path: str) -> Dict:
        """Upload a file to Zulip"""
        url = f"{self.base_url}/user_uploads"
        
        filename = os.path.basename(file_path)
        content_type, _ = mimetypes.guess_type(file_path)
        
        with open(file_path, 'rb') as f:
            files = {
                'file': (filename, f, content_type or 'application/octet-stream')
            }
            response = self._request("post".upper(), url, files=files,
                                    )
            
        result = self._handle_response(response)
        logger.info(f"Successfully uploaded file: {filename}")
        return result
    
    # Topics
    @with_resilience("zulip", rate_limit=20.0)
    def get_stream_topics(self, stream_id: int) -> List[Dict]:
        """Get all topics in a stream"""
        url = f"{self.base_url}/users/me/{stream_id}/topics"
        response = self._request("get".upper(), url,
                               )
        result = self._handle_response(response)
        return result.get("topics", [])
    
    @with_resilience("zulip", rate_limit=20.0)
    def delete_topic(self, stream_id: int, topic_name: str) -> Dict:
        """Delete all messages in a topic"""
        url = f"{self.base_url}/streams/{stream_id}/delete_topic"
        data = {"topic_name": topic_name}
        
        response = self._request("post".upper(), url, data=data,
                                )
        result = self._handle_response(response)
        logger.info(f"Successfully deleted topic '{topic_name}' from stream {stream_id}")
        return result
    
    @with_resilience("zulip", rate_limit=20.0)
    def update_message_flags(self, messages: List[int], op: str, flag: str) -> Dict:
        """
        Update message flags (e.g., mark as read)
        
        Args:
            messages: List of message IDs
            op: Operation ('add' or 'remove')
            flag: Flag name ('read', 'starred', 'collapsed', 'mentioned')
        """
        url = f"{self.base_url}/messages/flags"
        data = {
            "messages": json.dumps(messages),
            "op": op,
            "flag": flag
        }
        
        response = self._request("post".upper(), url, data=data,
                                )
        return self._handle_response(response)
    
    # Webhooks
    @with_resilience("zulip", rate_limit=20.0)
    def create_outgoing_webhook(self, bot_email: str, interface_type: int,
                                url: str, events: List[str], config: Dict = None) -> Dict:
        """
        Create an outgoing webhook bot
        
        Args:
            bot_email: Email for the bot
            interface_type: 1=generic, 2=slack-compatible
            url: Webhook URL
            events: List of event types to send
            config: Additional configuration
        """
        # This would typically require admin privileges
        # Implementation depends on Zulip server configuration
        raise NotImplementedError("Outgoing webhook creation requires server-side configuration")
    
    @with_resilience("zulip", rate_limit=20.0)
    def list_linkifiers(self) -> List[Dict]:
        """Get configured linkifiers (URL patterns)"""
        url = f"{self.base_url}/realm/linkifiers"
        response = self._request("get".upper(), url,
                               )
        result = self._handle_response(response)
        return result.get("linkifiers", [])
    
    # Miscellaneous
    @with_resilience("zulip", rate_limit=20.0)
    def get_server_settings(self) -> Dict:
        """Get public server settings"""
        url = f"{self.base_url}/server_settings"
        response = requests.get(url, timeout=self.timeout, verify=self.verify_ssl)
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def mark_all_as_read(self) -> Dict:
        """Mark all messages as read"""
        url = f"{self.base_url}/mark_all_as_read"
        response = self._request("post".upper(), url,
                                )
        return self._handle_response(response)
    
    @with_resilience("zulip", rate_limit=20.0)
    def get_user_groups(self) -> List[Dict]:
        """Get all user groups"""
        url = f"{self.base_url}/user_groups"
        response = self._request("get".upper(), url,
                               )
        result = self._handle_response(response)
        return result.get("user_groups", [])
    
    def typing_notification(self, op: str, to: Union[List[str], str], 
                           message_type: str = "private") -> Dict:
        """
        Send typing notification
        
        Args:
            op: 'start' or 'stop'
            to: Recipients (email list for private, stream name for stream)
            message_type: 'private' or 'stream'
        """
        url = f"{self.base_url}/typing"
        data = {
            "op": op,
            "type": message_type
        }
        
        if message_type == "private":
            data["to"] = json.dumps(to) if isinstance(to, list) else json.dumps([to])
        else:
            data["stream_id"] = self.get_stream_id(to)
            
        response = self._request("post".upper(), url, data=data,
                                )
        return self._handle_response(response) 