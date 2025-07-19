"""
Notification channel implementations.
"""

import logging
from typing import Dict, Any, Optional
import httpx
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from platformq_shared.zulip_client import ZulipClient

from .core import NotificationChannelBase, Notification

logger = logging.getLogger(__name__)


class EmailChannel(NotificationChannelBase):
    """Email notification channel"""
    
    def __init__(self,
                 smtp_host: str,
                 smtp_port: int,
                 smtp_user: str,
                 smtp_password: str,
                 from_email: str,
                 use_tls: bool = True):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        self.use_tls = use_tls
        
    async def send(self, notification: Notification) -> bool:
        """Send email notification"""
        try:
            # Get recipient email from metadata or user service
            recipient_email = notification.metadata.get("email")
            if not recipient_email:
                logger.error(f"No email address for notification {notification.notification_id}")
                return False
                
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = recipient_email
            msg['Subject'] = notification.subject
            
            # Add body
            body = MIMEText(notification.content, 'plain')
            msg.attach(body)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                
            logger.info(f"Email sent to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
            
    async def health_check(self) -> bool:
        """Check SMTP server connectivity"""
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=5) as server:
                if self.use_tls:
                    server.starttls()
                server.noop()
            return True
        except Exception:
            return False


class ZulipChannel(NotificationChannelBase):
    """Zulip notification channel"""
    
    def __init__(self, zulip_client: ZulipClient):
        self.zulip_client = zulip_client
        
    async def send(self, notification: Notification) -> bool:
        """Send Zulip notification"""
        try:
            # Determine target
            stream = notification.metadata.get("zulip_stream", "general")
            topic = notification.metadata.get("zulip_topic", "Notifications")
            
            # Format message
            message_content = f"**{notification.subject}**\n\n{notification.content}"
            
            # Add priority indicator
            if notification.priority.value >= 4:  # URGENT or CRITICAL
                message_content = f"🚨 {message_content}"
                
            # Send message
            result = self.zulip_client.send_message(
                message_type="stream",
                to=stream,
                topic=topic,
                content=message_content
            )
            
            if result.get("result") == "success":
                logger.info(f"Zulip message sent to {stream}/{topic}")
                return True
            else:
                logger.error(f"Zulip API error: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Zulip message: {e}")
            return False
            
    async def health_check(self) -> bool:
        """Check Zulip API connectivity"""
        try:
            result = self.zulip_client.get_server_settings()
            return result.get("result") == "success"
        except Exception:
            return False


class WebhookChannel(NotificationChannelBase):
    """Webhook notification channel"""
    
    def __init__(self, 
                 default_timeout: float = 30.0,
                 default_headers: Optional[Dict[str, str]] = None):
        self.default_timeout = default_timeout
        self.default_headers = default_headers or {}
        self._client = httpx.AsyncClient(timeout=default_timeout)
        
    async def send(self, notification: Notification) -> bool:
        """Send webhook notification"""
        try:
            # Get webhook URL from metadata
            webhook_url = notification.metadata.get("webhook_url")
            if not webhook_url:
                logger.error(f"No webhook URL for notification {notification.notification_id}")
                return False
                
            # Prepare payload
            payload = {
                "notification_id": notification.notification_id,
                "subject": notification.subject,
                "content": notification.content,
                "priority": notification.priority.name,
                "metadata": notification.metadata,
                "timestamp": notification.created_at.isoformat()
            }
            
            # Merge headers
            headers = self.default_headers.copy()
            custom_headers = notification.metadata.get("webhook_headers", {})
            headers.update(custom_headers)
            
            # Send webhook
            response = await self._client.post(
                webhook_url,
                json=payload,
                headers=headers
            )
            
            if response.status_code < 300:
                logger.info(f"Webhook sent to {webhook_url}")
                return True
            else:
                logger.error(f"Webhook failed with status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send webhook: {e}")
            return False
            
    async def health_check(self) -> bool:
        """Webhook channel is always healthy"""
        return True
        
    async def close(self):
        """Close HTTP client"""
        await self._client.aclose()


class SMSChannel(NotificationChannelBase):
    """SMS notification channel (placeholder for actual implementation)"""
    
    def __init__(self,
                 provider: str = "twilio",
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 from_number: Optional[str] = None):
        self.provider = provider
        self.api_key = api_key
        self.api_secret = api_secret
        self.from_number = from_number
        
    async def send(self, notification: Notification) -> bool:
        """Send SMS notification"""
        try:
            # Get phone number from metadata
            phone_number = notification.metadata.get("phone_number")
            if not phone_number:
                logger.error(f"No phone number for notification {notification.notification_id}")
                return False
                
            # Truncate content for SMS
            sms_content = f"{notification.subject}: {notification.content}"[:160]
            
            # TODO: Implement actual SMS sending based on provider
            # For now, just log
            logger.info(f"SMS would be sent to {phone_number}: {sms_content}")
            
            # In production, integrate with Twilio, AWS SNS, etc.
            return True
            
        except Exception as e:
            logger.error(f"Failed to send SMS: {e}")
            return False
            
    async def health_check(self) -> bool:
        """Check SMS provider connectivity"""
        # TODO: Implement actual health check
        return True 