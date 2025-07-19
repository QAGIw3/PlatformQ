# PlatformQ Notifications Library

A multi-channel notification library for PlatformQ services, replacing the standalone notification service with a reusable library.

## Features

- **Multi-channel Support**: Email, Zulip, Webhooks, SMS
- **Async/Await**: Fully asynchronous notification delivery
- **Retry Logic**: Automatic retries with exponential backoff
- **Trust-based Routing**: Optional trust evaluation for notifications
- **Event Publishing**: Integration with platform event system
- **Extensible**: Easy to add new notification channels

## Installation

```bash
pip install platformq-notifications
```

## Quick Start

```python
from platformq_notifications import (
    NotificationManager,
    NotificationChannel,
    ZulipChannel,
    EmailChannel
)
from platformq_shared.zulip_client import ZulipClient

# Initialize notification manager
notification_manager = NotificationManager()

# Register channels
zulip_client = ZulipClient(api_url="...", email="...", api_key="...")
notification_manager.register_channel(
    NotificationChannel.ZULIP,
    ZulipChannel(zulip_client)
)

notification_manager.register_channel(
    NotificationChannel.EMAIL,
    EmailChannel(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_user="notifications@example.com",
        smtp_password="password",
        from_email="notifications@example.com"
    )
)

# Send notification
notification = await notification_manager.send_notification(
    tenant_id="tenant123",
    user_id="user456",
    channels=["email", "zulip"],
    subject="Important Update",
    content="Your project has been approved!",
    priority="high",
    metadata={
        "email": "user@example.com",
        "zulip_stream": "project-updates"
    }
)
```

## Channel Configuration

### Email Channel
```python
from platformq_notifications import EmailChannel

email_channel = EmailChannel(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    smtp_user="your-email@gmail.com",
    smtp_password="your-app-password",
    from_email="notifications@yourcompany.com",
    use_tls=True
)
```

### Zulip Channel
```python
from platformq_notifications import ZulipChannel

zulip_channel = ZulipChannel(
    zulip_client=your_zulip_client
)
```

### Webhook Channel
```python
from platformq_notifications import WebhookChannel

webhook_channel = WebhookChannel(
    default_timeout=30.0,
    default_headers={"X-Platform": "PlatformQ"}
)
```

## Trust-Enhanced Notifications

```python
from platformq_notifications import TrustEnhancedNotificationSystem

trust_system = TrustEnhancedNotificationSystem(
    graph_service_url="http://graph-intelligence-service:8000",
    min_trust_score=0.5
)

# Check if notification should be routed
should_send = await trust_system.should_route_notification(
    source_user_id="sender123",
    target_user_id="recipient456",
    tenant_id="tenant789",
    notification_type="collaboration"
)
```

## Custom Channels

Create custom notification channels by extending `NotificationChannelBase`:

```python
from platformq_notifications.core import NotificationChannelBase, Notification

class SlackChannel(NotificationChannelBase):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
    async def send(self, notification: Notification) -> bool:
        # Implement Slack notification logic
        pass
        
    async def health_check(self) -> bool:
        # Check Slack webhook connectivity
        return True
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/
ruff check src/
``` 