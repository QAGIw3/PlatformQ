"""
PlatformQ Notifications Library

Provides multi-channel notification capabilities for platform services.
"""

from .core import (
    NotificationChannel,
    NotificationPriority,
    NotificationStatus,
    Notification,
    NotificationManager
)

from .channels import (
    EmailChannel,
    ZulipChannel,
    WebhookChannel,
    SMSChannel
)

from .trust import (
    TrustEnhancedNotificationSystem,
    NotificationTrustLevel
)

__all__ = [
    # Core
    "NotificationChannel",
    "NotificationPriority",
    "NotificationStatus",
    "Notification",
    "NotificationManager",
    
    # Channels
    "EmailChannel",
    "ZulipChannel",
    "WebhookChannel",
    "SMSChannel",
    
    # Trust
    "TrustEnhancedNotificationSystem",
    "NotificationTrustLevel"
]

__version__ = "1.0.0" 