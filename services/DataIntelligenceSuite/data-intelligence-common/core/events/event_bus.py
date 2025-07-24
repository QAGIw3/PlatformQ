"""
Unified Event Bus for DataIntelligenceSuite

Provides pub/sub capabilities with multiple backend support.
"""

import asyncio
import logging
import json
from typing import Any, Dict, Optional, List, Callable, Set, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import uuid

from .base import (
    Event, EventPriority, EventDeliveryMode, EventHandler,
    EventProcessingConfig, EventRouter, BaseEventProcessor
)
from .bus import UnifiedEventBus, EventSubscription, SubscriptionType
from ...monitoring import MetricsCollector

logger = logging.getLogger(__name__)


# Backward compatibility - re-export everything from bus.py
EventBus = UnifiedEventBus


# Re-export for backward compatibility
__all__ = [
    'Event', 'EventPriority', 'EventDeliveryMode', 'EventHandler',
    'EventProcessingConfig', 'EventRouter', 'BaseEventProcessor',
    'EventBus', 'UnifiedEventBus', 'EventSubscription', 'SubscriptionType'
] 