"""
Event Sourcing support for DataIntelligenceSuite

Provides event sourcing patterns and aggregate root implementation.
"""

import logging
from typing import Any, Dict, Optional, List, Type, TypeVar
from datetime import datetime
from dataclasses import dataclass, field
import json
from abc import ABC, abstractmethod

from .event_bus import Event, EventBus
from .event_store import EventStore

logger = logging.getLogger(__name__)

T = TypeVar('T', bound='AggregateRoot')


@dataclass
class DomainEvent(Event):
    """Base class for domain events"""
    aggregate_id: str = ""
    aggregate_version: int = 0
    
    def apply_to(self, aggregate: 'AggregateRoot'):
        """Apply event to aggregate - override in subclasses"""
        pass


class EventSourcingMixin:
    """
    Mixin for event sourcing capabilities.
    
    Provides:
    - Event recording
    - Event application
    - State reconstruction
    """
    
    def __init__(self):
        self._pending_events: List[DomainEvent] = []
        self._version: int = 0
        
    def record_event(self, event: DomainEvent):
        """Record a domain event"""
        event.aggregate_version = self._version + 1
        self._pending_events.append(event)
        self._apply_event(event)
        self._version += 1
        
    def _apply_event(self, event: DomainEvent):
        """Apply event to current state"""
        # Use convention-based handler lookup
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        
        if handler:
            handler(event)
        else:
            # Fallback to event's apply_to method
            event.apply_to(self)
            
    def get_pending_events(self) -> List[DomainEvent]:
        """Get pending events and clear list"""
        events = self._pending_events.copy()
        self._pending_events.clear()
        return events
        
    def mark_events_committed(self):
        """Mark all pending events as committed"""
        self._pending_events.clear()
        
    def replay_events(self, events: List[DomainEvent]):
        """Replay events to reconstruct state"""
        for event in events:
            self._apply_event(event)
            self._version = event.aggregate_version


class AggregateRoot(EventSourcingMixin, ABC):
    """
    Base class for aggregate roots in domain-driven design.
    
    Features:
    - Event sourcing
    - Command handling
    - State management
    - Snapshot support
    """
    
    def __init__(self, aggregate_id: str):
        super().__init__()
        self.aggregate_id = aggregate_id
        self._created_at = datetime.utcnow()
        self._updated_at = datetime.utcnow()
        
    @property
    def version(self) -> int:
        """Get current version"""
        return self._version
        
    @property
    def is_new(self) -> bool:
        """Check if aggregate is new (no events)"""
        return self._version == 0
        
    @abstractmethod
    def validate_invariants(self):
        """Validate aggregate invariants - must be implemented"""
        pass
        
    def execute_command(self, command: Dict[str, Any]):
        """Execute command and record resulting events"""
        # Command handler convention
        command_type = command.get("type", "unknown")
        handler_name = f"handle_{command_type}"
        handler = getattr(self, handler_name, None)
        
        if not handler:
            raise ValueError(f"No handler for command type: {command_type}")
            
        # Execute handler
        handler(command)
        
        # Validate invariants after command
        self.validate_invariants()
        
        # Update timestamp
        self._updated_at = datetime.utcnow()
        
    def to_snapshot(self) -> Dict[str, Any]:
        """Create snapshot of current state"""
        return {
            "aggregate_id": self.aggregate_id,
            "version": self._version,
            "created_at": self._created_at.isoformat(),
            "updated_at": self._updated_at.isoformat(),
            "state": self._get_state()
        }
        
    def from_snapshot(self, snapshot: Dict[str, Any]):
        """Restore from snapshot"""
        self.aggregate_id = snapshot["aggregate_id"]
        self._version = snapshot["version"]
        self._created_at = datetime.fromisoformat(snapshot["created_at"])
        self._updated_at = datetime.fromisoformat(snapshot["updated_at"])
        self._set_state(snapshot["state"])
        
    @abstractmethod
    def _get_state(self) -> Dict[str, Any]:
        """Get internal state for snapshot - must be implemented"""
        pass
        
    @abstractmethod
    def _set_state(self, state: Dict[str, Any]):
        """Set internal state from snapshot - must be implemented"""
        pass


class EventSourcingRepository:
    """
    Repository for event-sourced aggregates.
    
    Features:
    - Load aggregates from events
    - Save aggregate events
    - Snapshot support
    - Event publishing
    """
    
    def __init__(
        self,
        event_store: EventStore,
        event_bus: Optional[EventBus] = None,
        snapshot_frequency: int = 10
    ):
        self.event_store = event_store
        self.event_bus = event_bus
        self.snapshot_frequency = snapshot_frequency
        
    async def load(
        self,
        aggregate_class: Type[T],
        aggregate_id: str,
        use_snapshot: bool = True
    ) -> Optional[T]:
        """Load aggregate from event store"""
        # Try to load from snapshot first
        snapshot = None
        if use_snapshot:
            snapshot = await self.event_store.get_snapshot(aggregate_id)
            
        # Create aggregate instance
        aggregate = aggregate_class(aggregate_id)
        
        # Restore from snapshot if available
        start_version = 0
        if snapshot:
            aggregate.from_snapshot(snapshot["state"])
            start_version = snapshot["version"]
            
        # Load events after snapshot
        events = await self.event_store.get_events_by_correlation(aggregate_id)
        
        # Filter events after snapshot version
        events_to_replay = [
            e for e in events
            if hasattr(e, 'aggregate_version') and e.aggregate_version > start_version
        ]
        
        # Replay events
        if events_to_replay:
            aggregate.replay_events(events_to_replay)
            
        # Return None if aggregate doesn't exist
        if aggregate.is_new and not snapshot:
            return None
            
        return aggregate
        
    async def save(self, aggregate: AggregateRoot):
        """Save aggregate events to store"""
        # Get pending events
        events = aggregate.get_pending_events()
        
        if not events:
            return
            
        # Set aggregate ID on events
        for event in events:
            event.aggregate_id = aggregate.aggregate_id
            event.correlation_id = aggregate.aggregate_id
            
        # Append to event store
        await self.event_store.append_events(events)
        
        # Publish events if event bus configured
        if self.event_bus:
            for event in events:
                await self.event_bus.publish(
                    f"aggregate.{event.event_type}",
                    event
                )
                
        # Create snapshot if needed
        if aggregate.version % self.snapshot_frequency == 0:
            await self._create_snapshot(aggregate, events[-1])
            
        # Mark events as committed
        aggregate.mark_events_committed()
        
    async def _create_snapshot(self, aggregate: AggregateRoot, last_event: DomainEvent):
        """Create aggregate snapshot"""
        await self.event_store.save_snapshot(
            aggregate_id=aggregate.aggregate_id,
            version=aggregate.version,
            state=aggregate.to_snapshot(),
            event_id=last_event.event_id
        )
        
    async def exists(self, aggregate_id: str) -> bool:
        """Check if aggregate exists"""
        events = await self.event_store.get_events_by_correlation(aggregate_id)
        return len(events) > 0


# Example usage
class OrderAggregate(AggregateRoot):
    """Example order aggregate"""
    
    def __init__(self, order_id: str):
        super().__init__(order_id)
        self.customer_id: Optional[str] = None
        self.items: List[Dict[str, Any]] = []
        self.status: str = "draft"
        self.total_amount: float = 0.0
        
    def handle_create_order(self, command: Dict[str, Any]):
        """Handle create order command"""
        if not self.is_new:
            raise ValueError("Order already exists")
            
        event = DomainEvent(
            event_type="order_created",
            source="order_aggregate",
            payload={
                "order_id": self.aggregate_id,
                "customer_id": command["customer_id"],
                "items": command["items"]
            }
        )
        
        self.record_event(event)
        
    def handle_add_item(self, command: Dict[str, Any]):
        """Handle add item command"""
        if self.status != "draft":
            raise ValueError("Cannot add items to non-draft order")
            
        event = DomainEvent(
            event_type="item_added",
            source="order_aggregate",
            payload={
                "order_id": self.aggregate_id,
                "item": command["item"]
            }
        )
        
        self.record_event(event)
        
    def handle_submit_order(self, command: Dict[str, Any]):
        """Handle submit order command"""
        if self.status != "draft":
            raise ValueError("Order already submitted")
            
        if not self.items:
            raise ValueError("Cannot submit empty order")
            
        event = DomainEvent(
            event_type="order_submitted",
            source="order_aggregate",
            payload={
                "order_id": self.aggregate_id,
                "total_amount": self.total_amount
            }
        )
        
        self.record_event(event)
        
    def _on_order_created(self, event: DomainEvent):
        """Apply order created event"""
        self.customer_id = event.payload["customer_id"]
        self.items = event.payload["items"]
        self._calculate_total()
        
    def _on_item_added(self, event: DomainEvent):
        """Apply item added event"""
        self.items.append(event.payload["item"])
        self._calculate_total()
        
    def _on_order_submitted(self, event: DomainEvent):
        """Apply order submitted event"""
        self.status = "submitted"
        
    def _calculate_total(self):
        """Calculate order total"""
        self.total_amount = sum(
            item.get("price", 0) * item.get("quantity", 1)
            for item in self.items
        )
        
    def validate_invariants(self):
        """Validate order invariants"""
        if self.total_amount < 0:
            raise ValueError("Order total cannot be negative")
            
        if self.status not in ["draft", "submitted", "cancelled"]:
            raise ValueError(f"Invalid order status: {self.status}")
            
    def _get_state(self) -> Dict[str, Any]:
        """Get state for snapshot"""
        return {
            "customer_id": self.customer_id,
            "items": self.items,
            "status": self.status,
            "total_amount": self.total_amount
        }
        
    def _set_state(self, state: Dict[str, Any]):
        """Set state from snapshot"""
        self.customer_id = state["customer_id"]
        self.items = state["items"]
        self.status = state["status"]
        self.total_amount = state["total_amount"] 