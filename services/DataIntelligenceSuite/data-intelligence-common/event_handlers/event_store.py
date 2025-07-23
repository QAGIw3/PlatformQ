"""Event store for event sourcing and replay capabilities."""

import logging
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
import json
import asyncio

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from .pulsar_bus import PulsarEventBus
from .event_types import EventType

logger = logging.getLogger(__name__)


class EventStore:
    """
    Store for event sourcing and replay with Vault/Consul integration.
    
    Provides capabilities for:
    - Storing events for audit and replay with encryption
    - Querying historical events with access control
    - Event replay for recovery or reprocessing
    - Secure event archival
    """
    
    def __init__(self, 
                 event_bus: PulsarEventBus,
                 vault_client: Optional[VaultClient] = None,
                 consul_client: Optional[ConsulClient] = None,
                 retention_days: int = 30):
        """
        Initialize event store.
        
        Args:
            event_bus: PulsarEventBus instance
            vault_client: Optional Vault client for encryption
            consul_client: Optional Consul client for configuration
            retention_days: How long to retain events
        """
        self.event_bus = event_bus
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.retention_days = retention_days
        
        # Configuration from Consul
        self._config: Dict[str, Any] = {
            "encryption_enabled": True,
            "encryption_key": "event-store",
            "compression_enabled": True,
            "archive_after_days": 7,
            "allowed_replay_roles": ["admin", "data-engineer"],
            "sensitive_event_types": []
        }
        self._config_task: Optional[asyncio.Task] = None
        
        # Cache for event metadata
        self._event_index: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize event store"""
        if self.consul_client:
            await self._load_config()
            self._config_task = asyncio.create_task(self._watch_config())
        logger.info("Initialized event store")
        
    async def shutdown(self):
        """Shutdown event store"""
        if self._config_task:
            self._config_task.cancel()
            try:
                await self._config_task
            except asyncio.CancelledError:
                pass
                
    async def _load_config(self):
        """Load configuration from Consul"""
        try:
            config_data = await self.consul_client.kv_get("data-intelligence/event-store/config")
            if config_data:
                self._config.update(json.loads(config_data))
                logger.info("Loaded event store configuration from Consul")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            
    async def _watch_config(self):
        """Watch for configuration changes"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._load_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config: {e}")
        
    async def store_event(self, event: Any) -> str:
        """
        Store event for replay with optional encryption.
        
        Args:
            event: Event to store
            
        Returns:
            Message ID of stored event
        """
        try:
            # Convert event to dict if needed
            event_data = event if isinstance(event, dict) else event.to_dict()
            
            # Check if this is a sensitive event type
            event_type = event_data.get("event_type", "")
            is_sensitive = event_type in self._config.get("sensitive_event_types", [])
            
            # Encrypt if enabled and Vault is available
            if self._config.get("encryption_enabled") and self.vault_client:
                # Always encrypt sensitive events
                if is_sensitive or self._config.get("encrypt_all", False):
                    plaintext = json.dumps(event_data)
                    encrypted = await self.vault_client.transit_encrypt(
                        self._config["encryption_key"],
                        plaintext
                    )
                    event_data = {
                        "encrypted": encrypted["ciphertext"],
                        "event_type": event_type,  # Keep for indexing
                        "timestamp": event_data.get("timestamp"),
                        "event_id": event_data.get("event_id"),
                        "correlation_id": event_data.get("correlation_id")
                    }
            
            # Add metadata
            event_data["_stored_at"] = datetime.utcnow().isoformat()
            event_data["_retention_days"] = self.retention_days
            event_data["_sensitive"] = is_sensitive
            
            # Publish to special event-store topic with retention policy
            msg_id = await self.event_bus.publish(
                event_data,
                priority=self.event_bus.EventPriority.LOW  # Storage is low priority
            )
            
            # Update index
            event_id = event_data.get("event_id")
            if event_id:
                self._event_index[event_id] = {
                    "message_id": str(msg_id),
                    "event_type": event_type,
                    "timestamp": event_data.get("timestamp"),
                    "correlation_id": event_data.get("correlation_id"),
                    "encrypted": "encrypted" in event_data,
                    "sensitive": is_sensitive
                }
            
            logger.debug(f"Stored event with ID: {msg_id}")
            return str(msg_id)
            
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            raise
        
    async def query_events(self,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          event_types: Optional[List[str]] = None,
                          correlation_id: Optional[str] = None,
                          limit: int = 100,
                          user_context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Query events with filters and access control.
        
        Args:
            start_time: Start of time range
            end_time: End of time range  
            event_types: Filter by event types
            correlation_id: Filter by correlation ID
            limit: Maximum events to return
            user_context: User context for access control
            
        Returns:
            List of events matching criteria
        """
        # Check replay permissions
        if user_context:
            user_roles = user_context.get("roles", [])
            allowed_roles = self._config.get("allowed_replay_roles", [])
            if not any(role in allowed_roles for role in user_roles):
                logger.warning(f"User lacks permission for event replay")
                return []
        
        # Build query
        events = []
        
        # Filter from index
        for event_id, metadata in self._event_index.items():
            # Apply filters
            if event_types and metadata["event_type"] not in event_types:
                continue
            if correlation_id and metadata.get("correlation_id") != correlation_id:
                continue
            if start_time and metadata.get("timestamp"):
                event_time = datetime.fromisoformat(metadata["timestamp"])
                if event_time < start_time:
                    continue
            if end_time and metadata.get("timestamp"):
                event_time = datetime.fromisoformat(metadata["timestamp"])
                if event_time > end_time:
                    continue
                    
            # Check if user can access sensitive events
            if metadata.get("sensitive") and user_context:
                if "admin" not in user_context.get("roles", []):
                    continue
                    
            events.append(metadata)
            
            if len(events) >= limit:
                break
                
        # TODO: Implement actual event retrieval from storage
        # This would fetch the full events based on message IDs
        
        return events
        
    async def get_event_by_id(self, event_id: str,
                             user_context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific event by ID with access control.
        
        Args:
            event_id: Event ID to retrieve
            user_context: User context for access control
            
        Returns:
            Event data if found and accessible, None otherwise
        """
        # Check if event exists in index
        metadata = self._event_index.get(event_id)
        if not metadata:
            logger.debug(f"Event {event_id} not found in index")
            return None
            
        # Check access permissions
        if metadata.get("sensitive") and user_context:
            if "admin" not in user_context.get("roles", []):
                logger.warning(f"User lacks permission to access sensitive event {event_id}")
                return None
                
        # TODO: Implement actual retrieval from storage
        # This would fetch from Pulsar based on message_id
        
        # For now, return metadata
        return metadata
        
    async def get_events_by_correlation_id(self, 
                                         correlation_id: str,
                                         user_context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Get all events with a specific correlation ID.
        
        Args:
            correlation_id: Correlation ID to search for
            user_context: User context for access control
            
        Returns:
            List of related events
        """
        return await self.query_events(
            correlation_id=correlation_id,
            user_context=user_context
        )
        
    async def replay_events(self,
                           start_time: datetime,
                           end_time: datetime,
                           event_types: Optional[List[str]] = None,
                           target_topic: Optional[str] = None,
                           user_context: Optional[Dict[str, Any]] = None) -> int:
        """
        Replay events for a time range.
        
        Args:
            start_time: Start of replay range
            end_time: End of replay range
            event_types: Optional filter by event types
            target_topic: Topic to replay to (default: original topics)
            user_context: User context for access control
            
        Returns:
            Number of events replayed
        """
        # Check replay permissions
        if user_context:
            user_roles = user_context.get("roles", [])
            allowed_roles = self._config.get("allowed_replay_roles", [])
            if not any(role in allowed_roles for role in user_roles):
                raise PermissionError("User lacks permission for event replay")
                
        # Query events
        events = await self.query_events(
            start_time=start_time,
            end_time=end_time,
            event_types=event_types,
            user_context=user_context
        )
        
        replayed = 0
        for event_metadata in events:
            try:
                # TODO: Fetch full event from storage
                # For now, just count
                replayed += 1
                
                # Would republish to target topic or original topic
                
            except Exception as e:
                logger.error(f"Failed to replay event {event_metadata.get('event_id')}: {e}")
                
        logger.info(f"Replayed {replayed} events from {start_time} to {end_time}")
        return replayed
        
    async def compact_events(self, 
                           entity_type: str,
                           entity_id: str,
                           up_to_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Compact events for an entity into current state.
        
        Args:
            entity_type: Type of entity
            entity_id: Entity ID
            up_to_time: Compact events up to this time
            
        Returns:
            Current state of the entity
        """
        logger.info(f"Compacting events for {entity_type}:{entity_id}")
        
        # This would:
        # 1. Retrieve all events for the entity
        # 2. Apply them in order to build current state
        # 3. Return the final state
        
        # TODO: Implement event compaction
        return {}
        
    async def archive_old_events(self) -> int:
        """
        Archive events older than retention period.
        
        Returns:
            Number of events archived
        """
        archive_after = self._config.get("archive_after_days", 7)
        cutoff_date = datetime.utcnow() - timedelta(days=archive_after)
        
        archived = 0
        
        # Find events to archive
        for event_id, metadata in list(self._event_index.items()):
            if metadata.get("timestamp"):
                event_time = datetime.fromisoformat(metadata["timestamp"])
                if event_time < cutoff_date:
                    # TODO: Move to cold storage (e.g., MinIO)
                    # For now, just remove from index
                    del self._event_index[event_id]
                    archived += 1
                    
        logger.info(f"Archived {archived} events older than {archive_after} days")
        return archived
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get event store statistics"""
        total_events = len(self._event_index)
        encrypted_events = sum(1 for m in self._event_index.values() if m.get("encrypted"))
        sensitive_events = sum(1 for m in self._event_index.values() if m.get("sensitive"))
        
        # Group by event type
        event_type_counts = {}
        for metadata in self._event_index.values():
            event_type = metadata.get("event_type", "unknown")
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            
        return {
            "total_events": total_events,
            "encrypted_events": encrypted_events,
            "sensitive_events": sensitive_events,
            "event_types": event_type_counts,
            "retention_days": self.retention_days,
            "config": {
                "encryption_enabled": self._config.get("encryption_enabled"),
                "compression_enabled": self._config.get("compression_enabled"),
                "archive_after_days": self._config.get("archive_after_days")
            }
        } 