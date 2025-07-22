"""
SBT Event Publisher
"""

import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone

import pulsar


class SBTEventPublisher:
    """
    Publishes SBT events to Apache Pulsar
    """
    
    def __init__(
        self,
        pulsar_url: str,
        topic_prefix: str = "sbt"
    ):
        self.pulsar_url = pulsar_url
        self.topic_prefix = topic_prefix
        self.client = None
        self.producers = {}
        self.connected = False
    
    async def connect(self):
        """Connect to Pulsar"""
        try:
            self.client = pulsar.Client(
                self.pulsar_url,
                operation_timeout_seconds=30
            )
            self.connected = True
            print(f"Connected to Pulsar at {self.pulsar_url}")
        except Exception as e:
            print(f"Failed to connect to Pulsar: {str(e)}")
            self.connected = False
            raise
    
    async def close(self):
        """Close Pulsar connection"""
        if self.client:
            # Close all producers
            for producer in self.producers.values():
                producer.close()
            
            self.client.close()
            self.connected = False
    
    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        key: Optional[str] = None
    ):
        """
        Publish an event to Pulsar
        
        Args:
            event_type: Type of event (e.g., 'sbt_minted', 'sbt_revoked')
            data: Event data
            key: Optional message key for ordering
        """
        if not self.connected:
            print(f"Cannot publish event {event_type}: Not connected to Pulsar")
            return
        
        # Get or create producer for this event type
        topic = f"persistent://public/default/{self.topic_prefix}-{event_type}"
        
        if event_type not in self.producers:
            try:
                self.producers[event_type] = self.client.create_producer(
                    topic,
                    schema=pulsar.schema.JsonSchema(EventMessage),
                    compression_type=pulsar.CompressionType.LZ4,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=100
                )
            except Exception as e:
                print(f"Failed to create producer for {topic}: {str(e)}")
                return
        
        # Create event message
        event = EventMessage(
            event_id=f"{event_type}-{datetime.now(timezone.utc).timestamp()}",
            event_type=event_type,
            timestamp=datetime.now(timezone.utc).isoformat(),
            data=data,
            source="sbt-service"
        )
        
        try:
            # Send message
            if key:
                self.producers[event_type].send_async(
                    event,
                    partition_key=key,
                    callback=lambda res, msg: self._send_callback(res, msg, event_type)
                )
            else:
                self.producers[event_type].send_async(
                    event,
                    callback=lambda res, msg: self._send_callback(res, msg, event_type)
                )
            
        except Exception as e:
            print(f"Failed to publish event {event_type}: {str(e)}")
    
    def _send_callback(self, res, msg, event_type):
        """Callback for async send"""
        if res == pulsar.Result.Ok:
            print(f"Event {event_type} published successfully")
        else:
            print(f"Failed to publish event {event_type}: {res}")
    
    async def publish_batch(
        self,
        events: list[tuple[str, Dict[str, Any], Optional[str]]]
    ):
        """
        Publish multiple events
        
        Args:
            events: List of (event_type, data, key) tuples
        """
        tasks = []
        for event_type, data, key in events:
            task = self.publish(event_type, data, key)
            tasks.append(task)
        
        await asyncio.gather(*tasks)


class EventMessage:
    """Event message schema"""
    
    def __init__(
        self,
        event_id: str = "",
        event_type: str = "",
        timestamp: str = "",
        data: Dict[str, Any] = None,
        source: str = ""
    ):
        self.event_id = event_id
        self.event_type = event_type
        self.timestamp = timestamp
        self.data = data or {}
        self.source = source


# Event types
class SBTEventTypes:
    """SBT event type constants"""
    
    # Lifecycle events
    SBT_MINTED = "sbt_minted"
    SBT_REVOKED = "sbt_revoked"
    SBT_BURNED = "sbt_burned"
    SBT_METADATA_UPDATED = "sbt_metadata_updated"
    
    # Transfer events
    SBT_TRANSFER_BLOCKED = "sbt_transfer_blocked"
    SBT_TRANSFER_ATTEMPTED = "sbt_transfer_attempted"
    
    # Error events
    SBT_MINT_FAILED = "sbt_mint_failed"
    SBT_REVOCATION_FAILED = "sbt_revocation_failed"
    SBT_BURN_FAILED = "sbt_burn_failed"
    
    # Query events
    SBT_QUERIED = "sbt_queried"
    SBT_VERIFICATION_REQUESTED = "sbt_verification_requested" 