"""Message replay mechanism for critical alerts."""

import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import msgpack

logger = logging.getLogger(__name__)


@dataclass
class ReplayableMessage:
    """Message that can be replayed."""
    message_id: str
    target_service: str
    msg_type: int
    data: Dict[str, Any]
    priority: int
    timestamp: float
    retries: int = 0
    max_retries: int = 3
    ack_required: bool = True
    replay_after_ms: float = 1000.0  # Replay after 1 second if not acked
    
    @property
    def is_expired(self) -> bool:
        """Check if message has exceeded retry limit."""
        return self.retries >= self.max_retries
        
    @property
    def next_replay_time(self) -> float:
        """Calculate next replay time with exponential backoff."""
        backoff_factor = 2 ** self.retries
        return self.timestamp + (self.replay_after_ms * backoff_factor / 1000.0)


class MessageReplayStore:
    """
    Stores and replays critical messages that require acknowledgment.
    
    Ensures critical alerts are delivered even if the initial
    transmission fails or the target service is temporarily unavailable.
    """
    
    def __init__(self, 
                 send_func,
                 ignite_client=None,
                 max_store_size: int = 10000):
        self.send_func = send_func
        self.ignite = ignite_client
        self.max_store_size = max_store_size
        
        # In-memory store for fast access
        self._pending_messages: Dict[str, ReplayableMessage] = {}
        self._replay_queue: deque = deque()
        self._acknowledged: Set[str] = set()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Background replay task
        self._running = False
        self._replay_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "messages_stored": 0,
            "messages_acknowledged": 0,
            "messages_replayed": 0,
            "messages_expired": 0,
            "messages_delivered": 0
        }
        
        # Persistent cache name if using Ignite
        self._cache_name = "direct_comm_replay_store"
        
    async def start(self):
        """Start the replay store."""
        self._running = True
        
        # Load from persistent store if available
        await self._load_from_persistent_store()
        
        # Start replay task
        self._replay_task = asyncio.create_task(self._replay_loop())
        
        logger.info("Message replay store started")
        
    async def stop(self):
        """Stop the replay store."""
        self._running = False
        
        # Save to persistent store
        await self._save_to_persistent_store()
        
        # Cancel replay task
        if self._replay_task:
            self._replay_task.cancel()
            await asyncio.gather(self._replay_task, return_exceptions=True)
            
        logger.info("Message replay store stopped")
        
    async def store_message(self,
                          message_id: str,
                          target_service: str,
                          msg_type: int,
                          data: Dict[str, Any],
                          priority: int = 0,
                          ack_required: bool = True,
                          replay_after_ms: float = 1000.0) -> bool:
        """
        Store a message for potential replay.
        
        Args:
            message_id: Unique message identifier
            target_service: Target service ID
            msg_type: Message type
            data: Message payload
            priority: Message priority
            ack_required: Whether acknowledgment is required
            replay_after_ms: Time to wait before replay
            
        Returns:
            True if stored successfully
        """
        async with self._lock:
            # Check store size
            if len(self._pending_messages) >= self.max_store_size:
                # Remove oldest expired messages
                await self._cleanup_expired()
                
                if len(self._pending_messages) >= self.max_store_size:
                    logger.warning("Replay store full, dropping message")
                    return False
                    
            # Create replayable message
            msg = ReplayableMessage(
                message_id=message_id,
                target_service=target_service,
                msg_type=msg_type,
                data=data,
                priority=priority,
                timestamp=time.time(),
                ack_required=ack_required,
                replay_after_ms=replay_after_ms
            )
            
            # Store message
            self._pending_messages[message_id] = msg
            self._replay_queue.append(message_id)
            self._stats["messages_stored"] += 1
            
            # Persist if available
            if self.ignite:
                await self._persist_message(msg)
                
            return True
            
    async def acknowledge_message(self, message_id: str) -> bool:
        """
        Acknowledge receipt of a message.
        
        Args:
            message_id: Message ID to acknowledge
            
        Returns:
            True if message was pending and is now acknowledged
        """
        async with self._lock:
            if message_id in self._pending_messages:
                # Remove from pending
                del self._pending_messages[message_id]
                self._acknowledged.add(message_id)
                self._stats["messages_acknowledged"] += 1
                
                # Remove from persistent store
                if self.ignite:
                    await self._remove_persisted_message(message_id)
                    
                return True
                
            return False
            
    async def _replay_loop(self):
        """Background loop to replay messages."""
        while self._running:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms
                await self._check_and_replay_messages()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in replay loop: {e}")
                
    async def _check_and_replay_messages(self):
        """Check for messages that need replay."""
        current_time = time.time()
        messages_to_replay = []
        
        async with self._lock:
            for msg_id, msg in list(self._pending_messages.items()):
                if msg.is_expired:
                    # Remove expired messages
                    del self._pending_messages[msg_id]
                    self._stats["messages_expired"] += 1
                    
                    if self.ignite:
                        await self._remove_persisted_message(msg_id)
                        
                elif current_time >= msg.next_replay_time:
                    messages_to_replay.append(msg)
                    
        # Replay messages (outside lock to avoid blocking)
        for msg in messages_to_replay:
            await self._replay_message(msg)
            
    async def _replay_message(self, msg: ReplayableMessage):
        """Replay a single message."""
        try:
            # Send the message
            await self.send_func(
                target_service=msg.target_service,
                msg_type=msg.msg_type,
                data={
                    **msg.data,
                    "_replay": True,
                    "_replay_count": msg.retries + 1,
                    "_message_id": msg.message_id
                },
                priority=msg.priority + 1  # Increase priority for replays
            )
            
            # Update retry count
            async with self._lock:
                if msg.message_id in self._pending_messages:
                    msg.retries += 1
                    msg.timestamp = time.time()  # Update timestamp
                    self._stats["messages_replayed"] += 1
                    
                    # Update in persistent store
                    if self.ignite:
                        await self._persist_message(msg)
                        
        except Exception as e:
            logger.error(f"Failed to replay message {msg.message_id}: {e}")
            
    async def _cleanup_expired(self):
        """Remove expired messages."""
        expired_ids = []
        
        for msg_id, msg in self._pending_messages.items():
            if msg.is_expired:
                expired_ids.append(msg_id)
                
        for msg_id in expired_ids:
            del self._pending_messages[msg_id]
            self._stats["messages_expired"] += 1
            
            if self.ignite:
                await self._remove_persisted_message(msg_id)
                
    async def _load_from_persistent_store(self):
        """Load messages from persistent store."""
        if not self.ignite:
            return
            
        try:
            cache = self.ignite.get_cache(self._cache_name)
            
            # Scan all entries
            with cache.scan() as cursor:
                for msg_id, msg_data in cursor:
                    # Deserialize
                    msg_dict = msgpack.unpackb(msg_data, raw=False)
                    
                    # Recreate message
                    msg = ReplayableMessage(**msg_dict)
                    
                    # Only load non-expired messages
                    if not msg.is_expired:
                        self._pending_messages[msg_id] = msg
                        
            logger.info(f"Loaded {len(self._pending_messages)} messages from persistent store")
            
        except Exception as e:
            logger.error(f"Failed to load from persistent store: {e}")
            
    async def _save_to_persistent_store(self):
        """Save all pending messages to persistent store."""
        if not self.ignite:
            return
            
        try:
            cache = self.ignite.get_cache(self._cache_name)
            
            for msg_id, msg in self._pending_messages.items():
                await self._persist_message(msg)
                
            logger.info(f"Saved {len(self._pending_messages)} messages to persistent store")
            
        except Exception as e:
            logger.error(f"Failed to save to persistent store: {e}")
            
    async def _persist_message(self, msg: ReplayableMessage):
        """Persist a single message."""
        if not self.ignite:
            return
            
        try:
            cache = self.ignite.get_cache(self._cache_name)
            
            # Serialize message
            msg_data = msgpack.packb({
                "message_id": msg.message_id,
                "target_service": msg.target_service,
                "msg_type": msg.msg_type,
                "data": msg.data,
                "priority": msg.priority,
                "timestamp": msg.timestamp,
                "retries": msg.retries,
                "max_retries": msg.max_retries,
                "ack_required": msg.ack_required,
                "replay_after_ms": msg.replay_after_ms
            }, use_bin_type=True)
            
            cache.put(msg.message_id, msg_data)
            
        except Exception as e:
            logger.error(f"Failed to persist message {msg.message_id}: {e}")
            
    async def _remove_persisted_message(self, message_id: str):
        """Remove message from persistent store."""
        if not self.ignite:
            return
            
        try:
            cache = self.ignite.get_cache(self._cache_name)
            cache.remove(message_id)
        except Exception:
            pass  # Ignore removal errors
            
    def get_stats(self) -> Dict[str, Any]:
        """Get replay store statistics."""
        return {
            "messages_stored": self._stats["messages_stored"],
            "messages_acknowledged": self._stats["messages_acknowledged"],
            "messages_replayed": self._stats["messages_replayed"],
            "messages_expired": self._stats["messages_expired"],
            "pending_messages": len(self._pending_messages),
            "acknowledged_count": len(self._acknowledged)
        } 