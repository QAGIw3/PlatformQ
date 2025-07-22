"""Message batching for efficient bulk operations."""

import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import msgpack

logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """Batch configuration."""
    max_batch_size: int = 100  # Maximum messages in a batch
    max_wait_time_ms: float = 10.0  # Maximum wait time before sending
    max_batch_bytes: int = 1024 * 1024  # 1MB max batch size
    compression_threshold_bytes: int = 10240  # Compress batches > 10KB


@dataclass 
class PendingMessage:
    """Message waiting to be batched."""
    target_service: str
    msg_type: int
    data: Dict[str, Any]
    priority: int
    callback: Optional[Callable] = None
    timestamp: float = field(default_factory=time.time)


class MessageBatcher:
    """
    Batches messages for efficient bulk transmission.
    
    Groups messages by target service and sends them in bulk
    to reduce overhead and improve throughput.
    """
    
    def __init__(self, 
                 send_func: Callable,
                 config: BatchConfig = BatchConfig()):
        self.send_func = send_func
        self.config = config
        
        # Pending messages grouped by target and priority
        self._batches: Dict[Tuple[str, int], List[PendingMessage]] = defaultdict(list)
        self._batch_lock = asyncio.Lock()
        
        # Background task for periodic flushing
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "messages_batched": 0,
            "batches_sent": 0,
            "bytes_sent": 0,
            "compression_count": 0,
            "avg_batch_size": 0.0
        }
        
    async def start(self):
        """Start the message batcher."""
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info("Message batcher started")
        
    async def stop(self):
        """Stop the message batcher and flush remaining messages."""
        self._running = False
        
        # Flush all pending messages
        await self.flush_all()
        
        # Cancel flush task
        if self._flush_task:
            self._flush_task.cancel()
            await asyncio.gather(self._flush_task, return_exceptions=True)
            
        logger.info("Message batcher stopped")
        
    async def add_message(self,
                         target_service: str,
                         msg_type: int,
                         data: Dict[str, Any],
                         priority: int = 0,
                         callback: Optional[Callable] = None) -> bool:
        """
        Add message to batch.
        
        Args:
            target_service: Target service ID
            msg_type: Message type
            data: Message payload
            priority: Message priority
            callback: Optional callback when sent
            
        Returns:
            True if batched, False if sent immediately
        """
        msg = PendingMessage(
            target_service=target_service,
            msg_type=msg_type,
            data=data,
            priority=priority,
            callback=callback
        )
        
        batch_key = (target_service, priority)
        
        async with self._batch_lock:
            self._batches[batch_key].append(msg)
            self._stats["messages_batched"] += 1
            
            # Check if we should send immediately
            if len(self._batches[batch_key]) >= self.config.max_batch_size:
                await self._flush_batch(batch_key)
                return False
                
        return True
        
    async def _periodic_flush(self):
        """Periodically flush old batches."""
        while self._running:
            try:
                await asyncio.sleep(self.config.max_wait_time_ms / 1000.0)
                await self._flush_old_batches()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
                
    async def _flush_old_batches(self):
        """Flush batches that have waited too long."""
        current_time = time.time()
        max_age = self.config.max_wait_time_ms / 1000.0
        
        async with self._batch_lock:
            keys_to_flush = []
            
            for key, messages in self._batches.items():
                if messages:
                    oldest_msg = messages[0]
                    if current_time - oldest_msg.timestamp >= max_age:
                        keys_to_flush.append(key)
                        
            # Flush old batches
            for key in keys_to_flush:
                await self._flush_batch(key)
                
    async def _flush_batch(self, batch_key: Tuple[str, int]):
        """Flush a specific batch."""
        messages = self._batches[batch_key]
        if not messages:
            return
            
        # Clear the batch
        self._batches[batch_key] = []
        
        target_service, priority = batch_key
        
        # Prepare batch payload
        batch_data = {
            "batch_id": f"{time.time_ns()}",
            "messages": [
                {
                    "type": msg.msg_type,
                    "data": msg.data
                }
                for msg in messages
            ],
            "count": len(messages),
            "priority": priority
        }
        
        # Serialize
        payload = msgpack.packb(batch_data, use_bin_type=True)
        payload_size = len(payload)
        
        # Compress if needed
        compressed = False
        if payload_size > self.config.compression_threshold_bytes:
            import zlib
            compressed_payload = zlib.compress(payload, level=6)
            if len(compressed_payload) < payload_size:
                payload = compressed_payload
                compressed = True
                self._stats["compression_count"] += 1
                
        # Send batch
        try:
            await self.send_func(
                target_service=target_service,
                msg_type=9999,  # Special batch message type
                data={
                    "payload": payload,
                    "compressed": compressed,
                    "original_size": payload_size
                },
                priority=priority
            )
            
            # Update stats
            self._stats["batches_sent"] += 1
            self._stats["bytes_sent"] += len(payload)
            
            # Update average
            alpha = 0.1
            self._stats["avg_batch_size"] = (
                alpha * len(messages) + 
                (1 - alpha) * self._stats["avg_batch_size"]
            )
            
            # Execute callbacks
            for msg in messages:
                if msg.callback:
                    try:
                        await msg.callback(True)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                        
        except Exception as e:
            logger.error(f"Failed to send batch: {e}")
            # Execute error callbacks
            for msg in messages:
                if msg.callback:
                    try:
                        await msg.callback(False)
                    except Exception:
                        pass
                        
    async def flush_all(self):
        """Flush all pending batches."""
        async with self._batch_lock:
            keys = list(self._batches.keys())
            
        for key in keys:
            await self._flush_batch(key)
            
    async def flush_target(self, target_service: str):
        """Flush all batches for a specific target."""
        async with self._batch_lock:
            keys = [k for k in self._batches.keys() if k[0] == target_service]
            
        for key in keys:
            await self._flush_batch(key)
            
    def get_stats(self) -> Dict[str, Any]:
        """Get batching statistics."""
        pending_count = sum(len(msgs) for msgs in self._batches.values())
        
        return {
            "messages_batched": self._stats["messages_batched"],
            "batches_sent": self._stats["batches_sent"],
            "bytes_sent": self._stats["bytes_sent"],
            "compression_count": self._stats["compression_count"],
            "avg_batch_size": round(self._stats["avg_batch_size"], 2),
            "pending_messages": pending_count,
            "active_batches": len([b for b in self._batches.values() if b])
        } 