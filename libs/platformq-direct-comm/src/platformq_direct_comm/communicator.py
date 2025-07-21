"""High-performance direct communication implementation."""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, Callable, Set
from collections import defaultdict
import msgpack
import uvloop

from pyignite import Client as IgniteClient
from pyignite.exceptions import CacheError

from .message_types import MessageType, DirectMessage
from .exceptions import CommunicationError, TimeoutError, ConnectionError


# Use uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = logging.getLogger(__name__)


class DirectCommunicator:
    """Ultra-low latency service-to-service communication."""
    
    def __init__(self, 
                 service_id: str, 
                 ignite_client: IgniteClient,
                 batch_size: int = 100,
                 process_interval_ms: float = 1.0):
        """
        Initialize direct communicator.
        
        Args:
            service_id: Unique identifier for this service
            ignite_client: Connected Ignite client
            batch_size: Number of messages to process in batch
            process_interval_ms: Message processing interval in milliseconds
        """
        self.service_id = service_id
        self.ignite = ignite_client
        self.batch_size = batch_size
        self.process_interval_ms = process_interval_ms
        
        # Message handlers
        self._handlers: Dict[MessageType, Callable] = {}
        self._response_futures: Dict[str, asyncio.Future] = {}
        
        # Performance optimization
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._priority_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        
        # Statistics
        self._stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "errors": 0,
            "avg_latency_us": 0
        }
        
        # Running state
        self._running = False
        self._tasks: Set[asyncio.Task] = set()
        
    async def start(self):
        """Start the communicator background tasks."""
        if self._running:
            return
            
        self._running = True
        
        # Start background tasks
        self._tasks.add(asyncio.create_task(self._process_incoming()))
        self._tasks.add(asyncio.create_task(self._process_responses()))
        self._tasks.add(asyncio.create_task(self._heartbeat_task()))
        
        logger.info(f"DirectCommunicator started for service: {self.service_id}")
        
    async def stop(self):
        """Stop the communicator and cleanup."""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Clear queues and futures
        while not self._message_queue.empty():
            self._message_queue.get_nowait()
            
        for future in self._response_futures.values():
            if not future.done():
                future.cancel()
                
        self._response_futures.clear()
        
        logger.info(f"DirectCommunicator stopped for service: {self.service_id}")
        
    async def register_handler(self, msg_type: MessageType, handler: Callable):
        """Register message handler with zero-copy optimization."""
        self._handlers[msg_type] = handler
        logger.debug(f"Registered handler for message type: {msg_type}")
        
    async def send_direct(self, 
                         target_service: str,
                         msg_type: MessageType,
                         data: Dict[str, Any],
                         wait_response: bool = False,
                         timeout_ms: float = 100.0,
                         priority: int = 0,
                         ttl_ms: Optional[int] = None) -> Optional[Any]:
        """
        Send message directly through shared memory.
        
        Args:
            target_service: Target service ID
            msg_type: Message type
            data: Message payload
            wait_response: Whether to wait for response
            timeout_ms: Timeout in milliseconds
            priority: Message priority (higher = more urgent)
            ttl_ms: Time to live in milliseconds
            
        Returns:
            Response data if wait_response=True, None otherwise
        """
        start_time = time.time_ns()
        
        try:
            # Serialize with msgpack (faster than JSON)
            payload = msgpack.packb(data, use_bin_type=True)
            
            # Create correlation ID
            correlation_id = f"{self.service_id}:{start_time}"
            
            # Build message
            msg = DirectMessage(
                msg_type=msg_type,
                sender_id=self.service_id,
                correlation_id=correlation_id,
                payload=payload,
                timestamp_ns=start_time,
                priority=priority,
                ttl_ms=ttl_ms
            )
            
            # Use Ignite messaging for direct memory transfer
            cache_name = f"messaging:{target_service}"
            try:
                messaging = self.ignite.get_cache(cache_name)
                
                # Priority-based insertion
                if priority > 0:
                    # For high priority, put at front
                    messaging.put(f"priority:{priority}:{correlation_id}", msg)
                else:
                    messaging.put(correlation_id, msg)
                    
            except CacheError as e:
                raise ConnectionError(f"Failed to access messaging cache: {e}")
            
            self._stats["messages_sent"] += 1
            
            if wait_response:
                # Create future for response
                future = asyncio.Future()
                self._response_futures[correlation_id] = future
                
                try:
                    # Wait with timeout
                    response = await asyncio.wait_for(
                        future, 
                        timeout=timeout_ms / 1000.0
                    )
                    
                    # Update latency stats
                    latency_us = (time.time_ns() - start_time) / 1000
                    self._update_latency_stats(latency_us)
                    
                    return response
                    
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Response timeout after {timeout_ms}ms")
                    
                finally:
                    self._response_futures.pop(correlation_id, None)
                    
            return None
            
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Error sending message: {e}")
            raise CommunicationError(f"Failed to send message: {e}")
            
    async def broadcast(self,
                       msg_type: MessageType,
                       data: Dict[str, Any],
                       target_services: Optional[Set[str]] = None):
        """
        Broadcast message to multiple services.
        
        Args:
            msg_type: Message type
            data: Message payload
            target_services: Set of target services (None = all known services)
        """
        if target_services is None:
            # Get all registered services from Consul/config
            target_services = await self._get_all_services()
            
        tasks = []
        for service in target_services:
            if service != self.service_id:  # Don't send to self
                task = self.send_direct(
                    service, 
                    msg_type, 
                    data, 
                    wait_response=False
                )
                tasks.append(task)
                
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def _process_incoming(self):
        """Process incoming messages with minimal overhead."""
        cache_name = f"messaging:{self.service_id}"
        
        while self._running:
            try:
                messaging = self.ignite.get_cache(cache_name)
                
                # Batch fetch for efficiency
                messages = messaging.scan(batch_size=self.batch_size)
                
                batch_tasks = []
                
                async for key, msg in messages:
                    if isinstance(msg, DirectMessage):
                        # Check if expired
                        if msg.is_expired(time.time_ns()):
                            messaging.remove(key)
                            continue
                            
                        # Route to handler
                        handler = self._handlers.get(msg.msg_type)
                        if handler:
                            # Process in parallel
                            task = self._process_message(msg, handler)
                            batch_tasks.append(task)
                        
                        # Remove processed message
                        messaging.remove(key)
                        
                # Wait for batch to complete
                if batch_tasks:
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    # Send responses
                    for msg, result in zip(messages, results):
                        if result is not None and not isinstance(result, Exception):
                            await self._send_response(
                                msg.sender_id, 
                                msg.correlation_id, 
                                result
                            )
                        
            except Exception as e:
                logger.error(f"Error processing incoming messages: {e}")
                self._stats["errors"] += 1
                
            # Small delay to prevent busy waiting
            await asyncio.sleep(self.process_interval_ms / 1000.0)
            
    async def _process_message(self, msg: DirectMessage, handler: Callable) -> Optional[Any]:
        """Process a single message."""
        try:
            # Deserialize payload
            data = msgpack.unpackb(msg.payload, raw=False)
            
            # Execute handler
            result = await handler(data, msg)
            
            self._stats["messages_received"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing message {msg.correlation_id}: {e}")
            return None
            
    async def _send_response(self, target: str, correlation_id: str, result: Any):
        """Send response back to caller."""
        try:
            cache_name = f"responses:{target}"
            response_cache = self.ignite.get_cache(cache_name)
            
            # Serialize response
            response_data = msgpack.packb(result, use_bin_type=True)
            
            response_cache.put(correlation_id, response_data)
            
        except Exception as e:
            logger.error(f"Error sending response: {e}")
            
    async def _process_responses(self):
        """Process response messages."""
        cache_name = f"responses:{self.service_id}"
        
        while self._running:
            try:
                response_cache = self.ignite.get_cache(cache_name)
                
                # Check for responses
                responses = response_cache.scan(batch_size=self.batch_size)
                
                async for correlation_id, response_data in responses:
                    # Find waiting future
                    future = self._response_futures.get(correlation_id)
                    if future and not future.done():
                        # Deserialize response
                        result = msgpack.unpackb(response_data, raw=False)
                        future.set_result(result)
                        
                    # Remove processed response
                    response_cache.remove(correlation_id)
                    
            except Exception as e:
                logger.error(f"Error processing responses: {e}")
                
            await asyncio.sleep(self.process_interval_ms / 1000.0)
            
    async def _heartbeat_task(self):
        """Send periodic heartbeats."""
        while self._running:
            try:
                # Send heartbeat to monitoring service
                await self.send_direct(
                    "monitoring-service",
                    MessageType.HEARTBEAT,
                    {
                        "service_id": self.service_id,
                        "timestamp": time.time(),
                        "stats": self._stats
                    },
                    wait_response=False
                )
                
            except Exception:
                pass  # Ignore heartbeat errors
                
            await asyncio.sleep(30)  # Every 30 seconds
            
    async def _get_all_services(self) -> Set[str]:
        """Get all registered services."""
        # This would normally query Consul or a service registry
        # For now, return a static set
        return {"trading-core", "trading-platform", "risk-engine"}
        
    def _update_latency_stats(self, latency_us: float):
        """Update latency statistics."""
        # Simple moving average
        alpha = 0.1
        self._stats["avg_latency_us"] = (
            alpha * latency_us + 
            (1 - alpha) * self._stats["avg_latency_us"]
        )
        
    def get_stats(self) -> Dict[str, Any]:
        """Get communication statistics."""
        return self._stats.copy() 