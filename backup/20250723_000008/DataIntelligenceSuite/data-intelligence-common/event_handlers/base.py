"""Base event processing for DataIntelligenceSuite services."""

from typing import Dict, Any, Callable, List, Optional, Set, Union
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
import asyncio
import logging
import json

from platformq_shared.event_publisher import EventPublisher
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient

logger = logging.getLogger(__name__)


@dataclass
class EventHandler:
    """Event handler registration."""
    
    event_type: str
    handler_func: Callable
    filter_func: Optional[Callable] = None
    priority: int = 0  # Higher priority handlers run first
    required_role: Optional[str] = None  # Required role for handler execution
    encrypt_response: bool = False  # Whether to encrypt handler response
    
    def __post_init__(self):
        if not asyncio.iscoroutinefunction(self.handler_func):
            raise ValueError(f"Handler function must be async: {self.handler_func.__name__}")


class EventRouter:
    """Routes events to appropriate handlers with security integration."""
    
    def __init__(self, vault_client: Optional[VaultClient] = None, 
                 consul_client: Optional[ConsulClient] = None):
        self.handlers: Dict[str, List[EventHandler]] = {}
        self._handler_metrics: Dict[str, Dict[str, int]] = {}
        self.vault_client = vault_client
        self.consul_client = consul_client
        self._handler_config: Dict[str, Dict[str, Any]] = {}
        self._config_watcher_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize router with configuration from Consul"""
        if self.consul_client:
            await self._load_handler_config()
            self._config_watcher_task = asyncio.create_task(self._watch_config_changes())
            
    async def shutdown(self):
        """Shutdown router"""
        if self._config_watcher_task:
            self._config_watcher_task.cancel()
            try:
                await self._config_watcher_task
            except asyncio.CancelledError:
                pass
                
    async def _load_handler_config(self):
        """Load handler configuration from Consul"""
        try:
            config_data = await self.consul_client.kv_get("data-intelligence/event-handlers/config")
            if config_data:
                self._handler_config = json.loads(config_data)
                logger.info("Loaded handler configuration from Consul")
        except Exception as e:
            logger.error(f"Failed to load handler config: {e}")
            
    async def _watch_config_changes(self):
        """Watch for configuration changes in Consul"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                await self._load_handler_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config changes: {e}")
        
    def register_handler(
        self,
        event_type: str,
        handler_func: Callable,
        filter_func: Optional[Callable] = None,
        priority: int = 0,
        required_role: Optional[str] = None,
        encrypt_response: bool = False
    ):
        """Register an event handler with security options."""
        handler = EventHandler(
            event_type=event_type,
            handler_func=handler_func,
            filter_func=filter_func,
            priority=priority,
            required_role=required_role,
            encrypt_response=encrypt_response
        )
        
        if event_type not in self.handlers:
            self.handlers[event_type] = []
            
        self.handlers[event_type].append(handler)
        
        # Sort by priority (descending)
        self.handlers[event_type].sort(key=lambda h: h.priority, reverse=True)
        
        logger.info(f"Registered handler for {event_type}: {handler_func.__name__}")
        
    def unregister_handler(self, event_type: str, handler_func: Callable):
        """Unregister an event handler."""
        if event_type in self.handlers:
            self.handlers[event_type] = [
                h for h in self.handlers[event_type]
                if h.handler_func != handler_func
            ]
            
    async def route_event(self, event_type: str, event_data: Dict[str, Any],
                         user_context: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Route event to appropriate handlers with security checks."""
        if event_type not in self.handlers:
            logger.debug(f"No handlers registered for event type: {event_type}")
            return []
            
        results = []
        handlers = self.handlers[event_type]
        
        # Get handler config from Consul if available
        handler_config = self._handler_config.get(event_type, {})
        
        for handler in handlers:
            try:
                # Check role requirements
                if handler.required_role and user_context:
                    user_roles = user_context.get("roles", [])
                    if handler.required_role not in user_roles:
                        logger.warning(f"User lacks required role {handler.required_role} for handler {handler.handler_func.__name__}")
                        continue
                
                # Apply filter if provided
                if handler.filter_func and not handler.filter_func(event_data):
                    continue
                    
                # Call handler
                result = await handler.handler_func(event_data)
                
                # Encrypt response if required
                if handler.encrypt_response and self.vault_client and result:
                    encryption_key = handler_config.get("encryption_key", "event-responses")
                    encrypted_result = await self.vault_client.transit_encrypt(
                        encryption_key,
                        json.dumps(result)
                    )
                    result = {"encrypted": encrypted_result["ciphertext"]}
                
                results.append(result)
                
                # Track metrics
                self._track_handler_success(event_type, handler.handler_func.__name__)
                
            except Exception as e:
                logger.error(
                    f"Error in event handler",
                    extra={
                        "event_type": event_type,
                        "handler": handler.handler_func.__name__,
                        "error": str(e)
                    }
                )
                self._track_handler_error(event_type, handler.handler_func.__name__)
                
        return results
        
    def _track_handler_success(self, event_type: str, handler_name: str):
        """Track successful handler execution."""
        key = f"{event_type}:{handler_name}"
        if key not in self._handler_metrics:
            self._handler_metrics[key] = {"success": 0, "error": 0}
        self._handler_metrics[key]["success"] += 1
        
    def _track_handler_error(self, event_type: str, handler_name: str):
        """Track handler error."""
        key = f"{event_type}:{handler_name}"
        if key not in self._handler_metrics:
            self._handler_metrics[key] = {"success": 0, "error": 0}
        self._handler_metrics[key]["error"] += 1
        
    def get_handler_metrics(self) -> Dict[str, Dict[str, int]]:
        """Get handler execution metrics."""
        return self._handler_metrics.copy()


class BaseEventProcessor(ABC):
    """
    Base class for event processing services with Vault/Consul integration.
    
    Provides standard event handling patterns and integration
    with the DataIntelligenceSuite event system.
    """
    
    def __init__(self, service_name: str, 
                 event_publisher: Optional[EventPublisher] = None,
                 vault_client: Optional[VaultClient] = None,
                 consul_client: Optional[ConsulClient] = None):
        self.service_name = service_name
        self.event_publisher = event_publisher
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.router = EventRouter(vault_client, consul_client)
        self._processing_metrics: Dict[str, int] = {
            "events_processed": 0,
            "events_failed": 0,
            "events_skipped": 0
        }
        self._user_context: Optional[Dict[str, Any]] = None
        
    async def initialize(self):
        """Initialize event processor"""
        await self.router.initialize()
        logger.info(f"Initialized event processor for {self.service_name}")
        
    async def shutdown(self):
        """Shutdown event processor"""
        await self.router.shutdown()
        logger.info(f"Shutdown event processor for {self.service_name}")
        
    def set_user_context(self, context: Dict[str, Any]):
        """Set user context for role-based access control"""
        self._user_context = context
        
    @abstractmethod
    async def process_event(self, event_type: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single event. Must be implemented by derived classes."""
        pass
        
    async def handle_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Main event handling entry point with security."""
        event_type = event.get("event_type", "unknown")
        event_id = event.get("event_id", "unknown")
        
        logger.info(f"Processing event {event_id} of type {event_type}")
        
        try:
            # Decrypt event data if encrypted
            if event.get("encrypted") and self.vault_client:
                decrypted_data = await self.vault_client.transit_decrypt(
                    "event-data",
                    event["encrypted"]
                )
                event = json.loads(decrypted_data)
            
            # Route to appropriate handler with user context
            results = await self.router.route_event(event_type, event, self._user_context)
            
            # If no specific handler, use generic processor
            if not results:
                result = await self.process_event(event_type, event)
                results = [result] if result else []
                
            self._processing_metrics["events_processed"] += 1
            
            # Publish result events if configured
            for result in results:
                if result and self.event_publisher:
                    await self._publish_result(event, result)
                    
            return results[0] if results else None
            
        except Exception as e:
            logger.error(f"Error processing event {event_id}: {e}")
            self._processing_metrics["events_failed"] += 1
            
            # Publish error event
            if self.event_publisher:
                await self._publish_error(event, e)
                
            raise
            
    async def _publish_result(self, original_event: Dict[str, Any], result: Dict[str, Any]):
        """Publish processing result as new event."""
        result_event = {
            "event_type": f"{original_event.get('event_type', 'unknown')}.processed",
            "correlation_id": original_event.get("event_id"),
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "result": result
        }
        
        await self.event_publisher.publish(
            f"{self.service_name}.results",
            result_event
        )
        
    async def _publish_error(self, original_event: Dict[str, Any], error: Exception):
        """Publish error event."""
        error_event = {
            "event_type": "processing.error",
            "correlation_id": original_event.get("event_id"),
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "event_type": original_event.get("event_type", "unknown")
            }
        }
        
        await self.event_publisher.publish(
            f"{self.service_name}.errors",
            error_event
        )
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get processing metrics."""
        return {
            "processing": self._processing_metrics.copy(),
            "handlers": self.router.get_handler_metrics()
        }


class EventProcessor:
    """
    Enhanced event processor for Pulsar-based events with Vault/Consul.
    Works with PulsarEventBus for event-driven processing.
    """
    
    def __init__(self, event_bus: 'PulsarEventBus',
                 vault_client: Optional[VaultClient] = None,
                 consul_client: Optional[ConsulClient] = None):
        self.event_bus = event_bus
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.processors: Dict[str, List[Callable]] = {}
        self._processor_config: Dict[str, Dict[str, Any]] = {}
        self._config_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize processor with Consul config"""
        if self.consul_client:
            await self._load_processor_config()
            self._config_task = asyncio.create_task(self._watch_config())
            
    async def shutdown(self):
        """Shutdown processor"""
        if self._config_task:
            self._config_task.cancel()
            try:
                await self._config_task
            except asyncio.CancelledError:
                pass
                
    async def _load_processor_config(self):
        """Load processor configuration from Consul"""
        try:
            config_data = await self.consul_client.kv_get("data-intelligence/processors/config")
            if config_data:
                self._processor_config = json.loads(config_data)
                logger.info("Loaded processor configuration from Consul")
        except Exception as e:
            logger.error(f"Failed to load processor config: {e}")
            
    async def _watch_config(self):
        """Watch for configuration changes"""
        while True:
            try:
                await asyncio.sleep(30)
                await self._load_processor_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config: {e}")
        
    def register_processor(self, event_type: Union[str, 'EventType'], processor: Callable):
        """Register a processor for an event type"""
        event_type_str = event_type.value if hasattr(event_type, 'value') else str(event_type)
        
        if event_type_str not in self.processors:
            self.processors[event_type_str] = []
        self.processors[event_type_str].append(processor)
        
    def process(self, event_type: Union[str, 'EventType']):
        """Decorator to register a processor"""
        def decorator(func):
            self.register_processor(event_type, func)
            return func
        return decorator
        
    async def handle_event(self, event: Dict[str, Any]):
        """Handle incoming event with security"""
        event_type = event.get("metadata", {}).get("event_type")
        
        # Get processor config
        config = self._processor_config.get(event_type, {})
        
        # Decrypt if needed
        if event.get("encrypted") and self.vault_client:
            decrypted = await self.vault_client.transit_decrypt(
                config.get("encryption_key", "event-data"),
                event["encrypted"]
            )
            event = json.loads(decrypted)
        
        if event_type in self.processors:
            for processor in self.processors[event_type]:
                try:
                    if asyncio.iscoroutinefunction(processor):
                        await processor(event)
                    else:
                        processor(event)
                except Exception as e:
                    logger.error(f"Error in processor {processor.__name__}: {e}")
                    
    def start(self):
        """Start processing events"""
        for event_type in self.processors:
            self.event_bus.subscribe(
                event_type,
                self.handle_event,
                subscription_name=f"{self.event_bus.service_name}-processor"
            ) 