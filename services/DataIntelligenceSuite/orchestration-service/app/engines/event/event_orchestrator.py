"""
Event Orchestrator

Orchestrates workflows based on events.
"""

import asyncio
from typing import Dict, Any, List, Optional, Set, Callable
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import uuid

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration
from platformq_events import EventStream, Event

logger = StructuredLogger.get_logger(__name__)


class EventMappingType(Enum):
    """Event mapping types"""
    DIRECT = "direct"  # One event triggers one workflow
    PATTERN = "pattern"  # Complex event pattern triggers workflow
    AGGREGATED = "aggregated"  # Multiple events aggregated trigger workflow
    CONDITIONAL = "conditional"  # Event with conditions triggers workflow


class EventCorrelationStrategy(Enum):
    """Event correlation strategies"""
    TIME_WINDOW = "time_window"
    COUNT_BASED = "count_based"
    SEQUENCE = "sequence"
    CUSTOM = "custom"


class EventOrchestrator:
    """
    Orchestrates workflows based on events
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 workflow_manager: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.workflow_manager = workflow_manager
        
        # Event stream for subscribing to events
        self.event_stream: Optional[EventStream] = None
        
        # Event mappings and handlers
        self.event_mappings: Dict[str, Dict[str, Any]] = {}
        self.event_handlers: Dict[str, Callable] = {}
        self.active_correlations: Dict[str, Dict[str, Any]] = {}
        self.event_buffer: Dict[str, List[Event]] = defaultdict(list)
        
        # Configuration
        self.config = {
            "max_buffer_size": 1000,
            "correlation_timeout": 300,  # 5 minutes
            "aggregation_window": 60,  # 1 minute
            "pattern_timeout": 600,  # 10 minutes
            "max_concurrent_handlers": 50
        }
        
        # Metrics
        self.metrics = {
            "events_received": 0,
            "workflows_triggered": 0,
            "patterns_matched": 0,
            "correlations_completed": 0,
            "errors": 0
        }
    
    async def initialize(self):
        """Initialize event orchestrator"""
        logger.info("initializing_event_orchestrator")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize event stream
        self.event_stream = EventStream(
            service_name="orchestration-service",
            pulsar_url="pulsar://pulsar:6650"
        )
        await self.event_stream.initialize()
        
        # Load event mappings
        await self._load_event_mappings()
        
        # Register default event handlers
        self._register_default_handlers()
        
        # Start event processing
        asyncio.create_task(self._process_events())
        asyncio.create_task(self._process_correlations())
        
        # Subscribe to relevant events
        await self._subscribe_to_events()
        
        logger.info("event_orchestrator_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.event_stream:
            await self.event_stream.close()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/event-orchestrator")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def register_event_mapping(self, event_type: str, workflow_id: str,
                                   mapping_type: EventMappingType = EventMappingType.DIRECT,
                                   conditions: Dict[str, Any] = None,
                                   correlation_config: Dict[str, Any] = None) -> str:
        """
        Register event to workflow mapping
        
        Args:
            event_type: Event type to map
            workflow_id: Workflow to trigger
            mapping_type: Type of mapping
            conditions: Conditions for triggering
            correlation_config: Correlation configuration for complex mappings
            
        Returns:
            Mapping ID
        """
        mapping_id = str(uuid.uuid4())
        
        # Create mapping record
        mapping = {
            "id": mapping_id,
            "event_type": event_type,
            "workflow_id": workflow_id,
            "type": mapping_type,
            "conditions": conditions or {},
            "correlation_config": correlation_config or {},
            "created_at": datetime.utcnow(),
            "enabled": True,
            "executions": 0
        }
        
        # Validate workflow exists
        try:
            await self.workflow_manager.get_workflow_status(workflow_id)
        except Exception as e:
            raise ValueError(f"Invalid workflow ID: {workflow_id}")
        
        # Store mapping
        self.event_mappings[mapping_id] = mapping
        
        # Subscribe to event type if not already subscribed
        await self._subscribe_to_event_type(event_type)
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.event_mapping.created",
            {
                "mapping_id": mapping_id,
                "event_type": event_type,
                "workflow_id": workflow_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Event mapping created: {event_type} -> {workflow_id}")
        return mapping_id
    
    async def remove_event_mapping(self, mapping_id: str) -> bool:
        """Remove event mapping"""
        mapping = self.event_mappings.get(mapping_id)
        if not mapping:
            raise ValueError(f"Mapping not found: {mapping_id}")
        
        # Disable mapping
        mapping["enabled"] = False
        
        # Remove from active mappings
        del self.event_mappings[mapping_id]
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.event_mapping.removed",
            {
                "mapping_id": mapping_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Event mapping removed: {mapping_id}")
        return True
    
    async def get_event_mappings(self, event_type: str = None) -> List[Dict[str, Any]]:
        """Get event mappings"""
        mappings = []
        
        for mapping in self.event_mappings.values():
            if not mapping.get("enabled", True):
                continue
                
            if event_type and mapping["event_type"] != event_type:
                continue
            
            mappings.append({
                "id": mapping["id"],
                "event_type": mapping["event_type"],
                "workflow_id": mapping["workflow_id"],
                "type": mapping["type"].value,
                "conditions": mapping["conditions"],
                "executions": mapping["executions"],
                "created_at": mapping["created_at"].isoformat()
            })
        
        return mappings
    
    async def _process_events(self):
        """Process incoming events"""
        while True:
            try:
                # Get events from stream
                # This is a simplified version - in production, you'd consume from Pulsar
                await asyncio.sleep(0.1)
                
                # Process events in buffer
                for event_type, events in list(self.event_buffer.items()):
                    if events:
                        event = events.pop(0)
                        await self._handle_event(event)
                
            except Exception as e:
                logger.error(f"Error processing events: {e}")
                self.metrics["errors"] += 1
                await asyncio.sleep(1)
    
    async def _handle_event(self, event: Event):
        """Handle incoming event"""
        logger.debug(f"Handling event: {event.type}")
        self.metrics["events_received"] += 1
        
        try:
            # Check direct mappings
            for mapping in self.event_mappings.values():
                if not mapping.get("enabled", True):
                    continue
                
                if mapping["type"] == EventMappingType.DIRECT and mapping["event_type"] == event.type:
                    # Check conditions
                    if await self._check_conditions(event, mapping["conditions"]):
                        await self._trigger_workflow(mapping, event)
                
                elif mapping["type"] == EventMappingType.PATTERN:
                    # Add to correlation buffer
                    await self._add_to_correlation(mapping, event)
                
                elif mapping["type"] == EventMappingType.AGGREGATED:
                    # Add to aggregation buffer
                    await self._add_to_aggregation(mapping, event)
                
                elif mapping["type"] == EventMappingType.CONDITIONAL:
                    # Complex conditional logic
                    if await self._check_complex_conditions(event, mapping):
                        await self._trigger_workflow(mapping, event)
            
            # Call specific handlers if registered
            if event.type in self.event_handlers:
                handler = self.event_handlers[event.type]
                await handler(event)
                
        except Exception as e:
            logger.error(f"Error handling event {event.type}: {e}")
            self.metrics["errors"] += 1
    
    async def _trigger_workflow(self, mapping: Dict[str, Any], event: Event):
        """Trigger workflow from event"""
        try:
            # Prepare context
            context = {
                "event_type": event.type,
                "event_data": event.data,
                "event_id": event.id,
                "event_timestamp": event.timestamp.isoformat(),
                "mapping_id": mapping["id"]
            }
            
            # Trigger workflow
            run_id = await self.workflow_manager.trigger_workflow(
                mapping["workflow_id"],
                context
            )
            
            # Update mapping statistics
            mapping["executions"] += 1
            
            # Update metrics
            self.metrics["workflows_triggered"] += 1
            
            # Emit event
            await self.event_bus.publish(
                "orchestration.workflow.triggered_by_event",
                {
                    "event_type": event.type,
                    "workflow_id": mapping["workflow_id"],
                    "run_id": run_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Workflow triggered: {mapping['workflow_id']} by event {event.type}")
            
        except Exception as e:
            logger.error(f"Error triggering workflow: {e}")
            self.metrics["errors"] += 1
    
    async def _check_conditions(self, event: Event, conditions: Dict[str, Any]) -> bool:
        """Check if event meets conditions"""
        if not conditions:
            return True
        
        # Check field conditions
        for field, condition in conditions.items():
            event_value = event.data.get(field)
            
            if isinstance(condition, dict):
                # Complex condition
                if "equals" in condition and event_value != condition["equals"]:
                    return False
                if "contains" in condition and condition["contains"] not in str(event_value):
                    return False
                if "greater_than" in condition and event_value <= condition["greater_than"]:
                    return False
                if "less_than" in condition and event_value >= condition["less_than"]:
                    return False
                if "in" in condition and event_value not in condition["in"]:
                    return False
            else:
                # Simple equality
                if event_value != condition:
                    return False
        
        return True
    
    async def _check_complex_conditions(self, event: Event, mapping: Dict[str, Any]) -> bool:
        """Check complex conditions for conditional mappings"""
        conditions = mapping["conditions"]
        
        # Check basic conditions first
        if not await self._check_conditions(event, conditions.get("fields", {})):
            return False
        
        # Check time-based conditions
        if "time_range" in conditions:
            current_hour = datetime.utcnow().hour
            time_range = conditions["time_range"]
            if not (time_range["start"] <= current_hour < time_range["end"]):
                return False
        
        # Check correlation with other events
        if "requires_events" in conditions:
            required_events = conditions["requires_events"]
            for required_event_type in required_events:
                # Check if required event occurred recently
                recent_events = await self._get_recent_events(
                    required_event_type,
                    timedelta(minutes=conditions.get("correlation_window", 5))
                )
                if not recent_events:
                    return False
        
        return True
    
    async def _add_to_correlation(self, mapping: Dict[str, Any], event: Event):
        """Add event to correlation buffer for pattern matching"""
        correlation_id = f"{mapping['id']}_{mapping['correlation_config'].get('correlation_key', 'default')}"
        
        if correlation_id not in self.active_correlations:
            self.active_correlations[correlation_id] = {
                "mapping": mapping,
                "events": [],
                "started_at": datetime.utcnow()
            }
        
        correlation = self.active_correlations[correlation_id]
        correlation["events"].append(event)
        
        # Check if pattern is complete
        if await self._check_pattern_complete(correlation):
            # Trigger workflow with all correlated events
            composite_event = Event(
                type=f"pattern_{mapping['event_type']}",
                data={
                    "pattern": mapping["correlation_config"].get("pattern"),
                    "events": [e.data for e in correlation["events"]]
                },
                timestamp=datetime.utcnow()
            )
            
            await self._trigger_workflow(mapping, composite_event)
            
            # Clean up correlation
            del self.active_correlations[correlation_id]
            
            # Update metrics
            self.metrics["patterns_matched"] += 1
            self.metrics["correlations_completed"] += 1
    
    async def _add_to_aggregation(self, mapping: Dict[str, Any], event: Event):
        """Add event to aggregation buffer"""
        aggregation_key = f"{mapping['id']}_aggregation"
        
        if aggregation_key not in self.active_correlations:
            self.active_correlations[aggregation_key] = {
                "mapping": mapping,
                "events": [],
                "started_at": datetime.utcnow()
            }
        
        correlation = self.active_correlations[aggregation_key]
        correlation["events"].append(event)
        
        # Check if aggregation window is complete
        window_duration = mapping["correlation_config"].get("window_seconds", 60)
        elapsed = (datetime.utcnow() - correlation["started_at"]).total_seconds()
        
        if elapsed >= window_duration:
            # Create aggregated event
            aggregated_event = Event(
                type=f"aggregated_{mapping['event_type']}",
                data={
                    "count": len(correlation["events"]),
                    "window_seconds": window_duration,
                    "events": [e.data for e in correlation["events"][-10:]]  # Last 10 events
                },
                timestamp=datetime.utcnow()
            )
            
            # Check aggregation conditions
            min_count = mapping["correlation_config"].get("min_count", 1)
            if len(correlation["events"]) >= min_count:
                await self._trigger_workflow(mapping, aggregated_event)
            
            # Reset aggregation
            del self.active_correlations[aggregation_key]
    
    async def _check_pattern_complete(self, correlation: Dict[str, Any]) -> bool:
        """Check if event pattern is complete"""
        mapping = correlation["mapping"]
        pattern = mapping["correlation_config"].get("pattern", [])
        events = correlation["events"]
        
        if not pattern:
            return False
        
        # Simple sequence matching
        if len(events) < len(pattern):
            return False
        
        # Check if events match pattern
        for i, pattern_event in enumerate(pattern):
            if i >= len(events):
                return False
            
            event = events[i]
            if event.type != pattern_event.get("type"):
                return False
            
            # Check pattern conditions
            if "conditions" in pattern_event:
                if not await self._check_conditions(event, pattern_event["conditions"]):
                    return False
        
        return True
    
    async def _process_correlations(self):
        """Process active correlations and check for timeouts"""
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                current_time = datetime.utcnow()
                
                # Check for timed out correlations
                for correlation_id, correlation in list(self.active_correlations.items()):
                    elapsed = (current_time - correlation["started_at"]).total_seconds()
                    timeout = correlation["mapping"]["correlation_config"].get(
                        "timeout_seconds",
                        self.config["correlation_timeout"]
                    )
                    
                    if elapsed > timeout:
                        logger.warning(f"Correlation timeout: {correlation_id}")
                        del self.active_correlations[correlation_id]
                
            except Exception as e:
                logger.error(f"Error processing correlations: {e}")
    
    async def _subscribe_to_events(self):
        """Subscribe to events based on mappings"""
        event_types = set()
        
        for mapping in self.event_mappings.values():
            event_types.add(mapping["event_type"])
            
            # Add pattern events
            if mapping["type"] == EventMappingType.PATTERN:
                pattern = mapping["correlation_config"].get("pattern", [])
                for pattern_event in pattern:
                    if "type" in pattern_event:
                        event_types.add(pattern_event["type"])
        
        # Subscribe to all event types
        for event_type in event_types:
            await self._subscribe_to_event_type(event_type)
    
    async def _subscribe_to_event_type(self, event_type: str):
        """Subscribe to specific event type"""
        # This would subscribe to Pulsar topic for the event type
        logger.info(f"Subscribed to event type: {event_type}")
    
    async def _get_recent_events(self, event_type: str, 
                               time_window: timedelta) -> List[Event]:
        """Get recent events of specific type"""
        # This would query recent events from storage
        # For now, return empty list
        return []
    
    async def _load_event_mappings(self):
        """Load event mappings from storage"""
        # This would load mappings from database/consul
        # For now, create some example mappings
        pass
    
    def _register_default_handlers(self):
        """Register default event handlers"""
        # Register handler for data quality issues
        async def handle_data_quality_issue(event: Event):
            logger.info(f"Data quality issue detected: {event.data}")
            # Could trigger remediation workflow
        
        self.event_handlers["DataQualityIssueDetected"] = handle_data_quality_issue
        
        # Register handler for resource alerts
        async def handle_resource_alert(event: Event):
            logger.info(f"Resource alert: {event.data}")
            # Could trigger scaling workflow
        
        self.event_handlers["ResourceAlertTriggered"] = handle_resource_alert
    
    async def get_event_metrics(self) -> Dict[str, Any]:
        """Get event orchestrator metrics"""
        return {
            **self.metrics,
            "active_mappings": len([m for m in self.event_mappings.values() 
                                  if m.get("enabled", True)]),
            "active_correlations": len(self.active_correlations),
            "buffered_events": sum(len(events) for events in self.event_buffer.values())
        } 