"""
Event-driven orchestration engine
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional, Set, Callable
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict

from platformq_shared.logging import get_logger
from platformq_events import EventStream, Event, EventType
from pyignite import AsyncClient
from ..core.config import settings

logger = get_logger(__name__)


class EventMappingType(str, Enum):
    """Event mapping types"""
    DIRECT = "direct"  # One event triggers one workflow
    PATTERN = "pattern"  # Complex event pattern triggers workflow
    AGGREGATED = "aggregated"  # Multiple events aggregated trigger workflow
    CONDITIONAL = "conditional"  # Event with conditions triggers workflow


class EventCorrelationStrategy(str, Enum):
    """Event correlation strategies"""
    TIME_WINDOW = "time_window"
    COUNT_BASED = "count_based"
    PATTERN_MATCHING = "pattern_matching"
    CUSTOM = "custom"


class EventOrchestrator:
    """Orchestrates workflows based on events"""
    
    def __init__(self):
        self.event_stream: Optional[EventStream] = None
        self.ignite_client: Optional[AsyncClient] = None
        self.event_mappings: Dict[str, Dict[str, Any]] = {}
        self.active_correlations: Dict[str, Dict[str, Any]] = {}
        self.event_handlers: Dict[str, Callable] = {}
        self.event_buffer: Dict[str, List[Event]] = defaultdict(list)
        
    async def initialize(self):
        """Initialize the event orchestrator"""
        logger.info("Initializing event orchestrator")
        
        # Initialize event stream
        self.event_stream = EventStream(
            service_name="unified-orchestration-service",
            pulsar_url=settings.pulsar_url
        )
        await self.event_stream.initialize()
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
        
        # Load event mappings
        await self._load_event_mappings()
        
        # Register default event handlers
        self._register_default_handlers()
        
        # Start event processing
        asyncio.create_task(self._process_events())
        asyncio.create_task(self._process_correlations())
        
        # Subscribe to relevant events
        await self._subscribe_to_events()
        
        logger.info("Event orchestrator initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.event_stream:
            await self.event_stream.close()
        if self.ignite_client:
            await self.ignite_client.close()
            
    async def _load_event_mappings(self):
        """Load event mappings from storage"""
        if self.ignite_client:
            try:
                cache = await self.ignite_client.get_or_create_cache("event_mappings")
                # Load all mappings (simplified - real implementation would paginate)
                async for key, value in cache.scan():
                    mapping = json.loads(value)
                    self.event_mappings[key] = mapping
                    logger.info(f"Loaded event mapping: {mapping['name']}")
            except Exception as e:
                logger.error(f"Failed to load event mappings: {e}")
                
    def _register_default_handlers(self):
        """Register default event handlers"""
        # Data quality events
        self.event_handlers['DataQualityIssueDetected'] = self._handle_quality_issue
        self.event_handlers['DataAnomalyDetected'] = self._handle_anomaly
        
        # Pipeline events
        self.event_handlers['PipelineCompleted'] = self._handle_pipeline_completion
        self.event_handlers['PipelineFailed'] = self._handle_pipeline_failure
        
        # Resource events
        self.event_handlers['ResourceThresholdExceeded'] = self._handle_resource_alert
        
        # ML events
        self.event_handlers['ModelDriftDetected'] = self._handle_model_drift
        self.event_handlers['TrainingCompleted'] = self._handle_training_completion
        
    async def _subscribe_to_events(self):
        """Subscribe to relevant event types"""
        # Subscribe to events based on mappings
        event_types = set()
        for mapping in self.event_mappings.values():
            if mapping['type'] == EventMappingType.DIRECT:
                event_types.add(mapping['event_type'])
            elif mapping['type'] == EventMappingType.PATTERN:
                for event in mapping.get('pattern', {}).get('events', []):
                    event_types.add(event)
                    
        # Subscribe to each event type
        for event_type in event_types:
            await self.event_stream.subscribe(
                event_type,
                self._handle_event,
                subscription_name=f"orchestration_{event_type}"
            )
            logger.info(f"Subscribed to event type: {event_type}")
            
    async def create_event_mapping(self,
                                 name: str,
                                 event_type: str,
                                 workflow_id: str,
                                 mapping_type: EventMappingType = EventMappingType.DIRECT,
                                 conditions: Optional[Dict[str, Any]] = None,
                                 correlation: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create an event to workflow mapping"""
        logger.info(f"Creating event mapping: {name}")
        
        mapping_id = str(uuid.uuid4())
        
        mapping = {
            "id": mapping_id,
            "name": name,
            "event_type": event_type,
            "workflow_id": workflow_id,
            "type": mapping_type,
            "conditions": conditions or {},
            "correlation": correlation or {},
            "created_at": datetime.utcnow().isoformat(),
            "enabled": True,
            "execution_count": 0,
            "last_triggered": None
        }
        
        # Validate mapping
        if mapping_type == EventMappingType.PATTERN and not correlation:
            raise ValueError("Pattern mapping requires correlation configuration")
            
        # Store mapping
        self.event_mappings[mapping_id] = mapping
        
        # Persist to Ignite
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("event_mappings")
            await cache.put(mapping_id, json.dumps(mapping))
            
        # Subscribe to new event type if needed
        if event_type not in [m['event_type'] for m in self.event_mappings.values() if m['id'] != mapping_id]:
            await self.event_stream.subscribe(
                event_type,
                self._handle_event,
                subscription_name=f"orchestration_{event_type}"
            )
            
        return mapping
        
    async def _handle_event(self, event: Event):
        """Handle incoming event"""
        logger.debug(f"Handling event: {event.type}")
        
        try:
            # Check direct mappings
            for mapping in self.event_mappings.values():
                if not mapping.get('enabled', True):
                    continue
                    
                if mapping['type'] == EventMappingType.DIRECT and mapping['event_type'] == event.type:
                    # Check conditions
                    if await self._check_conditions(event, mapping['conditions']):
                        await self._trigger_workflow(mapping, event)
                        
                elif mapping['type'] == EventMappingType.PATTERN:
                    # Add to correlation buffer
                    await self._add_to_correlation(mapping, event)
                    
                elif mapping['type'] == EventMappingType.AGGREGATED:
                    # Add to aggregation buffer
                    await self._add_to_aggregation(mapping, event)
                    
            # Call specific handlers if registered
            if event.type in self.event_handlers:
                handler = self.event_handlers[event.type]
                await handler(event)
                
        except Exception as e:
            logger.error(f"Error handling event {event.type}: {e}")
            
    async def _check_conditions(self, event: Event, conditions: Dict[str, Any]) -> bool:
        """Check if event meets conditions"""
        if not conditions:
            return True
            
        try:
            # Field conditions
            for field, condition in conditions.get('fields', {}).items():
                value = event.data.get(field)
                
                if isinstance(condition, dict):
                    # Complex condition
                    if 'eq' in condition and value != condition['eq']:
                        return False
                    if 'ne' in condition and value == condition['ne']:
                        return False
                    if 'gt' in condition and value <= condition['gt']:
                        return False
                    if 'lt' in condition and value >= condition['lt']:
                        return False
                    if 'in' in condition and value not in condition['in']:
                        return False
                    if 'regex' in condition:
                        import re
                        if not re.match(condition['regex'], str(value)):
                            return False
                else:
                    # Simple equality
                    if value != condition:
                        return False
                        
            # Custom conditions
            if 'custom' in conditions:
                # Evaluate custom condition (simplified - real implementation would be sandboxed)
                # This is a placeholder for custom condition evaluation
                pass
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking conditions: {e}")
            return False
            
    async def _trigger_workflow(self, mapping: Dict[str, Any], event: Event):
        """Trigger workflow based on mapping"""
        logger.info(f"Triggering workflow {mapping['workflow_id']} for event {event.type}")
        
        try:
            # Update mapping statistics
            mapping['execution_count'] += 1
            mapping['last_triggered'] = datetime.utcnow().isoformat()
            
            # Prepare workflow context
            context = {
                "trigger_type": "event",
                "event_id": event.id,
                "event_type": event.type,
                "event_data": event.data,
                "mapping_id": mapping['id'],
                "timestamp": event.timestamp
            }
            
            # Emit workflow trigger event
            await self.event_stream.emit(Event(
                type="WorkflowTriggered",
                data={
                    "workflow_id": mapping['workflow_id'],
                    "context": context,
                    "mapping_name": mapping['name']
                }
            ))
            
            # Update mapping in storage
            if self.ignite_client:
                cache = await self.ignite_client.get_or_create_cache("event_mappings")
                await cache.put(mapping['id'], json.dumps(mapping))
                
        except Exception as e:
            logger.error(f"Failed to trigger workflow: {e}")
            
    async def _add_to_correlation(self, mapping: Dict[str, Any], event: Event):
        """Add event to correlation tracking"""
        correlation_config = mapping.get('correlation', {})
        strategy = EventCorrelationStrategy(correlation_config.get('strategy', 'time_window'))
        
        correlation_key = f"{mapping['id']}_{self._get_correlation_key(event, correlation_config)}"
        
        if correlation_key not in self.active_correlations:
            self.active_correlations[correlation_key] = {
                "mapping_id": mapping['id'],
                "events": [],
                "started_at": datetime.utcnow(),
                "strategy": strategy,
                "config": correlation_config
            }
            
        self.active_correlations[correlation_key]['events'].append(event)
        
    async def _add_to_aggregation(self, mapping: Dict[str, Any], event: Event):
        """Add event to aggregation buffer"""
        buffer_key = f"{mapping['id']}_{event.type}"
        self.event_buffer[buffer_key].append(event)
        
        # Check if aggregation threshold is met
        aggregation_config = mapping.get('correlation', {})
        threshold = aggregation_config.get('threshold', 10)
        
        if len(self.event_buffer[buffer_key]) >= threshold:
            # Trigger workflow with aggregated events
            aggregated_event = Event(
                type=f"Aggregated_{event.type}",
                data={
                    "events": [e.dict() for e in self.event_buffer[buffer_key]],
                    "count": len(self.event_buffer[buffer_key]),
                    "aggregation_type": "count_based"
                }
            )
            
            await self._trigger_workflow(mapping, aggregated_event)
            
            # Clear buffer
            self.event_buffer[buffer_key].clear()
            
    def _get_correlation_key(self, event: Event, config: Dict[str, Any]) -> str:
        """Get correlation key for event"""
        # Use configured fields for correlation
        key_fields = config.get('key_fields', ['source'])
        key_parts = []
        
        for field in key_fields:
            value = event.data.get(field, 'unknown')
            key_parts.append(str(value))
            
        return "_".join(key_parts)
        
    async def _process_correlations(self):
        """Process active correlations"""
        while True:
            try:
                current_time = datetime.utcnow()
                completed_correlations = []
                
                for key, correlation in self.active_correlations.items():
                    strategy = correlation['strategy']
                    config = correlation['config']
                    
                    if strategy == EventCorrelationStrategy.TIME_WINDOW:
                        # Check if time window expired
                        window = timedelta(seconds=config.get('window_seconds', 60))
                        if current_time - correlation['started_at'] > window:
                            # Evaluate pattern
                            if await self._evaluate_correlation_pattern(correlation):
                                # Get mapping
                                mapping = self.event_mappings.get(correlation['mapping_id'])
                                if mapping:
                                    # Create composite event
                                    composite_event = Event(
                                        type=f"Pattern_{mapping['name']}",
                                        data={
                                            "pattern": config.get('pattern'),
                                            "events": [e.dict() for e in correlation['events']],
                                            "correlation_key": key
                                        }
                                    )
                                    await self._trigger_workflow(mapping, composite_event)
                                    
                            completed_correlations.append(key)
                            
                    elif strategy == EventCorrelationStrategy.COUNT_BASED:
                        # Check if count reached
                        required_count = config.get('required_count', 5)
                        if len(correlation['events']) >= required_count:
                            # Trigger workflow
                            mapping = self.event_mappings.get(correlation['mapping_id'])
                            if mapping:
                                composite_event = Event(
                                    type=f"Pattern_{mapping['name']}",
                                    data={
                                        "events": [e.dict() for e in correlation['events']],
                                        "count": len(correlation['events'])
                                    }
                                )
                                await self._trigger_workflow(mapping, composite_event)
                                
                            completed_correlations.append(key)
                            
                # Remove completed correlations
                for key in completed_correlations:
                    del self.active_correlations[key]
                    
            except Exception as e:
                logger.error(f"Correlation processing error: {e}")
                
            await asyncio.sleep(5)  # Process every 5 seconds
            
    async def _evaluate_correlation_pattern(self, correlation: Dict[str, Any]) -> bool:
        """Evaluate if events match correlation pattern"""
        config = correlation['config']
        pattern = config.get('pattern', {})
        events = correlation['events']
        
        # Check required event types
        required_types = pattern.get('events', [])
        event_types = {e.type for e in events}
        
        if not all(req_type in event_types for req_type in required_types):
            return False
            
        # Check event order if specified
        if pattern.get('ordered', False):
            # Verify events occurred in specified order
            type_positions = {t: [] for t in required_types}
            for i, event in enumerate(events):
                if event.type in type_positions:
                    type_positions[event.type].append(i)
                    
            # Check order
            for i in range(len(required_types) - 1):
                type1, type2 = required_types[i], required_types[i + 1]
                if not type_positions[type1] or not type_positions[type2]:
                    return False
                if min(type_positions[type1]) > max(type_positions[type2]):
                    return False
                    
        # Check additional pattern conditions
        if 'conditions' in pattern:
            # Evaluate pattern-level conditions
            # This is a placeholder for complex pattern evaluation
            pass
            
        return True
        
    async def _process_events(self):
        """Main event processing loop"""
        # This method is a placeholder as event processing
        # happens in the event handlers
        pass
        
    # Event-specific handlers
    async def _handle_quality_issue(self, event: Event):
        """Handle data quality issue events"""
        logger.info(f"Handling quality issue: {event.data}")
        # Custom logic for quality issues
        
    async def _handle_anomaly(self, event: Event):
        """Handle anomaly detection events"""
        logger.info(f"Handling anomaly: {event.data}")
        # Custom logic for anomalies
        
    async def _handle_pipeline_completion(self, event: Event):
        """Handle pipeline completion events"""
        logger.info(f"Pipeline completed: {event.data.get('pipeline_id')}")
        # Trigger dependent workflows
        
    async def _handle_pipeline_failure(self, event: Event):
        """Handle pipeline failure events"""
        logger.warning(f"Pipeline failed: {event.data.get('pipeline_id')}")
        # Trigger error handling workflows
        
    async def _handle_resource_alert(self, event: Event):
        """Handle resource threshold events"""
        logger.warning(f"Resource alert: {event.data}")
        # Trigger resource optimization workflows
        
    async def _handle_model_drift(self, event: Event):
        """Handle model drift events"""
        logger.warning(f"Model drift detected: {event.data.get('model_id')}")
        # Trigger retraining workflows
        
    async def _handle_training_completion(self, event: Event):
        """Handle training completion events"""
        logger.info(f"Training completed: {event.data.get('model_id')}")
        # Trigger deployment workflows
        
    async def list_event_mappings(self,
                                event_type: Optional[str] = None,
                                workflow_id: Optional[str] = None,
                                enabled_only: bool = True) -> List[Dict[str, Any]]:
        """List event mappings with filtering"""
        mappings = list(self.event_mappings.values())
        
        if event_type:
            mappings = [m for m in mappings if m['event_type'] == event_type]
            
        if workflow_id:
            mappings = [m for m in mappings if m['workflow_id'] == workflow_id]
            
        if enabled_only:
            mappings = [m for m in mappings if m.get('enabled', True)]
            
        return mappings
        
    async def delete_event_mapping(self, mapping_id: str) -> bool:
        """Delete an event mapping"""
        if mapping_id not in self.event_mappings:
            return False
            
        # Remove from memory
        del self.event_mappings[mapping_id]
        
        # Remove from storage
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("event_mappings")
            await cache.remove(mapping_id)
            
        logger.info(f"Deleted event mapping: {mapping_id}")
        return True
        
    async def get_event_statistics(self) -> Dict[str, Any]:
        """Get event processing statistics"""
        stats = {
            "total_mappings": len(self.event_mappings),
            "enabled_mappings": sum(1 for m in self.event_mappings.values() if m.get('enabled', True)),
            "active_correlations": len(self.active_correlations),
            "buffered_events": sum(len(events) for events in self.event_buffer.values()),
            "mapping_types": {},
            "most_triggered": None
        }
        
        # Count by type
        for mapping in self.event_mappings.values():
            mapping_type = mapping['type']
            stats['mapping_types'][mapping_type] = stats['mapping_types'].get(mapping_type, 0) + 1
            
        # Find most triggered
        if self.event_mappings:
            most_triggered = max(
                self.event_mappings.values(),
                key=lambda m: m.get('execution_count', 0)
            )
            stats['most_triggered'] = {
                "name": most_triggered['name'],
                "count": most_triggered.get('execution_count', 0),
                "last_triggered": most_triggered.get('last_triggered')
            }
            
        return stats 