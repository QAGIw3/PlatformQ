"""
Event-driven orchestration for reactive workflows.

Provides event routing, filtering, and workflow triggering capabilities.
"""

import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, Set, Pattern
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict, deque
import re
import json

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class EventPattern(str, Enum):
    """Event matching patterns"""
    EXACT = "exact"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    REGEX = "regex"
    WILDCARD = "wildcard"


class ActionType(str, Enum):
    """Event action types"""
    PIPELINE = "pipeline"
    FUNCTION = "function"
    WEBHOOK = "webhook"
    NOTIFICATION = "notification"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"


class AggregationStrategy(str, Enum):
    """Event aggregation strategies"""
    COUNT = "count"
    TIME_WINDOW = "time_window"
    UNIQUE = "unique"
    BATCH = "batch"


@dataclass
class EventFilter:
    """Event filtering criteria"""
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, in, not_in, contains, regex
    value: Any
    
    def matches(self, event: Event) -> bool:
        """Check if event matches filter"""
        # Get field value from event
        field_value = self._get_field_value(event, self.field)
        
        # Apply operator
        if self.operator == "eq":
            return field_value == self.value
        elif self.operator == "ne":
            return field_value != self.value
        elif self.operator == "gt":
            return field_value > self.value
        elif self.operator == "lt":
            return field_value < self.value
        elif self.operator == "gte":
            return field_value >= self.value
        elif self.operator == "lte":
            return field_value <= self.value
        elif self.operator == "in":
            return field_value in self.value
        elif self.operator == "not_in":
            return field_value not in self.value
        elif self.operator == "contains":
            return self.value in str(field_value)
        elif self.operator == "regex":
            return bool(re.match(self.value, str(field_value)))
        else:
            return False
            
    def _get_field_value(self, event: Event, field: str) -> Any:
        """Get nested field value from event"""
        parts = field.split(".")
        value = event
        
        for part in parts:
            if hasattr(value, part):
                value = getattr(value, part)
            elif isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
                
        return value


@dataclass
class EventRule:
    """Event routing rule"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Matching criteria
    event_type_pattern: str = "*"
    pattern_type: EventPattern = EventPattern.WILDCARD
    filters: List[EventFilter] = field(default_factory=list)
    
    # Action configuration
    action_type: ActionType = ActionType.FUNCTION
    action_config: Dict[str, Any] = field(default_factory=dict)
    
    # Processing options
    is_active: bool = True
    priority: int = 0
    
    # Aggregation
    aggregate: bool = False
    aggregation_config: Optional[Dict[str, Any]] = None
    
    # Metadata
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def matches_event_type(self, event_type: str) -> bool:
        """Check if event type matches pattern"""
        if self.pattern_type == EventPattern.EXACT:
            return event_type == self.event_type_pattern
        elif self.pattern_type == EventPattern.PREFIX:
            return event_type.startswith(self.event_type_pattern)
        elif self.pattern_type == EventPattern.SUFFIX:
            return event_type.endswith(self.event_type_pattern)
        elif self.pattern_type == EventPattern.REGEX:
            return bool(re.match(self.event_type_pattern, event_type))
        elif self.pattern_type == EventPattern.WILDCARD:
            # Convert wildcard to regex
            pattern = self.event_type_pattern.replace("*", ".*").replace("?", ".")
            return bool(re.match(f"^{pattern}$", event_type))
        else:
            return False
            
    def matches_filters(self, event: Event) -> bool:
        """Check if event matches all filters"""
        if not self.filters:
            return True
            
        return all(f.matches(event) for f in self.filters)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "event_type_pattern": self.event_type_pattern,
            "pattern_type": self.pattern_type.value,
            "filters": [
                {"field": f.field, "operator": f.operator, "value": f.value}
                for f in self.filters
            ],
            "action_type": self.action_type.value,
            "action_config": self.action_config,
            "is_active": self.is_active,
            "priority": self.priority,
            "aggregate": self.aggregate,
            "aggregation_config": self.aggregation_config,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class EventAggregate:
    """Aggregated events"""
    rule_id: str
    strategy: AggregationStrategy
    
    # Aggregated data
    events: List[Event] = field(default_factory=list)
    count: int = 0
    
    # Window info
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    
    # State
    is_complete: bool = False
    
    def add_event(self, event: Event):
        """Add event to aggregate"""
        self.events.append(event)
        self.count += 1
        
        if not self.window_start:
            self.window_start = event.timestamp
            
    def should_trigger(self, config: Dict[str, Any]) -> bool:
        """Check if aggregate should trigger action"""
        if self.strategy == AggregationStrategy.COUNT:
            threshold = config.get("count", 10)
            return self.count >= threshold
            
        elif self.strategy == AggregationStrategy.TIME_WINDOW:
            window_seconds = config.get("window_seconds", 60)
            if self.window_start:
                elapsed = (datetime.utcnow() - self.window_start).total_seconds()
                return elapsed >= window_seconds
                
        elif self.strategy == AggregationStrategy.BATCH:
            batch_size = config.get("batch_size", 100)
            return self.count >= batch_size
            
        return False


class BaseEventHandler(ABC):
    """Base class for event handlers"""
    
    @abstractmethod
    async def handle(
        self,
        event: Union[Event, EventAggregate],
        rule: EventRule,
        context: Dict[str, Any]
    ) -> Any:
        """Handle event or aggregate"""
        pass
        
    @abstractmethod
    def can_handle(self, action_type: ActionType) -> bool:
        """Check if handler can handle action type"""
        pass


class PipelineEventHandler(BaseEventHandler):
    """Handler for triggering pipelines"""
    
    def __init__(self, pipeline_orchestrator):
        self.pipeline_orchestrator = pipeline_orchestrator
        
    async def handle(
        self,
        event: Union[Event, EventAggregate],
        rule: EventRule,
        context: Dict[str, Any]
    ) -> Any:
        """Trigger pipeline execution"""
        config = rule.action_config
        pipeline_id = config.get("pipeline_id")
        
        if not pipeline_id:
            raise ValueError("Pipeline ID not specified in action config")
            
        # Prepare parameters
        parameters = config.get("parameters", {})
        
        # Add event data to parameters
        if isinstance(event, Event):
            parameters["event_data"] = event.data
            parameters["event_type"] = event.type
        else:
            # Aggregate
            parameters["event_count"] = event.count
            parameters["events"] = [e.to_dict() for e in event.events[:10]]  # First 10
            
        # Execute pipeline
        run = await self.pipeline_orchestrator.execute_pipeline(
            pipeline_id=pipeline_id,
            parameters=parameters,
            triggered_by=f"event_rule:{rule.id}",
            trigger_type="event"
        )
        
        logger.info(f"Triggered pipeline {pipeline_id} from event rule {rule.name}")
        return {"pipeline_run_id": run.id}
        
    def can_handle(self, action_type: ActionType) -> bool:
        return action_type == ActionType.PIPELINE


class FunctionEventHandler(BaseEventHandler):
    """Handler for executing functions"""
    
    def __init__(self):
        self._functions: Dict[str, Callable] = {}
        
    def register_function(self, name: str, func: Callable):
        """Register function for event handling"""
        self._functions[name] = func
        
    async def handle(
        self,
        event: Union[Event, EventAggregate],
        rule: EventRule,
        context: Dict[str, Any]
    ) -> Any:
        """Execute function"""
        config = rule.action_config
        function_name = config.get("function")
        
        if not function_name or function_name not in self._functions:
            raise ValueError(f"Function not found: {function_name}")
            
        func = self._functions[function_name]
        
        # Prepare arguments
        args = config.get("args", [])
        kwargs = config.get("kwargs", {})
        
        # Add event to kwargs
        kwargs["event"] = event
        kwargs["context"] = context
        
        # Execute function
        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)
            
        logger.info(f"Executed function {function_name} from event rule {rule.name}")
        return result
        
    def can_handle(self, action_type: ActionType) -> bool:
        return action_type == ActionType.FUNCTION


class EventOrchestrator:
    """
    Event-driven workflow orchestrator.
    
    Features:
    - Event pattern matching
    - Rule-based routing
    - Event aggregation
    - Action execution
    - Event replay
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        pipeline_orchestrator: Optional[Any] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._rules: Dict[str, EventRule] = {}
        self._rule_index: Dict[str, List[str]] = defaultdict(list)  # event_type -> rule_ids
        
        # Handlers
        self._handlers: List[BaseEventHandler] = []
        
        # Add default handlers
        if pipeline_orchestrator:
            self._handlers.append(PipelineEventHandler(pipeline_orchestrator))
        self._handlers.append(FunctionEventHandler())
        
        # Aggregation state
        self._aggregates: Dict[str, EventAggregate] = {}
        
        # Event history
        self._event_history: deque = deque(maxlen=10000)
        
        # Subscribe to event bus
        if event_bus:
            event_bus.subscribe("*", self._handle_event)
            
    def register_rule(self, rule: EventRule):
        """Register event rule"""
        self._rules[rule.id] = rule
        
        # Update index for efficient matching
        if rule.pattern_type == EventPattern.EXACT:
            self._rule_index[rule.event_type_pattern].append(rule.id)
        else:
            # For patterns, add to wildcard index
            self._rule_index["*"].append(rule.id)
            
        logger.info(f"Registered event rule: {rule.name}")
        
    async def _handle_event(self, event: Event):
        """Handle incoming event"""
        # Store in history
        self._event_history.append(event)
        
        # Find matching rules
        matching_rules = self._find_matching_rules(event)
        
        # Sort by priority
        matching_rules.sort(key=lambda r: r.priority, reverse=True)
        
        # Process each rule
        for rule in matching_rules:
            try:
                if rule.aggregate:
                    await self._handle_aggregate_rule(event, rule)
                else:
                    await self._handle_single_rule(event, rule)
            except Exception as e:
                logger.error(f"Error processing rule {rule.name}: {e}")
                
    def _find_matching_rules(self, event: Event) -> List[EventRule]:
        """Find rules matching event"""
        matching_rules = []
        
        # Check exact matches
        for rule_id in self._rule_index.get(event.type, []):
            rule = self._rules.get(rule_id)
            if rule and rule.is_active and rule.matches_filters(event):
                matching_rules.append(rule)
                
        # Check pattern matches
        for rule_id in self._rule_index.get("*", []):
            rule = self._rules.get(rule_id)
            if rule and rule.is_active:
                if rule.matches_event_type(event.type) and rule.matches_filters(event):
                    matching_rules.append(rule)
                    
        return matching_rules
        
    async def _handle_single_rule(self, event: Event, rule: EventRule):
        """Handle single event rule"""
        # Get handler
        handler = self._get_handler_for_action(rule.action_type)
        if not handler:
            logger.error(f"No handler for action type: {rule.action_type}")
            return
            
        # Create context
        context = {
            "rule_id": rule.id,
            "rule_name": rule.name,
            "timestamp": datetime.utcnow()
        }
        
        # Execute action
        try:
            result = await handler.handle(event, rule, context)
            
            # Publish completion event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="orchestration.rule.executed",
                    source="event_orchestrator",
                    data={
                        "rule_id": rule.id,
                        "event_type": event.type,
                        "result": result
                    }
                ))
                
        except Exception as e:
            logger.error(f"Failed to execute rule {rule.name}: {e}")
            
            # Publish error event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="orchestration.rule.failed",
                    source="event_orchestrator",
                    data={
                        "rule_id": rule.id,
                        "event_type": event.type,
                        "error": str(e)
                    }
                ))
                
    async def _handle_aggregate_rule(self, event: Event, rule: EventRule):
        """Handle aggregate event rule"""
        # Get or create aggregate
        aggregate_key = f"{rule.id}:{rule.aggregation_config.get('key', 'default')}"
        
        if aggregate_key not in self._aggregates:
            strategy = AggregationStrategy(
                rule.aggregation_config.get("strategy", "count")
            )
            self._aggregates[aggregate_key] = EventAggregate(
                rule_id=rule.id,
                strategy=strategy
            )
            
        aggregate = self._aggregates[aggregate_key]
        
        # Add event
        aggregate.add_event(event)
        
        # Check if should trigger
        if aggregate.should_trigger(rule.aggregation_config):
            # Mark as complete
            aggregate.is_complete = True
            aggregate.window_end = datetime.utcnow()
            
            # Execute action
            handler = self._get_handler_for_action(rule.action_type)
            if handler:
                context = {
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "aggregate_key": aggregate_key,
                    "timestamp": datetime.utcnow()
                }
                
                try:
                    await handler.handle(aggregate, rule, context)
                except Exception as e:
                    logger.error(f"Failed to execute aggregate rule {rule.name}: {e}")
                    
            # Remove completed aggregate
            del self._aggregates[aggregate_key]
            
    def _get_handler_for_action(
        self,
        action_type: ActionType
    ) -> Optional[BaseEventHandler]:
        """Get handler for action type"""
        for handler in self._handlers:
            if handler.can_handle(action_type):
                return handler
        return None
        
    def register_handler(self, handler: BaseEventHandler):
        """Register custom event handler"""
        self._handlers.append(handler)
        
    def register_function(self, name: str, func: Callable):
        """Register function for event handling"""
        # Find function handler
        for handler in self._handlers:
            if isinstance(handler, FunctionEventHandler):
                handler.register_function(name, func)
                break
                
    def get_rule(self, rule_id: str) -> Optional[EventRule]:
        """Get rule by ID"""
        return self._rules.get(rule_id)
        
    def list_rules(
        self,
        event_type: Optional[str] = None,
        action_type: Optional[ActionType] = None,
        is_active: Optional[bool] = None
    ) -> List[EventRule]:
        """List event rules"""
        rules = list(self._rules.values())
        
        if event_type:
            rules = [r for r in rules if r.matches_event_type(event_type)]
            
        if action_type:
            rules = [r for r in rules if r.action_type == action_type]
            
        if is_active is not None:
            rules = [r for r in rules if r.is_active == is_active]
            
        return rules
        
    def update_rule(self, rule_id: str, updates: Dict[str, Any]):
        """Update event rule"""
        rule = self._rules.get(rule_id)
        if not rule:
            raise ValueError(f"Rule not found: {rule_id}")
            
        # Apply updates
        for key, value in updates.items():
            if hasattr(rule, key):
                setattr(rule, key, value)
                
        logger.info(f"Updated rule: {rule.name}")
        
    def delete_rule(self, rule_id: str):
        """Delete event rule"""
        if rule_id in self._rules:
            rule = self._rules[rule_id]
            del self._rules[rule_id]
            
            # Update index
            if rule.pattern_type == EventPattern.EXACT:
                self._rule_index[rule.event_type_pattern].remove(rule_id)
            else:
                self._rule_index["*"].remove(rule_id)
                
            logger.info(f"Deleted rule: {rule.name}")
            
    async def replay_events(
        self,
        start_time: datetime,
        end_time: datetime,
        event_types: Optional[List[str]] = None,
        rule_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Replay historical events"""
        replayed_count = 0
        matched_count = 0
        
        # Filter events by time
        events_to_replay = [
            e for e in self._event_history
            if start_time <= e.timestamp <= end_time
        ]
        
        # Filter by event types
        if event_types:
            events_to_replay = [
                e for e in events_to_replay
                if e.type in event_types
            ]
            
        # Process each event
        for event in events_to_replay:
            replayed_count += 1
            
            # Find matching rules
            if rule_ids:
                rules = [self._rules[rid] for rid in rule_ids if rid in self._rules]
            else:
                rules = self._find_matching_rules(event)
                
            if rules:
                matched_count += 1
                
            # Process rules
            for rule in rules:
                try:
                    if not rule.aggregate:  # Skip aggregates for replay
                        await self._handle_single_rule(event, rule)
                except Exception as e:
                    logger.error(f"Error replaying event for rule {rule.name}: {e}")
                    
        return {
            "replayed_events": replayed_count,
            "matched_events": matched_count,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            }
        }
        
    def get_event_history(
        self,
        limit: int = 100,
        event_types: Optional[List[str]] = None
    ) -> List[Event]:
        """Get recent event history"""
        events = list(self._event_history)
        
        if event_types:
            events = [e for e in events if e.type in event_types]
            
        # Return most recent
        return events[-limit:]
        
    def get_aggregation_status(self) -> Dict[str, Any]:
        """Get current aggregation status"""
        status = {}
        
        for key, aggregate in self._aggregates.items():
            rule = self._rules.get(aggregate.rule_id)
            status[key] = {
                "rule_name": rule.name if rule else "Unknown",
                "event_count": aggregate.count,
                "window_start": aggregate.window_start.isoformat() if aggregate.window_start else None,
                "strategy": aggregate.strategy.value
            }
            
        return status 