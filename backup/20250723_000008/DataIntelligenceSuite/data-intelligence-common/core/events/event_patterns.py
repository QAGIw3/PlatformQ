"""
Event Pattern Matching for DataIntelligenceSuite

Provides complex event pattern detection capabilities.
"""

import logging
from typing import Any, Dict, Optional, List, Callable, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import re

from .event_bus import Event

logger = logging.getLogger(__name__)


class PatternOperator(Enum):
    """Pattern matching operators"""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUAL = "ge"
    LESS_EQUAL = "le"
    CONTAINS = "contains"
    REGEX = "regex"
    IN = "in"
    NOT_IN = "not_in"


class PatternCombinator(Enum):
    """Pattern combination operators"""
    AND = "and"
    OR = "or"
    NOT = "not"
    FOLLOWED_BY = "followed_by"
    NOT_FOLLOWED_BY = "not_followed_by"
    WITHIN = "within"


@dataclass
class EventCondition:
    """Condition for event matching"""
    field: str
    operator: PatternOperator
    value: Any
    
    def matches(self, event: Event) -> bool:
        """Check if event matches condition"""
        # Get field value from event
        field_value = self._get_field_value(event, self.field)
        
        # Apply operator
        if self.operator == PatternOperator.EQUALS:
            return field_value == self.value
        elif self.operator == PatternOperator.NOT_EQUALS:
            return field_value != self.value
        elif self.operator == PatternOperator.GREATER_THAN:
            return field_value > self.value
        elif self.operator == PatternOperator.LESS_THAN:
            return field_value < self.value
        elif self.operator == PatternOperator.GREATER_EQUAL:
            return field_value >= self.value
        elif self.operator == PatternOperator.LESS_EQUAL:
            return field_value <= self.value
        elif self.operator == PatternOperator.CONTAINS:
            return self.value in str(field_value)
        elif self.operator == PatternOperator.REGEX:
            return bool(re.match(self.value, str(field_value)))
        elif self.operator == PatternOperator.IN:
            return field_value in self.value
        elif self.operator == PatternOperator.NOT_IN:
            return field_value not in self.value
        else:
            return False
            
    def _get_field_value(self, event: Event, field: str) -> Any:
        """Get field value from event"""
        # Support nested fields with dot notation
        parts = field.split('.')
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
class EventPattern:
    """Complex event pattern definition"""
    pattern_id: str
    name: str
    conditions: List[EventCondition] = field(default_factory=list)
    combinator: PatternCombinator = PatternCombinator.AND
    sub_patterns: List['EventPattern'] = field(default_factory=list)
    
    # Temporal constraints
    time_window: Optional[timedelta] = None
    min_occurrences: int = 1
    max_occurrences: Optional[int] = None
    
    # Sequence constraints
    ordered: bool = False
    allow_other_events: bool = True
    
    # Actions
    on_match: Optional[Callable[[List[Event]], Any]] = None
    
    def matches(self, event: Event) -> bool:
        """Check if event matches pattern"""
        if self.combinator == PatternCombinator.AND:
            # All conditions must match
            return all(cond.matches(event) for cond in self.conditions)
        elif self.combinator == PatternCombinator.OR:
            # At least one condition must match
            return any(cond.matches(event) for cond in self.conditions)
        elif self.combinator == PatternCombinator.NOT:
            # None of the conditions should match
            return not any(cond.matches(event) for cond in self.conditions)
        else:
            # Complex patterns handled by PatternMatcher
            return False


@dataclass
class PatternMatch:
    """Result of pattern matching"""
    pattern_id: str
    matched_events: List[Event]
    start_time: datetime
    end_time: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class PatternMatcher:
    """
    Complex event pattern matcher.
    
    Features:
    - Multi-event pattern detection
    - Temporal constraints
    - Sequence detection
    - State management
    - Pattern composition
    """
    
    def __init__(self):
        self._patterns: Dict[str, EventPattern] = {}
        self._active_matches: Dict[str, List[Dict[str, Any]]] = {}
        self._completed_matches: List[PatternMatch] = []
        
    def add_pattern(self, pattern: EventPattern):
        """Add pattern to matcher"""
        self._patterns[pattern.pattern_id] = pattern
        self._active_matches[pattern.pattern_id] = []
        
    def remove_pattern(self, pattern_id: str):
        """Remove pattern from matcher"""
        self._patterns.pop(pattern_id, None)
        self._active_matches.pop(pattern_id, None)
        
    async def process_event(self, event: Event) -> List[PatternMatch]:
        """Process event and check for pattern matches"""
        new_matches = []
        
        for pattern_id, pattern in self._patterns.items():
            # Check simple patterns
            if pattern.combinator in [PatternCombinator.AND, PatternCombinator.OR, PatternCombinator.NOT]:
                if pattern.matches(event):
                    # Create match for simple pattern
                    match = PatternMatch(
                        pattern_id=pattern_id,
                        matched_events=[event],
                        start_time=event.timestamp,
                        end_time=event.timestamp
                    )
                    new_matches.append(match)
                    
                    # Execute action if defined
                    if pattern.on_match:
                        await self._execute_action(pattern.on_match, [event])
                        
            else:
                # Handle complex patterns
                matches = await self._process_complex_pattern(pattern, event)
                new_matches.extend(matches)
                
        # Clean expired matches
        self._clean_expired_matches()
        
        return new_matches
        
    async def _process_complex_pattern(
        self,
        pattern: EventPattern,
        event: Event
    ) -> List[PatternMatch]:
        """Process complex pattern with temporal/sequence constraints"""
        new_matches = []
        pattern_id = pattern.pattern_id
        
        if pattern.combinator == PatternCombinator.FOLLOWED_BY:
            # Check if this event can start or continue a sequence
            active_matches = self._active_matches[pattern_id]
            
            # Try to continue existing matches
            continued = False
            for match_state in active_matches[:]:
                if await self._can_continue_sequence(pattern, match_state, event):
                    match_state["events"].append(event)
                    match_state["last_event_time"] = event.timestamp
                    
                    # Check if pattern is complete
                    if self._is_pattern_complete(pattern, match_state):
                        completed_match = self._complete_match(pattern_id, match_state)
                        new_matches.append(completed_match)
                        active_matches.remove(match_state)
                        
                        # Execute action
                        if pattern.on_match:
                            await self._execute_action(
                                pattern.on_match,
                                match_state["events"]
                            )
                            
                    continued = True
                    
            # Try to start new match if event matches first condition
            if pattern.conditions and pattern.conditions[0].matches(event):
                match_state = {
                    "events": [event],
                    "start_time": event.timestamp,
                    "last_event_time": event.timestamp,
                    "position": 0
                }
                active_matches.append(match_state)
                
        elif pattern.combinator == PatternCombinator.WITHIN:
            # Collect events within time window
            active_matches = self._active_matches[pattern_id]
            
            # Add to existing windows
            for match_state in active_matches[:]:
                if self._is_within_window(pattern, match_state, event):
                    if pattern.matches(event):
                        match_state["events"].append(event)
                        
                        # Check occurrence constraints
                        if len(match_state["events"]) >= pattern.min_occurrences:
                            if not pattern.max_occurrences or len(match_state["events"]) <= pattern.max_occurrences:
                                completed_match = self._complete_match(pattern_id, match_state)
                                new_matches.append(completed_match)
                                
                                if pattern.on_match:
                                    await self._execute_action(
                                        pattern.on_match,
                                        match_state["events"]
                                    )
                                    
            # Start new window if event matches
            if pattern.matches(event):
                match_state = {
                    "events": [event],
                    "start_time": event.timestamp,
                    "window_end": event.timestamp + pattern.time_window
                }
                active_matches.append(match_state)
                
        return new_matches
        
    async def _can_continue_sequence(
        self,
        pattern: EventPattern,
        match_state: Dict[str, Any],
        event: Event
    ) -> bool:
        """Check if event can continue a sequence"""
        # Check time constraints
        if pattern.time_window:
            if event.timestamp - match_state["start_time"] > pattern.time_window:
                return False
                
        # Check position in sequence
        position = match_state.get("position", 0) + 1
        if position >= len(pattern.conditions):
            return False
            
        # Check if event matches next condition
        if not pattern.conditions[position].matches(event):
            return False
            
        # Check ordering constraints
        if pattern.ordered and not pattern.allow_other_events:
            # Strict ordering - no other events allowed
            last_event_time = match_state["last_event_time"]
            if event.timestamp <= last_event_time:
                return False
                
        match_state["position"] = position
        return True
        
    def _is_pattern_complete(
        self,
        pattern: EventPattern,
        match_state: Dict[str, Any]
    ) -> bool:
        """Check if pattern match is complete"""
        # Check all conditions matched
        if match_state.get("position", 0) + 1 < len(pattern.conditions):
            return False
            
        # Check occurrence constraints
        event_count = len(match_state["events"])
        if event_count < pattern.min_occurrences:
            return False
            
        if pattern.max_occurrences and event_count > pattern.max_occurrences:
            return False
            
        return True
        
    def _is_within_window(
        self,
        pattern: EventPattern,
        match_state: Dict[str, Any],
        event: Event
    ) -> bool:
        """Check if event is within time window"""
        window_end = match_state.get("window_end")
        if window_end and event.timestamp > window_end:
            return False
        return True
        
    def _complete_match(
        self,
        pattern_id: str,
        match_state: Dict[str, Any]
    ) -> PatternMatch:
        """Complete a pattern match"""
        events = match_state["events"]
        
        match = PatternMatch(
            pattern_id=pattern_id,
            matched_events=events,
            start_time=events[0].timestamp,
            end_time=events[-1].timestamp,
            metadata={
                "event_count": len(events),
                "duration": (events[-1].timestamp - events[0].timestamp).total_seconds()
            }
        )
        
        self._completed_matches.append(match)
        return match
        
    def _clean_expired_matches(self):
        """Clean expired active matches"""
        current_time = datetime.utcnow()
        
        for pattern_id, pattern in self._patterns.items():
            if pattern.time_window:
                active_matches = self._active_matches[pattern_id]
                
                # Remove expired matches
                active_matches[:] = [
                    match for match in active_matches
                    if current_time - match["start_time"] <= pattern.time_window
                ]
                
    async def _execute_action(self, action: Callable, events: List[Event]):
        """Execute pattern action"""
        try:
            if asyncio.iscoroutinefunction(action):
                await action(events)
            else:
                action(events)
        except Exception as e:
            logger.error(f"Error executing pattern action: {e}")
            
    def get_active_matches(self, pattern_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get active pattern matches"""
        if pattern_id:
            return {pattern_id: self._active_matches.get(pattern_id, [])}
        return self._active_matches
        
    def get_completed_matches(
        self,
        pattern_id: Optional[str] = None,
        since: Optional[datetime] = None
    ) -> List[PatternMatch]:
        """Get completed pattern matches"""
        matches = self._completed_matches
        
        if pattern_id:
            matches = [m for m in matches if m.pattern_id == pattern_id]
            
        if since:
            matches = [m for m in matches if m.end_time >= since]
            
        return matches 