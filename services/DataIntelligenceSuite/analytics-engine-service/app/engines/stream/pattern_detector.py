"""
Pattern Detection for Complex Event Processing (CEP).
"""

from typing import Dict, List, Any, Optional, Callable, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import re
import asyncio
from collections import deque, defaultdict

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class PatternType(str, Enum):
    """Types of patterns for detection."""
    SEQUENCE = "sequence"
    CONJUNCTION = "conjunction"
    DISJUNCTION = "disjunction"
    NEGATION = "negation"
    ITERATION = "iteration"
    TEMPORAL = "temporal"


class MatchStrategy(str, Enum):
    """Pattern matching strategies."""
    STRICT = "strict"  # Strict contiguity
    RELAXED = "relaxed"  # Relaxed contiguity
    NON_DETERMINISTIC = "non_deterministic"  # Non-deterministic relaxed


@dataclass
class PatternCondition:
    """Condition for pattern matching."""
    field: str
    operator: str  # ==, !=, >, <, >=, <=, in, contains, regex
    value: Any
    
    def evaluate(self, event: Dict[str, Any]) -> bool:
        """Evaluate condition against event."""
        if self.field not in event:
            return False
        
        event_value = event[self.field]
        
        if self.operator == "==":
            return event_value == self.value
        elif self.operator == "!=":
            return event_value != self.value
        elif self.operator == ">":
            return event_value > self.value
        elif self.operator == ">=":
            return event_value >= self.value
        elif self.operator == "<":
            return event_value < self.value
        elif self.operator == "<=":
            return event_value <= self.value
        elif self.operator == "in":
            return event_value in self.value
        elif self.operator == "contains":
            return self.value in str(event_value)
        elif self.operator == "regex":
            return bool(re.match(self.value, str(event_value)))
        
        return False


@dataclass
class PatternElement:
    """Single element in a pattern."""
    name: str
    conditions: List[PatternCondition]
    quantifier: Optional[str] = None  # +, *, ?, {n}, {n,m}
    within: Optional[timedelta] = None  # Time constraint
    
    def matches(self, event: Dict[str, Any]) -> bool:
        """Check if event matches this pattern element."""
        return all(condition.evaluate(event) for condition in self.conditions)


@dataclass
class Pattern:
    """Complex event pattern definition."""
    pattern_id: str
    name: str
    pattern_type: PatternType
    elements: List[PatternElement]
    match_strategy: MatchStrategy = MatchStrategy.STRICT
    within: Optional[timedelta] = None  # Overall time window
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatternMatch:
    """Represents a pattern match."""
    pattern_id: str
    match_id: str
    events: List[Dict[str, Any]]
    start_time: datetime
    end_time: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatternState:
    """State for pattern matching."""
    pattern: Pattern
    partial_matches: List[List[Dict[str, Any]]] = field(default_factory=list)
    current_element_index: int = 0
    start_time: Optional[datetime] = None


class PatternDetector:
    """
    Complex Event Processing pattern detector.
    """
    
    def __init__(self, buffer_size: int = 10000):
        self.patterns: Dict[str, Pattern] = {}
        self.pattern_states: Dict[str, PatternState] = {}
        self.event_buffer = deque(maxlen=buffer_size)
        self.matches: List[PatternMatch] = []
        self.match_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
    def register_pattern(
        self,
        pattern: Pattern,
        callback: Optional[Callable[[PatternMatch], None]] = None
    ):
        """Register a pattern for detection."""
        self.patterns[pattern.pattern_id] = pattern
        self.pattern_states[pattern.pattern_id] = PatternState(pattern=pattern)
        
        if callback:
            self.match_callbacks[pattern.pattern_id].append(callback)
        
        logger.info(f"Registered pattern: {pattern.name} ({pattern.pattern_id})")
        
    def unregister_pattern(self, pattern_id: str):
        """Unregister a pattern."""
        if pattern_id in self.patterns:
            del self.patterns[pattern_id]
            del self.pattern_states[pattern_id]
            if pattern_id in self.match_callbacks:
                del self.match_callbacks[pattern_id]
            
            logger.info(f"Unregistered pattern: {pattern_id}")
            
    async def process_event(self, event: Dict[str, Any]) -> List[PatternMatch]:
        """Process an event and detect patterns."""
        # Add to buffer
        self.event_buffer.append(event)
        
        # Extract event time
        event_time = self._extract_event_time(event)
        
        # Check each pattern
        new_matches = []
        
        for pattern_id, pattern in self.patterns.items():
            matches = await self._check_pattern(pattern_id, event, event_time)
            new_matches.extend(matches)
        
        # Trigger callbacks
        for match in new_matches:
            await self._trigger_callbacks(match)
        
        return new_matches
        
    async def _check_pattern(
        self,
        pattern_id: str,
        event: Dict[str, Any],
        event_time: datetime
    ) -> List[PatternMatch]:
        """Check if event contributes to pattern match."""
        pattern = self.patterns[pattern_id]
        state = self.pattern_states[pattern_id]
        matches = []
        
        if pattern.pattern_type == PatternType.SEQUENCE:
            matches = await self._check_sequence_pattern(pattern, state, event, event_time)
        elif pattern.pattern_type == PatternType.CONJUNCTION:
            matches = await self._check_conjunction_pattern(pattern, state, event, event_time)
        elif pattern.pattern_type == PatternType.TEMPORAL:
            matches = await self._check_temporal_pattern(pattern, state, event, event_time)
        # Add other pattern types as needed
        
        return matches
        
    async def _check_sequence_pattern(
        self,
        pattern: Pattern,
        state: PatternState,
        event: Dict[str, Any],
        event_time: datetime
    ) -> List[PatternMatch]:
        """Check sequence pattern (ordered events)."""
        matches = []
        
        # Get current element
        if state.current_element_index >= len(pattern.elements):
            return matches
        
        current_element = pattern.elements[state.current_element_index]
        
        # Check if event matches current element
        if current_element.matches(event):
            # Start new partial match if needed
            if state.current_element_index == 0:
                state.partial_matches.append([event])
                state.start_time = event_time
            else:
                # Add to existing partial matches
                new_partial_matches = []
                
                for partial_match in state.partial_matches:
                    # Check time constraints
                    if pattern.within:
                        first_event_time = self._extract_event_time(partial_match[0])
                        if event_time - first_event_time > pattern.within:
                            continue  # Timeout
                    
                    # Create new partial match
                    new_match = partial_match + [event]
                    
                    # Check if complete
                    if len(new_match) == len(pattern.elements):
                        # Complete match found
                        match = PatternMatch(
                            pattern_id=pattern.pattern_id,
                            match_id=f"{pattern.pattern_id}_{event_time.timestamp()}",
                            events=new_match,
                            start_time=self._extract_event_time(new_match[0]),
                            end_time=event_time,
                            metadata={"pattern_name": pattern.name}
                        )
                        matches.append(match)
                        self.matches.append(match)
                    else:
                        new_partial_matches.append(new_match)
                
                state.partial_matches = new_partial_matches
                
            # Handle match strategy
            if pattern.match_strategy == MatchStrategy.STRICT:
                # Move to next element only
                state.current_element_index += 1
            elif pattern.match_strategy == MatchStrategy.RELAXED:
                # Can skip events
                pass
        
        # Clean up old partial matches
        if pattern.within:
            state.partial_matches = [
                pm for pm in state.partial_matches
                if event_time - self._extract_event_time(pm[0]) <= pattern.within
            ]
        
        return matches
        
    async def _check_conjunction_pattern(
        self,
        pattern: Pattern,
        state: PatternState,
        event: Dict[str, Any],
        event_time: datetime
    ) -> List[PatternMatch]:
        """Check conjunction pattern (all elements must occur)."""
        matches = []
        
        # Track which elements have been matched
        if not hasattr(state, 'matched_elements'):
            state.matched_elements = set()
            state.matched_events = []
        
        # Check which elements this event matches
        for i, element in enumerate(pattern.elements):
            if element.matches(event) and i not in state.matched_elements:
                state.matched_elements.add(i)
                state.matched_events.append(event)
                
                if not state.start_time:
                    state.start_time = event_time
                
                # Check if all elements matched
                if len(state.matched_elements) == len(pattern.elements):
                    # Complete match
                    match = PatternMatch(
                        pattern_id=pattern.pattern_id,
                        match_id=f"{pattern.pattern_id}_{event_time.timestamp()}",
                        events=state.matched_events,
                        start_time=state.start_time,
                        end_time=event_time,
                        metadata={"pattern_name": pattern.name}
                    )
                    matches.append(match)
                    self.matches.append(match)
                    
                    # Reset state
                    state.matched_elements.clear()
                    state.matched_events.clear()
                    state.start_time = None
                
                break  # Event can only match one element
        
        # Check timeout
        if pattern.within and state.start_time:
            if event_time - state.start_time > pattern.within:
                # Reset on timeout
                state.matched_elements.clear()
                state.matched_events.clear()
                state.start_time = None
        
        return matches
        
    async def _check_temporal_pattern(
        self,
        pattern: Pattern,
        state: PatternState,
        event: Dict[str, Any],
        event_time: datetime
    ) -> List[PatternMatch]:
        """Check temporal pattern (time-based constraints)."""
        matches = []
        
        # Simple temporal pattern: events within time window
        if not hasattr(state, 'window_events'):
            state.window_events = deque()
        
        # Add event if it matches any element
        for element in pattern.elements:
            if element.matches(event):
                state.window_events.append((event, event_time))
                break
        
        # Remove old events outside window
        if pattern.within:
            while state.window_events:
                oldest_event, oldest_time = state.window_events[0]
                if event_time - oldest_time > pattern.within:
                    state.window_events.popleft()
                else:
                    break
        
        # Check if we have matches for all elements
        element_matches = [False] * len(pattern.elements)
        matched_events = []
        
        for evt, evt_time in state.window_events:
            for i, element in enumerate(pattern.elements):
                if not element_matches[i] and element.matches(evt):
                    element_matches[i] = True
                    matched_events.append(evt)
                    break
        
        if all(element_matches):
            # All elements matched within window
            match = PatternMatch(
                pattern_id=pattern.pattern_id,
                match_id=f"{pattern.pattern_id}_{event_time.timestamp()}",
                events=matched_events,
                start_time=state.window_events[0][1] if state.window_events else event_time,
                end_time=event_time,
                metadata={"pattern_name": pattern.name}
            )
            matches.append(match)
            self.matches.append(match)
            
            # Clear window after match
            state.window_events.clear()
        
        return matches
        
    async def _trigger_callbacks(self, match: PatternMatch):
        """Trigger callbacks for pattern match."""
        callbacks = self.match_callbacks.get(match.pattern_id, [])
        
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(match)
                else:
                    callback(match)
            except Exception as e:
                logger.error(f"Error in pattern match callback: {e}")
                
    def _extract_event_time(self, event: Dict[str, Any]) -> datetime:
        """Extract timestamp from event."""
        for field in ["timestamp", "event_time", "time", "ts"]:
            if field in event:
                ts = event[field]
                if isinstance(ts, datetime):
                    return ts
                elif isinstance(ts, str):
                    return datetime.fromisoformat(ts)
                elif isinstance(ts, (int, float)):
                    return datetime.fromtimestamp(ts / 1000)  # Assume ms
        
        return datetime.utcnow()
        
    def get_pattern_stats(self, pattern_id: Optional[str] = None) -> Dict[str, Any]:
        """Get pattern detection statistics."""
        if pattern_id:
            pattern_matches = [m for m in self.matches if m.pattern_id == pattern_id]
            return {
                "pattern_id": pattern_id,
                "total_matches": len(pattern_matches),
                "recent_matches": [
                    {
                        "match_id": m.match_id,
                        "start_time": m.start_time.isoformat(),
                        "end_time": m.end_time.isoformat(),
                        "event_count": len(m.events)
                    }
                    for m in pattern_matches[-10:]  # Last 10 matches
                ]
            }
        else:
            # Overall stats
            pattern_counts = defaultdict(int)
            for match in self.matches:
                pattern_counts[match.pattern_id] += 1
            
            return {
                "total_patterns": len(self.patterns),
                "total_matches": len(self.matches),
                "matches_by_pattern": dict(pattern_counts),
                "buffer_size": len(self.event_buffer)
            }
            
    def clear_matches(self, before: Optional[datetime] = None):
        """Clear old matches."""
        if before:
            self.matches = [
                m for m in self.matches
                if m.end_time >= before
            ]
        else:
            self.matches.clear()
            
        logger.info(f"Cleared matches, remaining: {len(self.matches)}")


# Helper functions for creating common patterns
def create_sequence_pattern(
    pattern_id: str,
    name: str,
    element_conditions: List[List[PatternCondition]],
    within: Optional[timedelta] = None,
    match_strategy: MatchStrategy = MatchStrategy.STRICT
) -> Pattern:
    """Create a sequence pattern."""
    elements = [
        PatternElement(
            name=f"element_{i}",
            conditions=conditions
        )
        for i, conditions in enumerate(element_conditions)
    ]
    
    return Pattern(
        pattern_id=pattern_id,
        name=name,
        pattern_type=PatternType.SEQUENCE,
        elements=elements,
        match_strategy=match_strategy,
        within=within
    )


def create_threshold_pattern(
    pattern_id: str,
    name: str,
    field: str,
    threshold: float,
    count: int,
    within: timedelta
) -> Pattern:
    """Create a threshold breach pattern."""
    condition = PatternCondition(
        field=field,
        operator=">",
        value=threshold
    )
    
    elements = [
        PatternElement(
            name="threshold_breach",
            conditions=[condition],
            quantifier=f"{{{count}}}"  # Exactly 'count' occurrences
        )
    ]
    
    return Pattern(
        pattern_id=pattern_id,
        name=name,
        pattern_type=PatternType.TEMPORAL,
        elements=elements,
        within=within,
        metadata={
            "threshold": threshold,
            "count": count,
            "field": field
        }
    ) 