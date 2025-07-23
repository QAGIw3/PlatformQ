"""
Data Aggregation Engine for Integration Hub.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict
import pandas as pd
import numpy as np

from data_intelligence_common.core.events import EventBus

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class AggregationType(str, Enum):
    """Types of aggregation operations."""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    MODE = "mode"
    STDDEV = "stddev"
    VARIANCE = "variance"
    PERCENTILE = "percentile"
    CUSTOM = "custom"


class WindowType(str, Enum):
    """Types of aggregation windows."""
    TUMBLING = "tumbling"  # Fixed-size, non-overlapping
    SLIDING = "sliding"  # Fixed-size, overlapping
    SESSION = "session"  # Variable-size based on activity
    GLOBAL = "global"  # All data


@dataclass
class AggregationRule:
    """Defines an aggregation rule."""
    rule_id: str
    name: str
    source_regions: List[str]  # Cache regions to aggregate from
    aggregation_type: AggregationType
    group_by: List[str]  # Fields to group by
    aggregation_field: str  # Field to aggregate
    
    # Window configuration
    window_type: WindowType = WindowType.TUMBLING
    window_size_seconds: int = 300  # 5 minutes default
    slide_interval_seconds: Optional[int] = None  # For sliding windows
    
    # Filtering
    filter_expression: Optional[str] = None
    filter_params: Dict[str, Any] = field(default_factory=dict)
    
    # Output configuration
    output_region: Optional[str] = None
    output_format: str = "json"
    
    # Scheduling
    schedule_interval_seconds: Optional[int] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    rule_id: str
    timestamp: datetime
    window_start: datetime
    window_end: datetime
    
    # Results
    aggregated_data: Dict[str, Any]
    group_count: int
    record_count: int
    
    # Performance
    execution_time_ms: float
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataAggregator:
    """
    Engine for performing data aggregations across cache regions.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        integration_hub: Any  # Avoid circular import
    ):
        self.event_bus = event_bus
        self.integration_hub = integration_hub
        
        # Aggregation rules
        self.rules: Dict[str, AggregationRule] = {}
        self.active_aggregations: Dict[str, Any] = {}
        
        # Custom aggregation functions
        self.custom_aggregators: Dict[str, Callable] = {}
        
        # Window management
        self.window_states: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Metrics
        self.metrics = defaultdict(int)
        
        # Background tasks
        self._scheduler_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info("Data Aggregator initialized")
        
    async def initialize(self):
        """Initialize data aggregator."""
        # Register default custom aggregators
        self._register_default_aggregators()
        
        # Subscribe to events
        await self.event_bus.subscribe("aggregation.trigger", self._handle_aggregation_trigger)
        
        # Start background tasks
        self._scheduler_task = asyncio.create_task(self._process_scheduled_aggregations())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_states())
        
        logger.info("Data Aggregator ready")
        
    async def cleanup(self):
        """Cleanup aggregator resources."""
        # Cancel background tasks
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Clear states
        self.window_states.clear()
        self.active_aggregations.clear()
        
        logger.info("Data Aggregator cleaned up")
        
    async def register_rule(self, rule: AggregationRule):
        """Register an aggregation rule."""
        self.rules[rule.rule_id] = rule
        
        # Initialize window state if needed
        if rule.window_type in [WindowType.SLIDING, WindowType.SESSION]:
            self.window_states[rule.rule_id] = {
                "buffer": [],
                "last_window": datetime.utcnow()
            }
        
        # Publish event
        await self.event_bus.publish("aggregation.rule.registered", {
            "rule_id": rule.rule_id,
            "name": rule.name,
            "type": rule.aggregation_type.value
        })
        
        logger.info(f"Registered aggregation rule: {rule.name}")
        
    async def unregister_rule(self, rule_id: str) -> bool:
        """Unregister an aggregation rule."""
        if rule_id not in self.rules:
            return False
        
        del self.rules[rule_id]
        
        # Clean up window state
        if rule_id in self.window_states:
            del self.window_states[rule_id]
        
        logger.info(f"Unregistered aggregation rule: {rule_id}")
        return True
        
    async def execute_aggregation(
        self,
        rule_id: str,
        force: bool = False
    ) -> Optional[AggregationResult]:
        """Execute an aggregation rule."""
        rule = self.rules.get(rule_id)
        if not rule or (not rule.enabled and not force):
            return None
        
        start_time = datetime.utcnow()
        
        try:
            # Determine window boundaries
            window_start, window_end = self._calculate_window(rule)
            
            # Collect data from source regions
            all_data = []
            for region in rule.source_regions:
                data = await self._fetch_region_data(region, window_start, window_end, rule)
                all_data.extend(data)
            
            if not all_data:
                logger.warning(f"No data found for aggregation rule {rule_id}")
                return None
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(all_data)
            
            # Apply filter if specified
            if rule.filter_expression:
                df = self._apply_filter(df, rule.filter_expression, rule.filter_params)
            
            # Perform aggregation
            aggregated_data = await self._perform_aggregation(df, rule)
            
            # Create result
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            result = AggregationResult(
                rule_id=rule_id,
                timestamp=datetime.utcnow(),
                window_start=window_start,
                window_end=window_end,
                aggregated_data=aggregated_data,
                group_count=len(aggregated_data),
                record_count=len(df),
                execution_time_ms=execution_time
            )
            
            # Store result if output region specified
            if rule.output_region:
                await self._store_result(rule.output_region, result)
            
            # Update metrics
            self.metrics["aggregations_executed"] += 1
            self.metrics["records_processed"] += len(df)
            
            # Publish event
            await self.event_bus.publish("aggregation.completed", {
                "rule_id": rule_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "group_count": result.group_count,
                "record_count": result.record_count
            })
            
            logger.info(f"Completed aggregation {rule_id}: {result.group_count} groups, {result.record_count} records")
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing aggregation {rule_id}: {e}")
            self.metrics["aggregation_errors"] += 1
            
            # Publish error event
            await self.event_bus.publish("aggregation.error", {
                "rule_id": rule_id,
                "error": str(e)
            })
            
            return None
            
    async def _perform_aggregation(
        self,
        df: pd.DataFrame,
        rule: AggregationRule
    ) -> Dict[str, Any]:
        """Perform the actual aggregation operation."""
        if rule.aggregation_type == AggregationType.CUSTOM:
            # Use custom aggregator
            aggregator = self.custom_aggregators.get(rule.aggregation_field)
            if not aggregator:
                raise ValueError(f"Custom aggregator '{rule.aggregation_field}' not found")
            
            return await aggregator(df, rule)
        
        # Group by specified fields
        if rule.group_by:
            grouped = df.groupby(rule.group_by)
        else:
            # No grouping, aggregate all data
            grouped = df
        
        # Apply aggregation
        if rule.aggregation_type == AggregationType.COUNT:
            if hasattr(grouped, 'size'):
                result = grouped.size()
            else:
                result = len(grouped)
                
        elif rule.aggregation_type == AggregationType.SUM:
            result = grouped[rule.aggregation_field].sum()
            
        elif rule.aggregation_type == AggregationType.AVG:
            result = grouped[rule.aggregation_field].mean()
            
        elif rule.aggregation_type == AggregationType.MIN:
            result = grouped[rule.aggregation_field].min()
            
        elif rule.aggregation_type == AggregationType.MAX:
            result = grouped[rule.aggregation_field].max()
            
        elif rule.aggregation_type == AggregationType.MEDIAN:
            result = grouped[rule.aggregation_field].median()
            
        elif rule.aggregation_type == AggregationType.MODE:
            result = grouped[rule.aggregation_field].agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None)
            
        elif rule.aggregation_type == AggregationType.STDDEV:
            result = grouped[rule.aggregation_field].std()
            
        elif rule.aggregation_type == AggregationType.VARIANCE:
            result = grouped[rule.aggregation_field].var()
            
        elif rule.aggregation_type == AggregationType.PERCENTILE:
            percentile = rule.metadata.get("percentile", 50)
            result = grouped[rule.aggregation_field].quantile(percentile / 100)
            
        else:
            raise ValueError(f"Unsupported aggregation type: {rule.aggregation_type}")
        
        # Convert result to dictionary
        if isinstance(result, pd.Series):
            return result.to_dict()
        elif isinstance(result, pd.DataFrame):
            return result.to_dict('records')
        else:
            return {"value": result}
            
    def _calculate_window(self, rule: AggregationRule) -> Tuple[datetime, datetime]:
        """Calculate window boundaries for aggregation."""
        now = datetime.utcnow()
        
        if rule.window_type == WindowType.TUMBLING:
            # Fixed-size, non-overlapping windows
            window_end = now
            window_start = now - timedelta(seconds=rule.window_size_seconds)
            
        elif rule.window_type == WindowType.SLIDING:
            # Fixed-size, overlapping windows
            window_end = now
            window_start = now - timedelta(seconds=rule.window_size_seconds)
            
            # Update window state
            state = self.window_states.get(rule.rule_id, {})
            state["last_window"] = now
            
        elif rule.window_type == WindowType.SESSION:
            # Session-based windows (not implemented)
            window_end = now
            window_start = now - timedelta(seconds=rule.window_size_seconds)
            
        else:  # GLOBAL
            # All available data
            window_end = now
            window_start = datetime.min
        
        return window_start, window_end
        
    async def _fetch_region_data(
        self,
        region: str,
        window_start: datetime,
        window_end: datetime,
        rule: AggregationRule
    ) -> List[Dict[str, Any]]:
        """Fetch data from a cache region within the window."""
        # Query the integration hub for data
        query = f"SELECT * FROM {region} WHERE updated_at >= ? AND updated_at <= ?"
        params = [window_start.isoformat(), window_end.isoformat()]
        
        entities = await self.integration_hub.query_entities(
            region,
            query,
            params,
            limit=10000  # Reasonable limit
        )
        
        # Convert entities to dictionaries
        return [entity.data for entity in entities]
        
    def _apply_filter(
        self,
        df: pd.DataFrame,
        filter_expression: str,
        filter_params: Dict[str, Any]
    ) -> pd.DataFrame:
        """Apply filter expression to DataFrame."""
        try:
            # Create safe evaluation context
            context = {
                'df': df,
                'pd': pd,
                'np': np,
                **filter_params
            }
            
            # Evaluate filter expression
            mask = eval(filter_expression, {"__builtins__": {}}, context)
            
            if isinstance(mask, pd.Series):
                return df[mask]
            else:
                return df
                
        except Exception as e:
            logger.error(f"Error applying filter: {e}")
            return df
            
    async def _store_result(self, output_region: str, result: AggregationResult):
        """Store aggregation result in output region."""
        from .integration_hub import DataEntity
        
        entity = DataEntity(
            entity_id=f"agg_{result.rule_id}_{result.timestamp.timestamp()}",
            entity_type="aggregation_result",
            data={
                "rule_id": result.rule_id,
                "window_start": result.window_start.isoformat(),
                "window_end": result.window_end.isoformat(),
                "aggregated_data": result.aggregated_data,
                "group_count": result.group_count,
                "record_count": result.record_count
            },
            metadata=result.metadata
        )
        
        await self.integration_hub.put_entity(output_region, entity)
        
    def register_custom_aggregator(self, name: str, func: Callable):
        """Register a custom aggregation function."""
        self.custom_aggregators[name] = func
        logger.info(f"Registered custom aggregator: {name}")
        
    def _register_default_aggregators(self):
        """Register default custom aggregators."""
        # Example: Moving average aggregator
        async def moving_average(df: pd.DataFrame, rule: AggregationRule) -> Dict[str, Any]:
            window_size = rule.metadata.get("window_size", 10)
            field = rule.aggregation_field
            
            if rule.group_by:
                result = {}
                for name, group in df.groupby(rule.group_by):
                    ma = group[field].rolling(window=window_size).mean()
                    result[str(name)] = ma.dropna().tolist()
                return result
            else:
                ma = df[field].rolling(window=window_size).mean()
                return {"moving_average": ma.dropna().tolist()}
        
        self.register_custom_aggregator("moving_average", moving_average)
        
        # Example: Weighted average aggregator
        async def weighted_average(df: pd.DataFrame, rule: AggregationRule) -> Dict[str, Any]:
            value_field = rule.aggregation_field
            weight_field = rule.metadata.get("weight_field", "weight")
            
            if weight_field not in df.columns:
                raise ValueError(f"Weight field '{weight_field}' not found")
            
            if rule.group_by:
                result = {}
                for name, group in df.groupby(rule.group_by):
                    weighted_avg = np.average(group[value_field], weights=group[weight_field])
                    result[str(name)] = weighted_avg
                return result
            else:
                weighted_avg = np.average(df[value_field], weights=df[weight_field])
                return {"weighted_average": weighted_avg}
        
        self.register_custom_aggregator("weighted_average", weighted_average)
        
    async def _process_scheduled_aggregations(self):
        """Background task to process scheduled aggregations."""
        while True:
            try:
                now = datetime.utcnow()
                
                for rule_id, rule in self.rules.items():
                    if not rule.enabled or not rule.schedule_interval_seconds:
                        continue
                    
                    # Check if it's time to run
                    last_run = rule.metadata.get("last_run")
                    if last_run:
                        last_run_time = datetime.fromisoformat(last_run)
                        if (now - last_run_time).seconds < rule.schedule_interval_seconds:
                            continue
                    
                    # Execute aggregation
                    await self.execute_aggregation(rule_id)
                    
                    # Update last run time
                    rule.metadata["last_run"] = now.isoformat()
                
                # Sleep for 30 seconds
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduled aggregations: {e}")
                await asyncio.sleep(30)
                
    async def _cleanup_old_states(self):
        """Background task to cleanup old window states."""
        while True:
            try:
                # Clean up old window states
                cutoff = datetime.utcnow() - timedelta(hours=24)
                
                for rule_id, state in list(self.window_states.items()):
                    last_window = state.get("last_window")
                    if last_window and last_window < cutoff:
                        # Remove old state
                        del self.window_states[rule_id]
                        logger.debug(f"Cleaned up window state for rule {rule_id}")
                
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(3600)
                
    async def _handle_aggregation_trigger(self, event_data: Dict[str, Any]):
        """Handle aggregation trigger event."""
        try:
            rule_id = event_data.get("rule_id")
            if rule_id:
                await self.execute_aggregation(rule_id, force=True)
        except Exception as e:
            logger.error(f"Error handling aggregation trigger: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregator statistics."""
        return {
            "total_rules": len(self.rules),
            "enabled_rules": len([r for r in self.rules.values() if r.enabled]),
            "scheduled_rules": len([r for r in self.rules.values() if r.schedule_interval_seconds]),
            "custom_aggregators": len(self.custom_aggregators),
            "metrics": dict(self.metrics)
        } 