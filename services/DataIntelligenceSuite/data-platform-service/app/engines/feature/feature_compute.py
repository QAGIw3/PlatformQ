"""
Feature Compute for feature engineering and transformations.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import pandas as pd
import numpy as np
from collections import defaultdict
import inspect

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ComputeEngine(str, Enum):
    """Compute engines for feature processing."""
    PANDAS = "pandas"
    SPARK = "spark"
    FLINK = "flink"
    PYTHON = "python"


@dataclass
class TransformFunction:
    """Feature transformation function."""
    name: str
    func: Callable
    input_features: List[str]
    output_feature: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    
    def apply(self, df: pd.DataFrame) -> pd.Series:
        """Apply transformation to dataframe."""
        # Get input columns
        inputs = df[self.input_features]
        
        # Apply function
        if len(self.input_features) == 1:
            return inputs.iloc[:, 0].apply(lambda x: self.func(x, **self.parameters))
        else:
            return inputs.apply(lambda row: self.func(*row, **self.parameters), axis=1)


@dataclass
class AggregationFunction:
    """Feature aggregation function."""
    name: str
    func: Union[str, Callable]  # 'mean', 'sum', etc. or custom function
    window_size: Optional[timedelta] = None
    group_by: Optional[List[str]] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def apply(self, df: pd.DataFrame, feature_name: str) -> pd.DataFrame:
        """Apply aggregation to dataframe."""
        if self.group_by:
            grouped = df.groupby(self.group_by)
            
            if isinstance(self.func, str):
                result = grouped[feature_name].agg(self.func)
            else:
                result = grouped[feature_name].apply(self.func, **self.parameters)
                
            return result.reset_index()
        else:
            # Global aggregation
            if isinstance(self.func, str):
                value = df[feature_name].agg(self.func)
            else:
                value = self.func(df[feature_name], **self.parameters)
                
            return pd.DataFrame([{feature_name: value}])


@dataclass
class FeaturePipeline:
    """Pipeline of feature transformations."""
    name: str
    steps: List[Union[TransformFunction, AggregationFunction]]
    input_features: List[str]
    output_features: List[str]
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    async def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute pipeline on dataframe."""
        result = df.copy()
        
        for step in self.steps:
            if isinstance(step, TransformFunction):
                result[step.output_feature] = step.apply(result)
            elif isinstance(step, AggregationFunction):
                # Handle aggregation
                agg_result = step.apply(result, step.name)
                # Merge back if needed
                if step.group_by:
                    result = result.merge(agg_result, on=step.group_by, how='left')
                else:
                    # Broadcast aggregated value
                    for col in agg_result.columns:
                        if col not in step.group_by:
                            result[col] = agg_result[col].iloc[0]
        
        return result


class FeatureCompute:
    """
    Feature computation engine for transformations and engineering.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        compute_engine: ComputeEngine = ComputeEngine.PANDAS
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.compute_engine = compute_engine
        
        # Registered functions and pipelines
        self.transform_functions: Dict[str, TransformFunction] = {}
        self.aggregation_functions: Dict[str, AggregationFunction] = {}
        self.pipelines: Dict[str, FeaturePipeline] = {}
        
        # Compute statistics
        self.compute_stats = defaultdict(lambda: defaultdict(int))
        
        # Background tasks
        self._compute_task: Optional[asyncio.Task] = None
        
        # Register built-in functions
        self._register_builtin_functions()
        
        logger.info(f"Feature Compute initialized with engine: {compute_engine}")
        
    async def initialize(self):
        """Initialize feature compute."""
        # Subscribe to events
        await self.event_bus.subscribe("compute.request", self._handle_compute_request)
        
        # Start background compute task
        self._compute_task = asyncio.create_task(self._process_compute_queue())
        
        logger.info("Feature Compute ready")
        
    async def cleanup(self):
        """Cleanup feature compute resources."""
        # Cancel background tasks
        if self._compute_task:
            self._compute_task.cancel()
        
        logger.info("Feature Compute cleaned up")
        
    def register_transform(
        self,
        name: str,
        func: Callable,
        input_features: List[str],
        output_feature: str,
        parameters: Optional[Dict[str, Any]] = None,
        description: str = ""
    ):
        """Register a transformation function."""
        transform = TransformFunction(
            name=name,
            func=func,
            input_features=input_features,
            output_feature=output_feature,
            parameters=parameters or {},
            description=description
        )
        
        self.transform_functions[name] = transform
        logger.info(f"Registered transform function: {name}")
        
    def register_aggregation(
        self,
        name: str,
        func: Union[str, Callable],
        window_size: Optional[timedelta] = None,
        group_by: Optional[List[str]] = None,
        parameters: Optional[Dict[str, Any]] = None
    ):
        """Register an aggregation function."""
        aggregation = AggregationFunction(
            name=name,
            func=func,
            window_size=window_size,
            group_by=group_by,
            parameters=parameters or {}
        )
        
        self.aggregation_functions[name] = aggregation
        logger.info(f"Registered aggregation function: {name}")
        
    def create_pipeline(
        self,
        name: str,
        steps: List[str],
        description: str = ""
    ) -> FeaturePipeline:
        """Create a feature pipeline from registered functions."""
        pipeline_steps = []
        input_features = set()
        output_features = set()
        
        for step_name in steps:
            if step_name in self.transform_functions:
                step = self.transform_functions[step_name]
                pipeline_steps.append(step)
                input_features.update(step.input_features)
                output_features.add(step.output_feature)
            elif step_name in self.aggregation_functions:
                step = self.aggregation_functions[step_name]
                pipeline_steps.append(step)
            else:
                raise ValueError(f"Unknown function: {step_name}")
        
        pipeline = FeaturePipeline(
            name=name,
            steps=pipeline_steps,
            input_features=list(input_features),
            output_features=list(output_features),
            description=description
        )
        
        self.pipelines[name] = pipeline
        logger.info(f"Created pipeline: {name} with {len(steps)} steps")
        
        return pipeline
        
    async def compute_features(
        self,
        df: pd.DataFrame,
        transforms: List[str],
        output_format: str = "dataframe"
    ) -> Union[pd.DataFrame, Dict[str, np.ndarray]]:
        """Compute features using specified transforms."""
        start_time = datetime.utcnow()
        result = df.copy()
        
        try:
            # Apply transforms
            for transform_name in transforms:
                if transform_name in self.transform_functions:
                    transform = self.transform_functions[transform_name]
                    result[transform.output_feature] = transform.apply(result)
                    
                    # Update statistics
                    self.compute_stats[transform_name]["executions"] += 1
                    
                elif transform_name in self.pipelines:
                    pipeline = self.pipelines[transform_name]
                    result = await pipeline.execute(result)
                    
                    # Update statistics
                    self.compute_stats[transform_name]["executions"] += 1
                    
                else:
                    logger.warning(f"Unknown transform: {transform_name}")
            
            # Convert output format
            if output_format == "dict":
                output = result.to_dict("series")
                output = {k: v.values for k, v in output.items()}
            else:
                output = result
            
            # Update statistics
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self.compute_stats["_global"]["total_computations"] += 1
            self.compute_stats["_global"]["avg_latency"] = \
                (self.compute_stats["_global"].get("avg_latency", 0) * 
                 (self.compute_stats["_global"]["total_computations"] - 1) + latency) / \
                self.compute_stats["_global"]["total_computations"]
            
            return output
            
        except Exception as e:
            logger.error(f"Error computing features: {e}")
            self.compute_stats["_global"]["errors"] += 1
            raise
            
    async def compute_streaming_features(
        self,
        stream_data: Dict[str, Any],
        transforms: List[str]
    ) -> Dict[str, Any]:
        """Compute features on streaming data."""
        # Convert to single-row DataFrame
        df = pd.DataFrame([stream_data])
        
        # Compute features
        result_df = await self.compute_features(df, transforms)
        
        # Convert back to dict
        if isinstance(result_df, pd.DataFrame):
            return result_df.iloc[0].to_dict()
        else:
            return {k: v[0] for k, v in result_df.items()}
            
    async def validate_pipeline(
        self,
        pipeline_name: str,
        sample_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """Validate a pipeline with sample data."""
        pipeline = self.pipelines.get(pipeline_name)
        if not pipeline:
            return {"valid": False, "error": f"Pipeline {pipeline_name} not found"}
        
        try:
            # Check input features
            missing_features = set(pipeline.input_features) - set(sample_data.columns)
            if missing_features:
                return {
                    "valid": False,
                    "error": f"Missing input features: {missing_features}"
                }
            
            # Execute pipeline
            result = await pipeline.execute(sample_data)
            
            # Check output features
            missing_outputs = set(pipeline.output_features) - set(result.columns)
            if missing_outputs:
                return {
                    "valid": False,
                    "error": f"Pipeline did not produce expected outputs: {missing_outputs}"
                }
            
            return {
                "valid": True,
                "input_shape": sample_data.shape,
                "output_shape": result.shape,
                "output_features": list(result.columns)
            }
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }
            
    def _register_builtin_functions(self):
        """Register built-in transformation functions."""
        # Numeric transformations
        self.register_transform(
            "normalize",
            lambda x, mean=0, std=1: (x - mean) / std,
            ["value"],
            "normalized_value",
            description="Normalize numeric values"
        )
        
        self.register_transform(
            "log_transform",
            lambda x: np.log1p(x) if x >= 0 else 0,
            ["value"],
            "log_value",
            description="Log transformation"
        )
        
        self.register_transform(
            "square",
            lambda x: x ** 2,
            ["value"],
            "squared_value",
            description="Square transformation"
        )
        
        # String transformations
        self.register_transform(
            "lowercase",
            lambda x: x.lower() if isinstance(x, str) else x,
            ["text"],
            "lowercase_text",
            description="Convert to lowercase"
        )
        
        self.register_transform(
            "string_length",
            lambda x: len(x) if isinstance(x, str) else 0,
            ["text"],
            "text_length",
            description="Calculate string length"
        )
        
        # Date transformations
        self.register_transform(
            "extract_hour",
            lambda x: pd.to_datetime(x).hour if pd.notna(x) else None,
            ["timestamp"],
            "hour",
            description="Extract hour from timestamp"
        )
        
        self.register_transform(
            "extract_dayofweek",
            lambda x: pd.to_datetime(x).dayofweek if pd.notna(x) else None,
            ["timestamp"],
            "dayofweek",
            description="Extract day of week from timestamp"
        )
        
        # Composite transformations
        self.register_transform(
            "ratio",
            lambda x, y: x / y if y != 0 else 0,
            ["numerator", "denominator"],
            "ratio",
            description="Calculate ratio"
        )
        
        self.register_transform(
            "difference",
            lambda x, y: x - y,
            ["value1", "value2"],
            "difference",
            description="Calculate difference"
        )
        
        # Register common aggregations
        for agg_name in ["mean", "sum", "min", "max", "std", "count"]:
            self.register_aggregation(
                f"{agg_name}_agg",
                agg_name,
                description=f"Calculate {agg_name}"
            )
            
    async def _process_compute_queue(self):
        """Process compute requests from queue."""
        while True:
            try:
                # This would process queued compute requests
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing compute queue: {e}")
                await asyncio.sleep(1)
                
    async def _handle_compute_request(self, event_data: Dict[str, Any]):
        """Handle compute request event."""
        try:
            data = pd.DataFrame(event_data.get("data", []))
            transforms = event_data.get("transforms", [])
            
            if not data.empty and transforms:
                result = await self.compute_features(data, transforms)
                
                # Publish result
                await self.event_bus.publish("compute.complete", {
                    "request_id": event_data.get("request_id"),
                    "result": result.to_dict("records") if isinstance(result, pd.DataFrame) else result
                })
                
        except Exception as e:
            logger.error(f"Error handling compute request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get compute statistics."""
        return {
            "registered_transforms": len(self.transform_functions),
            "registered_aggregations": len(self.aggregation_functions),
            "registered_pipelines": len(self.pipelines),
            "compute_stats": dict(self.compute_stats)
        } 