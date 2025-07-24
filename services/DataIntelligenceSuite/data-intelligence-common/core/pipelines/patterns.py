"""
Pipeline Patterns

Common patterns and templates for pipeline construction.
"""

from typing import Any, Dict, List, Optional, Callable, Union, TypeVar, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from datetime import datetime

from .base import Pipeline, PipelineStage, PipelineConfig
from .builder import PipelineBuilder
from ..processing import BaseProcessor
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class PipelinePattern(str, Enum):
    """Common pipeline patterns"""
    ETL = "etl"
    MAP_REDUCE = "map_reduce"
    SCATTER_GATHER = "scatter_gather"
    FORK_JOIN = "fork_join"
    PIPELINE_BREAKER = "pipeline_breaker"
    CONDITIONAL_FLOW = "conditional_flow"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    CIRCUIT_BREAKER = "circuit_breaker"
    BULKHEAD = "bulkhead"
    SAGA = "saga"


class ETLPipeline:
    """Extract-Transform-Load pipeline pattern"""
    
    @staticmethod
    def create(
        name: str,
        extractor: Callable,
        transformer: Callable,
        loader: Callable,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create ETL pipeline"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        return (
            builder
            .add_stage("extract", extractor)
            .add_stage("transform", transformer, depends_on=["extract"])
            .add_stage("load", loader, depends_on=["transform"])
            .build()
        )


class MapReducePipeline:
    """Map-Reduce pipeline pattern"""
    
    @staticmethod
    def create(
        name: str,
        mapper: Callable,
        reducer: Callable,
        partitioner: Optional[Callable] = None,
        num_partitions: int = 4,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create Map-Reduce pipeline"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Add partitioning stage if provided
        if partitioner:
            builder.add_stage("partition", partitioner)
            map_deps = ["partition"]
        else:
            map_deps = []
            
        # Add map stages
        map_stages = []
        for i in range(num_partitions):
            stage_name = f"map_{i}"
            builder.add_stage(
                stage_name,
                lambda data, idx=i: mapper(data[idx] if isinstance(data, list) else data),
                depends_on=map_deps
            )
            map_stages.append(stage_name)
            
        # Add reduce stage
        builder.add_stage(
            "reduce",
            reducer,
            depends_on=map_stages
        )
        
        return builder.build()


class ScatterGatherPipeline:
    """Scatter-Gather pipeline pattern"""
    
    @staticmethod
    def create(
        name: str,
        scatter_func: Callable,
        process_funcs: List[Callable],
        gather_func: Callable,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create Scatter-Gather pipeline"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Scatter stage
        builder.add_stage("scatter", scatter_func)
        
        # Process stages (parallel)
        process_stages = []
        for i, func in enumerate(process_funcs):
            stage_name = f"process_{i}"
            builder.add_stage(
                stage_name,
                func,
                depends_on=["scatter"]
            )
            process_stages.append(stage_name)
            
        # Gather stage
        builder.add_stage(
            "gather",
            gather_func,
            depends_on=process_stages
        )
        
        return builder.build()


class ForkJoinPipeline:
    """Fork-Join pipeline pattern"""
    
    @staticmethod
    def create(
        name: str,
        fork_condition: Callable[[Any], bool],
        main_branch: List[Tuple[str, Callable]],
        fork_branch: List[Tuple[str, Callable]],
        join_func: Callable,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create Fork-Join pipeline"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Fork decision stage
        builder.add_stage("fork_decision", fork_condition)
        
        # Main branch
        prev_stage = "fork_decision"
        for stage_name, func in main_branch:
            builder.add_stage(
                f"main_{stage_name}",
                func,
                depends_on=[prev_stage],
                condition=lambda result: not result  # Main branch when condition is false
            )
            prev_stage = f"main_{stage_name}"
            
        # Fork branch
        fork_prev = "fork_decision"
        for stage_name, func in fork_branch:
            builder.add_stage(
                f"fork_{stage_name}",
                func,
                depends_on=[fork_prev],
                condition=lambda result: result  # Fork branch when condition is true
            )
            fork_prev = f"fork_{stage_name}"
            
        # Join stage
        builder.add_stage(
            "join",
            join_func,
            depends_on=[prev_stage, fork_prev]
        )
        
        return builder.build()


class RetryPipeline:
    """Pipeline with retry and exponential backoff"""
    
    @staticmethod
    def create(
        name: str,
        stages: List[Tuple[str, Callable]],
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create pipeline with retry logic"""
        if not config:
            config = PipelineConfig(name=name)
            
        builder = PipelineBuilder(config)
        
        prev_stage = None
        for stage_name, func in stages:
            retry_config = {
                "max_retries": max_retries,
                "retry_delay": 1,  # seconds
                "exponential_backoff": True,
                "backoff_factor": backoff_factor
            }
            
            builder.add_stage(
                stage_name,
                func,
                depends_on=[prev_stage] if prev_stage else [],
                retry_config=retry_config
            )
            prev_stage = stage_name
            
        return builder.build()


class CircuitBreakerPipeline:
    """Pipeline with circuit breaker pattern"""
    
    @staticmethod
    def create(
        name: str,
        stages: List[Tuple[str, Callable]],
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        fallback_func: Optional[Callable] = None,
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create pipeline with circuit breaker"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Add circuit breaker wrapper
        def circuit_breaker_wrapper(func: Callable) -> Callable:
            circuit_state = {"failures": 0, "last_failure": None, "is_open": False}
            
            async def wrapped(data):
                # Check if circuit is open
                if circuit_state["is_open"]:
                    if circuit_state["last_failure"]:
                        elapsed = (datetime.utcnow() - circuit_state["last_failure"]).seconds
                        if elapsed < recovery_timeout:
                            if fallback_func:
                                return await fallback_func(data)
                            raise Exception("Circuit breaker is open")
                        else:
                            # Try to close circuit
                            circuit_state["is_open"] = False
                            circuit_state["failures"] = 0
                            
                try:
                    result = await func(data) if asyncio.iscoroutinefunction(func) else func(data)
                    # Reset on success
                    circuit_state["failures"] = 0
                    return result
                except Exception as e:
                    circuit_state["failures"] += 1
                    circuit_state["last_failure"] = datetime.utcnow()
                    
                    if circuit_state["failures"] >= failure_threshold:
                        circuit_state["is_open"] = True
                        logger.warning(f"Circuit breaker opened after {failure_threshold} failures")
                        
                    raise
                    
            return wrapped
            
        # Add stages with circuit breaker
        prev_stage = None
        for stage_name, func in stages:
            builder.add_stage(
                stage_name,
                circuit_breaker_wrapper(func),
                depends_on=[prev_stage] if prev_stage else []
            )
            prev_stage = stage_name
            
        return builder.build()


class BulkheadPipeline:
    """Pipeline with bulkhead isolation pattern"""
    
    @staticmethod
    def create(
        name: str,
        stages: List[Tuple[str, Callable, int]],  # (name, func, max_concurrent)
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create pipeline with bulkhead isolation"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Add bulkhead wrapper
        def bulkhead_wrapper(func: Callable, max_concurrent: int) -> Callable:
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def wrapped(data):
                async with semaphore:
                    if asyncio.iscoroutinefunction(func):
                        return await func(data)
                    else:
                        loop = asyncio.get_event_loop()
                        return await loop.run_in_executor(None, func, data)
                        
            return wrapped
            
        # Add stages with bulkhead
        prev_stage = None
        for stage_name, func, max_concurrent in stages:
            builder.add_stage(
                stage_name,
                bulkhead_wrapper(func, max_concurrent),
                depends_on=[prev_stage] if prev_stage else []
            )
            prev_stage = stage_name
            
        return builder.build()


class SagaPipeline:
    """Pipeline implementing Saga pattern for distributed transactions"""
    
    @staticmethod
    def create(
        name: str,
        transactions: List[Tuple[str, Callable, Callable]],  # (name, action, compensate)
        config: Optional[PipelineConfig] = None
    ) -> Pipeline:
        """Create Saga pipeline with compensation"""
        builder = PipelineBuilder(config or PipelineConfig(name=name))
        
        # Track completed transactions for compensation
        completed = []
        
        async def saga_wrapper(
            action: Callable,
            compensate: Callable,
            stage_name: str,
            data: Any
        ):
            try:
                result = await action(data) if asyncio.iscoroutinefunction(action) else action(data)
                completed.append((stage_name, compensate, data))
                return result
            except Exception as e:
                # Compensate in reverse order
                logger.error(f"Saga failed at {stage_name}, compensating...")
                for comp_name, comp_func, comp_data in reversed(completed):
                    try:
                        if asyncio.iscoroutinefunction(comp_func):
                            await comp_func(comp_data)
                        else:
                            comp_func(comp_data)
                        logger.info(f"Compensated {comp_name}")
                    except Exception as comp_error:
                        logger.error(f"Compensation failed for {comp_name}: {comp_error}")
                raise
                
        # Add transaction stages
        prev_stage = None
        for stage_name, action, compensate in transactions:
            builder.add_stage(
                stage_name,
                lambda data, a=action, c=compensate, n=stage_name: 
                    saga_wrapper(a, c, n, data),
                depends_on=[prev_stage] if prev_stage else []
            )
            prev_stage = stage_name
            
        return builder.build()


class PipelineTemplates:
    """Pre-built pipeline templates"""
    
    @staticmethod
    def data_validation_pipeline(
        validators: List[Callable],
        name: str = "data_validation"
    ) -> Pipeline:
        """Create data validation pipeline"""
        stages = [
            ("validate_schema", validators[0]) if len(validators) > 0 else None,
            ("validate_business_rules", validators[1]) if len(validators) > 1 else None,
            ("validate_consistency", validators[2]) if len(validators) > 2 else None,
        ]
        
        stages = [(n, f) for n, f in stages if f is not None]
        
        return RetryPipeline.create(
            name=name,
            stages=stages,
            max_retries=2
        )
        
    @staticmethod
    def ml_training_pipeline(
        name: str = "ml_training"
    ) -> Pipeline:
        """Create ML training pipeline template"""
        
        async def load_data(params):
            # Placeholder for data loading
            return {"data": "loaded"}
            
        async def preprocess(data):
            # Placeholder for preprocessing
            return {"preprocessed": data}
            
        async def train_model(data):
            # Placeholder for training
            return {"model": "trained"}
            
        async def evaluate(model):
            # Placeholder for evaluation
            return {"metrics": "evaluated"}
            
        async def save_model(result):
            # Placeholder for saving
            return {"saved": True}
            
        return ETLPipeline.create(
            name=name,
            extractor=load_data,
            transformer=lambda d: preprocess(d["data"]),
            loader=save_model
        )
        
    @staticmethod
    def data_quality_pipeline(
        quality_checks: List[Callable],
        name: str = "data_quality"
    ) -> Pipeline:
        """Create data quality pipeline"""
        
        # Create scatter-gather for parallel quality checks
        def scatter(data):
            # Distribute data to all quality checks
            return [data] * len(quality_checks)
            
        def gather(results):
            # Aggregate quality check results
            return {
                "passed": all(r.get("passed", False) for r in results),
                "details": results
            }
            
        return ScatterGatherPipeline.create(
            name=name,
            scatter_func=scatter,
            process_funcs=quality_checks,
            gather_func=gather
        ) 