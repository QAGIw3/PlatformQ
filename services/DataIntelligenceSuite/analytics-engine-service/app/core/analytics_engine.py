"""
Analytics Engine Core

Uses the algorithm factory pattern for dynamic algorithm selection.
"""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import pandas as pd

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig
from data_intelligence_common.core.algorithms import BaseAlgorithm, AlgorithmConfig
from data_intelligence_common.monitoring import StructuredLogger

from ..algorithms import algorithm_factory

logger = StructuredLogger.get_logger(__name__)


class AnalyticsMode(str, Enum):
    """Analytics execution modes"""
    BATCH = "batch"
    REALTIME = "realtime"
    STREAM = "stream"
    HYBRID = "hybrid"


@dataclass
class AnalyticsEngineConfig(UnifiedServiceConfig):
    """Configuration for analytics engine"""
    # Engine settings
    default_mode: AnalyticsMode = AnalyticsMode.BATCH
    enable_auto_optimization: bool = True
    
    # Algorithm settings
    algorithm_timeout: timedelta = timedelta(minutes=30)
    max_concurrent_algorithms: int = 10
    
    # Caching
    enable_result_caching: bool = True
    cache_ttl: timedelta = timedelta(hours=1)
    
    # Resource limits
    max_memory_per_algorithm: int = 4 * 1024 * 1024 * 1024  # 4GB
    max_cpu_per_algorithm: float = 2.0


class AnalyticsEngine(DataIntelligenceBaseService):
    """
    Analytics engine with dynamic algorithm management.
    
    Features:
    - Dynamic algorithm loading via factory pattern
    - Multi-mode execution (batch, realtime, stream)
    - Automatic resource management
    - Result caching and optimization
    """
    
    def __init__(self, config: AnalyticsEngineConfig):
        super().__init__(config)
        self.config = config
        
        # Algorithm management
        self._active_algorithms: Dict[str, BaseAlgorithm] = {}
        self._algorithm_tasks: Dict[str, asyncio.Task] = {}
        
    async def _initialize_internal(self):
        """Initialize analytics engine components"""
        await super()._initialize_internal()
        
        # Register health checks
        self.register_health_check(
            "algorithm_factory",
            self._check_algorithm_factory_health,
            critical=True
        )
        
        # Log available algorithms
        available_algorithms = algorithm_factory.list_registered()
        logger.info(
            "Analytics engine initialized",
            available_algorithms=available_algorithms,
            algorithm_count=len(available_algorithms)
        )
        
    async def create_algorithm(
        self,
        algorithm_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> BaseAlgorithm:
        """
        Create an algorithm instance using the factory.
        
        Args:
            algorithm_name: Name of the algorithm to create
            config: Algorithm configuration
            
        Returns:
            Algorithm instance
        """
        try:
            # Get algorithm class from factory
            algorithm_class = algorithm_factory.create(algorithm_name)
            
            # Create config if not provided
            if config is None:
                config = {}
                
            # Add default config values
            config.setdefault("name", f"{algorithm_name}_{datetime.utcnow().timestamp()}")
            config.setdefault("timeout", self.config.algorithm_timeout.total_seconds())
            
            # Create algorithm config
            if hasattr(algorithm_class, "__config_class__"):
                config_class = algorithm_class.__config_class__
                algorithm_config = config_class(**config)
            else:
                algorithm_config = AlgorithmConfig(**config)
                
            # Create algorithm instance
            algorithm = algorithm_class(algorithm_config)
            
            # Initialize algorithm
            await algorithm.initialize()
            
            # Track active algorithm
            self._active_algorithms[algorithm_config.name] = algorithm
            
            logger.info(
                "Algorithm created",
                algorithm_name=algorithm_name,
                algorithm_id=algorithm_config.name
            )
            
            return algorithm
            
        except Exception as e:
            logger.error(f"Failed to create algorithm {algorithm_name}: {e}")
            raise
            
    async def execute_algorithm(
        self,
        algorithm_name: str,
        data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
        operation: str = "predict",
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute an algorithm on data.
        
        Args:
            algorithm_name: Name of the algorithm to use
            data: Input data
            operation: Operation to perform (predict, train, update)
            config: Algorithm configuration
            **kwargs: Additional arguments for the algorithm
            
        Returns:
            Execution results
        """
        start_time = datetime.utcnow()
        
        # Create or get algorithm
        algorithm_id = f"{algorithm_name}_{hash(str(config))}"
        
        if algorithm_id not in self._active_algorithms:
            algorithm = await self.create_algorithm(algorithm_name, config)
        else:
            algorithm = self._active_algorithms[algorithm_id]
            
        try:
            # Convert data to DataFrame if needed
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                df = data
                
            # Execute operation
            if operation == "train":
                result = await algorithm.train(df, **kwargs)
            elif operation == "predict":
                result = await algorithm.predict(df, **kwargs)
            elif operation == "update":
                result = await algorithm.update(df, **kwargs)
            else:
                raise ValueError(f"Unknown operation: {operation}")
                
            # Add execution metadata
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            final_result = {
                "algorithm": algorithm_name,
                "operation": operation,
                "execution_time": execution_time,
                "result": result,
                "metadata": {
                    "algorithm_id": algorithm_id,
                    "data_shape": df.shape if hasattr(df, 'shape') else len(df),
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
            # Record metrics
            self.record_operation(f"algorithm_{operation}", {
                "algorithm": algorithm_name,
                "duration": execution_time,
                "success": True
            })
            
            # Cache result if enabled
            if self.config.enable_result_caching and operation == "predict":
                cache_key = f"{algorithm_name}:{hash(df.to_string())}"
                await self.cache_result(cache_key, final_result)
                
            return final_result
            
        except Exception as e:
            logger.error(f"Algorithm execution failed: {e}")
            
            # Record failure metric
            self.record_operation(f"algorithm_{operation}", {
                "algorithm": algorithm_name,
                "duration": (datetime.utcnow() - start_time).total_seconds(),
                "success": False,
                "error": str(e)
            })
            
            raise
            
    async def execute_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        data: Any,
        mode: Optional[AnalyticsMode] = None
    ) -> Dict[str, Any]:
        """
        Execute a pipeline of algorithms.
        
        Args:
            pipeline: List of algorithm configurations
            data: Input data
            mode: Execution mode
            
        Returns:
            Pipeline execution results
        """
        mode = mode or self.config.default_mode
        results = []
        current_data = data
        
        for stage in pipeline:
            algorithm_name = stage.get("algorithm")
            operation = stage.get("operation", "predict")
            config = stage.get("config", {})
            
            # Execute stage
            stage_result = await self.execute_algorithm(
                algorithm_name=algorithm_name,
                data=current_data,
                operation=operation,
                config=config,
                **stage.get("params", {})
            )
            
            results.append(stage_result)
            
            # Use output as input for next stage
            if "result" in stage_result and isinstance(stage_result["result"], (list, pd.DataFrame)):
                current_data = stage_result["result"]
                
        return {
            "pipeline": [s.get("algorithm") for s in pipeline],
            "mode": mode.value,
            "stages": len(pipeline),
            "results": results,
            "final_output": current_data
        }
        
    async def list_algorithms(self) -> List[Dict[str, Any]]:
        """List all available algorithms"""
        algorithms = []
        
        for name in algorithm_factory.list_registered():
            try:
                # Get algorithm class
                algorithm_class = algorithm_factory._registry.get(name)
                
                # Extract metadata
                algorithms.append({
                    "name": name,
                    "description": algorithm_class.__doc__.strip() if algorithm_class.__doc__ else "",
                    "active_instances": sum(
                        1 for aid in self._active_algorithms 
                        if aid.startswith(name)
                    )
                })
                
            except Exception as e:
                logger.error(f"Failed to get info for algorithm {name}: {e}")
                
        return algorithms
        
    async def get_algorithm_status(self, algorithm_id: str) -> Dict[str, Any]:
        """Get status of a specific algorithm instance"""
        algorithm = self._active_algorithms.get(algorithm_id)
        
        if not algorithm:
            raise ValueError(f"Algorithm {algorithm_id} not found")
            
        return {
            "algorithm_id": algorithm_id,
            "status": "active",
            "metrics": algorithm.get_metrics() if hasattr(algorithm, 'get_metrics') else {},
            "metadata": algorithm.get_metadata() if hasattr(algorithm, 'get_metadata') else {}
        }
        
    async def cleanup_algorithms(self, inactive_threshold: timedelta = timedelta(hours=1)):
        """Clean up inactive algorithm instances"""
        current_time = datetime.utcnow()
        to_remove = []
        
        for algorithm_id, algorithm in self._active_algorithms.items():
            # Check last activity
            if hasattr(algorithm, 'last_activity'):
                if current_time - algorithm.last_activity > inactive_threshold:
                    to_remove.append(algorithm_id)
                    
        # Remove inactive algorithms
        for algorithm_id in to_remove:
            algorithm = self._active_algorithms.pop(algorithm_id)
            if hasattr(algorithm, 'cleanup'):
                await algorithm.cleanup()
                
            logger.info(f"Cleaned up inactive algorithm: {algorithm_id}")
            
        return len(to_remove)
        
    async def _check_algorithm_factory_health(self) -> Dict[str, Any]:
        """Check algorithm factory health"""
        try:
            registered = algorithm_factory.list_registered()
            
            return {
                "healthy": len(registered) > 0,
                "registered_algorithms": len(registered),
                "active_instances": len(self._active_algorithms)
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            }
            
    async def _shutdown_internal(self):
        """Cleanup on shutdown"""
        # Clean up all active algorithms
        for algorithm_id, algorithm in self._active_algorithms.items():
            if hasattr(algorithm, 'cleanup'):
                await algorithm.cleanup()
                
        self._active_algorithms.clear()
        
        await super()._shutdown_internal()


# Export main class
__all__ = ['AnalyticsEngine', 'AnalyticsEngineConfig', 'AnalyticsMode'] 