"""
ML inference engine.

Provides unified interface for model inference with batching, caching, and monitoring.
"""

import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict
import time

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger, MetricsCollector
from ...models.ml_models import (
    PredictionRequest,
    PredictionResult,
    ModelStage,
    ModelType
)

logger = StructuredLogger.get_logger(__name__)


class InferenceMode(str, Enum):
    """Inference execution modes"""
    REALTIME = "realtime"
    BATCH = "batch"
    STREAMING = "streaming"
    ASYNC = "async"


class ModelBackend(str, Enum):
    """Model serving backends"""
    SKLEARN = "sklearn"
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    ONNX = "onnx"
    CUSTOM = "custom"


class LoadBalancingStrategy(str, Enum):
    """Load balancing strategies"""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    RANDOM = "random"
    WEIGHTED = "weighted"
    STICKY = "sticky"


@dataclass
class ModelEndpoint:
    """Model serving endpoint"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: str = ""
    url: str = ""
    backend: ModelBackend = ModelBackend.CUSTOM
    
    # Health and load
    is_healthy: bool = True
    current_load: int = 0
    max_load: int = 100
    
    # Performance metrics
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    
    # Metadata
    region: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    last_health_check: Optional[datetime] = None
    
    def get_load_factor(self) -> float:
        """Get current load factor (0-1)"""
        return self.current_load / self.max_load if self.max_load > 0 else 1.0


@dataclass
class BatchRequest:
    """Batch inference request"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    requests: List[PredictionRequest] = field(default_factory=list)
    model_id: str = ""
    version: Optional[str] = None
    
    # Batch configuration
    batch_size: int = 0
    timeout_ms: Optional[int] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    priority: int = 0
    
    def add_request(self, request: PredictionRequest):
        """Add request to batch"""
        self.requests.append(request)
        self.batch_size = len(self.requests)


@dataclass
class InferenceMetrics:
    """Inference performance metrics"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    
    # Latency metrics (in ms)
    min_latency: float = float('inf')
    max_latency: float = 0.0
    avg_latency: float = 0.0
    p50_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    
    # Throughput
    requests_per_second: float = 0.0
    
    # Model metrics
    model_load_time_ms: Optional[float] = None
    preprocessing_time_ms: float = 0.0
    inference_time_ms: float = 0.0
    postprocessing_time_ms: float = 0.0
    
    # Resource usage
    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    gpu_usage_percent: Optional[float] = None
    
    def update_latency(self, latency_ms: float):
        """Update latency metrics"""
        self.min_latency = min(self.min_latency, latency_ms)
        self.max_latency = max(self.max_latency, latency_ms)
        # Simple moving average (would use proper percentile tracking in production)
        self.avg_latency = (
            (self.avg_latency * self.total_requests + latency_ms) /
            (self.total_requests + 1)
        )


class BasePredictor(ABC):
    """Base predictor interface"""
    
    @abstractmethod
    async def predict(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        model_metadata: Dict[str, Any]
    ) -> Union[Any, List[Any]]:
        """Make predictions"""
        pass
        
    @abstractmethod
    async def load_model(self, model_path: str, config: Dict[str, Any]):
        """Load model into memory"""
        pass
        
    @abstractmethod
    async def unload_model(self):
        """Unload model from memory"""
        pass


class SklearnPredictor(BasePredictor):
    """Scikit-learn model predictor"""
    
    def __init__(self):
        self.model = None
        self.preprocessor = None
        
    async def load_model(self, model_path: str, config: Dict[str, Any]):
        """Load sklearn model"""
        import joblib
        self.model = joblib.load(model_path)
        
        # Load preprocessor if provided
        preprocessor_path = config.get("preprocessor_path")
        if preprocessor_path:
            self.preprocessor = joblib.load(preprocessor_path)
            
    async def predict(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        model_metadata: Dict[str, Any]
    ) -> Union[Any, List[Any]]:
        """Make predictions with sklearn model"""
        if self.model is None:
            raise ValueError("Model not loaded")
            
        # Convert to numpy array
        if isinstance(data, dict):
            data = [data]
            
        # Extract features
        feature_names = model_metadata.get("feature_names", [])
        if feature_names:
            X = np.array([[row.get(f) for f in feature_names] for row in data])
        else:
            X = np.array(data)
            
        # Preprocess if needed
        if self.preprocessor:
            X = self.preprocessor.transform(X)
            
        # Predict
        predictions = self.model.predict(X)
        
        # Get probabilities if available
        if hasattr(self.model, "predict_proba") and model_metadata.get("include_probabilities"):
            probabilities = self.model.predict_proba(X)
            return list(zip(predictions, probabilities))
            
        return predictions.tolist()
        
    async def unload_model(self):
        """Unload model from memory"""
        self.model = None
        self.preprocessor = None


class InferenceEngine:
    """
    Unified ML inference engine.
    
    Features:
    - Multi-backend support
    - Request batching
    - Result caching
    - Load balancing
    - Model versioning
    - Performance monitoring
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        self.metrics = metrics_collector
        
        # Model registry
        self._models: Dict[str, Dict[str, Any]] = {}  # model_id -> model_info
        self._endpoints: Dict[str, List[ModelEndpoint]] = defaultdict(list)
        
        # Predictors
        self._predictors: Dict[ModelBackend, BasePredictor] = {
            ModelBackend.SKLEARN: SklearnPredictor(),
            # Add more predictors as needed
        }
        
        # Loaded models cache
        self._loaded_models: Dict[str, BasePredictor] = {}
        
        # Batch processing
        self._batch_queues: Dict[str, List[PredictionRequest]] = defaultdict(list)
        self._batch_processors: Dict[str, asyncio.Task] = {}
        
        # Metrics
        self._model_metrics: Dict[str, InferenceMetrics] = defaultdict(InferenceMetrics)
        
        # Configuration
        self.default_batch_size = 32
        self.default_batch_timeout_ms = 100
        self.cache_ttl = 300  # 5 minutes
        
    def register_model(
        self,
        model_id: str,
        version: str,
        model_path: str,
        backend: ModelBackend,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Register model for inference"""
        model_key = f"{model_id}:{version}"
        
        self._models[model_key] = {
            "model_id": model_id,
            "version": version,
            "model_path": model_path,
            "backend": backend,
            "metadata": metadata or {},
            "registered_at": datetime.utcnow()
        }
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="inference.model.registered",
                source="inference_engine",
                data={
                    "model_id": model_id,
                    "version": version,
                    "backend": backend.value
                }
            ))
            
        logger.info(f"Registered model: {model_id} v{version}")
        
    def add_endpoint(
        self,
        model_id: str,
        version: str,
        endpoint: ModelEndpoint
    ):
        """Add model serving endpoint"""
        model_key = f"{model_id}:{version}"
        endpoint.model_id = model_id
        endpoint.version = version
        
        self._endpoints[model_key].append(endpoint)
        
        logger.info(f"Added endpoint for model {model_id} v{version}: {endpoint.url}")
        
    async def predict(
        self,
        request: PredictionRequest,
        mode: InferenceMode = InferenceMode.REALTIME
    ) -> PredictionResult:
        """Make prediction"""
        start_time = time.time()
        
        # Get model info
        model_key = f"{request.model_id}:{request.version or 'latest'}"
        model_info = self._models.get(model_key)
        
        if not model_info:
            raise ValueError(f"Model not found: {model_key}")
            
        # Check cache
        if self.cache and mode == InferenceMode.REALTIME:
            cache_key = self._get_cache_key(request)
            cached = self.cache.get(cache_key)
            if cached:
                return self._dict_to_result(cached)
                
        # Route based on mode
        if mode == InferenceMode.BATCH:
            result = await self._batch_predict(request, model_info)
        elif mode == InferenceMode.STREAMING:
            result = await self._stream_predict(request, model_info)
        else:
            result = await self._realtime_predict(request, model_info)
            
        # Update metrics
        latency_ms = (time.time() - start_time) * 1000
        self._update_metrics(model_key, latency_ms, success=True)
        
        # Cache result
        if self.cache and mode == InferenceMode.REALTIME:
            cache_key = self._get_cache_key(request)
            self.cache.set(cache_key, result.to_dict(), ttl=self.cache_ttl)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="inference.prediction.completed",
                source="inference_engine",
                data={
                    "request_id": request.id,
                    "model_id": request.model_id,
                    "latency_ms": latency_ms
                }
            ))
            
        return result
        
    async def _realtime_predict(
        self,
        request: PredictionRequest,
        model_info: Dict[str, Any]
    ) -> PredictionResult:
        """Real-time prediction"""
        # Get or load model
        model_key = f"{request.model_id}:{request.version or 'latest'}"
        predictor = await self._get_or_load_model(model_key, model_info)
        
        # Preprocess
        start_preprocess = time.time()
        processed_data = await self._preprocess(request.data, model_info)
        preprocess_time = (time.time() - start_preprocess) * 1000
        
        # Predict
        start_inference = time.time()
        predictions = await predictor.predict(processed_data, model_info["metadata"])
        inference_time = (time.time() - start_inference) * 1000
        
        # Postprocess
        start_postprocess = time.time()
        final_predictions = await self._postprocess(predictions, model_info)
        postprocess_time = (time.time() - start_postprocess) * 1000
        
        # Create result
        result = PredictionResult(
            request_id=request.id,
            model_id=request.model_id,
            version=request.version or model_info["version"],
            predictions=final_predictions,
            latency_ms=preprocess_time + inference_time + postprocess_time
        )
        
        # Add probabilities if requested
        if request.include_probabilities and isinstance(predictions, list):
            if all(isinstance(p, tuple) for p in predictions):
                result.predictions = [p[0] for p in predictions]
                result.probabilities = [p[1] for p in predictions]
                
        # Update detailed metrics
        metrics = self._model_metrics[model_key]
        metrics.preprocessing_time_ms = preprocess_time
        metrics.inference_time_ms = inference_time
        metrics.postprocessing_time_ms = postprocess_time
        
        return result
        
    async def _batch_predict(
        self,
        request: PredictionRequest,
        model_info: Dict[str, Any]
    ) -> PredictionResult:
        """Batch prediction with queuing"""
        model_key = f"{request.model_id}:{request.version or 'latest'}"
        
        # Add to batch queue
        self._batch_queues[model_key].append(request)
        
        # Start batch processor if not running
        if model_key not in self._batch_processors:
            self._batch_processors[model_key] = asyncio.create_task(
                self._process_batch(model_key, model_info)
            )
            
        # Wait for result
        # In production, would use proper async queue/future
        await asyncio.sleep(self.default_batch_timeout_ms / 1000)
        
        # For now, process immediately
        return await self._realtime_predict(request, model_info)
        
    async def _process_batch(
        self,
        model_key: str,
        model_info: Dict[str, Any]
    ):
        """Process batched requests"""
        while True:
            try:
                # Wait for batch to fill or timeout
                await asyncio.sleep(self.default_batch_timeout_ms / 1000)
                
                # Get requests
                requests = self._batch_queues[model_key][:self.default_batch_size]
                if not requests:
                    continue
                    
                # Remove from queue
                self._batch_queues[model_key] = self._batch_queues[model_key][self.default_batch_size:]
                
                # Get model
                predictor = await self._get_or_load_model(model_key, model_info)
                
                # Batch predict
                batch_data = [r.data for r in requests]
                predictions = await predictor.predict(batch_data, model_info["metadata"])
                
                # Create results
                for i, request in enumerate(requests):
                    result = PredictionResult(
                        request_id=request.id,
                        model_id=request.model_id,
                        version=request.version or model_info["version"],
                        predictions=predictions[i] if isinstance(predictions, list) else predictions
                    )
                    
                    # Store result (in production, would notify waiting coroutine)
                    if self.cache:
                        cache_key = f"batch_result:{request.id}"
                        self.cache.set(cache_key, result.to_dict(), ttl=60)
                        
            except Exception as e:
                logger.error(f"Batch processing error: {e}")
                await asyncio.sleep(1)
                
    async def _stream_predict(
        self,
        request: PredictionRequest,
        model_info: Dict[str, Any]
    ) -> PredictionResult:
        """Streaming prediction"""
        # For streaming, would integrate with streaming framework
        # For now, fall back to realtime
        return await self._realtime_predict(request, model_info)
        
    async def _get_or_load_model(
        self,
        model_key: str,
        model_info: Dict[str, Any]
    ) -> BasePredictor:
        """Get or load model predictor"""
        if model_key in self._loaded_models:
            return self._loaded_models[model_key]
            
        # Load model
        backend = model_info["backend"]
        predictor = self._predictors.get(backend)
        
        if not predictor:
            raise ValueError(f"Unsupported backend: {backend}")
            
        # Load model
        start_time = time.time()
        await predictor.load_model(
            model_info["model_path"],
            model_info.get("metadata", {})
        )
        load_time = (time.time() - start_time) * 1000
        
        # Cache loaded model
        self._loaded_models[model_key] = predictor
        
        # Update metrics
        self._model_metrics[model_key].model_load_time_ms = load_time
        
        logger.info(f"Loaded model {model_key} in {load_time:.2f}ms")
        
        return predictor
        
    async def _preprocess(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        model_info: Dict[str, Any]
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Preprocess input data"""
        # Custom preprocessing based on model metadata
        preprocessing_config = model_info.get("metadata", {}).get("preprocessing", {})
        
        if preprocessing_config:
            # Apply preprocessing steps
            # This would be implemented based on specific requirements
            pass
            
        return data
        
    async def _postprocess(
        self,
        predictions: Any,
        model_info: Dict[str, Any]
    ) -> Any:
        """Postprocess predictions"""
        # Custom postprocessing based on model metadata
        postprocessing_config = model_info.get("metadata", {}).get("postprocessing", {})
        
        if postprocessing_config:
            # Apply postprocessing steps
            # This would be implemented based on specific requirements
            pass
            
        return predictions
        
    def _get_cache_key(self, request: PredictionRequest) -> str:
        """Generate cache key for request"""
        import hashlib
        import json
        
        # Create deterministic key from request data
        key_data = {
            "model_id": request.model_id,
            "version": request.version,
            "data": request.data
        }
        
        key_str = json.dumps(key_data, sort_keys=True)
        return f"prediction:{hashlib.md5(key_str.encode()).hexdigest()}"
        
    def _update_metrics(
        self,
        model_key: str,
        latency_ms: float,
        success: bool = True
    ):
        """Update inference metrics"""
        metrics = self._model_metrics[model_key]
        
        metrics.total_requests += 1
        if success:
            metrics.successful_requests += 1
        else:
            metrics.failed_requests += 1
            
        metrics.update_latency(latency_ms)
        
        # Update Prometheus metrics if available
        if self.metrics:
            self.metrics.increment_counter(
                "inference_requests_total",
                labels={"model": model_key, "status": "success" if success else "failure"}
            )
            self.metrics.observe_histogram(
                "inference_latency_ms",
                latency_ms,
                labels={"model": model_key}
            )
            
    def get_model_metrics(self, model_id: str, version: Optional[str] = None) -> InferenceMetrics:
        """Get inference metrics for model"""
        model_key = f"{model_id}:{version or 'latest'}"
        return self._model_metrics.get(model_key, InferenceMetrics())
        
    def get_endpoint_status(
        self,
        model_id: str,
        version: Optional[str] = None
    ) -> List[ModelEndpoint]:
        """Get status of model endpoints"""
        model_key = f"{model_id}:{version or 'latest'}"
        return self._endpoints.get(model_key, [])
        
    async def health_check_endpoints(self):
        """Health check all endpoints"""
        for model_key, endpoints in self._endpoints.items():
            for endpoint in endpoints:
                try:
                    # Perform health check (would make actual HTTP request)
                    endpoint.is_healthy = True
                    endpoint.last_health_check = datetime.utcnow()
                except Exception as e:
                    endpoint.is_healthy = False
                    logger.error(f"Endpoint health check failed: {endpoint.url}, error: {e}")
                    
    def select_endpoint(
        self,
        model_id: str,
        version: Optional[str] = None,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.LEAST_LOADED
    ) -> Optional[ModelEndpoint]:
        """Select endpoint for inference"""
        model_key = f"{model_id}:{version or 'latest'}"
        endpoints = [e for e in self._endpoints.get(model_key, []) if e.is_healthy]
        
        if not endpoints:
            return None
            
        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            # Simple round-robin (would track index in production)
            return endpoints[0]
        elif strategy == LoadBalancingStrategy.LEAST_LOADED:
            return min(endpoints, key=lambda e: e.get_load_factor())
        elif strategy == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(endpoints)
        else:
            return endpoints[0]
            
    async def unload_model(self, model_id: str, version: Optional[str] = None):
        """Unload model from memory"""
        model_key = f"{model_id}:{version or 'latest'}"
        
        if model_key in self._loaded_models:
            predictor = self._loaded_models[model_key]
            await predictor.unload_model()
            del self._loaded_models[model_key]
            
            logger.info(f"Unloaded model: {model_key}")
            
    def _dict_to_result(self, data: Dict[str, Any]) -> PredictionResult:
        """Convert dictionary to PredictionResult"""
        return PredictionResult(
            request_id=data["request_id"],
            model_id=data["model_id"],
            version=data["version"],
            predictions=data["predictions"],
            probabilities=data.get("probabilities"),
            latency_ms=data["latency_ms"]
        ) 