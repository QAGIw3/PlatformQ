"""
Neuromorphic Computing API endpoints.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.config import settings
from app.engines.neuromorphic import (
    NeuromorphicEngine,
    NeuromorphicConfig,
    NeuromorphicFramework,
    SpikeCoding,
    NeuronModel,
    EncodingScheme
)
from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Global instances (would be dependency injected in production)
_neuromorphic_engine = None


def get_neuromorphic_engine() -> NeuromorphicEngine:
    """Get or create neuromorphic engine instance."""
    global _neuromorphic_engine
    if _neuromorphic_engine is None:
        event_bus = EventBus()
        cache_manager = CacheManager()
        ignite_client = IgniteClient() if settings.IGNITE_URL else None
        
        _neuromorphic_engine = NeuromorphicEngine(
            event_bus=event_bus,
            cache_manager=cache_manager,
            ignite_client=ignite_client
        )
    
    return _neuromorphic_engine


# Request/Response models
class NetworkArchitecture(BaseModel):
    """Neural network architecture specification."""
    input_size: int = Field(..., description="Number of input neurons")
    hidden_sizes: List[int] = Field(default=[128, 64], description="Hidden layer sizes")
    output_size: int = Field(..., description="Number of output neurons")


class NetworkCreateRequest(BaseModel):
    """Request to create a spiking neural network."""
    network_id: str = Field(..., description="Unique network identifier")
    architecture: NetworkArchitecture = Field(..., description="Network architecture")
    framework: Optional[NeuromorphicFramework] = Field(default=NeuromorphicFramework.CUSTOM)
    neuron_model: Optional[NeuronModel] = Field(default=NeuronModel.LIF)
    spike_coding: Optional[SpikeCoding] = Field(default=SpikeCoding.RATE)
    learning_rule: Optional[str] = Field(default="stdp")
    sparse_connectivity: Optional[float] = Field(default=0.2, ge=0, le=1)
    config_overrides: Optional[Dict[str, Any]] = Field(default=None)


class TrainingDataItem(BaseModel):
    """Single training data item."""
    data: List[float] = Field(..., description="Input data")
    target: List[float] = Field(..., description="Target output")


class TrainRequest(BaseModel):
    """Request to train a network."""
    network_id: str = Field(..., description="Network to train")
    training_data: List[TrainingDataItem] = Field(..., description="Training dataset")
    epochs: int = Field(default=10, ge=1)
    batch_size: int = Field(default=32, ge=1)


class SimulateRequest(BaseModel):
    """Request to run simulation."""
    network_id: str = Field(..., description="Network to simulate")
    input_data: List[float] = Field(..., description="Input data")
    simulation_time: Optional[float] = Field(default=None, description="Simulation time (ms)")


class AnomalyDetectionRequest(BaseModel):
    """Request for anomaly detection."""
    network_id: str = Field(..., description="Network to use")
    data_stream: List[Dict[str, Any]] = Field(..., description="Data stream to analyze")
    threshold: float = Field(default=2.0, description="Anomaly threshold (std deviations)")


class EncodeDataRequest(BaseModel):
    """Request to encode data as spikes."""
    data: List[float] = Field(..., description="Data to encode")
    encoding_scheme: EncodingScheme = Field(default=EncodingScheme.RATE)
    time_window: float = Field(default=100.0, description="Time window (ms)")
    dt: float = Field(default=1.0, description="Time step (ms)")


class NetworkResponse(BaseModel):
    """Network creation response."""
    network_id: str
    architecture: Dict[str, Any]
    config: Dict[str, Any]
    created_at: str
    device: str


class TrainingResponse(BaseModel):
    """Training response."""
    network_id: str
    epochs_trained: int
    final_metrics: Dict[str, Any]
    training_history: List[Dict[str, Any]]


class SimulationResponse(BaseModel):
    """Simulation response."""
    output: List[float]
    simulation_id: str
    inference_time_ms: float
    total_spikes: int
    sparsity: float
    estimated_energy_pJ: float
    spikes_per_ms: float


class AnomalyResponse(BaseModel):
    """Anomaly detection response."""
    anomalies: List[Dict[str, Any]]
    total_analyzed: int
    anomaly_rate: float


# API Endpoints
@router.post("/networks", response_model=NetworkResponse)
async def create_network(
    request: NetworkCreateRequest,
    background_tasks: BackgroundTasks,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Create a new spiking neural network.
    
    This endpoint creates a brain-inspired neural network with configurable
    neuron models, connectivity patterns, and learning rules.
    """
    try:
        # Initialize engine if needed
        if not hasattr(engine, '_initialized'):
            await engine.initialize()
            engine._initialized = True
        
        # Create configuration
        config = NeuromorphicConfig(
            framework=request.framework,
            neuron_model=request.neuron_model,
            spike_coding=request.spike_coding,
            learning_rule=request.learning_rule,
            sparse_connectivity=request.sparse_connectivity
        )
        
        # Apply overrides
        if request.config_overrides:
            for key, value in request.config_overrides.items():
                if hasattr(config, key):
                    setattr(config, key, value)
        
        # Create network
        result = await engine.create_spiking_network(
            network_id=request.network_id,
            architecture=request.architecture.dict(),
            config=config
        )
        
        return NetworkResponse(**result)
        
    except Exception as e:
        logger.error(f"Error creating network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train", response_model=TrainingResponse)
async def train_network(
    request: TrainRequest,
    background_tasks: BackgroundTasks,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Train a spiking neural network.
    
    Uses spike-based learning rules like STDP (Spike-Timing Dependent Plasticity)
    for energy-efficient training.
    """
    try:
        # Convert training data
        training_data = [
            {"data": item.data, "target": item.target}
            for item in request.training_data
        ]
        
        # Train in background for large datasets
        if len(training_data) > 1000:
            background_tasks.add_task(
                engine.train_network,
                request.network_id,
                training_data,
                request.epochs,
                request.batch_size
            )
            
            return TrainingResponse(
                network_id=request.network_id,
                epochs_trained=0,
                final_metrics={"status": "training_started"},
                training_history=[]
            )
        else:
            # Train synchronously for small datasets
            result = await engine.train_network(
                request.network_id,
                training_data,
                request.epochs,
                request.batch_size
            )
            
            return TrainingResponse(**result)
            
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error training network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/simulate", response_model=SimulationResponse)
async def simulate(
    request: SimulateRequest,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Run simulation on a spiking neural network.
    
    Performs energy-efficient inference using spike-based computation.
    """
    try:
        result = await engine.simulate(
            network_id=request.network_id,
            input_data={"data": request.input_data}
        )
        
        return SimulationResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error running simulation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/detect-anomalies", response_model=AnomalyResponse)
async def detect_anomalies(
    request: AnomalyDetectionRequest,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Detect anomalies using spike pattern analysis.
    
    Neuromorphic networks can detect anomalies through unusual spike patterns,
    providing energy-efficient real-time anomaly detection.
    """
    try:
        anomalies = await engine.detect_anomalies(
            network_id=request.network_id,
            data_stream=request.data_stream,
            threshold=request.threshold
        )
        
        anomaly_rate = len(anomalies) / len(request.data_stream) if request.data_stream else 0
        
        return AnomalyResponse(
            anomalies=anomalies,
            total_analyzed=len(request.data_stream),
            anomaly_rate=anomaly_rate
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/encode", response_model=Dict[str, Any])
async def encode_data(
    request: EncodeDataRequest,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Encode continuous data into spike trains.
    
    Supports various encoding schemes:
    - Rate coding: firing rate encodes value
    - Temporal coding: spike timing encodes value
    - Phase coding: phase relationship encodes value
    - Population coding: distributed representation
    """
    try:
        spike_trains = await engine.encode_data(
            data=request.data,
            encoding_scheme=request.encoding_scheme,
            time_window=request.time_window,
            dt=request.dt
        )
        
        return {
            "spike_trains_shape": list(spike_trains.shape),
            "encoding_scheme": request.encoding_scheme.value,
            "time_window": request.time_window,
            "num_time_steps": spike_trains.shape[0],
            "sample_spikes": spike_trains[:10].tolist()  # First 10 time steps
        }
        
    except Exception as e:
        logger.error(f"Error encoding data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/networks/{network_id}/analyze", response_model=Dict[str, Any])
async def analyze_network(
    network_id: str,
    time_window: float = 1000.0,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """
    Analyze spike patterns in a network.
    
    Provides insights into:
    - Firing rates and patterns
    - Inter-spike intervals
    - Synchronization
    - Network dynamics
    """
    try:
        analysis = await engine.analyze_spike_patterns(
            network_id=network_id,
            time_window=time_window
        )
        
        return analysis
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error analyzing network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/networks/{network_id}", response_model=Dict[str, Any])
async def get_network_info(
    network_id: str,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get network information."""
    try:
        # Retrieve from cache
        info = await engine.cache_manager.get(f"neuromorphic:model:{network_id}")
        
        if not info:
            raise HTTPException(status_code=404, detail="Network not found")
        
        return info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving network info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=Dict[str, Any])
async def get_metrics(
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get current neuromorphic engine metrics."""
    try:
        metrics = await engine.cache_manager.get("neuromorphic:metrics:current")
        
        if not metrics:
            metrics = {
                "total_spikes": engine.metrics.total_spikes,
                "average_firing_rate": engine.metrics.average_firing_rate,
                "sparsity": engine.metrics.sparsity,
                "energy_consumption": engine.metrics.energy_consumption
            }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Utility endpoints
@router.get("/neuron-models", response_model=List[str])
async def get_neuron_models():
    """Get available neuron models."""
    return [model.value for model in NeuronModel]


@router.get("/encoding-schemes", response_model=List[str])
async def get_encoding_schemes():
    """Get available spike encoding schemes."""
    return [scheme.value for scheme in EncodingScheme]


@router.get("/frameworks", response_model=List[str])
async def get_frameworks():
    """Get supported neuromorphic frameworks."""
    return [fw.value for fw in NeuromorphicFramework] 