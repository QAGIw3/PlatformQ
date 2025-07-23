"""
Neuromorphic computing API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import logging
import torch

from ..core.neuromorphic_engine import (
    NeuromorphicEngine,
    NeuromorphicConfig,
    NeuromorphicFramework,
    SpikeCoding,
    NeuronModel
)

logger = logging.getLogger(__name__)
router = APIRouter()


class NetworkArchitecture(BaseModel):
    """Neural network architecture"""
    input_size: int = Field(..., description="Number of input neurons")
    hidden_sizes: List[int] = Field(..., description="Hidden layer sizes")
    output_size: int = Field(..., description="Number of output neurons")


class NetworkConfig(BaseModel):
    """Spiking neural network configuration"""
    framework: str = Field("custom", description="Neuromorphic framework")
    neuron_model: str = Field("leaky_integrate_fire", description="Neuron model")
    spike_threshold: float = Field(1.0, description="Spike threshold")
    membrane_time_constant: float = Field(20.0, description="Membrane time constant (ms)")
    learning_rate: float = Field(0.01, description="Learning rate")
    learning_rule: str = Field("STDP", description="Learning rule")
    spike_coding: str = Field("rate", description="Spike encoding scheme")
    simulation_time: float = Field(1000.0, description="Simulation time (ms)")
    sparse_connectivity: float = Field(0.2, description="Connection probability")


class NetworkCreate(BaseModel):
    """Create spiking neural network request"""
    network_id: str = Field(..., description="Unique network identifier")
    architecture: NetworkArchitecture
    config: Optional[NetworkConfig] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class TrainingData(BaseModel):
    """Training data for network"""
    data: List[List[float]] = Field(..., description="Input data")
    target: List[int] = Field(..., description="Target labels")


class SimulationInput(BaseModel):
    """Simulation input data"""
    data: List[float] = Field(..., description="Input data vector")


class AnomalyDetectionRequest(BaseModel):
    """Anomaly detection request"""
    data_stream: List[SimulationInput] = Field(..., description="Data stream to analyze")
    threshold: float = Field(2.0, description="Anomaly threshold (standard deviations)")


def get_neuromorphic_engine(request: Request) -> NeuromorphicEngine:
    """Get neuromorphic engine from app state"""
    return request.app.state.neuromorphic_engine


@router.post("/networks/create")
async def create_network(
    network_data: NetworkCreate,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Create a new spiking neural network"""
    try:
        # Convert config if provided
        config = None
        if network_data.config:
            config = NeuromorphicConfig(
                framework=NeuromorphicFramework(network_data.config.framework),
                neuron_model=NeuronModel(network_data.config.neuron_model),
                spike_threshold=network_data.config.spike_threshold,
                membrane_time_constant=network_data.config.membrane_time_constant,
                learning_rate=network_data.config.learning_rate,
                learning_rule=network_data.config.learning_rule,
                spike_coding=SpikeCoding(network_data.config.spike_coding),
                simulation_time=network_data.config.simulation_time,
                sparse_connectivity=network_data.config.sparse_connectivity
            )
        
        # Create network
        result = await engine.create_spiking_network(
            network_id=network_data.network_id,
            architecture=network_data.architecture.dict(),
            config=config
        )
        
        return {
            "status": "success",
            "network": result
        }
        
    except Exception as e:
        logger.error(f"Error creating network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/networks")
async def list_networks(
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """List all spiking neural networks"""
    try:
        networks = []
        
        # Get networks from cache
        for key, metadata in engine.model_cache.scan():
            networks.append(metadata)
            
        return {
            "networks": networks,
            "count": len(networks)
        }
        
    except Exception as e:
        logger.error(f"Error listing networks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/networks/{network_id}")
async def get_network(
    network_id: str,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get network details"""
    try:
        metadata = engine.model_cache.get(network_id)
        
        if not metadata:
            raise HTTPException(status_code=404, detail=f"Network {network_id} not found")
            
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/networks/{network_id}/train")
async def train_network(
    network_id: str,
    training_data: List[TrainingData],
    epochs: int = Query(10, description="Number of training epochs"),
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Train a spiking neural network"""
    try:
        # Convert training data
        train_batches = []
        for batch in training_data:
            train_batches.append({
                "data": batch.data,
                "target": batch.target
            })
        
        # Train network
        result = await engine.train_network(
            network_id=network_id,
            training_data=train_batches,
            epochs=epochs
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error training network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/networks/{network_id}/simulate")
async def simulate_network(
    network_id: str,
    input_data: SimulationInput,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Run simulation on spiking neural network"""
    try:
        result = await engine.simulate(
            network_id=network_id,
            input_data={"data": input_data.data}
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error running simulation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/networks/{network_id}/detect-anomalies")
async def detect_anomalies(
    network_id: str,
    request: AnomalyDetectionRequest,
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Detect anomalies using spike patterns"""
    try:
        # Convert data stream
        data_stream = []
        for i, item in enumerate(request.data_stream):
            data_stream.append({
                "data": item.data,
                "id": f"sample_{i}"
            })
        
        # Run anomaly detection
        anomalies = await engine.detect_anomalies(
            network_id=network_id,
            data_stream=data_stream,
            threshold=request.threshold
        )
        
        return {
            "network_id": network_id,
            "samples_analyzed": len(data_stream),
            "anomalies_detected": len(anomalies),
            "anomalies": anomalies
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_metrics(
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get neuromorphic engine metrics"""
    try:
        # Get current metrics from cache
        metrics = engine.metrics_cache.get("current_metrics")
        
        if not metrics:
            metrics = {
                "total_spikes": engine.metrics.total_spikes,
                "average_firing_rate": engine.metrics.average_firing_rate,
                "sparsity": engine.metrics.sparsity,
                "energy_consumption": engine.metrics.energy_consumption,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/spike-events/{network_id}")
async def get_spike_events(
    network_id: str,
    limit: int = Query(100, description="Maximum number of events"),
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get recent spike events for a network"""
    try:
        events = []
        
        # Get spike events from cache
        for key, value in engine.spike_cache.scan():
            if key.startswith(f"{network_id}:"):
                events.append(value)
                if len(events) >= limit:
                    break
                    
        # Sort by timestamp
        events.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return {
            "network_id": network_id,
            "events": events[:limit]
        }
        
    except Exception as e:
        logger.error(f"Error getting spike events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hardware-info")
async def get_hardware_info(
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Get hardware acceleration information"""
    return {
        "device": str(engine.device),
        "cuda_available": torch.cuda.is_available(),
        "supported_frameworks": ["custom", "nengo", "bindsnet", "norse"],
        "neuromorphic_chips": {
            "loihi": {
                "neurons": 130000,
                "synapses": 130000000,
                "power_mw": 100
            },
            "truenorth": {
                "neurons": 1000000,
                "synapses": 256000000,
                "power_mw": 70
            },
            "spinnaker": {
                "cores": 1000000,
                "power_w": 90
            }
        }
    }


@router.get("/health")
async def health_check(
    engine: NeuromorphicEngine = Depends(get_neuromorphic_engine)
):
    """Health check endpoint"""
    try:
        # Check connections
        ignite_connected = engine.ignite_client is not None
        pulsar_connected = engine.pulsar_client is not None
        
        return {
            "status": "healthy" if ignite_connected and pulsar_connected else "unhealthy",
            "checks": {
                "ignite": "connected" if ignite_connected else "disconnected",
                "pulsar": "connected" if pulsar_connected else "disconnected",
                "device": str(engine.device),
                "models_loaded": len(engine.models)
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        } 