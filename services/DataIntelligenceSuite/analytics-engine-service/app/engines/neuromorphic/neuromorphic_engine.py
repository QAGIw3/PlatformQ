"""
Neuromorphic Engine for brain-inspired computing.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import json
import torch

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

from .models import SpikingNeuralNetwork, NetworkConfig
from .spike_processing import SpikeEncoder, SpikeDecoder, EncodingScheme, SpikePattern

logger = get_logger(__name__)


class NeuromorphicFramework(str, Enum):
    """Supported neuromorphic frameworks."""
    CUSTOM = "custom"
    NENGO = "nengo"
    BINDSNET = "bindsnet"
    NORSE = "norse"


class SpikeCoding(str, Enum):
    """Spike encoding schemes."""
    RATE = "rate"
    TEMPORAL = "temporal"
    PHASE = "phase"
    BURST = "burst"
    POPULATION = "population"


class NeuronModel(str, Enum):
    """Neuron models."""
    LIF = "lif"
    IZHIKEVICH = "izhikevich"
    HODGKIN_HUXLEY = "hodgkin_huxley"


@dataclass
class NeuromorphicConfig:
    """Configuration for neuromorphic computing."""
    framework: NeuromorphicFramework = NeuromorphicFramework.CUSTOM
    neuron_model: NeuronModel = NeuronModel.LIF
    spike_threshold: float = 1.0
    reset_potential: float = 0.0
    membrane_time_constant: float = 20.0  # ms
    refractory_period: float = 2.0  # ms
    learning_rate: float = 0.01
    learning_rule: str = "stdp"
    spike_coding: SpikeCoding = SpikeCoding.RATE
    time_step: float = 1.0  # ms
    simulation_time: float = 100.0  # ms
    enable_gpu: bool = True
    sparse_connectivity: float = 0.2
    max_firing_rate: float = 100.0  # Hz


@dataclass
class SpikeEvent:
    """Represents a spike event."""
    neuron_id: int
    timestamp: float
    layer: int
    value: float = 1.0


@dataclass
class NeuromorphicMetrics:
    """Metrics for neuromorphic processing."""
    total_spikes: int = 0
    average_firing_rate: float = 0.0
    energy_consumption: float = 0.0  # nJ
    latency: float = 0.0  # ms
    accuracy: float = 0.0
    sparsity: float = 0.0


class NeuromorphicEngine:
    """
    Main neuromorphic computing engine for brain-inspired ML.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[IgniteClient] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        
        # Model storage
        self.models: Dict[str, SpikingNeuralNetwork] = {}
        self.spike_buffer = deque(maxlen=10000)
        self.metrics = NeuromorphicMetrics()
        
        # Hardware acceleration
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # Background tasks
        self._running = True
        self._background_tasks = []
        
        logger.info(f"Neuromorphic Engine initialized on device: {self.device}")
        
    async def initialize(self):
        """Initialize neuromorphic engine."""
        # Start background tasks
        task = asyncio.create_task(self._process_spike_events())
        self._background_tasks.append(task)
        
        # Subscribe to events
        await self.event_bus.subscribe("neuromorphic.train", self._handle_train_event)
        await self.event_bus.subscribe("neuromorphic.inference", self._handle_inference_event)
        
    async def create_spiking_network(
        self,
        network_id: str,
        architecture: Dict[str, Any],
        config: Optional[NeuromorphicConfig] = None
    ) -> Dict[str, Any]:
        """Create a new spiking neural network."""
        if config is None:
            config = NeuromorphicConfig()
            
        # Parse architecture
        input_size = architecture.get("input_size", 784)
        hidden_sizes = architecture.get("hidden_sizes", [128, 64])
        output_size = architecture.get("output_size", 10)
        
        # Create network configuration
        network_config = NetworkConfig(
            input_size=input_size,
            hidden_sizes=hidden_sizes,
            output_size=output_size,
            neuron_type=config.neuron_model.value,
            neuron_config={
                "spike_threshold": config.spike_threshold,
                "reset_potential": config.reset_potential,
                "membrane_time_constant": config.membrane_time_constant,
                "refractory_period": config.refractory_period
            },
            learning_rule=config.learning_rule,
            sparse_connectivity=config.sparse_connectivity,
            time_step=config.time_step,
            simulation_time=config.simulation_time
        )
        
        # Create network
        model = SpikingNeuralNetwork(network_config)
        model = model.to(self.device)
        
        # Store model
        self.models[network_id] = model
        
        # Cache model metadata
        metadata = {
            "network_id": network_id,
            "architecture": architecture,
            "config": {
                "framework": config.framework.value,
                "neuron_model": config.neuron_model.value,
                "spike_coding": config.spike_coding.value,
                "learning_rule": config.learning_rule
            },
            "created_at": datetime.utcnow().isoformat(),
            "device": str(self.device)
        }
        
        await self.cache_manager.set(f"neuromorphic:model:{network_id}", metadata)
        
        # Publish event
        await self.event_bus.publish("neuromorphic.network.created", metadata)
        
        logger.info(f"Created spiking network {network_id}")
        
        return metadata
        
    async def train_network(
        self,
        network_id: str,
        training_data: List[Dict[str, Any]],
        epochs: int = 10,
        batch_size: int = 32
    ) -> Dict[str, Any]:
        """Train a spiking neural network."""
        if network_id not in self.models:
            raise ValueError(f"Network {network_id} not found")
            
        model = self.models[network_id]
        model.train()
        
        # Training metrics
        train_metrics = []
        
        for epoch in range(epochs):
            epoch_metrics = {
                "epoch": epoch,
                "loss": 0.0,
                "accuracy": 0.0,
                "total_spikes": 0
            }
            
            # Process batches
            for i in range(0, len(training_data), batch_size):
                batch = training_data[i:i+batch_size]
                
                # Prepare batch data
                batch_data = torch.stack([
                    torch.tensor(item["data"], dtype=torch.float32)
                    for item in batch
                ]).to(self.device)
                
                batch_targets = torch.stack([
                    torch.tensor(item["target"], dtype=torch.float32)
                    for item in batch
                ]).to(self.device)
                
                # Train step
                metrics = model.train_step(batch_data, batch_targets)
                
                # Accumulate metrics
                epoch_metrics["loss"] += metrics["loss"]
                epoch_metrics["total_spikes"] += metrics["total_spikes"]
                
            # Average metrics
            num_batches = len(training_data) // batch_size
            epoch_metrics["loss"] /= num_batches
            
            train_metrics.append(epoch_metrics)
            
            # Log progress
            logger.info(
                f"Epoch {epoch}: Loss={epoch_metrics['loss']:.4f}, "
                f"Spikes={epoch_metrics['total_spikes']}"
            )
            
            # Publish progress event
            await self.event_bus.publish("neuromorphic.training.progress", {
                "network_id": network_id,
                "epoch": epoch,
                "metrics": epoch_metrics
            })
            
        # Save final metrics
        training_result = {
            "network_id": network_id,
            "epochs_trained": epochs,
            "final_metrics": train_metrics[-1],
            "training_history": train_metrics
        }
        
        await self.cache_manager.set(
            f"neuromorphic:training:{network_id}",
            training_result,
            ttl=3600
        )
        
        return training_result
        
    async def simulate(
        self,
        network_id: str,
        input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run simulation on neuromorphic network."""
        if network_id not in self.models:
            raise ValueError(f"Network {network_id} not found")
            
        model = self.models[network_id]
        model.eval()
        
        # Prepare input
        data = torch.tensor(input_data["data"], dtype=torch.float32).to(self.device)
        
        # Run simulation
        start_time = datetime.utcnow()
        
        with torch.no_grad():
            output, spike_events = model(data)
            
        inference_time = (datetime.utcnow() - start_time).total_seconds() * 1000  # ms
        
        # Calculate metrics
        total_spikes = len(spike_events)
        total_neurons = sum(layer.output_size for layer in model.layers)
        num_steps = int(model.config.simulation_time / model.config.time_step)
        sparsity = 1.0 - (total_spikes / (num_steps * total_neurons))
        
        # Estimate energy
        energy_consumption = total_spikes * 0.9  # pJ per spike
        
        # Update metrics
        self.metrics.total_spikes += total_spikes
        self.metrics.latency = inference_time
        self.metrics.sparsity = sparsity
        self.metrics.energy_consumption = energy_consumption
        
        # Store spike events
        spike_data = {
            "network_id": network_id,
            "timestamp": datetime.utcnow().isoformat(),
            "spike_events": [
                {
                    "neuron_id": event.neuron_id,
                    "timestamp": event.timestamp,
                    "layer": event.layer
                }
                for event in spike_events[:100]  # Limit to first 100 spikes
            ]
        }
        
        # Cache results
        await self.cache_manager.set(
            f"neuromorphic:simulation:{network_id}:{datetime.utcnow().timestamp()}",
            spike_data,
            ttl=300
        )
        
        # Publish spike events
        await self.event_bus.publish("neuromorphic.spikes", spike_data)
        
        return {
            "output": output.cpu().numpy().tolist(),
            "simulation_id": f"sim_{datetime.utcnow().timestamp()}",
            "inference_time_ms": inference_time,
            "total_spikes": total_spikes,
            "sparsity": sparsity,
            "estimated_energy_pJ": energy_consumption,
            "spikes_per_ms": total_spikes / model.config.simulation_time
        }
        
    async def detect_anomalies(
        self,
        network_id: str,
        data_stream: List[Dict[str, Any]],
        threshold: float = 2.0
    ) -> List[Dict[str, Any]]:
        """Detect anomalies using spike patterns."""
        if network_id not in self.models:
            raise ValueError(f"Network {network_id} not found")
            
        anomalies = []
        baseline_spike_rates = []
        
        for data_point in data_stream:
            # Run simulation
            result = await self.simulate(network_id, data_point)
            
            spike_rate = result["spikes_per_ms"]
            baseline_spike_rates.append(spike_rate)
            
            # Detect anomalies after establishing baseline
            if len(baseline_spike_rates) > 10:
                import numpy as np
                mean_rate = np.mean(baseline_spike_rates[-50:])
                std_rate = np.std(baseline_spike_rates[-50:])
                
                if abs(spike_rate - mean_rate) > threshold * std_rate:
                    anomaly = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "anomaly_detected": True,
                        "spike_rate": spike_rate,
                        "expected_rate": mean_rate,
                        "deviation": abs(spike_rate - mean_rate) / (std_rate + 1e-6),
                        "data_id": data_point.get("id", "unknown")
                    }
                    
                    anomalies.append(anomaly)
                    
                    # Publish anomaly
                    await self.event_bus.publish("neuromorphic.anomaly", anomaly)
                    
        return anomalies
        
    async def encode_data(
        self,
        data: Union[List, torch.Tensor],
        encoding_scheme: Optional[EncodingScheme] = None,
        **kwargs
    ) -> torch.Tensor:
        """Encode data into spike trains."""
        if encoding_scheme is None:
            encoding_scheme = EncodingScheme.RATE
            
        encoder = SpikeEncoder(encoding_scheme)
        
        if isinstance(data, list):
            data = torch.tensor(data, dtype=torch.float32)
            
        return encoder.encode(data, **kwargs)
        
    async def decode_spikes(
        self,
        spike_trains: torch.Tensor,
        decoding_method: str = "rate",
        **kwargs
    ) -> torch.Tensor:
        """Decode spike trains to continuous values."""
        decoder = SpikeDecoder(decoding_method)
        return decoder.decode(spike_trains, **kwargs)
        
    async def analyze_spike_patterns(
        self,
        network_id: str,
        time_window: float = 1000.0
    ) -> Dict[str, Any]:
        """Analyze spike patterns in a network."""
        if network_id not in self.models:
            raise ValueError(f"Network {network_id} not found")
            
        model = self.models[network_id]
        
        # Get spike raster data
        raster_data = model.get_spike_raster()
        
        # Analyze patterns
        patterns = []
        
        for layer_idx, layer_data in enumerate(raster_data["layers"]):
            spike_times = layer_data["spike_times"]
            neuron_ids = layer_data["neuron_ids"]
            
            if spike_times:
                # Calculate statistics
                import numpy as np
                spike_times_array = np.array(spike_times)
                
                # Inter-spike intervals
                if len(spike_times_array) > 1:
                    isis = np.diff(np.sort(spike_times_array))
                    mean_isi = np.mean(isis)
                    cv_isi = np.std(isis) / (mean_isi + 1e-6)
                else:
                    mean_isi = 0
                    cv_isi = 0
                
                # Firing rate
                firing_rate = len(spike_times) / (time_window / 1000)  # Hz
                
                # Synchrony (simplified)
                time_bins = np.histogram(spike_times_array, bins=int(time_window))[0]
                synchrony = np.std(time_bins) / (np.mean(time_bins) + 1e-6)
                
                pattern = SpikePattern(
                    spike_times=spike_times[:100],  # Limit for storage
                    neuron_ids=neuron_ids[:100],
                    pattern_id=f"layer_{layer_idx}",
                    metadata={
                        "mean_isi": mean_isi,
                        "cv_isi": cv_isi,
                        "firing_rate": firing_rate,
                        "synchrony": synchrony
                    }
                )
                
                patterns.append(pattern)
        
        analysis_result = {
            "network_id": network_id,
            "time_window": time_window,
            "patterns": [
                {
                    "pattern_id": p.pattern_id,
                    "num_spikes": len(p.spike_times),
                    "metadata": p.metadata
                }
                for p in patterns
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Cache analysis
        await self.cache_manager.set(
            f"neuromorphic:analysis:{network_id}",
            analysis_result,
            ttl=600
        )
        
        return analysis_result
        
    async def _process_spike_events(self):
        """Background task to process spike events."""
        while self._running:
            try:
                # Process buffered spikes
                if len(self.spike_buffer) > 0:
                    recent_spikes = list(self.spike_buffer)
                    
                    # Calculate firing statistics
                    if recent_spikes:
                        spike_times = [s.timestamp for s in recent_spikes]
                        if spike_times:
                            import numpy as np
                            avg_rate = len(spike_times) / (max(spike_times) - min(spike_times) + 1e-6)
                            self.metrics.average_firing_rate = avg_rate
                            
                            # Update metrics cache
                            await self.cache_manager.set(
                                "neuromorphic:metrics:current",
                                {
                                    "total_spikes": self.metrics.total_spikes,
                                    "average_firing_rate": self.metrics.average_firing_rate,
                                    "sparsity": self.metrics.sparsity,
                                    "energy_consumption": self.metrics.energy_consumption,
                                    "timestamp": datetime.utcnow().isoformat()
                                },
                                ttl=60
                            )
                            
                await asyncio.sleep(0.1)  # Process every 100ms
                
            except Exception as e:
                logger.error(f"Error processing spike events: {e}")
                await asyncio.sleep(1)
                
    async def _handle_train_event(self, event_data: Dict[str, Any]):
        """Handle training request events."""
        try:
            network_id = event_data["network_id"]
            training_data = event_data["training_data"]
            epochs = event_data.get("epochs", 10)
            
            result = await self.train_network(network_id, training_data, epochs)
            
            # Publish completion event
            await self.event_bus.publish("neuromorphic.training.complete", result)
            
        except Exception as e:
            logger.error(f"Error handling train event: {e}")
            await self.event_bus.publish("neuromorphic.training.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def _handle_inference_event(self, event_data: Dict[str, Any]):
        """Handle inference request events."""
        try:
            network_id = event_data["network_id"]
            input_data = event_data["input_data"]
            
            result = await self.simulate(network_id, input_data)
            
            # Publish result event
            await self.event_bus.publish("neuromorphic.inference.complete", result)
            
        except Exception as e:
            logger.error(f"Error handling inference event: {e}")
            await self.event_bus.publish("neuromorphic.inference.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def close(self):
        """Clean up resources."""
        self._running = False
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        
        logger.info("Neuromorphic engine closed") 