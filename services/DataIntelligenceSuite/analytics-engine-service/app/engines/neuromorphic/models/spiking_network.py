"""
Spiking Neural Network implementation.
"""

import torch
import torch.nn as nn
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass

from platformq_shared.logging_config import get_logger
from .layers import SpikingLayer, SpikeEvent, LearningRule

logger = get_logger(__name__)


@dataclass
class NetworkConfig:
    """Configuration for spiking neural network."""
    input_size: int
    hidden_sizes: List[int]
    output_size: int
    neuron_type: str = "lif"
    neuron_config: Optional[Dict[str, Any]] = None
    learning_rule: Optional[str] = "stdp"
    sparse_connectivity: float = 0.2
    inhibitory_ratio: float = 0.2
    time_step: float = 1.0  # ms
    simulation_time: float = 100.0  # ms


class SpikingNeuralNetwork(nn.Module):
    """
    Multi-layer spiking neural network with configurable architecture.
    """
    
    def __init__(self, config: NetworkConfig):
        super().__init__()
        self.config = config
        
        # Build network layers
        self.layers = nn.ModuleList()
        
        layer_sizes = [config.input_size] + config.hidden_sizes + [config.output_size]
        
        for i in range(len(layer_sizes) - 1):
            layer = SpikingLayer(
                input_size=layer_sizes[i],
                output_size=layer_sizes[i + 1],
                neuron_type=config.neuron_type,
                neuron_config=config.neuron_config,
                learning_rule=LearningRule(config.learning_rule) if config.learning_rule else None,
                sparse_connectivity=config.sparse_connectivity,
                inhibitory_ratio=config.inhibitory_ratio
            )
            self.layers.append(layer)
        
        # Network state
        self.spike_history = []
        self.current_time = 0.0
        
    def reset(self):
        """Reset network state."""
        for layer in self.layers:
            layer.reset()
        
        self.spike_history.clear()
        self.current_time = 0.0
        
    def forward(
        self,
        input_data: torch.Tensor,
        simulation_time: Optional[float] = None
    ) -> Tuple[torch.Tensor, List[SpikeEvent]]:
        """
        Run network simulation.
        
        Args:
            input_data: Input data to encode as spikes
            simulation_time: Total simulation time (uses config default if None)
            
        Returns:
            Tuple of (output, spike_events)
        """
        if simulation_time is None:
            simulation_time = self.config.simulation_time
            
        # Reset network
        self.reset()
        
        # Calculate number of time steps
        num_steps = int(simulation_time / self.config.time_step)
        
        # Encode input data
        input_spikes = self._encode_input(input_data, num_steps)
        
        # Run simulation
        all_spike_events = []
        output_accumulator = []
        
        for t in range(num_steps):
            current_time = t * self.config.time_step
            self.current_time = current_time
            
            # Get input spikes for this time step
            if t < len(input_spikes):
                current_input = input_spikes[t]
            else:
                current_input = torch.zeros_like(input_spikes[0])
            
            # Forward through layers
            layer_input = current_input
            
            for layer_idx, layer in enumerate(self.layers):
                layer_output, spike_events = layer(
                    layer_input,
                    current_time,
                    self.config.time_step
                )
                
                # Set layer index for spike events
                for event in spike_events:
                    event.layer = layer_idx
                    all_spike_events.append(event)
                
                layer_input = layer_output
            
            # Accumulate output
            output_accumulator.append(layer_input)
        
        # Decode output
        output = self._decode_output(output_accumulator)
        
        # Store spike history
        self.spike_history = all_spike_events
        
        return output, all_spike_events
    
    def _encode_input(
        self,
        input_data: torch.Tensor,
        num_steps: int
    ) -> List[torch.Tensor]:
        """
        Encode input data as spike trains.
        
        Different encoding schemes can be used based on the data type.
        """
        # Rate coding: convert values to spike probabilities
        spike_trains = []
        
        # Normalize input to [0, 1] range
        normalized_input = torch.sigmoid(input_data)
        
        for _ in range(num_steps):
            # Generate spikes based on firing rate
            spikes = (torch.rand_like(normalized_input) < normalized_input).float()
            spike_trains.append(spikes)
        
        return spike_trains
    
    def _decode_output(self, spike_trains: List[torch.Tensor]) -> torch.Tensor:
        """
        Decode output spike trains to continuous values.
        
        Uses spike count or rate decoding.
        """
        if not spike_trains:
            return torch.zeros(self.config.output_size)
        
        # Stack all time steps
        stacked = torch.stack(spike_trains)
        
        # Average spike rate over time
        spike_rate = stacked.mean(dim=0)
        
        return spike_rate
    
    def train_step(
        self,
        input_data: torch.Tensor,
        target: torch.Tensor,
        loss_fn: Optional[nn.Module] = None
    ) -> Dict[str, float]:
        """
        Single training step.
        
        Args:
            input_data: Input batch
            target: Target output
            loss_fn: Loss function (MSE if None)
            
        Returns:
            Dictionary of metrics
        """
        self.train()
        
        # Forward pass
        output, spike_events = self(input_data)
        
        # Compute loss
        if loss_fn is None:
            loss_fn = nn.MSELoss()
        
        loss = loss_fn(output, target)
        
        # Backward pass (if using gradient-based learning)
        if any(p.requires_grad for p in self.parameters()):
            loss.backward()
        
        # Compute metrics
        metrics = {
            'loss': loss.item(),
            'total_spikes': len(spike_events),
            'output_mean': output.mean().item(),
            'output_std': output.std().item()
        }
        
        # Add layer-wise statistics
        for i, layer in enumerate(self.layers):
            stats = layer.get_statistics()
            for key, value in stats.items():
                metrics[f'layer_{i}_{key}'] = value
        
        return metrics
    
    def evaluate(
        self,
        input_data: torch.Tensor,
        target: torch.Tensor
    ) -> Dict[str, float]:
        """
        Evaluate network performance.
        
        Args:
            input_data: Input batch
            target: Target output
            
        Returns:
            Dictionary of evaluation metrics
        """
        self.eval()
        
        with torch.no_grad():
            output, spike_events = self(input_data)
            
            # Compute accuracy (for classification tasks)
            if target.dim() == 1:  # Classification
                predictions = output.argmax(dim=-1)
                accuracy = (predictions == target).float().mean().item()
            else:  # Regression
                accuracy = 1.0 - torch.abs(output - target).mean().item()
            
            # Compute energy efficiency metrics
            total_spikes = len(spike_events)
            total_neurons = sum(layer.output_size for layer in self.layers)
            num_steps = int(self.config.simulation_time / self.config.time_step)
            
            sparsity = 1.0 - (total_spikes / (num_steps * total_neurons))
            energy_per_spike = 0.9  # pJ
            total_energy = total_spikes * energy_per_spike
            
            metrics = {
                'accuracy': accuracy,
                'total_spikes': total_spikes,
                'sparsity': sparsity,
                'energy_pJ': total_energy,
                'spikes_per_ms': total_spikes / self.config.simulation_time
            }
            
        return metrics
    
    def get_spike_raster(self) -> Dict[str, Any]:
        """
        Get spike raster plot data.
        
        Returns:
            Dictionary with spike times and neuron IDs for visualization
        """
        raster_data = {
            'layers': []
        }
        
        for layer_idx in range(len(self.layers)):
            layer_spikes = {
                'neuron_ids': [],
                'spike_times': []
            }
            
            for event in self.spike_history:
                if event.layer == layer_idx:
                    layer_spikes['neuron_ids'].append(event.neuron_id)
                    layer_spikes['spike_times'].append(event.timestamp)
            
            raster_data['layers'].append(layer_spikes)
        
        return raster_data
    
    def save_checkpoint(self, path: str):
        """Save network checkpoint."""
        checkpoint = {
            'config': self.config,
            'state_dict': self.state_dict(),
            'spike_history': self.spike_history[-1000:] if self.spike_history else []  # Save recent history
        }
        torch.save(checkpoint, path)
        logger.info(f"Saved checkpoint to {path}")
    
    def load_checkpoint(self, path: str):
        """Load network checkpoint."""
        checkpoint = torch.load(path)
        self.load_state_dict(checkpoint['state_dict'])
        self.spike_history = checkpoint.get('spike_history', [])
        logger.info(f"Loaded checkpoint from {path}") 