"""
Spiking layer implementations for neuromorphic networks.
"""

import torch
import torch.nn as nn
from typing import List, Tuple, Optional, Dict, Any
from enum import Enum

from platformq_shared.logging_config import get_logger
from .neurons import LeakyIntegrateFireNeuron, IzhikevichNeuron, HodgkinHuxleyNeuron

logger = get_logger(__name__)


class LearningRule(str, Enum):
    """Supported learning rules for synaptic plasticity."""
    STDP = "stdp"  # Spike-Timing Dependent Plasticity
    RSTDP = "rstdp"  # Reward-modulated STDP
    BCM = "bcm"  # Bienenstock-Cooper-Munro
    HEBBIAN = "hebbian"  # Classical Hebbian


class SpikeEvent:
    """Represents a spike event in the network."""
    
    def __init__(self, neuron_id: int, timestamp: float, layer: int, value: float = 1.0):
        self.neuron_id = neuron_id
        self.timestamp = timestamp
        self.layer = layer
        self.value = value


class SpikingLayer(nn.Module):
    """
    A layer of spiking neurons with configurable dynamics and learning rules.
    """
    
    def __init__(
        self,
        input_size: int,
        output_size: int,
        neuron_type: str = "lif",
        neuron_config: Optional[Dict[str, Any]] = None,
        learning_rule: Optional[LearningRule] = None,
        sparse_connectivity: float = 1.0,
        inhibitory_ratio: float = 0.2
    ):
        super().__init__()
        
        self.input_size = input_size
        self.output_size = output_size
        self.learning_rule = learning_rule
        self.sparse_connectivity = sparse_connectivity
        self.inhibitory_ratio = inhibitory_ratio
        
        # Initialize neurons
        neuron_config = neuron_config or {}
        if neuron_type == "lif":
            self.neurons = LeakyIntegrateFireNeuron(output_size, neuron_config)
        elif neuron_type == "izhikevich":
            self.neurons = IzhikevichNeuron(output_size, neuron_config)
        elif neuron_type == "hodgkin_huxley":
            self.neurons = HodgkinHuxleyNeuron(output_size, neuron_config)
        else:
            raise ValueError(f"Unknown neuron type: {neuron_type}")
        
        # Synaptic weights
        self.weight = nn.Parameter(torch.randn(output_size, input_size) * 0.1)
        
        # Connectivity mask (for sparse connections)
        if sparse_connectivity < 1.0:
            mask = torch.rand(output_size, input_size) < sparse_connectivity
            self.register_buffer('connectivity_mask', mask.float())
        else:
            self.register_buffer('connectivity_mask', torch.ones(output_size, input_size))
        
        # Inhibitory neurons mask
        num_inhibitory = int(output_size * inhibitory_ratio)
        inhibitory_mask = torch.zeros(output_size, dtype=torch.bool)
        inhibitory_mask[:num_inhibitory] = True
        inhibitory_mask = inhibitory_mask[torch.randperm(output_size)]
        self.register_buffer('inhibitory_mask', inhibitory_mask)
        
        # STDP-related buffers
        if learning_rule == LearningRule.STDP:
            self.register_buffer('pre_spike_trace', torch.zeros(input_size))
            self.register_buffer('post_spike_trace', torch.zeros(output_size))
            self.stdp_window = 20.0  # ms
            self.a_plus = 0.01
            self.a_minus = 0.01
            self.tau_plus = 20.0  # ms
            self.tau_minus = 20.0  # ms
        
        # Track spike history
        self.spike_history = []
        
    def reset(self):
        """Reset layer state."""
        self.neurons.reset()
        
        if hasattr(self, 'pre_spike_trace'):
            self.pre_spike_trace.zero_()
            self.post_spike_trace.zero_()
        
        self.spike_history.clear()
        
    def forward(
        self,
        input_spikes: torch.Tensor,
        time_step: float = 1.0,
        dt: float = 1.0
    ) -> Tuple[torch.Tensor, List[SpikeEvent]]:
        """
        Process input spikes and generate output spikes.
        
        Args:
            input_spikes: Binary tensor of input spikes
            time_step: Current simulation time
            dt: Time step size
            
        Returns:
            Tuple of (output_spikes, spike_events)
        """
        # Apply connectivity mask and compute input current
        effective_weight = self.weight * self.connectivity_mask
        
        # Apply Dale's law (neurons are either excitatory or inhibitory)
        effective_weight = torch.where(
            self.inhibitory_mask.unsqueeze(1),
            -torch.abs(effective_weight),  # Inhibitory weights are negative
            torch.abs(effective_weight)     # Excitatory weights are positive
        )
        
        # Compute input current
        input_current = torch.matmul(input_spikes, effective_weight.t())
        
        # Update neurons and get spikes
        output_spikes, membrane_potential = self.neurons(input_current, dt)
        
        # Record spike events
        spike_events = []
        spike_indices = torch.nonzero(output_spikes).squeeze()
        
        if spike_indices.numel() > 0:
            if spike_indices.dim() == 0:
                spike_indices = spike_indices.unsqueeze(0)
            
            for idx in spike_indices:
                neuron_id = idx.item() if idx.dim() > 0 else idx
                event = SpikeEvent(
                    neuron_id=neuron_id,
                    timestamp=time_step,
                    layer=-1  # Will be set by network
                )
                spike_events.append(event)
        
        # Apply learning rule if in training mode
        if self.training and self.learning_rule:
            self._apply_learning_rule(input_spikes, output_spikes, dt)
        
        # Store spike history
        self.spike_history.append({
            'time': time_step,
            'input_spikes': input_spikes.sum().item(),
            'output_spikes': output_spikes.sum().item()
        })
        
        return output_spikes, spike_events
    
    def _apply_learning_rule(
        self,
        pre_spikes: torch.Tensor,
        post_spikes: torch.Tensor,
        dt: float
    ):
        """Apply the configured learning rule."""
        if self.learning_rule == LearningRule.STDP:
            self._apply_stdp(pre_spikes, post_spikes, dt)
        elif self.learning_rule == LearningRule.HEBBIAN:
            self._apply_hebbian(pre_spikes, post_spikes)
        # Add other learning rules as needed
    
    def _apply_stdp(
        self,
        pre_spikes: torch.Tensor,
        post_spikes: torch.Tensor,
        dt: float
    ):
        """
        Apply Spike-Timing Dependent Plasticity.
        
        Potentiation occurs when pre-synaptic spike precedes post-synaptic spike.
        Depression occurs when post-synaptic spike precedes pre-synaptic spike.
        """
        # Update spike traces
        self.pre_spike_trace *= torch.exp(-dt / self.tau_plus)
        self.post_spike_trace *= torch.exp(-dt / self.tau_minus)
        
        # Add current spikes to traces
        self.pre_spike_trace += pre_spikes
        self.post_spike_trace += post_spikes
        
        # Calculate weight changes
        # Potentiation: pre-spike trace at time of post-spike
        potentiation = torch.outer(post_spikes, self.pre_spike_trace) * self.a_plus
        
        # Depression: post-spike trace at time of pre-spike
        depression = torch.outer(self.post_spike_trace, pre_spikes) * self.a_minus
        
        # Apply weight changes
        weight_change = potentiation - depression
        
        # Only update weights that are connected
        weight_change *= self.connectivity_mask
        
        # Update weights with bounds
        self.weight.data += weight_change
        self.weight.data = torch.clamp(self.weight.data, -2.0, 2.0)
    
    def _apply_hebbian(
        self,
        pre_spikes: torch.Tensor,
        post_spikes: torch.Tensor
    ):
        """
        Apply simple Hebbian learning rule.
        
        "Neurons that fire together, wire together."
        """
        learning_rate = 0.001
        
        # Coincident spikes strengthen connections
        weight_change = torch.outer(post_spikes, pre_spikes) * learning_rate
        
        # Apply connectivity mask
        weight_change *= self.connectivity_mask
        
        # Update weights
        self.weight.data += weight_change
        self.weight.data = torch.clamp(self.weight.data, -2.0, 2.0)
    
    def get_statistics(self) -> Dict[str, float]:
        """Get layer statistics."""
        stats = {
            'mean_weight': self.weight.mean().item(),
            'std_weight': self.weight.std().item(),
            'sparsity': (self.weight.abs() < 0.01).float().mean().item(),
            'mean_spike_count': self.neurons.spike_count.mean().item() if hasattr(self.neurons, 'spike_count') else 0
        }
        
        if self.spike_history:
            recent_history = self.spike_history[-100:]  # Last 100 time steps
            input_rates = [h['input_spikes'] / self.input_size for h in recent_history]
            output_rates = [h['output_spikes'] / self.output_size for h in recent_history]
            
            stats['mean_input_rate'] = sum(input_rates) / len(input_rates) if input_rates else 0
            stats['mean_output_rate'] = sum(output_rates) / len(output_rates) if output_rates else 0
        
        return stats 