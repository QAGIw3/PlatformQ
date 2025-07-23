"""
Spike encoding and decoding utilities for neuromorphic computing.
"""

import torch
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from enum import Enum
from dataclasses import dataclass

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class EncodingScheme(str, Enum):
    """Supported spike encoding schemes."""
    RATE = "rate"
    TEMPORAL = "temporal"
    PHASE = "phase"
    BURST = "burst"
    POPULATION = "population"
    RANK_ORDER = "rank_order"


@dataclass
class SpikePattern:
    """Represents a spike pattern for analysis."""
    spike_times: List[float]
    neuron_ids: List[int]
    pattern_id: Optional[str] = None
    confidence: float = 1.0
    metadata: Optional[Dict[str, Any]] = None


class SpikeEncoder:
    """
    Encodes continuous data into spike trains using various encoding schemes.
    """
    
    def __init__(self, encoding_scheme: EncodingScheme = EncodingScheme.RATE):
        self.encoding_scheme = encoding_scheme
        
    def encode(
        self,
        data: torch.Tensor,
        time_window: float = 100.0,
        dt: float = 1.0,
        **kwargs
    ) -> torch.Tensor:
        """
        Encode data into spike trains.
        
        Args:
            data: Input data to encode
            time_window: Total time window for encoding (ms)
            dt: Time step (ms)
            **kwargs: Additional encoding parameters
            
        Returns:
            Spike trains tensor of shape (time_steps, *data.shape)
        """
        num_steps = int(time_window / dt)
        
        if self.encoding_scheme == EncodingScheme.RATE:
            return self._rate_encode(data, num_steps, **kwargs)
        elif self.encoding_scheme == EncodingScheme.TEMPORAL:
            return self._temporal_encode(data, num_steps, dt, **kwargs)
        elif self.encoding_scheme == EncodingScheme.PHASE:
            return self._phase_encode(data, num_steps, dt, **kwargs)
        elif self.encoding_scheme == EncodingScheme.BURST:
            return self._burst_encode(data, num_steps, **kwargs)
        elif self.encoding_scheme == EncodingScheme.POPULATION:
            return self._population_encode(data, num_steps, **kwargs)
        elif self.encoding_scheme == EncodingScheme.RANK_ORDER:
            return self._rank_order_encode(data, num_steps, dt, **kwargs)
        else:
            raise ValueError(f"Unknown encoding scheme: {self.encoding_scheme}")
    
    def _rate_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        max_rate: float = 100.0
    ) -> torch.Tensor:
        """
        Rate coding: firing rate proportional to input value.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            max_rate: Maximum firing rate (Hz)
            
        Returns:
            Spike trains
        """
        # Normalize data to [0, 1]
        normalized = torch.sigmoid(data)
        
        # Convert to firing probability
        firing_prob = normalized * (max_rate / 1000.0)  # Convert Hz to probability per ms
        
        # Generate spikes
        spike_trains = []
        for _ in range(num_steps):
            spikes = (torch.rand_like(data) < firing_prob).float()
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)
    
    def _temporal_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        dt: float,
        latency_scale: float = 50.0
    ) -> torch.Tensor:
        """
        Temporal coding: time to first spike encodes value.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            dt: Time step
            latency_scale: Scale factor for latency
            
        Returns:
            Spike trains
        """
        # Normalize data
        normalized = torch.sigmoid(data)
        
        # Calculate spike times (inverse relationship - higher value = earlier spike)
        spike_times = latency_scale * (1 - normalized)
        
        # Generate spike trains
        spike_trains = []
        for t in range(num_steps):
            current_time = t * dt
            spikes = ((spike_times <= current_time) & 
                     (spike_times > current_time - dt)).float()
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)
    
    def _phase_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        dt: float,
        frequency: float = 40.0
    ) -> torch.Tensor:
        """
        Phase coding: phase of oscillation encodes value.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            dt: Time step
            frequency: Oscillation frequency (Hz)
            
        Returns:
            Spike trains
        """
        # Normalize data to phase [0, 2π]
        phase = torch.sigmoid(data) * 2 * np.pi
        
        # Generate spike trains
        spike_trains = []
        omega = 2 * np.pi * frequency / 1000  # Convert to rad/ms
        
        for t in range(num_steps):
            current_time = t * dt
            oscillation = torch.sin(omega * current_time + phase)
            spikes = (oscillation > 0.9).float()  # Spike at peaks
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)
    
    def _burst_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        burst_length: int = 5,
        inter_burst_interval: int = 20
    ) -> torch.Tensor:
        """
        Burst coding: number of spikes in burst encodes value.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            burst_length: Maximum burst length
            inter_burst_interval: Interval between bursts
            
        Returns:
            Spike trains
        """
        # Normalize data
        normalized = torch.sigmoid(data)
        
        # Calculate spikes per burst
        spikes_per_burst = (normalized * burst_length).int()
        
        # Generate spike trains
        spike_trains = []
        burst_counter = 0
        
        for t in range(num_steps):
            if t % (burst_length + inter_burst_interval) < burst_length:
                burst_position = t % (burst_length + inter_burst_interval)
                spikes = (burst_position < spikes_per_burst).float()
            else:
                spikes = torch.zeros_like(data)
            
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)
    
    def _population_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        population_size: int = 10
    ) -> torch.Tensor:
        """
        Population coding: distributed representation across multiple neurons.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            population_size: Number of neurons per input dimension
            
        Returns:
            Spike trains with expanded population dimension
        """
        # Expand data to population
        expanded_shape = list(data.shape) + [population_size]
        expanded_data = data.unsqueeze(-1).expand(*expanded_shape)
        
        # Create receptive fields
        centers = torch.linspace(0, 1, population_size)
        width = 1.0 / (population_size - 1)
        
        # Calculate activation based on distance to centers
        normalized = torch.sigmoid(data).unsqueeze(-1)
        distances = torch.abs(normalized - centers)
        activations = torch.exp(-distances**2 / (2 * width**2))
        
        # Generate spikes based on activation
        spike_trains = []
        for _ in range(num_steps):
            spikes = (torch.rand_like(activations) < activations * 0.5).float()
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)
    
    def _rank_order_encode(
        self,
        data: torch.Tensor,
        num_steps: int,
        dt: float
    ) -> torch.Tensor:
        """
        Rank order coding: order of spikes encodes value.
        
        Args:
            data: Input data
            num_steps: Number of time steps
            dt: Time step
            
        Returns:
            Spike trains
        """
        # Flatten data for ranking
        flat_data = data.flatten()
        
        # Get ranks (higher value = earlier spike)
        ranks = torch.argsort(torch.argsort(flat_data, descending=True))
        
        # Calculate spike times based on rank
        spike_times = ranks.float() * dt * 2  # Spread over time
        
        # Reshape back
        spike_times = spike_times.reshape(data.shape)
        
        # Generate spike trains
        spike_trains = []
        for t in range(num_steps):
            current_time = t * dt
            spikes = ((spike_times <= current_time) & 
                     (spike_times > current_time - dt)).float()
            spike_trains.append(spikes)
        
        return torch.stack(spike_trains)


class SpikeDecoder:
    """
    Decodes spike trains back to continuous values.
    """
    
    def __init__(self, decoding_method: str = "rate"):
        self.decoding_method = decoding_method
        
    def decode(
        self,
        spike_trains: torch.Tensor,
        dt: float = 1.0,
        **kwargs
    ) -> torch.Tensor:
        """
        Decode spike trains to continuous values.
        
        Args:
            spike_trains: Spike trains of shape (time_steps, ...)
            dt: Time step
            **kwargs: Additional decoding parameters
            
        Returns:
            Decoded continuous values
        """
        if self.decoding_method == "rate":
            return self._rate_decode(spike_trains, dt, **kwargs)
        elif self.decoding_method == "first_spike":
            return self._first_spike_decode(spike_trains, dt, **kwargs)
        elif self.decoding_method == "spike_count":
            return self._spike_count_decode(spike_trains, **kwargs)
        elif self.decoding_method == "weighted_sum":
            return self._weighted_sum_decode(spike_trains, dt, **kwargs)
        elif self.decoding_method == "kernel":
            return self._kernel_decode(spike_trains, dt, **kwargs)
        else:
            raise ValueError(f"Unknown decoding method: {self.decoding_method}")
    
    def _rate_decode(
        self,
        spike_trains: torch.Tensor,
        dt: float,
        window_size: Optional[int] = None
    ) -> torch.Tensor:
        """
        Decode using average firing rate.
        
        Args:
            spike_trains: Spike trains
            dt: Time step
            window_size: Sliding window size (use all if None)
            
        Returns:
            Firing rates
        """
        if window_size is None:
            # Use entire spike train
            spike_count = spike_trains.sum(dim=0)
            time_window = spike_trains.shape[0] * dt
            return spike_count / time_window * 1000  # Convert to Hz
        else:
            # Sliding window
            rates = []
            for i in range(spike_trains.shape[0] - window_size + 1):
                window = spike_trains[i:i+window_size]
                spike_count = window.sum(dim=0)
                rate = spike_count / (window_size * dt) * 1000
                rates.append(rate)
            
            return torch.stack(rates).mean(dim=0)
    
    def _first_spike_decode(
        self,
        spike_trains: torch.Tensor,
        dt: float,
        max_latency: float = 50.0
    ) -> torch.Tensor:
        """
        Decode using time to first spike.
        
        Args:
            spike_trains: Spike trains
            dt: Time step
            max_latency: Maximum latency to consider
            
        Returns:
            Decoded values based on latency
        """
        # Find first spike time for each neuron
        first_spike_indices = torch.argmax(spike_trains, dim=0)
        
        # Convert to time
        first_spike_times = first_spike_indices.float() * dt
        
        # No spike case
        no_spike_mask = spike_trains.sum(dim=0) == 0
        first_spike_times[no_spike_mask] = max_latency
        
        # Convert latency to value (inverse relationship)
        values = 1.0 - (first_spike_times / max_latency)
        
        return values
    
    def _spike_count_decode(
        self,
        spike_trains: torch.Tensor,
        normalize: bool = True
    ) -> torch.Tensor:
        """
        Decode using total spike count.
        
        Args:
            spike_trains: Spike trains
            normalize: Whether to normalize by maximum possible count
            
        Returns:
            Spike counts
        """
        counts = spike_trains.sum(dim=0)
        
        if normalize:
            max_count = spike_trains.shape[0]
            counts = counts / max_count
        
        return counts
    
    def _weighted_sum_decode(
        self,
        spike_trains: torch.Tensor,
        dt: float,
        decay_constant: float = 20.0
    ) -> torch.Tensor:
        """
        Decode using exponentially weighted sum.
        
        Args:
            spike_trains: Spike trains
            dt: Time step
            decay_constant: Exponential decay time constant
            
        Returns:
            Weighted sum values
        """
        num_steps = spike_trains.shape[0]
        
        # Create exponential weights (recent spikes weighted more)
        times = torch.arange(num_steps) * dt
        weights = torch.exp(-times / decay_constant)
        weights = weights.flip(0)  # Recent spikes first
        
        # Apply weights
        weighted_trains = spike_trains * weights.unsqueeze(-1)
        
        return weighted_trains.sum(dim=0)
    
    def _kernel_decode(
        self,
        spike_trains: torch.Tensor,
        dt: float,
        kernel_type: str = "exponential",
        kernel_width: float = 10.0
    ) -> torch.Tensor:
        """
        Decode using kernel convolution.
        
        Args:
            spike_trains: Spike trains
            dt: Time step
            kernel_type: Type of kernel ("exponential", "alpha", "gaussian")
            kernel_width: Kernel time constant
            
        Returns:
            Filtered spike trains
        """
        num_steps = spike_trains.shape[0]
        times = torch.arange(num_steps) * dt
        
        # Create kernel
        if kernel_type == "exponential":
            kernel = torch.exp(-times / kernel_width)
        elif kernel_type == "alpha":
            kernel = times * torch.exp(-times / kernel_width)
            kernel = kernel / kernel.max()  # Normalize
        elif kernel_type == "gaussian":
            kernel = torch.exp(-times**2 / (2 * kernel_width**2))
        else:
            raise ValueError(f"Unknown kernel type: {kernel_type}")
        
        # Convolve with spike trains
        filtered = torch.nn.functional.conv1d(
            spike_trains.transpose(0, -1).unsqueeze(1),
            kernel.flip(0).unsqueeze(0).unsqueeze(0),
            padding=num_steps-1
        )
        
        # Take the final value
        return filtered[..., num_steps-1].squeeze(1).transpose(0, -1) 