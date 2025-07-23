"""
Individual neuron model implementations for spiking neural networks.
"""

import torch
import torch.nn as nn
from typing import Tuple, Optional
from abc import ABC, abstractmethod

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class BaseNeuron(nn.Module, ABC):
    """Base class for all neuron models."""
    
    def __init__(self, size: int, config: dict):
        super().__init__()
        self.size = size
        self.config = config
        self.reset()
    
    @abstractmethod
    def reset(self):
        """Reset neuron state."""
        pass
    
    @abstractmethod
    def forward(
        self,
        input_current: torch.Tensor,
        dt: float
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Update neuron state and generate spikes.
        
        Args:
            input_current: Input current to neurons
            dt: Time step
            
        Returns:
            Tuple of (spikes, membrane_potential)
        """
        pass


class LeakyIntegrateFireNeuron(BaseNeuron):
    """
    Leaky Integrate-and-Fire (LIF) neuron model.
    
    The simplest and most commonly used spiking neuron model.
    """
    
    def __init__(self, size: int, config: dict):
        super().__init__(size, config)
        
        # LIF parameters
        self.tau_m = config.get('membrane_time_constant', 20.0)  # ms
        self.v_thresh = config.get('spike_threshold', 1.0)
        self.v_reset = config.get('reset_potential', 0.0)
        self.v_rest = config.get('resting_potential', 0.0)
        self.refractory_period = config.get('refractory_period', 2.0)  # ms
        
        # Learnable parameters
        self.tau_m_param = nn.Parameter(torch.full((size,), self.tau_m))
        self.v_thresh_param = nn.Parameter(torch.full((size,), self.v_thresh))
        
    def reset(self):
        """Reset neuron state."""
        self.v_mem = torch.zeros(self.size)
        self.refractory_timer = torch.zeros(self.size)
        self.spike_count = torch.zeros(self.size)
        
    def forward(
        self,
        input_current: torch.Tensor,
        dt: float = 1.0
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """LIF dynamics."""
        # Decay factor
        decay = torch.exp(-dt / self.tau_m_param)
        
        # Update membrane potential for non-refractory neurons
        active_mask = self.refractory_timer <= 0
        self.v_mem = torch.where(
            active_mask,
            self.v_mem * decay + input_current * (1 - decay),
            self.v_reset
        )
        
        # Generate spikes
        spikes = (self.v_mem >= self.v_thresh_param).float()
        
        # Reset spiked neurons
        self.v_mem = torch.where(spikes > 0, self.v_reset, self.v_mem)
        
        # Update refractory period
        self.refractory_timer = torch.where(
            spikes > 0,
            torch.full_like(self.refractory_timer, self.refractory_period),
            torch.maximum(self.refractory_timer - dt, torch.zeros_like(self.refractory_timer))
        )
        
        # Update spike count
        self.spike_count += spikes
        
        return spikes, self.v_mem


class IzhikevichNeuron(BaseNeuron):
    """
    Izhikevich neuron model.
    
    More biologically realistic than LIF while still computationally efficient.
    Can reproduce various neuron firing patterns.
    """
    
    def __init__(self, size: int, config: dict):
        super().__init__(size, config)
        
        # Izhikevich parameters (default: regular spiking)
        self.a = config.get('a', 0.02)  # Recovery time scale
        self.b = config.get('b', 0.2)   # Recovery sensitivity
        self.c = config.get('c', -65.0) # Reset voltage (mV)
        self.d = config.get('d', 8.0)   # Recovery reset
        
        # Voltage threshold
        self.v_thresh = 30.0  # mV
        
        # Make parameters learnable
        self.a_param = nn.Parameter(torch.full((size,), self.a))
        self.b_param = nn.Parameter(torch.full((size,), self.b))
        self.c_param = nn.Parameter(torch.full((size,), self.c))
        self.d_param = nn.Parameter(torch.full((size,), self.d))
        
    def reset(self):
        """Reset neuron state."""
        self.v = torch.full((self.size,), -65.0)  # Membrane potential (mV)
        self.u = self.b * self.v  # Recovery variable
        self.spike_count = torch.zeros(self.size)
        
    def forward(
        self,
        input_current: torch.Tensor,
        dt: float = 0.5
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Izhikevich neuron dynamics."""
        # Scale input current (pA to appropriate range)
        I = input_current * 100
        
        # Update membrane potential and recovery variable
        v_dot = 0.04 * self.v**2 + 5 * self.v + 140 - self.u + I
        u_dot = self.a_param * (self.b_param * self.v - self.u)
        
        self.v = self.v + dt * v_dot
        self.u = self.u + dt * u_dot
        
        # Generate spikes
        spikes = (self.v >= self.v_thresh).float()
        
        # Reset spiked neurons
        self.v = torch.where(spikes > 0, self.c_param, self.v)
        self.u = torch.where(spikes > 0, self.u + self.d_param, self.u)
        
        # Update spike count
        self.spike_count += spikes
        
        # Return normalized membrane potential
        v_normalized = (self.v + 65) / 95  # Normalize to ~[0, 1]
        
        return spikes, v_normalized


class HodgkinHuxleyNeuron(BaseNeuron):
    """
    Hodgkin-Huxley neuron model.
    
    The most biologically accurate model, but computationally expensive.
    Models ion channel dynamics explicitly.
    """
    
    def __init__(self, size: int, config: dict):
        super().__init__(size, config)
        
        # HH parameters
        self.C_m = config.get('membrane_capacitance', 1.0)  # μF/cm²
        self.g_Na = config.get('sodium_conductance', 120.0)  # mS/cm²
        self.g_K = config.get('potassium_conductance', 36.0)  # mS/cm²
        self.g_L = config.get('leak_conductance', 0.3)  # mS/cm²
        
        # Reversal potentials (mV)
        self.E_Na = config.get('sodium_reversal', 50.0)
        self.E_K = config.get('potassium_reversal', -77.0)
        self.E_L = config.get('leak_reversal', -54.4)
        
        # Spike threshold
        self.v_thresh = config.get('spike_threshold', 0.0)
        
        # Make key parameters learnable
        self.g_Na_param = nn.Parameter(torch.full((size,), self.g_Na))
        self.g_K_param = nn.Parameter(torch.full((size,), self.g_K))
        
    def reset(self):
        """Reset neuron state."""
        self.v = torch.full((self.size,), -65.0)  # Membrane potential
        self.m = torch.full((self.size,), 0.05)   # Sodium activation
        self.h = torch.full((self.size,), 0.6)    # Sodium inactivation
        self.n = torch.full((self.size,), 0.32)   # Potassium activation
        self.spike_count = torch.zeros(self.size)
        self.last_spike_v = torch.full((self.size,), -65.0)
        
    def _alpha_m(self, v: torch.Tensor) -> torch.Tensor:
        """Sodium activation rate."""
        return 0.1 * (v + 40) / (1 - torch.exp(-(v + 40) / 10))
    
    def _beta_m(self, v: torch.Tensor) -> torch.Tensor:
        """Sodium deactivation rate."""
        return 4 * torch.exp(-(v + 65) / 18)
    
    def _alpha_h(self, v: torch.Tensor) -> torch.Tensor:
        """Sodium inactivation rate."""
        return 0.07 * torch.exp(-(v + 65) / 20)
    
    def _beta_h(self, v: torch.Tensor) -> torch.Tensor:
        """Sodium deinactivation rate."""
        return 1 / (1 + torch.exp(-(v + 35) / 10))
    
    def _alpha_n(self, v: torch.Tensor) -> torch.Tensor:
        """Potassium activation rate."""
        return 0.01 * (v + 55) / (1 - torch.exp(-(v + 55) / 10))
    
    def _beta_n(self, v: torch.Tensor) -> torch.Tensor:
        """Potassium deactivation rate."""
        return 0.125 * torch.exp(-(v + 65) / 80)
    
    def forward(
        self,
        input_current: torch.Tensor,
        dt: float = 0.01
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Hodgkin-Huxley dynamics."""
        # Scale input current
        I = input_current * 10  # μA/cm²
        
        # Calculate rate constants
        am = self._alpha_m(self.v)
        bm = self._beta_m(self.v)
        ah = self._alpha_h(self.v)
        bh = self._beta_h(self.v)
        an = self._alpha_n(self.v)
        bn = self._beta_n(self.v)
        
        # Update gating variables
        self.m = self.m + dt * (am * (1 - self.m) - bm * self.m)
        self.h = self.h + dt * (ah * (1 - self.h) - bh * self.h)
        self.n = self.n + dt * (an * (1 - self.n) - bn * self.n)
        
        # Calculate currents
        I_Na = self.g_Na_param * self.m**3 * self.h * (self.v - self.E_Na)
        I_K = self.g_K_param * self.n**4 * (self.v - self.E_K)
        I_L = self.g_L * (self.v - self.E_L)
        
        # Update membrane potential
        dv_dt = (I - I_Na - I_K - I_L) / self.C_m
        self.v = self.v + dt * dv_dt
        
        # Detect spikes (threshold crossing with refractory check)
        spikes = ((self.v >= self.v_thresh) & 
                  (self.last_spike_v < self.v_thresh)).float()
        
        self.last_spike_v = self.v.clone()
        self.spike_count += spikes
        
        # Normalize membrane potential for output
        v_normalized = (self.v + 80) / 130  # Normalize to ~[0, 1]
        
        return spikes, v_normalized 