"""
Spiking Neural Network Core Implementation
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
import brian2 as b2
from brian2 import *
import torch
import torch.nn as nn
from dataclasses import dataclass
import json
from datetime import datetime

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

@dataclass
class SNNConfig:
    """Configuration for Spiking Neural Network"""
    input_size: int = 1000
    reservoir_size: int = 10000
    output_size: int = 100
    connectivity: float = 0.1
    spectral_radius: float = 0.9
    tau_membrane: float = 20.0  # ms
    tau_synapse: float = 5.0    # ms
    v_threshold: float = 1.0
    v_reset: float = 0.0
    refractory: float = 2.0     # ms
    dt: float = 0.1            # ms
    learning_rate: float = 0.01

class SpikingNeuralNetwork:
    """
    Main Spiking Neural Network implementation using Brian2
    """
    
    def __init__(self, 
                 input_size: int,
                 reservoir_size: int,
                 output_size: int,
                 connectivity: float = 0.1,
                 spectral_radius: float = 0.9,
                 config: Optional[SNNConfig] = None):
        
        self.config = config or SNNConfig(
            input_size=input_size,
            reservoir_size=reservoir_size,
            output_size=output_size,
            connectivity=connectivity,
            spectral_radius=spectral_radius
        )
        
        self.events_processed = 0
        self.spike_history = []
        self.recent_spikes = None
        
        # Initialize Brian2 components
        self._initialize_network()
        
        logger.info(f"Initialized SNN with {input_size} inputs, "
                   f"{reservoir_size} reservoir neurons, {output_size} outputs")
        
    def _initialize_network(self):
        """Initialize the Brian2 network components"""
        
        # Set Brian2 preferences for better performance
        b2.prefs.codegen.target = 'numpy'
        b2.start_scope()
        
        # Neuron model equations (Leaky Integrate-and-Fire)
        eqs = '''
        dv/dt = (v_rest - v + I_syn) / tau_m : volt (unless refractory)
        dI_syn/dt = -I_syn / tau_s : volt
        tau_m : second
        tau_s : second
        v_rest : volt
        '''
        
        # Create neuron groups
        self.input_neurons = b2.PoissonGroup(
            self.config.input_size, 
            rates=0*Hz,
            name='input'
        )
        
        self.reservoir = b2.NeuronGroup(
            self.config.reservoir_size,
            eqs,
            threshold='v > v_threshold',
            reset='v = v_reset',
            refractory=self.config.refractory*ms,
            method='euler',
            name='reservoir'
        )
        
        self.output_neurons = b2.NeuronGroup(
            self.config.output_size,
            eqs,
            threshold='v > v_threshold',
            reset='v = v_reset',
            refractory=self.config.refractory*ms,
            method='euler',
            name='output'
        )
        
        # Set parameters
        self.reservoir.tau_m = self.config.tau_membrane * ms
        self.reservoir.tau_s = self.config.tau_synapse * ms
        self.reservoir.v_rest = self.config.v_reset * volt
        self.reservoir.v = self.config.v_reset * volt
        
        self.output_neurons.tau_m = self.config.tau_membrane * ms
        self.output_neurons.tau_s = self.config.tau_synapse * ms
        self.output_neurons.v_rest = self.config.v_reset * volt
        self.output_neurons.v = self.config.v_reset * volt
        
        # Create synapses
        self._create_synapses()
        
        # Create monitors
        self.spike_monitor_res = b2.SpikeMonitor(self.reservoir)
        self.spike_monitor_out = b2.SpikeMonitor(self.output_neurons)
        self.state_monitor = b2.StateMonitor(
            self.output_neurons, 
            'v', 
            record=True
        )
        
        # Create network
        self.network = b2.Network(
            self.input_neurons,
            self.reservoir,
            self.output_neurons,
            self.syn_input_reservoir,
            self.syn_reservoir_reservoir,
            self.syn_reservoir_output,
            self.spike_monitor_res,
            self.spike_monitor_out,
            self.state_monitor
        )
        
    def _create_synapses(self):
        """Create synaptic connections with proper weights"""
        
        # Input to reservoir connections
        self.syn_input_reservoir = b2.Synapses(
            self.input_neurons,
            self.reservoir,
            'w : volt',
            on_pre='I_syn += w'
        )
        self.syn_input_reservoir.connect(p=self.config.connectivity)
        self.syn_input_reservoir.w = np.random.randn(len(self.syn_input_reservoir)) * 0.1 * mV
        
        # Recurrent reservoir connections
        self.syn_reservoir_reservoir = b2.Synapses(
            self.reservoir,
            self.reservoir,
            'w : volt',
            on_pre='I_syn += w'
        )
        self.syn_reservoir_reservoir.connect(p=self.config.connectivity)
        
        # Initialize reservoir weights with spectral radius normalization
        W = np.random.randn(len(self.syn_reservoir_reservoir)) * 0.1
        # Normalize to desired spectral radius
        W = W * (self.config.spectral_radius / np.max(np.abs(np.linalg.eigvals(
            self._weight_matrix_from_synapses(W)))))
        self.syn_reservoir_reservoir.w = W * mV
        
        # Reservoir to output connections
        self.syn_reservoir_output = b2.Synapses(
            self.reservoir,
            self.output_neurons,
            '''
            w : volt
            dApre/dt = -Apre / tau_pre : 1 (event-driven)
            dApost/dt = -Apost / tau_post : 1 (event-driven)
            ''',
            on_pre='''
            I_syn += w
            Apre += 1.
            w += learning_rate * Apost * volt
            ''',
            on_post='''
            Apost += 1.
            w += learning_rate * Apre * volt
            '''
        )
        self.syn_reservoir_output.connect(p=0.1)
        self.syn_reservoir_output.w = np.random.randn(len(self.syn_reservoir_output)) * 0.01 * mV
        
        # STDP parameters
        self.syn_reservoir_output.tau_pre = 20 * ms
        self.syn_reservoir_output.tau_post = 20 * ms
        self.syn_reservoir_output.learning_rate = self.config.learning_rate
        
    def _weight_matrix_from_synapses(self, weights):
        """Convert synapse weights to matrix form for spectral radius calculation"""
        W = np.zeros((self.config.reservoir_size, self.config.reservoir_size))
        sources = self.syn_reservoir_reservoir.i
        targets = self.syn_reservoir_reservoir.j
        for idx, (i, j) in enumerate(zip(sources, targets)):
            W[j, i] = weights[idx]
        return W
        
    def process(self, spike_train: np.ndarray) -> np.ndarray:
        """
        Process input spike train through the network
        
        Args:
            spike_train: Input spike train array
            
        Returns:
            Output spike pattern
        """
        self.events_processed += 1
        
        # Convert input to firing rates
        rates = spike_train * 100 * Hz  # Scale to reasonable firing rates
        self.input_neurons.rates = rates
        
        # Run simulation
        self.network.run(100 * ms)
        
        # Extract output spikes
        output_spikes = self._extract_output_spikes()
        
        # Store recent spikes for visualization
        self.recent_spikes = {
            'reservoir': self.spike_monitor_res.spike_trains(),
            'output': self.spike_monitor_out.spike_trains(),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Update spike history
        self.spike_history.append(output_spikes)
        if len(self.spike_history) > 1000:
            self.spike_history.pop(0)
            
        return output_spikes
        
    def _extract_output_spikes(self) -> np.ndarray:
        """Extract and normalize output spike pattern"""
        spike_counts = np.zeros(self.config.output_size)
        
        for neuron_id in range(self.config.output_size):
            if neuron_id in self.spike_monitor_out.spike_trains():
                spike_counts[neuron_id] = len(
                    self.spike_monitor_out.spike_trains()[neuron_id]
                )
                
        # Normalize
        if spike_counts.max() > 0:
            spike_counts = spike_counts / spike_counts.max()
            
        return spike_counts
        
    def get_spike_rate(self) -> float:
        """Get average spike rate across the network"""
        total_spikes = self.spike_monitor_res.num_spikes + self.spike_monitor_out.num_spikes
        total_time = self.network.t / second
        
        if total_time > 0:
            return total_spikes / total_time
        return 0.0
        
    def get_recent_spikes(self, window_ms: int = 100) -> Dict:
        """Get recent spike activity for visualization"""
        if not self.recent_spikes:
            return {'reservoir': {}, 'output': {}}
            
        # Filter spikes within time window
        current_time = self.network.t / ms
        window_start = max(0, current_time - window_ms)
        
        filtered_spikes = {
            'reservoir': {},
            'output': {},
            'window': [window_start, current_time]
        }
        
        # Filter reservoir spikes
        for neuron_id, spike_times in self.recent_spikes['reservoir'].items():
            recent = spike_times[spike_times >= window_start]
            if len(recent) > 0:
                filtered_spikes['reservoir'][int(neuron_id)] = recent.tolist()
                
        # Filter output spikes  
        for neuron_id, spike_times in self.recent_spikes['output'].items():
            recent = spike_times[spike_times >= window_start]
            if len(recent) > 0:
                filtered_spikes['output'][int(neuron_id)] = recent.tolist()
                
        return filtered_spikes
        
    def get_firing_rates(self) -> Dict[str, List[float]]:
        """Get current firing rates for all neurons"""
        rates = {
            'reservoir': [],
            'output': []
        }
        
        # Calculate reservoir firing rates
        for i in range(self.config.reservoir_size):
            if i in self.spike_monitor_res.spike_trains():
                spikes = self.spike_monitor_res.spike_trains()[i]
                if len(spikes) > 0:
                    # Rate over last 50ms
                    recent_spikes = spikes[spikes > (self.network.t/ms - 50)]
                    rate = len(recent_spikes) * 20  # Convert to Hz
                    rates['reservoir'].append(rate)
                else:
                    rates['reservoir'].append(0)
            else:
                rates['reservoir'].append(0)
                
        # Calculate output firing rates
        for i in range(self.config.output_size):
            if i in self.spike_monitor_out.spike_trains():
                spikes = self.spike_monitor_out.spike_trains()[i]
                if len(spikes) > 0:
                    recent_spikes = spikes[spikes > (self.network.t/ms - 50)]
                    rate = len(recent_spikes) * 20
                    rates['output'].append(rate)
                else:
                    rates['output'].append(0)
            else:
                rates['output'].append(0)
                
        return rates
        
    def save_state(self) -> Dict:
        """Save current network state"""
        return {
            'config': self.config.__dict__,
            'events_processed': self.events_processed,
            'network_time': self.network.t / ms,
            'weights': {
                'input_reservoir': self.syn_input_reservoir.w[:].tolist(),
                'reservoir_reservoir': self.syn_reservoir_reservoir.w[:].tolist(),
                'reservoir_output': self.syn_reservoir_output.w[:].tolist()
            }
        }
        
    def load_state(self, state: Dict):
        """Load network state"""
        self.events_processed = state['events_processed']
        
        # Restore weights
        if 'weights' in state:
            self.syn_input_reservoir.w = state['weights']['input_reservoir'] * volt
            self.syn_reservoir_reservoir.w = state['weights']['reservoir_reservoir'] * volt
            self.syn_reservoir_output.w = state['weights']['reservoir_output'] * volt
            
        logger.info(f"Loaded SNN state with {self.events_processed} events processed")


class ReservoirComputingSNN:
    """
    Alternative implementation using reservoir computing principles
    Faster but less biologically realistic
    """
    
    def __init__(self, 
                 input_size: int,
                 reservoir_size: int,
                 output_size: int,
                 spectral_radius: float = 0.9,
                 sparsity: float = 0.1):
        
        self.input_size = input_size
        self.reservoir_size = reservoir_size
        self.output_size = output_size
        self.spectral_radius = spectral_radius
        self.sparsity = sparsity
        
        # Initialize weights
        self._initialize_weights()
        
        # State variables
        self.reservoir_state = np.zeros(reservoir_size)
        self.events_processed = 0
        
    def _initialize_weights(self):
        """Initialize weight matrices"""
        # Input weights
        self.W_in = np.random.randn(self.reservoir_size, self.input_size) * 0.1
        
        # Reservoir weights (sparse)
        W = np.random.randn(self.reservoir_size, self.reservoir_size)
        mask = np.random.rand(self.reservoir_size, self.reservoir_size) < self.sparsity
        W = W * mask
        
        # Normalize spectral radius
        eigenvalues = np.linalg.eigvals(W)
        W = W * (self.spectral_radius / np.max(np.abs(eigenvalues)))
        self.W_res = W
        
        # Output weights (trainable)
        self.W_out = np.random.randn(self.output_size, self.reservoir_size) * 0.01
        
    def process(self, input_data: np.ndarray) -> np.ndarray:
        """Process input through reservoir"""
        # Update reservoir state
        self.reservoir_state = np.tanh(
            np.dot(self.W_in, input_data) + 
            np.dot(self.W_res, self.reservoir_state)
        )
        
        # Compute output
        output = np.dot(self.W_out, self.reservoir_state)
        
        self.events_processed += 1
        return output
        
    def train_output_layer(self, 
                          inputs: List[np.ndarray],
                          targets: List[np.ndarray],
                          regularization: float = 1e-4):
        """Train output weights using ridge regression"""
        # Collect reservoir states
        states = []
        for inp in inputs:
            self.process(inp)
            states.append(self.reservoir_state.copy())
            
        X = np.array(states)
        Y = np.array(targets)
        
        # Ridge regression
        XTX = np.dot(X.T, X)
        XTY = np.dot(X.T, Y)
        self.W_out = np.dot(
            np.linalg.inv(XTX + regularization * np.eye(self.reservoir_size)),
            XTY
        ).T
        
    def get_spike_rate(self) -> float:
        """Estimate spike rate from reservoir activity"""
        return np.mean(np.abs(self.reservoir_state)) * 100  # Approximate Hz 