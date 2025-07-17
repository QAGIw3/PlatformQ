"""
Advanced Spiking Neural Network Implementation

Implements biologically-inspired neural networks with:
- Leaky Integrate-and-Fire (LIF) neurons
- Spike-Timing Dependent Plasticity (STDP)
- Event-based processing
- Energy-efficient computation
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
from dataclasses import dataclass
from collections import deque
import time

# Brian2 for SNN simulation
try:
    from brian2 import *
    BRIAN2_AVAILABLE = True
except ImportError:
    BRIAN2_AVAILABLE = False
    logging.warning("Brian2 not available, using custom implementation")

# PyTorch for neuromorphic algorithms
try:
    import torch
    import torch.nn as nn
    import snntorch as snn
    from snntorch import surrogate
    SNNTORCH_AVAILABLE = True
except ImportError:
    SNNTORCH_AVAILABLE = False
    logging.warning("snnTorch not available")

logger = logging.getLogger(__name__)


@dataclass
class SpikeTrain:
    """Represents a train of spikes from neurons"""
    neuron_ids: np.ndarray
    spike_times: np.ndarray
    
    def get_spike_count(self, time_window: Tuple[float, float]) -> int:
        """Get number of spikes in time window"""
        mask = (self.spike_times >= time_window[0]) & (self.spike_times < time_window[1])
        return np.sum(mask)
    
    def get_firing_rate(self, neuron_id: int, window_size: float = 0.1) -> float:
        """Calculate firing rate for a specific neuron"""
        neuron_spikes = self.spike_times[self.neuron_ids == neuron_id]
        if len(neuron_spikes) < 2:
            return 0.0
        duration = neuron_spikes[-1] - neuron_spikes[0]
        return len(neuron_spikes) / max(duration, window_size)


class SpikingNeuralNetwork:
    """Advanced Spiking Neural Network with multiple neuron models"""
    
    def __init__(self, 
                 input_size: int,
                 hidden_sizes: List[int],
                 output_size: int,
                 neuron_model: str = "LIF",
                 learning_rule: str = "STDP",
                 dt: float = 1.0):  # ms
        """
        Initialize SNN
        
        Args:
            input_size: Number of input neurons
            hidden_sizes: List of hidden layer sizes
            output_size: Number of output neurons
            neuron_model: Type of neuron model (LIF, Izhikevich, AdEx)
            learning_rule: Learning rule (STDP, R-STDP, Hebbian)
            dt: Time step in milliseconds
        """
        self.input_size = input_size
        self.hidden_sizes = hidden_sizes
        self.output_size = output_size
        self.neuron_model = neuron_model
        self.learning_rule = learning_rule
        self.dt = dt
        
        # Initialize network
        if BRIAN2_AVAILABLE and neuron_model in ["LIF", "Izhikevich", "AdEx"]:
            self._init_brian2_network()
        elif SNNTORCH_AVAILABLE:
            self._init_snntorch_network()
        else:
            self._init_custom_network()
            
        # Spike history for analysis
        self.spike_history = deque(maxlen=10000)
        
    def _init_brian2_network(self):
        """Initialize network using Brian2"""
        # Clear any existing network
        b2.start_scope()
        
        # Total number of neurons
        total_neurons = self.input_size + sum(self.hidden_sizes) + self.output_size
        
        # Define neuron model based on type
        if self.neuron_model == "LIF":
            # Leaky Integrate-and-Fire model
            eqs = '''
            dv/dt = (v_rest - v + I) / tau : volt
            I : volt
            tau : second
            v_rest : volt
            v_thresh : volt
            '''
            threshold = 'v > v_thresh'
            reset = 'v = v_rest'
            
        elif self.neuron_model == "Izhikevich":
            # Izhikevich model - more biologically realistic
            eqs = '''
            dv/dt = (0.04*v**2 + 5*v + 140 - u + I)/ms : 1
            du/dt = a*(b*v - u)/ms : 1
            I : 1
            a : 1
            b : 1
            c : 1
            d : 1
            '''
            threshold = 'v > 30'
            reset = '''
            v = c
            u = u + d
            '''
            
        elif self.neuron_model == "AdEx":
            # Adaptive Exponential Integrate-and-Fire
            eqs = '''
            dv/dt = (-g_L*(v-E_L) + g_L*D_T*exp((v-V_T)/D_T) - w + I)/C : volt
            dw/dt = (a*(v-E_L) - w)/tau_w : amp
            I : amp
            C : farad
            g_L : siemens
            E_L : volt
            V_T : volt
            D_T : volt
            a : siemens
            tau_w : second
            b : amp
            V_r : volt
            '''
            threshold = 'v > 0*mV'
            reset = '''
            v = V_r
            w = w + b
            '''
        
        # Create neuron group
        self.neurons = b2.NeuronGroup(
            total_neurons, 
            eqs,
            threshold=threshold,
            reset=reset,
            method='euler'
        )
        
        # Set default parameters
        if self.neuron_model == "LIF":
            self.neurons.v_rest = -70*mV
            self.neurons.v_thresh = -50*mV
            self.neurons.tau = 10*ms
            self.neurons.v = self.neurons.v_rest
            
        elif self.neuron_model == "Izhikevich":
            # Regular spiking parameters
            self.neurons.a = 0.02
            self.neurons.b = 0.2
            self.neurons.c = -65
            self.neurons.d = 8
            self.neurons.v = -65
            self.neurons.u = self.neurons.b * self.neurons.v
            
        # Create synapses with plasticity
        if self.learning_rule == "STDP":
            # Spike-Timing Dependent Plasticity
            synapse_model = '''
            w : 1
            dApre/dt = -Apre/tau_pre : 1 (event-driven)
            dApost/dt = -Apost/tau_post : 1 (event-driven)
            '''
            
            on_pre = '''
            I_post += w
            Apre += dApre_
            w = clip(w + Apost, 0, w_max)
            '''
            
            on_post = '''
            Apost += dApost_
            w = clip(w + Apre, 0, w_max)
            '''
            
        else:
            # Simple static synapses
            synapse_model = 'w : 1'
            on_pre = 'I_post += w'
            on_post = ''
        
        # Create synaptic connections
        self.synapses = b2.Synapses(
            self.neurons, 
            self.neurons,
            synapse_model,
            on_pre=on_pre,
            on_post=on_post
        )
        
        # Connect layers
        self._connect_layers_brian2()
        
        # Create monitors
        self.spike_monitor = b2.SpikeMonitor(self.neurons)
        self.state_monitor = b2.StateMonitor(self.neurons, 'v', record=True)
        
        # Create network
        self.network = b2.Network(
            self.neurons, 
            self.synapses, 
            self.spike_monitor, 
            self.state_monitor
        )
        
    def _connect_layers_brian2(self):
        """Connect layers in Brian2 network"""
        layer_sizes = [self.input_size] + self.hidden_sizes + [self.output_size]
        layer_starts = [sum(layer_sizes[:i]) for i in range(len(layer_sizes))]
        
        # Connect each layer to the next
        for i in range(len(layer_sizes) - 1):
            source_start = layer_starts[i]
            source_end = layer_starts[i+1]
            target_start = layer_starts[i+1]
            target_end = layer_starts[i+2] if i+2 < len(layer_starts) else sum(layer_sizes)
            
            # Full connectivity between layers
            for s in range(source_start, source_end):
                for t in range(target_start, target_end):
                    self.synapses.connect(i=s, j=t)
                    
        # Initialize weights
        self.synapses.w = 'rand() * 0.5'
        
        # Set STDP parameters if using
        if hasattr(self.synapses, 'tau_pre'):
            self.synapses.tau_pre = 20*ms
            self.synapses.tau_post = 20*ms
            self.synapses.dApre_ = 0.01
            self.synapses.dApost_ = -0.012
            self.synapses.w_max = 1.0
            
    def _init_snntorch_network(self):
        """Initialize network using snnTorch"""
        beta = 0.95  # Membrane potential decay rate
        
        # Build layers
        self.layers = nn.ModuleList()
        
        # Input to first hidden
        if self.hidden_sizes:
            self.layers.append(nn.Linear(self.input_size, self.hidden_sizes[0]))
            self.layers.append(snn.Leaky(beta=beta))
            
            # Hidden layers
            for i in range(len(self.hidden_sizes) - 1):
                self.layers.append(nn.Linear(self.hidden_sizes[i], self.hidden_sizes[i+1]))
                self.layers.append(snn.Leaky(beta=beta))
                
            # Last hidden to output
            self.layers.append(nn.Linear(self.hidden_sizes[-1], self.output_size))
            self.layers.append(snn.Leaky(beta=beta))
        else:
            # Direct input to output
            self.layers.append(nn.Linear(self.input_size, self.output_size))
            self.layers.append(snn.Leaky(beta=beta))
            
        # Wrap as sequential
        self.model = nn.Sequential(*self.layers)
        
    def _init_custom_network(self):
        """Initialize custom SNN implementation"""
        # Custom implementation for when libraries aren't available
        layer_sizes = [self.input_size] + self.hidden_sizes + [self.output_size]
        
        # Initialize neurons
        self.membrane_potentials = []
        self.thresholds = []
        self.refractory_periods = []
        
        for size in layer_sizes:
            self.membrane_potentials.append(np.zeros(size))
            self.thresholds.append(np.ones(size) * 1.0)  # Default threshold
            self.refractory_periods.append(np.zeros(size))
            
        # Initialize weights
        self.weights = []
        for i in range(len(layer_sizes) - 1):
            W = np.random.randn(layer_sizes[i], layer_sizes[i+1]) * 0.1
            self.weights.append(W)
            
        # Time constants
        self.tau_membrane = 10.0  # ms
        self.tau_refractory = 5.0  # ms
        
    def encode_data(self, data: np.ndarray, encoding: str = "rate") -> SpikeTrain:
        """
        Encode input data as spike trains
        
        Args:
            data: Input data to encode
            encoding: Encoding method (rate, temporal, delta)
            
        Returns:
            SpikeTrain object
        """
        if encoding == "rate":
            return self._rate_encoding(data)
        elif encoding == "temporal":
            return self._temporal_encoding(data)
        elif encoding == "delta":
            return self._delta_encoding(data)
        else:
            raise ValueError(f"Unknown encoding: {encoding}")
            
    def _rate_encoding(self, data: np.ndarray, duration: float = 100.0) -> SpikeTrain:
        """Rate-based encoding - firing rate proportional to input value"""
        # Normalize data to [0, 1]
        data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
        
        # Maximum firing rate
        max_rate = 100.0  # Hz
        
        neuron_ids = []
        spike_times = []
        
        for i, value in enumerate(data_norm):
            rate = value * max_rate
            if rate > 0:
                # Generate Poisson spike train
                num_spikes = np.random.poisson(rate * duration / 1000.0)
                spike_times_i = np.sort(np.random.uniform(0, duration, num_spikes))
                
                neuron_ids.extend([i] * num_spikes)
                spike_times.extend(spike_times_i)
                
        return SpikeTrain(
            neuron_ids=np.array(neuron_ids),
            spike_times=np.array(spike_times)
        )
        
    def _temporal_encoding(self, data: np.ndarray, duration: float = 100.0) -> SpikeTrain:
        """Temporal encoding - spike timing encodes value"""
        # Normalize data to [0, 1]
        data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
        
        neuron_ids = []
        spike_times = []
        
        for i, value in enumerate(data_norm):
            # Earlier spike = higher value
            spike_time = duration * (1 - value)
            neuron_ids.append(i)
            spike_times.append(spike_time)
            
        return SpikeTrain(
            neuron_ids=np.array(neuron_ids),
            spike_times=np.array(spike_times)
        )
        
    def _delta_encoding(self, data: np.ndarray, threshold: float = 0.1) -> SpikeTrain:
        """Delta encoding - spikes on significant changes"""
        neuron_ids = []
        spike_times = []
        
        # Initialize accumulators
        accumulators = np.zeros_like(data)
        
        for t in range(1, len(data)):
            # Calculate changes
            delta = data[t] - data[t-1]
            accumulators += delta
            
            # Generate spikes where accumulator exceeds threshold
            spike_mask = np.abs(accumulators) > threshold
            spike_indices = np.where(spike_mask)[0]
            
            for idx in spike_indices:
                neuron_ids.append(idx)
                spike_times.append(float(t))
                
            # Reset accumulators that spiked
            accumulators[spike_mask] = 0
            
        return SpikeTrain(
            neuron_ids=np.array(neuron_ids),
            spike_times=np.array(spike_times)
        )
        
    def process(self, spike_train: SpikeTrain, duration: float = 100.0) -> SpikeTrain:
        """
        Process input spike train through the network
        
        Args:
            spike_train: Input spike train
            duration: Simulation duration in ms
            
        Returns:
            Output spike train
        """
        if BRIAN2_AVAILABLE and hasattr(self, 'network'):
            return self._process_brian2(spike_train, duration)
        elif SNNTORCH_AVAILABLE and hasattr(self, 'model'):
            return self._process_snntorch(spike_train, duration)
        else:
            return self._process_custom(spike_train, duration)
            
    def _process_brian2(self, spike_train: SpikeTrain, duration: float) -> SpikeTrain:
        """Process using Brian2 network"""
        # Reset network state
        self.network.restore()
        
        # Create input spike generator
        input_indices = spike_train.neuron_ids[spike_train.neuron_ids < self.input_size]
        input_times = spike_train.spike_times[spike_train.neuron_ids < self.input_size]
        
        spike_generator = b2.SpikeGeneratorGroup(
            self.input_size,
            input_indices,
            input_times * ms
        )
        
        # Connect to input layer
        input_connection = b2.Synapses(
            spike_generator,
            self.neurons,
            on_pre='I_post += 1*mV'
        )
        input_connection.connect(i='i', j='i')
        
        # Run simulation
        temp_network = b2.Network(
            self.network,
            spike_generator,
            input_connection
        )
        temp_network.run(duration * ms)
        
        # Extract output spikes
        output_start = self.input_size + sum(self.hidden_sizes)
        output_mask = self.spike_monitor.i >= output_start
        
        output_neurons = self.spike_monitor.i[output_mask] - output_start
        output_times = self.spike_monitor.t[output_mask] / ms
        
        return SpikeTrain(
            neuron_ids=output_neurons,
            spike_times=output_times
        )
        
    def _process_snntorch(self, spike_train: SpikeTrain, duration: float) -> SpikeTrain:
        """Process using snnTorch network"""
        # Convert spike train to dense tensor
        time_steps = int(duration / self.dt)
        input_spikes = torch.zeros((1, time_steps, self.input_size))
        
        for neuron_id, spike_time in zip(spike_train.neuron_ids, spike_train.spike_times):
            if neuron_id < self.input_size:
                time_idx = int(spike_time / self.dt)
                if time_idx < time_steps:
                    input_spikes[0, time_idx, neuron_id] = 1.0
                    
        # Process through network
        mem_states = []
        spk_states = []
        
        # Initialize hidden states
        for layer in self.layers:
            if isinstance(layer, snn.Leaky):
                mem_states.append(layer.init_leaky())
                
        # Forward pass through time
        output_spikes = []
        for t in range(time_steps):
            x = input_spikes[:, t, :]
            
            layer_idx = 0
            for i, layer in enumerate(self.layers):
                if isinstance(layer, nn.Linear):
                    x = layer(x)
                elif isinstance(layer, snn.Leaky):
                    x, mem_states[layer_idx] = layer(x, mem_states[layer_idx])
                    layer_idx += 1
                    
            output_spikes.append(x)
            
        # Convert output to spike train
        output_spikes = torch.stack(output_spikes, dim=1)
        spike_indices = torch.nonzero(output_spikes[0])
        
        if len(spike_indices) > 0:
            output_times = spike_indices[:, 0].numpy() * self.dt
            output_neurons = spike_indices[:, 1].numpy()
        else:
            output_times = np.array([])
            output_neurons = np.array([])
            
        return SpikeTrain(
            neuron_ids=output_neurons,
            spike_times=output_times
        )
        
    def _process_custom(self, spike_train: SpikeTrain, duration: float) -> SpikeTrain:
        """Process using custom implementation"""
        time_steps = int(duration / self.dt)
        
        # Reset states
        for i in range(len(self.membrane_potentials)):
            self.membrane_potentials[i].fill(0)
            self.refractory_periods[i].fill(0)
            
        output_spikes = []
        output_times = []
        
        # Simulate
        for t in range(time_steps):
            current_time = t * self.dt
            
            # Apply input spikes
            input_mask = (spike_train.spike_times >= current_time) & \
                        (spike_train.spike_times < current_time + self.dt)
            input_neurons = spike_train.neuron_ids[input_mask]
            
            # Process layer by layer
            layer_inputs = np.zeros(self.input_size)
            layer_inputs[input_neurons[input_neurons < self.input_size]] = 1.0
            
            layer_outputs = []
            
            for layer_idx in range(len(self.weights)):
                # Update membrane potentials
                self.membrane_potentials[layer_idx] *= np.exp(-self.dt / self.tau_membrane)
                
                # Add input current
                if layer_idx == 0:
                    self.membrane_potentials[layer_idx] += layer_inputs
                else:
                    self.membrane_potentials[layer_idx] += layer_outputs[-1] @ self.weights[layer_idx-1]
                    
                # Update refractory periods
                self.refractory_periods[layer_idx] = np.maximum(
                    0, self.refractory_periods[layer_idx] - self.dt
                )
                
                # Check for spikes
                can_spike = self.refractory_periods[layer_idx] == 0
                will_spike = (self.membrane_potentials[layer_idx] > self.thresholds[layer_idx]) & can_spike
                
                # Reset spiking neurons
                self.membrane_potentials[layer_idx][will_spike] = 0
                self.refractory_periods[layer_idx][will_spike] = self.tau_refractory
                
                # Record output layer spikes
                if layer_idx == len(self.weights):
                    spike_indices = np.where(will_spike)[0]
                    output_spikes.extend(spike_indices)
                    output_times.extend([current_time] * len(spike_indices))
                    
                layer_outputs.append(will_spike.astype(float))
                
        return SpikeTrain(
            neuron_ids=np.array(output_spikes),
            spike_times=np.array(output_times)
        )
        
    def train(self, 
              input_trains: List[SpikeTrain],
              target_trains: List[SpikeTrain],
              learning_rate: float = 0.01,
              epochs: int = 10):
        """
        Train the network using spike trains
        
        Args:
            input_trains: List of input spike trains
            target_trains: List of corresponding target spike trains
            learning_rate: Learning rate
            epochs: Number of training epochs
        """
        if self.learning_rule == "STDP" and BRIAN2_AVAILABLE:
            # STDP learning is automatic in Brian2
            for epoch in range(epochs):
                for input_train, target_train in zip(input_trains, target_trains):
                    # Process input
                    output_train = self.process(input_train)
                    
                    # STDP will automatically adjust weights
                    logger.info(f"Epoch {epoch}: Output spikes = {len(output_train.spike_times)}")
                    
        elif self.learning_rule == "R-STDP":
            # Reward-modulated STDP
            self._train_rstdp(input_trains, target_trains, learning_rate, epochs)
            
        else:
            # Supervised learning with surrogate gradients
            self._train_supervised(input_trains, target_trains, learning_rate, epochs)
            
    def _train_rstdp(self, 
                     input_trains: List[SpikeTrain],
                     target_trains: List[SpikeTrain],
                     learning_rate: float,
                     epochs: int):
        """Train using Reward-modulated STDP"""
        # R-STDP: STDP modulated by reward signal
        for epoch in range(epochs):
            total_reward = 0
            
            for input_train, target_train in zip(input_trains, target_trains):
                # Process input
                output_train = self.process(input_train)
                
                # Calculate reward based on similarity to target
                reward = self._calculate_spike_train_similarity(output_train, target_train)
                total_reward += reward
                
                # Update weights based on STDP and reward
                if hasattr(self, 'synapses'):
                    # Modulate weight changes by reward
                    self.synapses.w *= (1 + learning_rate * (reward - 0.5))
                    self.synapses.w = np.clip(self.synapses.w, 0, 1)
                    
            logger.info(f"Epoch {epoch}: Average reward = {total_reward / len(input_trains):.4f}")
            
    def _train_supervised(self,
                         input_trains: List[SpikeTrain],
                         target_trains: List[SpikeTrain],
                         learning_rate: float,
                         epochs: int):
        """Train using supervised learning with surrogate gradients"""
        if not SNNTORCH_AVAILABLE:
            logger.warning("Supervised training requires snnTorch")
            return
            
        # Convert to PyTorch tensors
        optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        loss_fn = nn.MSELoss()
        
        for epoch in range(epochs):
            total_loss = 0
            
            for input_train, target_train in zip(input_trains, target_trains):
                # Convert to dense representation
                duration = max(input_train.spike_times.max(), target_train.spike_times.max()) + 10
                
                input_tensor = self._spike_train_to_tensor(input_train, duration)
                target_tensor = self._spike_train_to_tensor(target_train, duration)
                
                # Forward pass
                output_tensor = self._forward_snntorch(input_tensor)
                
                # Calculate loss
                loss = loss_fn(output_tensor, target_tensor)
                
                # Backward pass
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                
            logger.info(f"Epoch {epoch}: Loss = {total_loss / len(input_trains):.4f}")
            
    def _calculate_spike_train_similarity(self, 
                                        train1: SpikeTrain,
                                        train2: SpikeTrain) -> float:
        """Calculate similarity between two spike trains"""
        # Van Rossum distance metric
        tau = 10.0  # ms
        
        # Convolve spike trains with exponential kernel
        max_time = max(
            train1.spike_times.max() if len(train1.spike_times) > 0 else 0,
            train2.spike_times.max() if len(train2.spike_times) > 0 else 0
        ) + 50
        
        time_bins = np.arange(0, max_time, self.dt)
        
        # Create filtered spike trains
        filtered1 = np.zeros(len(time_bins))
        filtered2 = np.zeros(len(time_bins))
        
        for t, time in enumerate(time_bins):
            # Exponential kernel
            kernel1 = np.exp(-(time - train1.spike_times) / tau)
            kernel1[train1.spike_times > time] = 0
            filtered1[t] = kernel1.sum()
            
            kernel2 = np.exp(-(time - train2.spike_times) / tau)
            kernel2[train2.spike_times > time] = 0
            filtered2[t] = kernel2.sum()
            
        # Calculate distance
        distance = np.sqrt(np.mean((filtered1 - filtered2) ** 2))
        
        # Convert to similarity (0 to 1)
        similarity = np.exp(-distance / 10)
        
        return similarity
        
    def analyze_activity(self) -> Dict[str, Any]:
        """Analyze network activity patterns"""
        if not hasattr(self, 'spike_monitor'):
            return {"error": "No spike monitor available"}
            
        analysis = {
            "total_spikes": len(self.spike_monitor.t),
            "duration": float(self.spike_monitor.t[-1]) if len(self.spike_monitor.t) > 0 else 0,
            "firing_rates": {},
            "synchrony": 0,
            "burst_detection": []
        }
        
        # Calculate firing rates per neuron
        unique_neurons = np.unique(self.spike_monitor.i)
        for neuron in unique_neurons:
            neuron_spikes = self.spike_monitor.t[self.spike_monitor.i == neuron]
            if len(neuron_spikes) > 1:
                rate = len(neuron_spikes) / (neuron_spikes[-1] - neuron_spikes[0])
                analysis["firing_rates"][int(neuron)] = float(rate)
                
        # Detect synchrony
        if len(self.spike_monitor.t) > 10:
            time_bins = np.histogram(self.spike_monitor.t, bins=100)[0]
            analysis["synchrony"] = float(np.std(time_bins) / np.mean(time_bins))
            
        # Detect bursts
        analysis["burst_detection"] = self._detect_bursts()
        
        return analysis
        
    def _detect_bursts(self, min_spikes: int = 5, max_isi: float = 10.0) -> List[Dict]:
        """Detect burst patterns in spike trains"""
        bursts = []
        
        if not hasattr(self, 'spike_monitor'):
            return bursts
            
        unique_neurons = np.unique(self.spike_monitor.i)
        
        for neuron in unique_neurons:
            neuron_times = self.spike_monitor.t[self.spike_monitor.i == neuron]
            
            if len(neuron_times) < min_spikes:
                continue
                
            # Find bursts using ISI criterion
            isis = np.diff(neuron_times)
            burst_start = 0
            
            for i, isi in enumerate(isis):
                if isi > max_isi:
                    # End of potential burst
                    if i - burst_start >= min_spikes - 1:
                        bursts.append({
                            "neuron": int(neuron),
                            "start_time": float(neuron_times[burst_start]),
                            "end_time": float(neuron_times[i]),
                            "spike_count": i - burst_start + 1,
                            "mean_rate": (i - burst_start + 1) / 
                                       (neuron_times[i] - neuron_times[burst_start])
                        })
                    burst_start = i + 1
                    
        return bursts
        
    def get_energy_consumption(self) -> Dict[str, float]:
        """Estimate energy consumption of the network"""
        # Neuromorphic computing is extremely energy efficient
        # Estimates based on spike activity
        
        if hasattr(self, 'spike_monitor'):
            num_spikes = len(self.spike_monitor.t)
            duration = self.spike_monitor.t[-1] if len(self.spike_monitor.t) > 0 else 1.0
        else:
            num_spikes = len(self.spike_history)
            duration = 1.0
            
        # Energy model: ~20 pJ per spike (based on neuromorphic hardware)
        energy_per_spike = 20e-12  # Joules
        total_energy = num_spikes * energy_per_spike
        
        # Power consumption
        power = total_energy / (duration / 1000)  # Convert ms to seconds
        
        # Compare to traditional computing
        # Assume equivalent traditional computation would use 1000x more energy
        traditional_energy = total_energy * 1000
        
        return {
            "total_energy_joules": total_energy,
            "average_power_watts": power,
            "energy_per_spike_joules": energy_per_spike,
            "traditional_energy_joules": traditional_energy,
            "energy_efficiency_factor": traditional_energy / total_energy if total_energy > 0 else 0
        }
        
    def save_model(self, filepath: str):
        """Save the trained network"""
        import pickle
        
        model_data = {
            "input_size": self.input_size,
            "hidden_sizes": self.hidden_sizes,
            "output_size": self.output_size,
            "neuron_model": self.neuron_model,
            "learning_rule": self.learning_rule,
            "dt": self.dt
        }
        
        if hasattr(self, 'weights'):
            model_data["weights"] = self.weights
            
        if hasattr(self, 'synapses'):
            model_data["synaptic_weights"] = self.synapses.w[:]
            
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
            
    @classmethod
    def load_model(cls, filepath: str) -> 'SpikingNeuralNetwork':
        """Load a saved network"""
        import pickle
        
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
            
        # Create network
        snn = cls(
            input_size=model_data["input_size"],
            hidden_sizes=model_data["hidden_sizes"],
            output_size=model_data["output_size"],
            neuron_model=model_data["neuron_model"],
            learning_rule=model_data["learning_rule"],
            dt=model_data["dt"]
        )
        
        # Restore weights
        if "weights" in model_data:
            snn.weights = model_data["weights"]
            
        if "synaptic_weights" in model_data and hasattr(snn, 'synapses'):
            snn.synapses.w[:] = model_data["synaptic_weights"]
            
        return snn 