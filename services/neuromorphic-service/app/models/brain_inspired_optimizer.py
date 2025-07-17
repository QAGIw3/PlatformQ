"""
Brain-Inspired Optimization Algorithms

Implements neuromorphic optimization algorithms inspired by brain processes:
- Spiking Neural Network optimization
- Quantum-inspired annealing
- Swarm intelligence with neural dynamics
- Hebbian learning optimization
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Callable
import logging
from dataclasses import dataclass
from collections import deque
import asyncio
from scipy.optimize import minimize
from scipy.special import expit  # Sigmoid function

from .spiking_neural_network import SpikingNeuralNetwork, SpikeTrain

logger = logging.getLogger(__name__)


@dataclass
class OptimizationProblem:
    """Defines an optimization problem"""
    objective_function: Callable[[np.ndarray], float]
    constraints: List[Dict[str, Any]]
    bounds: List[Tuple[float, float]]
    dimension: int
    problem_type: str  # 'minimize' or 'maximize'
    metadata: Dict[str, Any] = None


@dataclass
class OptimizationResult:
    """Result of optimization"""
    solution: np.ndarray
    objective_value: float
    iterations: int
    convergence_history: List[float]
    spike_activity: Optional[Dict[str, Any]] = None
    energy_consumed: float = 0.0
    success: bool = True
    message: str = ""


class BrainInspiredOptimizer:
    """
    Brain-inspired optimization using neuromorphic principles
    """
    
    def __init__(self,
                 neuron_count: int = 1000,
                 connectivity: float = 0.1,
                 learning_rate: float = 0.01):
        """
        Initialize brain-inspired optimizer
        
        Args:
            neuron_count: Number of neurons in the network
            connectivity: Connectivity probability between neurons
            learning_rate: Learning rate for weight updates
        """
        self.neuron_count = neuron_count
        self.connectivity = connectivity
        self.learning_rate = learning_rate
        
        # Initialize neural substrate
        self._init_neural_substrate()
        
        # Optimization history
        self.optimization_history = deque(maxlen=1000)
        
    def _init_neural_substrate(self):
        """Initialize the neural substrate for optimization"""
        # Create weight matrix with sparse connectivity
        self.weights = np.random.randn(self.neuron_count, self.neuron_count) * 0.1
        
        # Apply connectivity mask
        mask = np.random.random((self.neuron_count, self.neuron_count)) < self.connectivity
        self.weights *= mask
        
        # No self-connections
        np.fill_diagonal(self.weights, 0)
        
        # Initialize neuron states
        self.membrane_potentials = np.zeros(self.neuron_count)
        self.spike_history = deque(maxlen=1000)
        
        # Neuron parameters
        self.tau_membrane = 10.0  # ms
        self.threshold = 1.0
        self.refractory_period = 2.0  # ms
        self.refractory_timers = np.zeros(self.neuron_count)
        
    async def optimize(self,
                      problem: OptimizationProblem,
                      method: str = "spiking_annealing",
                      max_iterations: int = 1000) -> OptimizationResult:
        """
        Solve optimization problem using brain-inspired methods
        
        Args:
            problem: Optimization problem definition
            method: Optimization method to use
            max_iterations: Maximum iterations
            
        Returns:
            OptimizationResult
        """
        if method == "spiking_annealing":
            return await self._spiking_annealing(problem, max_iterations)
        elif method == "hebbian_optimization":
            return await self._hebbian_optimization(problem, max_iterations)
        elif method == "neural_swarm":
            return await self._neural_swarm_optimization(problem, max_iterations)
        elif method == "quantum_inspired":
            return await self._quantum_inspired_optimization(problem, max_iterations)
        else:
            raise ValueError(f"Unknown optimization method: {method}")
            
    async def _spiking_annealing(self,
                                problem: OptimizationProblem,
                                max_iterations: int) -> OptimizationResult:
        """
        Spiking neural network based simulated annealing
        """
        # Encode problem into neural network
        solution_neurons = problem.dimension * 10  # 10 neurons per dimension
        
        # Initialize SNN for optimization
        snn = SpikingNeuralNetwork(
            input_size=problem.dimension,
            hidden_sizes=[solution_neurons, solution_neurons // 2],
            output_size=problem.dimension,
            neuron_model="Izhikevich",
            learning_rule="R-STDP"
        )
        
        # Initialize solution
        current_solution = np.random.uniform(
            [b[0] for b in problem.bounds],
            [b[1] for b in problem.bounds]
        )
        current_value = problem.objective_function(current_solution)
        
        best_solution = current_solution.copy()
        best_value = current_value
        
        # Temperature schedule
        initial_temp = 1.0
        final_temp = 0.001
        
        convergence_history = []
        spike_activity_log = []
        
        for iteration in range(max_iterations):
            # Calculate temperature
            temp = initial_temp * (final_temp / initial_temp) ** (iteration / max_iterations)
            
            # Encode current solution as spike train
            input_spikes = snn.encode_data(current_solution, encoding="temporal")
            
            # Add noise based on temperature
            noise_spikes = self._generate_noise_spikes(temp, problem.dimension)
            combined_spikes = self._combine_spike_trains(input_spikes, noise_spikes)
            
            # Process through SNN
            output_spikes = snn.process(combined_spikes, duration=50.0)
            
            # Decode new solution
            new_solution = self._decode_solution(output_spikes, problem.bounds)
            
            # Evaluate new solution
            new_value = problem.objective_function(new_solution)
            
            # Acceptance criterion with spike-based probability
            spike_prob = len(output_spikes.spike_times) / (50.0 * problem.dimension)
            acceptance_prob = np.exp(-(new_value - current_value) / temp) * spike_prob
            
            if new_value < current_value or np.random.random() < acceptance_prob:
                current_solution = new_solution
                current_value = new_value
                
                if new_value < best_value:
                    best_solution = new_solution.copy()
                    best_value = new_value
                    
            convergence_history.append(best_value)
            
            # Record spike activity
            if iteration % 100 == 0:
                activity = snn.analyze_activity()
                spike_activity_log.append({
                    "iteration": iteration,
                    "total_spikes": activity["total_spikes"],
                    "synchrony": activity["synchrony"]
                })
                
            # Early stopping
            if len(convergence_history) > 100:
                recent_improvement = abs(convergence_history[-1] - convergence_history[-100])
                if recent_improvement < 1e-6:
                    break
                    
        # Calculate energy consumption
        energy_stats = snn.get_energy_consumption()
        
        return OptimizationResult(
            solution=best_solution,
            objective_value=best_value,
            iterations=iteration + 1,
            convergence_history=convergence_history,
            spike_activity={
                "activity_log": spike_activity_log,
                "final_stats": snn.analyze_activity()
            },
            energy_consumed=energy_stats["total_energy_joules"],
            success=True,
            message="Spiking annealing completed"
        )
        
    async def _hebbian_optimization(self,
                                   problem: OptimizationProblem,
                                   max_iterations: int) -> OptimizationResult:
        """
        Optimization using Hebbian learning principles
        """
        # Initialize population of solutions as neural activations
        population_size = self.neuron_count // problem.dimension
        
        # Each row represents a solution
        population = np.random.uniform(
            [b[0] for b in problem.bounds],
            [b[1] for b in problem.bounds],
            size=(population_size, problem.dimension)
        )
        
        # Evaluate initial population
        fitness = np.array([problem.objective_function(sol) for sol in population])
        
        # Initialize Hebbian weights
        hebbian_weights = np.zeros((problem.dimension, problem.dimension))
        
        convergence_history = []
        best_idx = np.argmin(fitness)
        best_solution = population[best_idx].copy()
        best_value = fitness[best_idx]
        
        for iteration in range(max_iterations):
            # Neural activation based on fitness
            activations = expit(-fitness / np.std(fitness))  # Sigmoid activation
            
            # Hebbian update: neurons that fire together wire together
            for i in range(population_size):
                if activations[i] > 0.5:  # Active neurons
                    # Update weights based on solution correlation
                    outer_prod = np.outer(population[i], population[i])
                    hebbian_weights += self.learning_rate * activations[i] * outer_prod
                    
            # Normalize weights
            hebbian_weights /= (np.linalg.norm(hebbian_weights) + 1e-8)
            
            # Generate new solutions using Hebbian connections
            new_population = []
            
            for i in range(population_size):
                # Select parent based on activation
                parent_idx = np.random.choice(
                    population_size,
                    p=activations / activations.sum()
                )
                
                # Apply Hebbian transformation
                direction = hebbian_weights @ population[parent_idx]
                direction /= (np.linalg.norm(direction) + 1e-8)
                
                # Create new solution
                step_size = 0.1 * (1 - iteration / max_iterations)  # Adaptive step
                new_solution = population[parent_idx] + step_size * direction
                
                # Ensure bounds
                new_solution = np.clip(
                    new_solution,
                    [b[0] for b in problem.bounds],
                    [b[1] for b in problem.bounds]
                )
                
                new_population.append(new_solution)
                
            # Evaluate new population
            new_population = np.array(new_population)
            new_fitness = np.array([problem.objective_function(sol) for sol in new_population])
            
            # Elitism: keep best solutions
            combined_pop = np.vstack([population, new_population])
            combined_fitness = np.concatenate([fitness, new_fitness])
            
            # Select best
            best_indices = np.argsort(combined_fitness)[:population_size]
            population = combined_pop[best_indices]
            fitness = combined_fitness[best_indices]
            
            # Update best
            if fitness[0] < best_value:
                best_solution = population[0].copy()
                best_value = fitness[0]
                
            convergence_history.append(best_value)
            
            # Check convergence
            if len(convergence_history) > 50:
                if np.std(convergence_history[-50:]) < 1e-6:
                    break
                    
        return OptimizationResult(
            solution=best_solution,
            objective_value=best_value,
            iterations=iteration + 1,
            convergence_history=convergence_history,
            spike_activity={
                "hebbian_weight_norm": float(np.linalg.norm(hebbian_weights)),
                "final_activation_mean": float(activations.mean())
            },
            energy_consumed=iteration * 1e-9,  # Estimated energy
            success=True,
            message="Hebbian optimization completed"
        )
        
    async def _neural_swarm_optimization(self,
                                       problem: OptimizationProblem,
                                       max_iterations: int) -> OptimizationResult:
        """
        Swarm optimization with neural dynamics
        """
        # Initialize swarm as spiking neurons
        swarm_size = min(50, self.neuron_count // problem.dimension)
        
        # Particle positions and velocities
        positions = np.random.uniform(
            [b[0] for b in problem.bounds],
            [b[1] for b in problem.bounds],
            size=(swarm_size, problem.dimension)
        )
        
        velocities = np.random.randn(swarm_size, problem.dimension) * 0.1
        
        # Neural states for each particle
        neural_states = np.zeros(swarm_size)
        spike_trains = [deque(maxlen=100) for _ in range(swarm_size)]
        
        # Best positions
        personal_best = positions.copy()
        personal_best_values = np.array([problem.objective_function(p) for p in positions])
        
        global_best_idx = np.argmin(personal_best_values)
        global_best = personal_best[global_best_idx].copy()
        global_best_value = personal_best_values[global_best_idx]
        
        convergence_history = []
        
        # PSO parameters with neural modulation
        w = 0.7  # Inertia
        c1 = 1.5  # Personal best weight
        c2 = 1.5  # Global best weight
        
        for iteration in range(max_iterations):
            # Update neural states based on fitness
            fitness_normalized = (personal_best_values - personal_best_values.min()) / \
                               (personal_best_values.max() - personal_best_values.min() + 1e-8)
            
            # Simulate neural dynamics
            neural_states *= 0.9  # Decay
            neural_states += (1 - fitness_normalized) * 0.5  # Fitness-based input
            
            # Generate spikes
            spikes = neural_states > self.threshold
            neural_states[spikes] = 0  # Reset
            
            for i, spike in enumerate(spikes):
                if spike:
                    spike_trains[i].append(iteration)
                    
            # Calculate spike rates
            spike_rates = np.array([
                len([s for s in train if iteration - s < 10]) / 10.0
                for train in spike_trains
            ])
            
            # Neural modulation of PSO parameters
            neural_mod = 1 + 0.5 * spike_rates
            
            # Update velocities with neural modulation
            r1, r2 = np.random.random(2)
            
            velocities = (w * velocities +
                         c1 * r1 * neural_mod[:, np.newaxis] * (personal_best - positions) +
                         c2 * r2 * (global_best - positions))
                         
            # Limit velocities
            max_vel = np.array([b[1] - b[0] for b in problem.bounds]) * 0.2
            velocities = np.clip(velocities, -max_vel, max_vel)
            
            # Update positions
            positions += velocities
            
            # Ensure bounds
            positions = np.clip(
                positions,
                [b[0] for b in problem.bounds],
                [b[1] for b in problem.bounds]
            )
            
            # Evaluate new positions
            values = np.array([problem.objective_function(p) for p in positions])
            
            # Update personal best
            improved = values < personal_best_values
            personal_best[improved] = positions[improved]
            personal_best_values[improved] = values[improved]
            
            # Update global best
            best_idx = np.argmin(personal_best_values)
            if personal_best_values[best_idx] < global_best_value:
                global_best = personal_best[best_idx].copy()
                global_best_value = personal_best_values[best_idx]
                
            convergence_history.append(global_best_value)
            
            # Adaptive parameters based on neural synchrony
            synchrony = np.std(spike_rates) / (np.mean(spike_rates) + 1e-8)
            w *= (1 - 0.001 * synchrony)  # Reduce inertia with high synchrony
            
        # Calculate total spikes
        total_spikes = sum(len(train) for train in spike_trains)
        
        return OptimizationResult(
            solution=global_best,
            objective_value=global_best_value,
            iterations=max_iterations,
            convergence_history=convergence_history,
            spike_activity={
                "total_spikes": total_spikes,
                "mean_spike_rate": total_spikes / (max_iterations * swarm_size),
                "final_synchrony": float(synchrony)
            },
            energy_consumed=total_spikes * 20e-12,  # 20 pJ per spike
            success=True,
            message="Neural swarm optimization completed"
        )
        
    async def _quantum_inspired_optimization(self,
                                           problem: OptimizationProblem,
                                           max_iterations: int) -> OptimizationResult:
        """
        Quantum-inspired optimization with neural implementation
        """
        # Quantum-inspired representation using neural phase oscillators
        num_qubits = int(np.ceil(np.log2(problem.dimension * 10)))
        
        # Phase angles for each qubit (implemented as neural oscillators)
        phases = np.random.uniform(0, 2 * np.pi, num_qubits)
        frequencies = np.random.uniform(0.5, 2.0, num_qubits)
        
        # Coupling matrix (quantum entanglement analog)
        coupling = np.random.randn(num_qubits, num_qubits) * 0.1
        coupling = (coupling + coupling.T) / 2  # Symmetric
        np.fill_diagonal(coupling, 0)
        
        best_solution = np.random.uniform(
            [b[0] for b in problem.bounds],
            [b[1] for b in problem.bounds]
        )
        best_value = problem.objective_function(best_solution)
        
        convergence_history = []
        phase_coherence = []
        
        dt = 0.1  # Time step
        
        for iteration in range(max_iterations):
            # Update phases (neural oscillator dynamics)
            phase_derivatives = frequencies.copy()
            
            # Coupling effects (quantum entanglement analog)
            for i in range(num_qubits):
                coupling_effect = np.sum(
                    coupling[i] * np.sin(phases - phases[i])
                )
                phase_derivatives[i] += coupling_effect
                
            # Update phases
            phases += phase_derivatives * dt
            phases = phases % (2 * np.pi)
            
            # Measure (collapse) to get solution
            measurement = np.cos(phases) > 0  # Binary measurement
            
            # Decode to solution space
            solution = self._decode_quantum_state(measurement, problem.bounds)
            value = problem.objective_function(solution)
            
            # Update best
            if value < best_value:
                best_solution = solution.copy()
                best_value = value
                
                # Quantum-inspired update: increase coherence
                target_phases = phases.copy()
                coupling *= 1.01  # Strengthen entanglement
                
            convergence_history.append(best_value)
            
            # Calculate phase coherence (quantum coherence analog)
            coherence = np.abs(np.mean(np.exp(1j * phases)))
            phase_coherence.append(coherence)
            
            # Decoherence (noise)
            decoherence_rate = 0.01 * (1 - iteration / max_iterations)
            phases += np.random.normal(0, decoherence_rate, num_qubits)
            
            # Adaptive frequency update
            if iteration % 100 == 0 and iteration > 0:
                # Adjust frequencies based on performance
                improvement = convergence_history[-100] - convergence_history[-1]
                if improvement < 1e-6:
                    frequencies *= np.random.uniform(0.8, 1.2, num_qubits)
                    
        return OptimizationResult(
            solution=best_solution,
            objective_value=best_value,
            iterations=max_iterations,
            convergence_history=convergence_history,
            spike_activity={
                "phase_coherence": phase_coherence,
                "final_coherence": float(phase_coherence[-1]),
                "entanglement_strength": float(np.linalg.norm(coupling))
            },
            energy_consumed=max_iterations * num_qubits * 1e-11,  # Estimated
            success=True,
            message="Quantum-inspired optimization completed"
        )
        
    def _generate_noise_spikes(self,
                              temperature: float,
                              dimension: int) -> SpikeTrain:
        """Generate noise spikes based on temperature"""
        # Higher temperature = more noise spikes
        num_spikes = int(temperature * dimension * 100)
        
        if num_spikes == 0:
            return SpikeTrain(
                neuron_ids=np.array([]),
                spike_times=np.array([])
            )
            
        neuron_ids = np.random.randint(0, dimension, num_spikes)
        spike_times = np.sort(np.random.uniform(0, 50, num_spikes))
        
        return SpikeTrain(
            neuron_ids=neuron_ids,
            spike_times=spike_times
        )
        
    def _combine_spike_trains(self,
                             train1: SpikeTrain,
                             train2: SpikeTrain) -> SpikeTrain:
        """Combine two spike trains"""
        combined_ids = np.concatenate([train1.neuron_ids, train2.neuron_ids])
        combined_times = np.concatenate([train1.spike_times, train2.spike_times])
        
        # Sort by time
        sort_idx = np.argsort(combined_times)
        
        return SpikeTrain(
            neuron_ids=combined_ids[sort_idx],
            spike_times=combined_times[sort_idx]
        )
        
    def _decode_solution(self,
                        spike_train: SpikeTrain,
                        bounds: List[Tuple[float, float]]) -> np.ndarray:
        """Decode spike train to solution vector"""
        dimension = len(bounds)
        solution = np.zeros(dimension)
        
        # Count spikes per dimension
        for i in range(dimension):
            dim_spikes = spike_train.spike_times[spike_train.neuron_ids == i]
            
            if len(dim_spikes) > 0:
                # Use average spike time as encoding
                avg_time = np.mean(dim_spikes)
                # Normalize to [0, 1]
                normalized = avg_time / 50.0  # Max simulation time
                # Scale to bounds
                solution[i] = bounds[i][0] + normalized * (bounds[i][1] - bounds[i][0])
            else:
                # No spikes - use middle of range
                solution[i] = (bounds[i][0] + bounds[i][1]) / 2
                
        return solution
        
    def _decode_quantum_state(self,
                             measurement: np.ndarray,
                             bounds: List[Tuple[float, float]]) -> np.ndarray:
        """Decode quantum measurement to solution"""
        dimension = len(bounds)
        solution = np.zeros(dimension)
        
        # Binary to real encoding
        bits_per_dim = len(measurement) // dimension
        
        for i in range(dimension):
            # Get bits for this dimension
            start_idx = i * bits_per_dim
            end_idx = (i + 1) * bits_per_dim
            bits = measurement[start_idx:end_idx]
            
            # Convert to decimal
            value = 0
            for j, bit in enumerate(bits):
                value += bit * (2 ** j)
                
            # Normalize and scale
            normalized = value / (2 ** bits_per_dim - 1)
            solution[i] = bounds[i][0] + normalized * (bounds[i][1] - bounds[i][0])
            
        return solution
        
    async def solve_qubo(self,
                        Q: np.ndarray,
                        method: str = "spiking_annealing") -> Dict[str, Any]:
        """
        Solve Quadratic Unconstrained Binary Optimization (QUBO) problem
        
        Args:
            Q: QUBO matrix
            method: Solution method
            
        Returns:
            Solution dictionary
        """
        n = Q.shape[0]
        
        # Define QUBO objective
        def qubo_objective(x):
            # Ensure binary
            x_binary = (x > 0.5).astype(int)
            return x_binary @ Q @ x_binary
            
        # Create problem
        problem = OptimizationProblem(
            objective_function=qubo_objective,
            constraints=[],
            bounds=[(0, 1)] * n,
            dimension=n,
            problem_type="minimize",
            metadata={"qubo_matrix": Q}
        )
        
        # Solve
        result = await self.optimize(problem, method=method)
        
        # Convert to binary
        binary_solution = (result.solution > 0.5).astype(int)
        
        return {
            "solution": binary_solution,
            "objective_value": result.objective_value,
            "iterations": result.iterations,
            "spike_activity": result.spike_activity,
            "energy_consumed": result.energy_consumed
        }
        
    def benchmark_against_classical(self,
                                  problem: OptimizationProblem) -> Dict[str, Any]:
        """
        Benchmark neuromorphic optimization against classical methods
        """
        results = {}
        
        # Classical optimization (scipy)
        start_time = time.time()
        
        x0 = np.random.uniform(
            [b[0] for b in problem.bounds],
            [b[1] for b in problem.bounds]
        )
        
        classical_result = minimize(
            problem.objective_function,
            x0,
            bounds=problem.bounds,
            method='L-BFGS-B'
        )
        
        classical_time = time.time() - start_time
        
        results["classical"] = {
            "solution": classical_result.x,
            "objective_value": classical_result.fun,
            "time_seconds": classical_time,
            "iterations": classical_result.nit,
            "energy_joules": classical_time * 100  # ~100W for CPU
        }
        
        # Neuromorphic optimization
        start_time = time.time()
        
        neuro_result = asyncio.run(
            self.optimize(problem, method="spiking_annealing", max_iterations=1000)
        )
        
        neuro_time = time.time() - start_time
        
        results["neuromorphic"] = {
            "solution": neuro_result.solution,
            "objective_value": neuro_result.objective_value,
            "time_seconds": neuro_time,
            "iterations": neuro_result.iterations,
            "energy_joules": neuro_result.energy_consumed
        }
        
        # Comparison
        results["comparison"] = {
            "speedup_factor": classical_time / neuro_time,
            "energy_efficiency_factor": results["classical"]["energy_joules"] / 
                                      results["neuromorphic"]["energy_joules"],
            "solution_quality_ratio": results["neuromorphic"]["objective_value"] /
                                    results["classical"]["objective_value"]
        }
        
        return results 