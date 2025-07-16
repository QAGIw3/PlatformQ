"""
Quantum Annealing Algorithm Implementation

Quantum annealing simulation for solving optimization problems by finding
the ground state of an Ising Hamiltonian.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from scipy.sparse import csr_matrix, kron, eye, diags
from scipy.linalg import expm
from scipy.optimize import minimize_scalar
import matplotlib.pyplot as plt
from dataclasses import dataclass
import networkx as nx

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class AnnealingSchedule:
    """Annealing schedule parameters"""
    initial_temp: float = 10.0
    final_temp: float = 0.1
    num_steps: int = 1000
    schedule_type: str = "linear"  # linear, exponential, adaptive
    hold_time: float = 0.0  # Time to hold at final temperature
    
    def get_schedule(self) -> np.ndarray:
        """Generate temperature schedule"""
        if self.schedule_type == "linear":
            temps = np.linspace(self.initial_temp, self.final_temp, self.num_steps)
        elif self.schedule_type == "exponential":
            temps = self.initial_temp * np.exp(
                np.linspace(0, np.log(self.final_temp / self.initial_temp), self.num_steps)
            )
        elif self.schedule_type == "adaptive":
            # Adaptive schedule with more time at critical temperatures
            t = np.linspace(0, 1, self.num_steps)
            temps = self.initial_temp * (1 - t) + self.final_temp * t
            # Add plateau around phase transition
            critical_region = (temps > 1.0) & (temps < 3.0)
            temps[critical_region] *= 0.8  # Slow down in critical region
        else:
            raise ValueError(f"Unknown schedule type: {self.schedule_type}")
            
        # Add hold time at final temperature
        if self.hold_time > 0:
            hold_steps = int(self.hold_time * self.num_steps)
            temps = np.concatenate([temps, np.full(hold_steps, self.final_temp)])
            
        return temps


class QuantumAnnealing:
    """
    Quantum Annealing Simulator
    
    Simulates quantum annealing process to find the ground state of 
    an Ising Hamiltonian. Includes both classical simulated annealing
    and quantum Monte Carlo approaches.
    """
    
    def __init__(self,
                 schedule: Optional[AnnealingSchedule] = None,
                 transverse_field_strength: float = 1.0,
                 use_quantum_fluctuations: bool = True,
                 num_replicas: int = 1):
        """
        Initialize Quantum Annealing
        
        Args:
            schedule: Annealing schedule parameters
            transverse_field_strength: Initial transverse field strength
            use_quantum_fluctuations: Whether to include quantum effects
            num_replicas: Number of parallel replicas for path integral
        """
        self.schedule = schedule or AnnealingSchedule()
        self.transverse_field_strength = transverse_field_strength
        self.use_quantum_fluctuations = use_quantum_fluctuations
        self.num_replicas = num_replicas
        
        # Results tracking
        self.energy_history = []
        self.state_history = []
        self.magnetization_history = []
        self.best_solution = None
        self.best_energy = float('inf')
        
        logger.info(
            f"Initialized Quantum Annealing with schedule: {self.schedule.schedule_type}, "
            f"quantum effects: {use_quantum_fluctuations}"
        )

    def solve(self,
            problem: Dict[str, Any],
            quantum_instance: Optional[Any] = None) -> Dict[str, Any]:
        """
        Solves an optimization problem using quantum annealing.
        This method conforms to the standard solver interface. The quantum_instance
        is ignored as this is a simulator.

        Args:
            problem: A dictionary that must contain either a 'qubo_matrix' or
                     Ising parameters ('J' and 'h').
            quantum_instance: Ignored for this simulator.

        Returns:
            A solution dictionary.
        """
        if 'qubo_matrix' in problem:
            return self.solve_qubo(Q=problem['qubo_matrix'])
        elif 'J' in problem and 'h' in problem:
            return self.solve_ising(J=problem['J'], h=problem['h'])
        else:
            raise ValueError("Problem dictionary must contain 'qubo_matrix' or ('J' and 'h').")
        
    def solve_ising(self,
                   J: Dict[Tuple[int, int], float],
                   h: Dict[int, float],
                   num_reads: int = 100) -> Dict:
        """
        Solve Ising model: H = Σ J_ij s_i s_j + Σ h_i s_i
        
        Args:
            J: Coupling strengths between spins
            h: Local field strengths
            num_reads: Number of annealing runs
            
        Returns:
            Solution dictionary
        """
        # Determine problem size
        nodes = set()
        for (i, j) in J.keys():
            nodes.add(i)
            nodes.add(j)
        for i in h.keys():
            nodes.add(i)
        
        num_spins = len(nodes)
        node_list = sorted(list(nodes))
        node_to_idx = {node: idx for idx, node in enumerate(node_list)}
        
        logger.info(f"Solving Ising model with {num_spins} spins")
        
        # Build matrices
        J_matrix = np.zeros((num_spins, num_spins))
        h_vector = np.zeros(num_spins)
        
        for (i, j), coupling in J.items():
            idx_i = node_to_idx[i]
            idx_j = node_to_idx[j]
            J_matrix[idx_i, idx_j] = coupling
            J_matrix[idx_j, idx_i] = coupling
            
        for i, field in h.items():
            h_vector[node_to_idx[i]] = field
            
        # Run multiple annealing reads
        solutions = []
        energies = []
        
        for read in range(num_reads):
            if self.use_quantum_fluctuations:
                solution, energy = self._quantum_anneal(J_matrix, h_vector)
            else:
                solution, energy = self._classical_anneal(J_matrix, h_vector)
                
            solutions.append(solution)
            energies.append(energy)
            
            if energy < self.best_energy:
                self.best_energy = energy
                self.best_solution = solution
                
            if (read + 1) % 10 == 0:
                logger.info(f"Completed {read + 1}/{num_reads} annealing runs")
                
        # Analyze results
        unique_solutions = {}
        for sol, energy in zip(solutions, energies):
            sol_tuple = tuple(sol)
            if sol_tuple not in unique_solutions:
                unique_solutions[sol_tuple] = {'energy': energy, 'count': 0}
            unique_solutions[sol_tuple]['count'] += 1
            
        # Sort by energy
        sorted_solutions = sorted(
            unique_solutions.items(),
            key=lambda x: x[1]['energy']
        )
        
        return {
            'best_solution': self.best_solution.tolist(),
            'best_energy': float(self.best_energy),
            'all_solutions': [
                {
                    'solution': list(sol),
                    'energy': info['energy'],
                    'probability': info['count'] / num_reads
                }
                for sol, info in sorted_solutions[:10]  # Top 10 solutions
            ],
            'convergence_history': self.energy_history[-1000:],  # Last 1000 steps
            'success_probability': unique_solutions[tuple(self.best_solution)]['count'] / num_reads,
            'num_unique_solutions': len(unique_solutions)
        }
        
    def _classical_anneal(self,
                         J: np.ndarray,
                         h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Classical simulated annealing"""
        num_spins = len(h)
        
        # Initialize random state
        state = np.random.choice([-1, 1], size=num_spins)
        
        # Get temperature schedule
        temperatures = self.schedule.get_schedule()
        
        for step, temp in enumerate(temperatures):
            # Randomly select spin to flip
            spin_idx = np.random.randint(num_spins)
            
            # Calculate energy change
            delta_E = self._calculate_flip_energy(state, J, h, spin_idx)
            
            # Metropolis acceptance
            if delta_E < 0 or np.random.rand() < np.exp(-delta_E / temp):
                state[spin_idx] *= -1
                
            # Track progress
            if step % 100 == 0:
                energy = self._calculate_energy(state, J, h)
                self.energy_history.append(energy)
                
        final_energy = self._calculate_energy(state, J, h)
        return state, final_energy
        
    def _quantum_anneal(self,
                       J: np.ndarray,
                       h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Quantum annealing with transverse field"""
        num_spins = len(h)
        
        if num_spins > 20:
            # For large systems, use quantum Monte Carlo
            return self._quantum_monte_carlo(J, h)
        else:
            # For small systems, use exact diagonalization
            return self._exact_quantum_anneal(J, h)
            
    def _exact_quantum_anneal(self,
                            J: np.ndarray,
                            h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Exact quantum annealing for small systems"""
        num_spins = len(h)
        dim = 2 ** num_spins
        
        # Build Pauli matrices
        sigma_z = np.array([[1, 0], [0, -1]], dtype=complex)
        sigma_x = np.array([[0, 1], [1, 0]], dtype=complex)
        I = np.eye(2, dtype=complex)
        
        # Build problem Hamiltonian
        H_problem = np.zeros((dim, dim), dtype=complex)
        
        # Add coupling terms
        for i in range(num_spins):
            for j in range(i + 1, num_spins):
                if J[i, j] != 0:
                    op = 1
                    for k in range(num_spins):
                        if k == i or k == j:
                            op = np.kron(op, sigma_z)
                        else:
                            op = np.kron(op, I)
                    H_problem += J[i, j] * op
                    
        # Add field terms
        for i in range(num_spins):
            if h[i] != 0:
                op = 1
                for k in range(num_spins):
                    if k == i:
                        op = np.kron(op, sigma_z)
                    else:
                        op = np.kron(op, I)
                H_problem += h[i] * op
                
        # Build transverse field Hamiltonian
        H_transverse = np.zeros((dim, dim), dtype=complex)
        for i in range(num_spins):
            op = 1
            for k in range(num_spins):
                if k == i:
                    op = np.kron(op, sigma_x)
                else:
                    op = np.kron(op, I)
            H_transverse += op
            
        # Initialize in ground state of transverse field
        eigenvalues, eigenvectors = np.linalg.eigh(H_transverse)
        state = eigenvectors[:, 0]
        
        # Annealing schedule
        schedule = np.linspace(0, 1, self.schedule.num_steps)
        
        for s in schedule:
            # Time-dependent Hamiltonian
            H_total = (1 - s) * self.transverse_field_strength * H_transverse + s * H_problem
            
            # Evolve state (simplified - should use time evolution)
            eigenvalues, eigenvectors = np.linalg.eigh(H_total)
            
            # Project to ground state (instantaneous approximation)
            overlap = np.abs(eigenvectors.conj().T @ state) ** 2
            ground_idx = np.argmax(overlap)
            state = eigenvectors[:, ground_idx]
            
        # Measure final state
        probabilities = np.abs(state) ** 2
        measured_state = np.random.choice(dim, p=probabilities)
        
        # Convert to spin configuration
        spins = np.array([1 if (measured_state >> i) & 1 else -1 
                         for i in range(num_spins)])
        
        energy = self._calculate_energy(spins, J, h)
        
        return spins, energy
        
    def _quantum_monte_carlo(self,
                           J: np.ndarray,
                           h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Path integral quantum Monte Carlo"""
        num_spins = len(h)
        
        # Initialize replica states
        states = np.random.choice([-1, 1], size=(self.num_replicas, num_spins))
        
        # Temperature and transverse field schedules
        temperatures = self.schedule.get_schedule()
        transverse_schedule = self.transverse_field_strength * (1 - np.linspace(0, 1, len(temperatures)))
        
        for step, (temp, gamma) in enumerate(zip(temperatures, transverse_schedule)):
            # Classical updates within each replica
            for replica in range(self.num_replicas):
                spin_idx = np.random.randint(num_spins)
                delta_E = self._calculate_flip_energy(states[replica], J, h, spin_idx)
                
                if delta_E < 0 or np.random.rand() < np.exp(-delta_E / temp):
                    states[replica, spin_idx] *= -1
                    
            # Quantum updates between replicas
            if gamma > 0 and self.num_replicas > 1:
                for spin_idx in range(num_spins):
                    # Try to flip spin across all replicas
                    coupling_strength = -0.5 * temp * np.log(np.tanh(gamma / (self.num_replicas * temp)))
                    
                    for replica in range(self.num_replicas):
                        next_replica = (replica + 1) % self.num_replicas
                        
                        # Coupling between replicas
                        delta_E = -2 * coupling_strength * states[replica, spin_idx] * states[next_replica, spin_idx]
                        
                        if delta_E < 0 or np.random.rand() < np.exp(-delta_E / temp):
                            states[replica, spin_idx] *= -1
                            
        # Select best replica
        energies = [self._calculate_energy(state, J, h) for state in states]
        best_idx = np.argmin(energies)
        
        return states[best_idx], energies[best_idx]
        
    def _calculate_energy(self,
                         state: np.ndarray,
                         J: np.ndarray,
                         h: np.ndarray) -> float:
        """Calculate Ising energy"""
        interaction_energy = state.T @ J @ state
        field_energy = h.T @ state
        return float(interaction_energy + field_energy)
        
    def _calculate_flip_energy(self,
                             state: np.ndarray,
                             J: np.ndarray,
                             h: np.ndarray,
                             spin_idx: int) -> float:
        """Calculate energy change from flipping a spin"""
        # Interaction contribution
        interaction_change = 2 * state[spin_idx] * np.sum(J[spin_idx, :] * state)
        
        # Field contribution
        field_change = 2 * state[spin_idx] * h[spin_idx]
        
        return interaction_change + field_change
        
    def solve_qubo(self,
                  Q: Union[np.ndarray, Dict[Tuple[int, int], float]],
                  num_reads: int = 100) -> Dict:
        """
        Solve QUBO problem: minimize x^T Q x
        
        Args:
            Q: QUBO matrix (numpy array or dictionary)
            num_reads: Number of annealing runs
            
        Returns:
            Solution dictionary
        """
        # Convert QUBO to Ising
        if isinstance(Q, dict):
            # Dictionary format
            nodes = set()
            for (i, j) in Q.keys():
                nodes.add(i)
                nodes.add(j)
            
            num_vars = len(nodes)
            node_list = sorted(list(nodes))
            node_to_idx = {node: idx for idx, node in enumerate(node_list)}
            
            Q_matrix = np.zeros((num_vars, num_vars))
            for (i, j), value in Q.items():
                idx_i = node_to_idx[i]
                idx_j = node_to_idx[j]
                Q_matrix[idx_i, idx_j] = value
        else:
            Q_matrix = np.array(Q)
            num_vars = len(Q_matrix)
            
        # Convert QUBO to Ising: x = (s + 1) / 2
        J = {}
        h = {}
        
        for i in range(num_vars):
            h[i] = 0.5 * (Q_matrix[i, i] + np.sum(Q_matrix[i, :]) + np.sum(Q_matrix[:, i]))
            
        for i in range(num_vars):
            for j in range(i + 1, num_vars):
                J[(i, j)] = 0.25 * (Q_matrix[i, j] + Q_matrix[j, i])
                
        # Solve Ising problem
        ising_result = self.solve_ising(J, h, num_reads)
        
        # Convert back to binary
        qubo_solutions = []
        for sol_info in ising_result['all_solutions']:
            spins = np.array(sol_info['solution'])
            binary = ((spins + 1) // 2).astype(int)
            
            # Recalculate QUBO energy
            qubo_energy = binary.T @ Q_matrix @ binary
            
            qubo_solutions.append({
                'solution': binary.tolist(),
                'energy': float(qubo_energy),
                'probability': sol_info['probability']
            })
            
        best_binary = ((np.array(ising_result['best_solution']) + 1) // 2).astype(int)
        best_qubo_energy = best_binary.T @ Q_matrix @ best_binary
        
        return {
            'best_solution': best_binary.tolist(),
            'best_energy': float(best_qubo_energy),
            'all_solutions': sorted(qubo_solutions, key=lambda x: x['energy'])[:10],
            'convergence_history': ising_result['convergence_history'],
            'success_probability': ising_result['success_probability'],
            'num_unique_solutions': ising_result['num_unique_solutions']
        }
        
    def plot_annealing_schedule(self, save_path: Optional[str] = None):
        """Plot the annealing schedule"""
        temps = self.schedule.get_schedule()
        transverse = self.transverse_field_strength * (1 - np.linspace(0, 1, len(temps)))
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        
        # Temperature schedule
        ax1.plot(temps, 'b-', linewidth=2)
        ax1.set_xlabel('Annealing Step')
        ax1.set_ylabel('Temperature')
        ax1.set_title('Temperature Schedule')
        ax1.grid(True, alpha=0.3)
        ax1.set_yscale('log')
        
        # Transverse field schedule
        ax2.plot(transverse, 'r-', linewidth=2)
        ax2.set_xlabel('Annealing Step')
        ax2.set_ylabel('Transverse Field Strength')
        ax2.set_title('Quantum Fluctuation Schedule')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()


class ReverseAnnealing(QuantumAnnealing):
    """
    Reverse Annealing
    
    Start from a known solution and explore nearby states.
    """
    
    def __init__(self,
                 initial_state: np.ndarray,
                 reinit_fraction: float = 0.5,
                 **kwargs):
        """
        Initialize Reverse Annealing
        
        Args:
            initial_state: Known initial solution
            reinit_fraction: How far to reverse (0=no reverse, 1=full reverse)
            **kwargs: Other parameters for QuantumAnnealing
        """
        super().__init__(**kwargs)
        self.initial_state = initial_state
        self.reinit_fraction = reinit_fraction
        
    def _quantum_anneal(self,
                       J: np.ndarray,
                       h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Reverse quantum annealing"""
        num_spins = len(h)
        
        # Start from provided state
        state = self.initial_state.copy()
        
        # Create reverse schedule
        forward_schedule = np.linspace(0, 1, self.schedule.num_steps // 3)
        reverse_schedule = np.linspace(1, self.reinit_fraction, self.schedule.num_steps // 3)
        re_forward_schedule = np.linspace(self.reinit_fraction, 1, self.schedule.num_steps // 3)
        
        full_schedule = np.concatenate([reverse_schedule, re_forward_schedule])
        
        # Use parent's annealing with modified schedule
        temps = self.schedule.get_schedule()[:len(full_schedule)]
        
        for step, (s, temp) in enumerate(zip(full_schedule, temps)):
            # Quantum Monte Carlo update with current transverse field
            gamma = self.transverse_field_strength * (1 - s)
            
            # Random spin flips
            for _ in range(num_spins):
                spin_idx = np.random.randint(num_spins)
                
                # Classical energy change
                delta_E_classical = self._calculate_flip_energy(state, J, h, spin_idx)
                
                # Quantum tunneling probability
                tunnel_prob = np.exp(-2 * gamma) if gamma > 0 else 0
                
                # Combined acceptance
                delta_E = delta_E_classical * (1 - tunnel_prob)
                
                if delta_E < 0 or np.random.rand() < np.exp(-delta_E / temp):
                    state[spin_idx] *= -1
                    
        energy = self._calculate_energy(state, J, h)
        return state, energy


class PopulationAnnealing(QuantumAnnealing):
    """
    Population Annealing
    
    Maintains a population of states through the annealing process.
    """
    
    def __init__(self,
                 population_size: int = 1000,
                 resample_threshold: float = 0.5,
                 **kwargs):
        """
        Initialize Population Annealing
        
        Args:
            population_size: Number of replicas in population
            resample_threshold: Effective population fraction for resampling
            **kwargs: Other parameters for QuantumAnnealing
        """
        super().__init__(**kwargs)
        self.population_size = population_size
        self.resample_threshold = resample_threshold
        
    def _quantum_anneal(self,
                       J: np.ndarray,
                       h: np.ndarray) -> Tuple[np.ndarray, float]:
        """Population annealing algorithm"""
        num_spins = len(h)
        
        # Initialize population
        population = np.random.choice([-1, 1], size=(self.population_size, num_spins))
        weights = np.ones(self.population_size) / self.population_size
        
        temperatures = self.schedule.get_schedule()
        
        for step, temp in enumerate(temperatures):
            # Update each member of population
            for i in range(self.population_size):
                # Multiple spin flip attempts
                for _ in range(num_spins):
                    spin_idx = np.random.randint(num_spins)
                    delta_E = self._calculate_flip_energy(population[i], J, h, spin_idx)
                    
                    if delta_E < 0 or np.random.rand() < np.exp(-delta_E / temp):
                        population[i, spin_idx] *= -1
                        
            # Reweight population
            if step < len(temperatures) - 1:
                next_temp = temperatures[step + 1]
                energies = np.array([self._calculate_energy(state, J, h) 
                                   for state in population])
                
                # Reweighting factors
                weight_factors = np.exp(-(1/next_temp - 1/temp) * energies)
                weights *= weight_factors
                weights /= np.sum(weights)
                
                # Check effective population size
                n_eff = 1 / np.sum(weights**2)
                
                if n_eff < self.resample_threshold * self.population_size:
                    # Resample population
                    indices = np.random.choice(
                        self.population_size,
                        size=self.population_size,
                        p=weights
                    )
                    population = population[indices]
                    weights = np.ones(self.population_size) / self.population_size
                    
        # Find best solution
        energies = np.array([self._calculate_energy(state, J, h) 
                           for state in population])
        best_idx = np.argmin(energies)
        
        return population[best_idx], energies[best_idx] 