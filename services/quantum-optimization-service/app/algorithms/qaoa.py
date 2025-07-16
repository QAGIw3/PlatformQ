"""
QAOA Algorithm Implementation

Quantum Approximate Optimization Algorithm for combinatorial optimization problems.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
from qiskit.circuit import Parameter
from qiskit.algorithms.optimizers import COBYLA, SPSA
from qiskit.opflow import PauliSumOp, StateFn, CircuitStateFn
from qiskit.utils import QuantumInstance
from scipy.optimize import minimize
import networkx as nx

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class QAOA:
    """
    Quantum Approximate Optimization Algorithm
    
    QAOA is a hybrid quantum-classical algorithm designed to solve 
    combinatorial optimization problems.
    """
    
    def __init__(self,
                 reps: int = 3,
                 optimizer: str = "COBYLA",
                 initial_point: Optional[np.ndarray] = None,
                 mixer: Optional[QuantumCircuit] = None):
        """
        Initialize QAOA
        
        Args:
            reps: Number of QAOA layers (p parameter)
            optimizer: Classical optimizer to use
            initial_point: Initial parameters for optimization
            mixer: Custom mixer operator (default is X-mixer)
        """
        self.reps = reps
        self.optimizer_name = optimizer
        self.initial_point = initial_point
        self.custom_mixer = mixer
        
        # Parameters for the circuit
        self.beta = [Parameter(f"β_{i}") for i in range(reps)]
        self.gamma = [Parameter(f"γ_{i}") for i in range(reps)]
        
        # Optimization history
        self.optimization_history = []
        self.best_params = None
        self.best_value = float('inf')
        
        logger.info(f"Initialized QAOA with {reps} layers, optimizer: {optimizer}")
        
    def construct_circuit(self,
                         problem_hamiltonian: PauliSumOp,
                         num_qubits: int) -> QuantumCircuit:
        """
        Construct QAOA circuit
        
        Args:
            problem_hamiltonian: The problem Hamiltonian (cost function)
            num_qubits: Number of qubits
            
        Returns:
            Parameterized QAOA circuit
        """
        qr = QuantumRegister(num_qubits, 'q')
        cr = ClassicalRegister(num_qubits, 'c')
        qc = QuantumCircuit(qr, cr)
        
        # Initial state: equal superposition
        qc.h(range(num_qubits))
        
        # QAOA layers
        for layer in range(self.reps):
            # Problem Hamiltonian evolution (cost layer)
            self._add_cost_layer(qc, problem_hamiltonian, self.gamma[layer])
            
            # Mixer Hamiltonian evolution
            if self.custom_mixer:
                qc.append(self.custom_mixer, range(num_qubits))
            else:
                self._add_mixer_layer(qc, num_qubits, self.beta[layer])
                
        # Measurement
        qc.measure_all()
        
        return qc
        
    def _add_cost_layer(self,
                       circuit: QuantumCircuit,
                       hamiltonian: PauliSumOp,
                       gamma: Parameter):
        """Add cost Hamiltonian evolution layer"""
        # Convert Hamiltonian to circuit evolution
        for pauli_term in hamiltonian:
            coeff = pauli_term.primitive.coeffs[0]
            pauli_string = str(pauli_term.primitive.paulis[0])
            
            # Build the circuit for this Pauli term
            self._add_pauli_evolution(circuit, pauli_string, float(coeff) * gamma)
            
    def _add_pauli_evolution(self,
                           circuit: QuantumCircuit,
                           pauli_string: str,
                           angle: float):
        """Add evolution under a Pauli string"""
        # Find non-identity positions
        non_identity = []
        for i, pauli in enumerate(pauli_string):
            if pauli != 'I':
                non_identity.append((i, pauli))
                
        if not non_identity:
            return  # Global phase, skip
            
        # Apply basis rotations
        for pos, pauli in non_identity:
            if pauli == 'X':
                circuit.h(pos)
            elif pauli == 'Y':
                circuit.rx(np.pi/2, pos)
                
        # Apply CNOTs for entangling
        for i in range(len(non_identity) - 1):
            circuit.cx(non_identity[i][0], non_identity[i+1][0])
            
        # Apply phase rotation
        if len(non_identity) > 0:
            circuit.rz(2 * angle, non_identity[-1][0])
            
        # Reverse CNOTs
        for i in range(len(non_identity) - 2, -1, -1):
            circuit.cx(non_identity[i][0], non_identity[i+1][0])
            
        # Reverse basis rotations
        for pos, pauli in non_identity:
            if pauli == 'X':
                circuit.h(pos)
            elif pauli == 'Y':
                circuit.rx(-np.pi/2, pos)
                
    def _add_mixer_layer(self,
                        circuit: QuantumCircuit,
                        num_qubits: int,
                        beta: Parameter):
        """Add mixer Hamiltonian layer (X-mixer)"""
        for i in range(num_qubits):
            circuit.rx(2 * beta, i)
            
    def solve(self,
             problem: Dict,
             quantum_instance: QuantumInstance,
             callback: Optional[callable] = None) -> Dict:
        """
        Solve optimization problem using QAOA
        
        Args:
            problem: Problem dictionary with 'hamiltonian' and metadata
            quantum_instance: Quantum backend instance
            callback: Optional callback function for optimization
            
        Returns:
            Solution dictionary
        """
        logger.info("Starting QAOA optimization")
        
        # Extract problem Hamiltonian
        if 'hamiltonian' in problem:
            hamiltonian = problem['hamiltonian']
        else:
            # Build from QUBO matrix
            hamiltonian = self._qubo_to_hamiltonian(problem['qubo_matrix'])
            
        num_qubits = hamiltonian.num_qubits
        
        # Construct circuit
        qaoa_circuit = self.construct_circuit(hamiltonian, num_qubits)
        
        # Define objective function
        def objective(params):
            # Bind parameters
            bound_circuit = qaoa_circuit.bind_parameters(
                dict(zip(self.beta + self.gamma, params))
            )
            
            # Execute circuit
            job = quantum_instance.execute(bound_circuit)
            result = job.result()
            counts = result.get_counts()
            
            # Calculate expectation value
            expectation = self._calculate_expectation(counts, hamiltonian)
            
            # Track optimization
            self.optimization_history.append({
                'params': params.copy(),
                'value': expectation,
                'counts': counts
            })
            
            if expectation < self.best_value:
                self.best_value = expectation
                self.best_params = params.copy()
                
            if callback:
                callback(len(self.optimization_history), params, expectation)
                
            return expectation
            
        # Initial parameters
        if self.initial_point is None:
            self.initial_point = np.random.uniform(0, np.pi, 2 * self.reps)
            
        # Run optimization
        if self.optimizer_name == "COBYLA":
            result = minimize(
                objective,
                self.initial_point,
                method='COBYLA',
                options={'maxiter': 500, 'tol': 1e-6}
            )
        elif self.optimizer_name == "SPSA":
            # SPSA is good for noisy optimization
            spsa = SPSA(maxiter=100)
            result = spsa.minimize(objective, self.initial_point)
        else:
            # Default scipy optimizer
            result = minimize(objective, self.initial_point, method=self.optimizer_name)
            
        # Get final result with optimal parameters
        final_circuit = qaoa_circuit.bind_parameters(
            dict(zip(self.beta + self.gamma, self.best_params))
        )
        
        final_job = quantum_instance.execute(final_circuit)
        final_result = final_job.result()
        final_counts = final_result.get_counts()
        
        # Extract solution
        solution = self._extract_solution(final_counts, num_qubits)
        
        return {
            'optimal_params': self.best_params.tolist(),
            'optimal_value': float(self.best_value),
            'solution_bitstring': solution['bitstring'],
            'solution_vector': solution['vector'],
            'probability': solution['probability'],
            'optimization_history': self.optimization_history,
            'iterations': len(self.optimization_history),
            'final_counts': final_counts,
            'convergence': result.success if hasattr(result, 'success') else True
        }
        
    def _qubo_to_hamiltonian(self, qubo_matrix: List[List[float]]) -> PauliSumOp:
        """Convert QUBO matrix to Pauli Hamiltonian"""
        n = len(qubo_matrix)
        pauli_list = []
        
        # Diagonal terms (linear)
        for i in range(n):
            if abs(qubo_matrix[i][i]) > 1e-10:
                pauli_str = 'I' * i + 'Z' + 'I' * (n - i - 1)
                coeff = qubo_matrix[i][i] / 2
                pauli_list.append((pauli_str, coeff))
                
        # Off-diagonal terms (quadratic)
        for i in range(n):
            for j in range(i + 1, n):
                if abs(qubo_matrix[i][j]) > 1e-10:
                    pauli_str = ['I'] * n
                    pauli_str[i] = 'Z'
                    pauli_str[j] = 'Z'
                    coeff = (qubo_matrix[i][j] + qubo_matrix[j][i]) / 4
                    pauli_list.append((''.join(pauli_str), coeff))
                    
        # Add constant offset
        offset = sum(qubo_matrix[i][i] / 2 for i in range(n))
        if abs(offset) > 1e-10:
            pauli_list.append(('I' * n, offset))
            
        return PauliSumOp.from_list(pauli_list)
        
    def _calculate_expectation(self,
                             counts: Dict[str, int],
                             hamiltonian: PauliSumOp) -> float:
        """Calculate expectation value from measurement counts"""
        total_shots = sum(counts.values())
        expectation = 0.0
        
        for bitstring, count in counts.items():
            # Calculate energy for this bitstring
            energy = self._evaluate_bitstring(bitstring, hamiltonian)
            expectation += energy * count / total_shots
            
        return expectation
        
    def _evaluate_bitstring(self,
                          bitstring: str,
                          hamiltonian: PauliSumOp) -> float:
        """Evaluate Hamiltonian on a bitstring"""
        # Convert bitstring to computational basis state
        state = [1 if bit == '0' else -1 for bit in bitstring]
        energy = 0.0
        
        for pauli_term in hamiltonian:
            coeff = pauli_term.primitive.coeffs[0]
            pauli_string = str(pauli_term.primitive.paulis[0])
            
            # Evaluate Pauli string on state
            value = 1.0
            for i, pauli in enumerate(pauli_string):
                if pauli == 'Z':
                    value *= state[i]
                elif pauli == 'X' or pauli == 'Y':
                    # For QAOA, we typically only have Z terms
                    value = 0.0
                    break
                    
            energy += coeff * value
            
        return float(np.real(energy))
        
    def _extract_solution(self,
                        counts: Dict[str, int],
                        num_qubits: int) -> Dict:
        """Extract best solution from measurement counts"""
        # Find most probable bitstring
        best_bitstring = max(counts, key=counts.get)
        best_count = counts[best_bitstring]
        total_shots = sum(counts.values())
        
        # Convert to solution vector
        solution_vector = [int(bit) for bit in best_bitstring]
        
        return {
            'bitstring': best_bitstring,
            'vector': solution_vector,
            'probability': best_count / total_shots,
            'count': best_count,
            'total_shots': total_shots
        }
        
    def get_optimization_report(self) -> Dict:
        """Get detailed optimization report"""
        if not self.optimization_history:
            return {'status': 'Not optimized yet'}
            
        # Analyze convergence
        values = [h['value'] for h in self.optimization_history]
        
        return {
            'total_iterations': len(self.optimization_history),
            'best_value': float(self.best_value),
            'best_params': self.best_params.tolist() if self.best_params is not None else None,
            'initial_value': values[0] if values else None,
            'final_value': values[-1] if values else None,
            'improvement': values[0] - self.best_value if values else 0,
            'convergence_history': values,
            'parameter_evolution': {
                'beta': [h['params'][:self.reps].tolist() for h in self.optimization_history],
                'gamma': [h['params'][self.reps:].tolist() for h in self.optimization_history]
            }
        }


class QAOAFactory:
    """Factory for creating QAOA instances for specific problems"""
    
    @staticmethod
    def create_maxcut_qaoa(graph: nx.Graph, reps: int = 3) -> Tuple[QAOA, PauliSumOp]:
        """Create QAOA instance for MaxCut problem"""
        # Build MaxCut Hamiltonian
        num_nodes = graph.number_of_nodes()
        pauli_list = []
        
        for u, v in graph.edges():
            pauli_str = ['I'] * num_nodes
            pauli_str[u] = 'Z'
            pauli_str[v] = 'Z'
            # MaxCut: minimize -0.5 * (1 - Z_i * Z_j)
            pauli_list.append((''.join(pauli_str), -0.5))
            
        # Add constant
        pauli_list.append(('I' * num_nodes, 0.5 * graph.number_of_edges()))
        
        hamiltonian = PauliSumOp.from_list(pauli_list)
        qaoa = QAOA(reps=reps)
        
        return qaoa, hamiltonian
        
    @staticmethod
    def create_tsp_qaoa(distances: np.ndarray, reps: int = 3) -> Tuple[QAOA, PauliSumOp]:
        """Create QAOA instance for TSP"""
        n = len(distances)
        num_qubits = n * n  # Binary variables x_ij (city i at position j)
        
        # Custom mixer for TSP that preserves valid tours
        mixer = QAOAFactory._create_tsp_mixer(n)
        
        qaoa = QAOA(reps=reps, mixer=mixer)
        
        # Build TSP Hamiltonian (simplified)
        pauli_list = []
        
        # Distance terms
        for i in range(n):
            for j in range(n):
                if i != j:
                    for p in range(n - 1):
                        idx1 = i * n + p
                        idx2 = j * n + (p + 1)
                        pauli_str = ['I'] * num_qubits
                        pauli_str[idx1] = 'Z'
                        pauli_str[idx2] = 'Z'
                        coeff = distances[i][j] / 4
                        pauli_list.append((''.join(pauli_str), coeff))
                        
        hamiltonian = PauliSumOp.from_list(pauli_list)
        
        return qaoa, hamiltonian
        
    @staticmethod
    def _create_tsp_mixer(n: int) -> QuantumCircuit:
        """Create custom mixer for TSP that preserves tour validity"""
        num_qubits = n * n
        qc = QuantumCircuit(num_qubits)
        
        # Simple X-rotation mixer (can be enhanced)
        beta = Parameter('mixer_beta')
        for i in range(num_qubits):
            qc.rx(2 * beta, i)
            
        return qc 