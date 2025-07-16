"""
VQE Algorithm Implementation

Variational Quantum Eigensolver for finding ground states and optimization.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
from qiskit.circuit import Parameter, ParameterVector
from qiskit.circuit.library import RealAmplitudes, TwoLocal, EfficientSU2, ExcitationPreserving
from qiskit.opflow import PauliSumOp, StateFn, OperatorBase, ExpectationBase, CircuitSampler
from qiskit.opflow.expectations import PauliExpectation, AerPauliExpectation
from qiskit.utils import QuantumInstance
from qiskit.algorithms.optimizers import Optimizer, COBYLA, SPSA, L_BFGS_B
from scipy.optimize import minimize
import matplotlib.pyplot as plt

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class VQE:
    """
    Variational Quantum Eigensolver
    
    VQE is a hybrid quantum-classical algorithm for finding the ground state
    of a given Hamiltonian. It's particularly useful for quantum chemistry
    and optimization problems.
    """
    
    def __init__(self,
                 ansatz: Optional[QuantumCircuit] = None,
                 optimizer: Optional[Union[Optimizer, str]] = None,
                 initial_point: Optional[np.ndarray] = None,
                 expectation: Optional[ExpectationBase] = None,
                 include_custom: bool = False,
                 max_evals_grouped: int = 1):
        """
        Initialize VQE
        
        Args:
            ansatz: Parameterized quantum circuit (default: RealAmplitudes)
            optimizer: Classical optimizer (default: COBYLA)
            initial_point: Initial parameter values
            expectation: Expectation value measurement method
            include_custom: Include custom ansätze
            max_evals_grouped: Maximum evaluations to group
        """
        self.ansatz = ansatz
        self._optimizer = optimizer or COBYLA(maxiter=500)
        self.initial_point = initial_point
        self.expectation = expectation or PauliExpectation()
        self.include_custom = include_custom
        self.max_evals_grouped = max_evals_grouped
        
        # Results tracking
        self.eigenvalue = None
        self.optimal_params = None
        self.optimal_circuit = None
        self.optimizer_result = None
        self.eigenstate = None
        
        # History tracking
        self.iteration_count = 0
        self.parameter_history = []
        self.value_history = []
        self.gradient_history = []
        
        logger.info("Initialized VQE with optimizer: %s", self._optimizer.__class__.__name__)
        
    def construct_ansatz(self, num_qubits: int, num_layers: int = 3) -> QuantumCircuit:
        """
        Construct ansatz circuit if not provided
        
        Args:
            num_qubits: Number of qubits
            num_layers: Number of ansatz layers
            
        Returns:
            Parameterized ansatz circuit
        """
        if self.ansatz is not None:
            return self.ansatz
            
        # Default to RealAmplitudes ansatz
        self.ansatz = RealAmplitudes(
            num_qubits=num_qubits,
            reps=num_layers,
            entanglement='linear',
            insert_barriers=True
        )
        
        logger.info(f"Created RealAmplitudes ansatz with {num_qubits} qubits, {num_layers} layers")
        return self.ansatz
        
    def compute_minimum_eigenvalue(self,
                                 operator: OperatorBase,
                                 quantum_instance: QuantumInstance) -> Dict:
        """
        Compute the minimum eigenvalue of the operator
        
        Args:
            operator: The Hamiltonian operator
            quantum_instance: Quantum backend instance
            
        Returns:
            Result dictionary with eigenvalue and optimal parameters
        """
        logger.info("Starting VQE optimization")
        
        # Setup
        num_qubits = operator.num_qubits
        self.construct_ansatz(num_qubits)
        
        # Initialize parameters
        num_parameters = self.ansatz.num_parameters
        if self.initial_point is None:
            self.initial_point = np.random.uniform(-np.pi, np.pi, num_parameters)
            
        # Reset tracking
        self.iteration_count = 0
        self.parameter_history = []
        self.value_history = []
        
        # Define objective function
        def objective_function(params):
            """Objective function for optimization"""
            # Bind parameters to circuit
            bound_circuit = self.ansatz.bind_parameters(params)
            
            # Create state function
            wave_function = StateFn(bound_circuit)
            
            # Create expectation value
            expectation_op = StateFn(operator, is_measurement=True) @ wave_function
            
            # Convert to expectation
            expectation = self.expectation.convert(expectation_op)
            
            # Sample
            sampler = CircuitSampler(quantum_instance)
            expectation_value = sampler.convert(expectation).eval()
            
            # Track history
            self.iteration_count += 1
            self.parameter_history.append(params.copy())
            self.value_history.append(float(np.real(expectation_value)))
            
            # Log progress
            if self.iteration_count % 10 == 0:
                logger.info(f"Iteration {self.iteration_count}: {expectation_value}")
                
            return float(np.real(expectation_value))
            
        # Run optimization
        if isinstance(self._optimizer, str):
            # Scipy optimizer
            result = minimize(
                objective_function,
                self.initial_point,
                method=self._optimizer,
                options={'maxiter': 500}
            )
            self.optimal_params = result.x
            self.eigenvalue = result.fun
            self.optimizer_result = result
        else:
            # Qiskit optimizer
            result = self._optimizer.minimize(
                objective_function,
                self.initial_point
            )
            self.optimal_params = result[0]
            self.eigenvalue = result[1]
            self.optimizer_result = result
            
        # Create optimal circuit
        self.optimal_circuit = self.ansatz.bind_parameters(self.optimal_params)
        
        # Get eigenstate
        eigenstate_circuit = self.optimal_circuit.copy()
        eigenstate_circuit.remove_final_measurements()
        job = quantum_instance.execute(eigenstate_circuit)
        self.eigenstate = job.result().get_statevector()
        
        logger.info(f"VQE converged. Eigenvalue: {self.eigenvalue}")
        
        return {
            'eigenvalue': self.eigenvalue,
            'eigenstate': self.eigenstate,
            'optimal_parameters': self.optimal_params,
            'optimal_circuit': self.optimal_circuit,
            'optimizer_result': self.optimizer_result,
            'num_iterations': self.iteration_count,
            'convergence_history': self.value_history,
            'parameter_history': self.parameter_history
        }
        
    def get_optimal_vector(self) -> Optional[np.ndarray]:
        """Get optimal parameter vector"""
        return self.optimal_params
        
    def get_optimal_circuit(self) -> Optional[QuantumCircuit]:
        """Get optimal circuit with bound parameters"""
        return self.optimal_circuit
        
    def plot_convergence(self, save_path: Optional[str] = None):
        """Plot convergence history"""
        if not self.value_history:
            logger.warning("No convergence history to plot")
            return
            
        plt.figure(figsize=(10, 6))
        plt.plot(self.value_history, 'b-', linewidth=2)
        plt.xlabel('Iteration')
        plt.ylabel('Energy')
        plt.title('VQE Convergence')
        plt.grid(True, alpha=0.3)
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()
            
    def supports_aux_operators(self) -> bool:
        """Check if auxiliary operators are supported"""
        return True
        
    def compute_auxiliary_values(self,
                               aux_operators: List[OperatorBase],
                               quantum_instance: QuantumInstance) -> List[float]:
        """
        Compute expectation values of auxiliary operators
        
        Args:
            aux_operators: List of auxiliary operators
            quantum_instance: Quantum backend instance
            
        Returns:
            List of expectation values
        """
        if self.optimal_circuit is None:
            raise ValueError("Must run VQE first")
            
        aux_values = []
        
        for aux_op in aux_operators:
            # Create expectation
            wave_function = StateFn(self.optimal_circuit)
            expectation_op = StateFn(aux_op, is_measurement=True) @ wave_function
            expectation = self.expectation.convert(expectation_op)
            
            # Sample
            sampler = CircuitSampler(quantum_instance)
            aux_value = sampler.convert(expectation).eval()
            aux_values.append(float(np.real(aux_value)))
            
        return aux_values


class VQEFactory:
    """Factory for creating VQE instances with different ansätze"""
    
    @staticmethod
    def create_hardware_efficient_vqe(num_qubits: int,
                                    num_layers: int = 3,
                                    entanglement: str = 'linear') -> VQE:
        """Create VQE with hardware-efficient ansatz"""
        ansatz = EfficientSU2(
            num_qubits=num_qubits,
            reps=num_layers,
            entanglement=entanglement,
            insert_barriers=True
        )
        
        return VQE(ansatz=ansatz, optimizer=L_BFGS_B(maxiter=1000))
        
    @staticmethod
    def create_chemistry_vqe(num_qubits: int,
                           num_particles: Tuple[int, int],
                           num_layers: int = 3) -> VQE:
        """Create VQE for quantum chemistry problems"""
        # Use excitation preserving ansatz for chemistry
        ansatz = ExcitationPreserving(
            num_qubits=num_qubits,
            reps=num_layers,
            entanglement='linear',
            insert_barriers=True
        )
        
        vqe = VQE(
            ansatz=ansatz,
            optimizer=SPSA(maxiter=500),
            expectation=AerPauliExpectation()
        )
        
        # Set initial state for particle conservation
        initial_state = QuantumCircuit(num_qubits)
        num_alpha, num_beta = num_particles
        
        # Initialize electrons
        for i in range(num_alpha):
            initial_state.x(2 * i)
        for i in range(num_beta):
            initial_state.x(2 * i + 1)
            
        # Prepend initial state to ansatz
        full_circuit = initial_state.compose(ansatz)
        vqe.ansatz = full_circuit
        
        return vqe
        
    @staticmethod
    def create_custom_vqe(ansatz_type: str,
                        num_qubits: int,
                        **kwargs) -> VQE:
        """Create VQE with custom ansatz types"""
        if ansatz_type == 'alternating':
            ansatz = VQEFactory._create_alternating_ansatz(num_qubits, **kwargs)
        elif ansatz_type == 'cascade':
            ansatz = VQEFactory._create_cascade_ansatz(num_qubits, **kwargs)
        elif ansatz_type == 'symmetric':
            ansatz = VQEFactory._create_symmetric_ansatz(num_qubits, **kwargs)
        else:
            raise ValueError(f"Unknown ansatz type: {ansatz_type}")
            
        return VQE(ansatz=ansatz)
        
    @staticmethod
    def _create_alternating_ansatz(num_qubits: int,
                                 num_layers: int = 3) -> QuantumCircuit:
        """Create alternating layer ansatz"""
        params = ParameterVector('θ', num_qubits * num_layers * 3)
        qc = QuantumCircuit(num_qubits)
        
        param_idx = 0
        for layer in range(num_layers):
            # Single qubit rotations
            for i in range(num_qubits):
                qc.rx(params[param_idx], i)
                qc.ry(params[param_idx + 1], i)
                qc.rz(params[param_idx + 2], i)
                param_idx += 3
                
            # Entangling layer (alternating pattern)
            if layer % 2 == 0:
                for i in range(0, num_qubits - 1, 2):
                    qc.cx(i, i + 1)
            else:
                for i in range(1, num_qubits - 1, 2):
                    qc.cx(i, i + 1)
                    
        return qc
        
    @staticmethod
    def _create_cascade_ansatz(num_qubits: int,
                             depth: int = 3) -> QuantumCircuit:
        """Create cascade entanglement ansatz"""
        num_params = num_qubits * (depth + 1) * 2
        params = ParameterVector('θ', num_params)
        qc = QuantumCircuit(num_qubits)
        
        param_idx = 0
        
        # Initial rotation layer
        for i in range(num_qubits):
            qc.ry(params[param_idx], i)
            qc.rz(params[param_idx + 1], i)
            param_idx += 2
            
        # Cascade layers
        for d in range(depth):
            # Cascade entanglement
            for i in range(num_qubits - 1):
                qc.cx(i, i + 1)
                
            # Rotation layer
            for i in range(num_qubits):
                qc.ry(params[param_idx], i)
                qc.rz(params[param_idx + 1], i)
                param_idx += 2
                
        return qc
        
    @staticmethod
    def _create_symmetric_ansatz(num_qubits: int,
                               symmetry: str = 'cyclic') -> QuantumCircuit:
        """Create ansatz with symmetry constraints"""
        if num_qubits < 3:
            raise ValueError("Symmetric ansatz requires at least 3 qubits")
            
        if symmetry == 'cyclic':
            # Cyclic symmetry
            num_unique_params = 6  # For a simple example
            params = ParameterVector('θ', num_unique_params)
            qc = QuantumCircuit(num_qubits)
            
            # Apply same rotations cyclically
            for i in range(num_qubits):
                qc.rx(params[0], i)
                qc.ry(params[1], i)
                qc.rz(params[2], i)
                
            # Cyclic entanglement
            for i in range(num_qubits):
                qc.cx(i, (i + 1) % num_qubits)
                
            # Second rotation layer
            for i in range(num_qubits):
                qc.rx(params[3], i)
                qc.ry(params[4], i)
                qc.rz(params[5], i)
                
        elif symmetry == 'mirror':
            # Mirror symmetry
            half = num_qubits // 2
            num_params = half * 6 + (num_qubits % 2) * 3
            params = ParameterVector('θ', num_params)
            qc = QuantumCircuit(num_qubits)
            
            param_idx = 0
            
            # Apply mirrored rotations
            for i in range(half):
                # Left side
                qc.rx(params[param_idx], i)
                qc.ry(params[param_idx + 1], i)
                qc.rz(params[param_idx + 2], i)
                
                # Right side (mirrored)
                qc.rx(params[param_idx], num_qubits - 1 - i)
                qc.ry(params[param_idx + 1], num_qubits - 1 - i)
                qc.rz(params[param_idx + 2], num_qubits - 1 - i)
                
                param_idx += 3
                
            # Middle qubit if odd number
            if num_qubits % 2 == 1:
                mid = num_qubits // 2
                qc.rx(params[param_idx], mid)
                qc.ry(params[param_idx + 1], mid)
                qc.rz(params[param_idx + 2], mid)
                
        else:
            raise ValueError(f"Unknown symmetry type: {symmetry}")
            
        return qc


class AdaptiveVQE(VQE):
    """
    Adaptive VQE that dynamically grows the ansatz
    """
    
    def __init__(self,
                 operator_pool: Optional[List[OperatorBase]] = None,
                 initial_state: Optional[QuantumCircuit] = None,
                 optimizer: Optional[Optimizer] = None,
                 gradient_threshold: float = 1e-3):
        """
        Initialize Adaptive VQE
        
        Args:
            operator_pool: Pool of operators to grow ansatz
            initial_state: Initial state preparation
            optimizer: Classical optimizer
            gradient_threshold: Threshold for adding operators
        """
        super().__init__(optimizer=optimizer)
        self.operator_pool = operator_pool or self._default_pool()
        self.initial_state = initial_state
        self.gradient_threshold = gradient_threshold
        self.selected_operators = []
        
    def _default_pool(self) -> List[OperatorBase]:
        """Create default operator pool"""
        # Would implement singles and doubles excitations
        return []
        
    def compute_minimum_eigenvalue(self,
                                 operator: OperatorBase,
                                 quantum_instance: QuantumInstance) -> Dict:
        """
        Compute minimum eigenvalue with adaptive ansatz growth
        """
        num_qubits = operator.num_qubits
        
        # Start with initial state or empty circuit
        if self.initial_state:
            current_ansatz = self.initial_state.copy()
        else:
            current_ansatz = QuantumCircuit(num_qubits)
            
        converged = False
        iteration = 0
        
        while not converged and iteration < 100:
            # Compute gradients for all operators in pool
            gradients = self._compute_gradients(
                current_ansatz, operator, quantum_instance
            )
            
            # Select operator with largest gradient
            max_grad_idx = np.argmax(np.abs(gradients))
            max_gradient = gradients[max_grad_idx]
            
            if abs(max_gradient) < self.gradient_threshold:
                converged = True
                logger.info(f"Converged at iteration {iteration}")
            else:
                # Add operator to ansatz
                selected_op = self.operator_pool[max_grad_idx]
                self.selected_operators.append(selected_op)
                
                # Update ansatz
                # This is simplified - would need proper implementation
                param = Parameter(f'θ_{iteration}')
                current_ansatz.rz(param, 0)  # Placeholder
                
            iteration += 1
            
        # Run final VQE with constructed ansatz
        self.ansatz = current_ansatz
        return super().compute_minimum_eigenvalue(operator, quantum_instance)
        
    def _compute_gradients(self,
                         ansatz: QuantumCircuit,
                         operator: OperatorBase,
                         quantum_instance: QuantumInstance) -> np.ndarray:
        """Compute gradients for operator pool"""
        # Simplified - would compute commutators
        return np.random.rand(len(self.operator_pool)) 