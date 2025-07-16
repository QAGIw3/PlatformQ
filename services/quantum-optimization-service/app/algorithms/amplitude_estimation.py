"""
Amplitude Estimation Algorithm Implementation

Quantum amplitude estimation for probabilistic optimization and Monte Carlo methods.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Callable, Union
from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister, AncillaRegister
from qiskit.circuit import Gate, ControlledGate
from qiskit.circuit.library import QFT, GroverOperator
from qiskit.utils import QuantumInstance
from qiskit.algorithms import EstimationProblem
from qiskit.opflow import StateFn, CircuitStateFn
import matplotlib.pyplot as plt
from scipy.stats import binom
from scipy.optimize import minimize_scalar

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class AmplitudeEstimation:
    """
    Quantum Amplitude Estimation
    
    Estimates the amplitude of marked states in a quantum superposition.
    Useful for Monte Carlo integration, risk analysis, and optimization.
    """
    
    def __init__(self,
                 num_eval_qubits: int = 5,
                 phase_estimation_method: str = "canonical",
                 confidence_level: float = 0.95,
                 use_maximum_likelihood: bool = True):
        """
        Initialize Amplitude Estimation
        
        Args:
            num_eval_qubits: Number of qubits for phase estimation
            phase_estimation_method: Method for phase estimation (canonical, iterative, adaptive)
            confidence_level: Confidence level for estimates
            use_maximum_likelihood: Use ML estimation vs simple estimation
        """
        self.num_eval_qubits = num_eval_qubits
        self.phase_estimation_method = phase_estimation_method
        self.confidence_level = confidence_level
        self.use_maximum_likelihood = use_maximum_likelihood
        
        # Results
        self.estimated_amplitude = None
        self.confidence_interval = None
        self.measurement_outcomes = None
        self.likelihood_function = None
        
        logger.info(
            f"Initialized Amplitude Estimation with {num_eval_qubits} evaluation qubits, "
            f"method: {phase_estimation_method}"
        )
        
    def construct_circuit(self,
                         state_preparation: QuantumCircuit,
                         grover_operator: QuantumCircuit) -> QuantumCircuit:
        """
        Construct amplitude estimation circuit
        
        Args:
            state_preparation: Circuit preparing the initial state
            grover_operator: Grover operator Q
            
        Returns:
            Complete amplitude estimation circuit
        """
        # Get number of state qubits
        num_state_qubits = state_preparation.num_qubits
        
        # Create registers
        eval_qubits = QuantumRegister(self.num_eval_qubits, 'eval')
        state_qubits = QuantumRegister(num_state_qubits, 'state')
        c_reg = ClassicalRegister(self.num_eval_qubits, 'c')
        
        qc = QuantumCircuit(eval_qubits, state_qubits, c_reg)
        
        # Initialize evaluation qubits in superposition
        qc.h(eval_qubits)
        
        # Prepare initial state
        qc.append(state_preparation, state_qubits)
        
        # Controlled Grover operations
        for j in range(self.num_eval_qubits):
            power = 2 ** j
            
            # Create controlled-Q^(2^j) gate
            controlled_grover = self._create_controlled_power(
                grover_operator, power, f"Q^{power}"
            )
            
            # Apply controlled operation
            qc.append(
                controlled_grover.control(1),
                [eval_qubits[j]] + list(state_qubits)
            )
            
        # Inverse QFT on evaluation qubits
        qc.append(QFT(self.num_eval_qubits, inverse=True), eval_qubits)
        
        # Measurement
        qc.measure(eval_qubits, c_reg)
        
        return qc
        
    def _create_controlled_power(self,
                               gate: QuantumCircuit,
                               power: int,
                               name: str) -> Gate:
        """Create gate^power as a single gate"""
        if power == 1:
            return gate.to_gate(label=name)
            
        # Create circuit with repeated applications
        num_qubits = gate.num_qubits
        powered_circuit = QuantumCircuit(num_qubits)
        
        for _ in range(power):
            powered_circuit.append(gate, range(num_qubits))
            
        return powered_circuit.to_gate(label=name)
        
    def estimate(self,
                problem: Dict,
                quantum_instance: QuantumInstance) -> Dict:
        """
        Estimate amplitude for given problem
        
        Args:
            problem: Problem dictionary with state_preparation and oracle
            quantum_instance: Quantum backend instance
            
        Returns:
            Estimation results
        """
        logger.info("Starting amplitude estimation")
        
        # Extract circuits
        state_preparation = problem['state_preparation']
        oracle = problem.get('oracle', None)
        
        # Build Grover operator if not provided
        if 'grover_operator' in problem:
            grover_operator = problem['grover_operator']
        else:
            grover_operator = self._build_grover_operator(
                state_preparation, oracle
            )
            
        # Choose estimation method
        if self.phase_estimation_method == "canonical":
            result = self._canonical_ae(
                state_preparation, grover_operator, quantum_instance
            )
        elif self.phase_estimation_method == "iterative":
            result = self._iterative_ae(
                state_preparation, grover_operator, quantum_instance
            )
        elif self.phase_estimation_method == "adaptive":
            result = self._adaptive_ae(
                state_preparation, grover_operator, quantum_instance
            )
        else:
            raise ValueError(f"Unknown method: {self.phase_estimation_method}")
            
        return result
        
    def _canonical_ae(self,
                     state_preparation: QuantumCircuit,
                     grover_operator: QuantumCircuit,
                     quantum_instance: QuantumInstance) -> Dict:
        """Canonical amplitude estimation using phase estimation"""
        # Build circuit
        ae_circuit = self.construct_circuit(state_preparation, grover_operator)
        
        # Execute
        job = quantum_instance.execute(ae_circuit, shots=8192)
        result = job.result()
        counts = result.get_counts()
        
        # Store measurement outcomes
        self.measurement_outcomes = counts
        
        # Process results
        if self.use_maximum_likelihood:
            amplitude, confidence = self._maximum_likelihood_estimation(counts)
        else:
            amplitude, confidence = self._simple_estimation(counts)
            
        self.estimated_amplitude = amplitude
        self.confidence_interval = confidence
        
        # Calculate derived quantities
        num_marked = amplitude ** 2 * (2 ** state_preparation.num_qubits)
        
        return {
            'amplitude': float(amplitude),
            'probability': float(amplitude ** 2),
            'confidence_interval': confidence,
            'num_marked_items': float(num_marked),
            'measurement_outcomes': counts,
            'num_oracle_calls': 2 ** self.num_eval_qubits - 1,
            'method': 'canonical_ae'
        }
        
    def _simple_estimation(self,
                          counts: Dict[str, int]) -> Tuple[float, Tuple[float, float]]:
        """Simple amplitude estimation from measurement outcomes"""
        total_shots = sum(counts.values())
        
        # Convert counts to phase estimates
        phases = []
        for outcome, count in counts.items():
            # Convert binary string to integer
            m = int(outcome, 2)
            # Convert to phase
            theta = m * np.pi / (2 ** self.num_eval_qubits)
            phases.extend([theta] * count)
            
        phases = np.array(phases)
        
        # Estimate amplitude
        # a = sin(theta/2)
        amplitudes = np.sin(phases / 2)
        
        # Point estimate
        amplitude = np.mean(amplitudes)
        
        # Confidence interval (using normal approximation)
        std_error = np.std(amplitudes) / np.sqrt(total_shots)
        z_score = 1.96  # 95% confidence
        
        lower = max(0, amplitude - z_score * std_error)
        upper = min(1, amplitude + z_score * std_error)
        
        return amplitude, (lower, upper)
        
    def _maximum_likelihood_estimation(self,
                                     counts: Dict[str, int]) -> Tuple[float, Tuple[float, float]]:
        """Maximum likelihood estimation of amplitude"""
        total_shots = sum(counts.values())
        
        # Build likelihood function
        def log_likelihood(a):
            """Log likelihood for amplitude a"""
            if a <= 0 or a >= 1:
                return -np.inf
                
            log_L = 0
            theta_a = 2 * np.arcsin(a)
            
            for outcome, count in counts.items():
                m = int(outcome, 2)
                M = 2 ** self.num_eval_qubits
                
                # Probability of measuring m given amplitude a
                prob = self._measurement_probability(m, M, theta_a)
                
                if prob > 0:
                    log_L += count * np.log(prob)
                else:
                    log_L += count * np.log(1e-10)  # Avoid log(0)
                    
            return log_L
            
        # Find MLE
        result = minimize_scalar(
            lambda a: -log_likelihood(a),
            bounds=(0.001, 0.999),
            method='bounded'
        )
        
        mle_amplitude = result.x
        
        # Fisher information for confidence interval
        fisher_info = self._fisher_information(mle_amplitude, total_shots)
        std_error = 1 / np.sqrt(fisher_info)
        
        z_score = 1.96  # 95% confidence
        lower = max(0, mle_amplitude - z_score * std_error)
        upper = min(1, mle_amplitude + z_score * std_error)
        
        # Store likelihood function
        self.likelihood_function = log_likelihood
        
        return mle_amplitude, (lower, upper)
        
    def _measurement_probability(self, m: int, M: int, theta: float) -> float:
        """Probability of measuring outcome m in phase estimation"""
        # Simplified model - should account for QFT details
        phase_est = m / M
        phase_actual = theta / (2 * np.pi)
        
        # Width of measurement bins
        delta = 1 / M
        
        # Probability using sinc function
        if abs(phase_est - phase_actual) < delta / 2:
            return 1 / M
        else:
            x = M * (phase_est - phase_actual)
            return (np.sin(np.pi * x) / (M * np.sin(np.pi * x / M))) ** 2
            
    def _fisher_information(self, amplitude: float, num_shots: int) -> float:
        """Calculate Fisher information for amplitude estimation"""
        if amplitude <= 0 or amplitude >= 1:
            return 1e-10
            
        # Fisher information for canonical AE
        theta = 2 * np.arcsin(amplitude)
        
        # Theoretical Fisher information
        fisher = 4 * num_shots / (np.sin(theta) ** 2)
        
        return fisher
        
    def _iterative_ae(self,
                     state_preparation: QuantumCircuit,
                     grover_operator: QuantumCircuit,
                     quantum_instance: QuantumInstance) -> Dict:
        """Iterative amplitude estimation with fewer qubits"""
        logger.info("Running iterative amplitude estimation")
        
        # Parameters
        num_iterations = self.num_eval_qubits
        shots_per_iteration = 1000
        
        # Initial bounds
        lower_bound = 0.0
        upper_bound = 1.0
        
        # Iteratively refine estimate
        for iteration in range(num_iterations):
            # Current estimate
            current_estimate = (lower_bound + upper_bound) / 2
            theta_estimate = 2 * np.arcsin(current_estimate)
            
            # Number of Grover iterations for this round
            k = int(np.pi / (4 * (upper_bound - lower_bound)))
            k = max(1, min(k, 100))  # Limit range
            
            # Build circuit for this iteration
            qc = QuantumCircuit(state_preparation.num_qubits, 1)
            qc.append(state_preparation, range(state_preparation.num_qubits))
            
            # Apply k Grover iterations
            for _ in range(k):
                qc.append(grover_operator, range(grover_operator.num_qubits))
                
            # Measure (simplified - should measure specific basis)
            qc.measure_all()
            
            # Execute
            job = quantum_instance.execute(qc, shots=shots_per_iteration)
            counts = job.result().get_counts()
            
            # Count successful outcomes
            success_count = sum(
                count for bitstring, count in counts.items()
                if self._is_marked_state(bitstring, state_preparation.num_qubits)
            )
            
            success_rate = success_count / shots_per_iteration
            
            # Update bounds based on measurement
            if success_rate > 0.5:
                lower_bound = current_estimate
            else:
                upper_bound = current_estimate
                
            logger.info(
                f"Iteration {iteration}: bounds=[{lower_bound:.4f}, {upper_bound:.4f}], "
                f"success_rate={success_rate:.4f}"
            )
            
        # Final estimate
        final_estimate = (lower_bound + upper_bound) / 2
        confidence_width = (upper_bound - lower_bound) / 2
        
        return {
            'amplitude': float(final_estimate),
            'probability': float(final_estimate ** 2),
            'confidence_interval': (float(lower_bound), float(upper_bound)),
            'num_oracle_calls': sum(range(1, num_iterations + 1)),
            'method': 'iterative_ae'
        }
        
    def _adaptive_ae(self,
                    state_preparation: QuantumCircuit,
                    grover_operator: QuantumCircuit,
                    quantum_instance: QuantumInstance) -> Dict:
        """Adaptive amplitude estimation"""
        logger.info("Running adaptive amplitude estimation")
        
        # Initialize with crude estimate
        crude_estimate = self._get_crude_estimate(
            state_preparation, quantum_instance
        )
        
        # Adaptive schedule based on crude estimate
        if crude_estimate < 0.1:
            schedule = [1, 2, 4, 8, 16, 32]
        elif crude_estimate < 0.3:
            schedule = [1, 3, 7, 15, 31]
        else:
            schedule = [1, 2, 3, 5, 8, 13]
            
        # Run measurements with adaptive schedule
        all_measurements = []
        total_oracle_calls = 0
        
        for num_grover in schedule:
            # Build circuit
            qc = QuantumCircuit(state_preparation.num_qubits, 1)
            qc.append(state_preparation, range(state_preparation.num_qubits))
            
            for _ in range(num_grover):
                qc.append(grover_operator, range(grover_operator.num_qubits))
                
            qc.measure_all()
            
            # Execute
            shots = 500
            job = quantum_instance.execute(qc, shots=shots)
            counts = job.result().get_counts()
            
            # Store results
            success_count = sum(
                count for bitstring, count in counts.items()
                if self._is_marked_state(bitstring, state_preparation.num_qubits)
            )
            
            all_measurements.append({
                'num_grover': num_grover,
                'success_count': success_count,
                'total_shots': shots
            })
            
            total_oracle_calls += num_grover * shots
            
        # Combine measurements using likelihood
        amplitude = self._combine_adaptive_measurements(all_measurements)
        
        # Bootstrap confidence interval
        lower, upper = self._bootstrap_confidence(all_measurements)
        
        return {
            'amplitude': float(amplitude),
            'probability': float(amplitude ** 2),
            'confidence_interval': (float(lower), float(upper)),
            'num_oracle_calls': total_oracle_calls,
            'measurement_schedule': schedule,
            'method': 'adaptive_ae'
        }
        
    def _get_crude_estimate(self,
                          state_preparation: QuantumCircuit,
                          quantum_instance: QuantumInstance) -> float:
        """Get crude amplitude estimate with direct measurement"""
        qc = state_preparation.copy()
        qc.measure_all()
        
        job = quantum_instance.execute(qc, shots=1000)
        counts = job.result().get_counts()
        
        success_count = sum(
            count for bitstring, count in counts.items()
            if self._is_marked_state(bitstring, state_preparation.num_qubits)
        )
        
        return np.sqrt(success_count / 1000)
        
    def _is_marked_state(self, bitstring: str, num_qubits: int) -> bool:
        """Check if bitstring represents a marked state"""
        # Simplified - would depend on specific problem
        # For now, assume marked if has specific pattern
        return '111' in bitstring
        
    def _combine_adaptive_measurements(self,
                                     measurements: List[Dict]) -> float:
        """Combine measurements from adaptive schedule"""
        def likelihood(a):
            """Combined likelihood for all measurements"""
            if a <= 0 or a >= 1:
                return -np.inf
                
            log_L = 0
            theta = 2 * np.arcsin(a)
            
            for meas in measurements:
                k = meas['num_grover']
                success = meas['success_count']
                total = meas['total_shots']
                
                # Probability of success after k Grover iterations
                prob_success = np.sin((2 * k + 1) * theta / 2) ** 2
                
                # Binomial likelihood
                log_L += success * np.log(prob_success + 1e-10)
                log_L += (total - success) * np.log(1 - prob_success + 1e-10)
                
            return log_L
            
        # Find MLE
        result = minimize_scalar(
            lambda a: -likelihood(a),
            bounds=(0.001, 0.999),
            method='bounded'
        )
        
        return result.x
        
    def _bootstrap_confidence(self,
                            measurements: List[Dict],
                            num_bootstrap: int = 1000) -> Tuple[float, float]:
        """Bootstrap confidence interval"""
        bootstrap_estimates = []
        
        for _ in range(num_bootstrap):
            # Resample measurements
            resampled = []
            for meas in measurements:
                success = np.random.binomial(
                    meas['total_shots'],
                    meas['success_count'] / meas['total_shots']
                )
                resampled.append({
                    'num_grover': meas['num_grover'],
                    'success_count': success,
                    'total_shots': meas['total_shots']
                })
                
            # Estimate from resampled data
            estimate = self._combine_adaptive_measurements(resampled)
            bootstrap_estimates.append(estimate)
            
        # Confidence interval from percentiles
        lower = np.percentile(bootstrap_estimates, 2.5)
        upper = np.percentile(bootstrap_estimates, 97.5)
        
        return lower, upper
        
    def _build_grover_operator(self,
                             state_preparation: QuantumCircuit,
                             oracle: Optional[QuantumCircuit]) -> QuantumCircuit:
        """Build Grover operator from state preparation and oracle"""
        num_qubits = state_preparation.num_qubits
        
        # If no oracle provided, create identity oracle
        if oracle is None:
            oracle = QuantumCircuit(num_qubits)
            oracle.z(num_qubits - 1)  # Mark last basis state
            
        # Build Grover operator
        grover = QuantumCircuit(num_qubits)
        
        # Oracle
        grover.append(oracle, range(num_qubits))
        
        # Diffusion operator
        grover.append(state_preparation.inverse(), range(num_qubits))
        grover.h(range(num_qubits))
        grover.x(range(num_qubits))
        
        # Multi-controlled Z
        grover.h(num_qubits - 1)
        grover.mcx(list(range(num_qubits - 1)), num_qubits - 1)
        grover.h(num_qubits - 1)
        
        grover.x(range(num_qubits))
        grover.h(range(num_qubits))
        grover.append(state_preparation, range(num_qubits))
        
        return grover
        
    def plot_likelihood(self, save_path: Optional[str] = None):
        """Plot likelihood function"""
        if self.likelihood_function is None:
            logger.warning("No likelihood function to plot")
            return
            
        amplitudes = np.linspace(0.01, 0.99, 200)
        likelihoods = [self.likelihood_function(a) for a in amplitudes]
        
        plt.figure(figsize=(10, 6))
        plt.plot(amplitudes, likelihoods, 'b-', linewidth=2)
        plt.axvline(self.estimated_amplitude, color='r', linestyle='--', 
                   label=f'MLE: {self.estimated_amplitude:.4f}')
        plt.axvline(self.confidence_interval[0], color='g', linestyle=':', 
                   label='95% CI')
        plt.axvline(self.confidence_interval[1], color='g', linestyle=':')
        
        plt.xlabel('Amplitude')
        plt.ylabel('Log Likelihood')
        plt.title('Amplitude Estimation Likelihood Function')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()


class MonteCarloAmplitudeEstimation(AmplitudeEstimation):
    """
    Amplitude Estimation for Monte Carlo Integration
    
    Specialized for estimating integrals and expectations.
    """
    
    def __init__(self,
                 integrand_function: Optional[Callable] = None,
                 domain_bounds: Optional[List[Tuple[float, float]]] = None,
                 **kwargs):
        """
        Initialize Monte Carlo Amplitude Estimation
        
        Args:
            integrand_function: Function to integrate
            domain_bounds: Integration domain bounds
            **kwargs: Parameters for AmplitudeEstimation
        """
        super().__init__(**kwargs)
        self.integrand_function = integrand_function
        self.domain_bounds = domain_bounds or [(0, 1)]
        
    def estimate_integral(self,
                         num_discretization_bits: int,
                         quantum_instance: QuantumInstance) -> Dict:
        """
        Estimate integral using amplitude estimation
        
        Args:
            num_discretization_bits: Bits for discretizing domain
            quantum_instance: Quantum backend
            
        Returns:
            Integral estimate
        """
        # Build state preparation for function encoding
        state_prep = self._build_function_encoding(num_discretization_bits)
        
        # Build oracle marking states above threshold
        threshold = 0.5  # Simplified
        oracle = self._build_threshold_oracle(
            num_discretization_bits, threshold
        )
        
        # Create problem
        problem = {
            'state_preparation': state_prep,
            'oracle': oracle
        }
        
        # Run amplitude estimation
        result = self.estimate(problem, quantum_instance)
        
        # Scale result to get integral
        domain_volume = np.prod([
            upper - lower for lower, upper in self.domain_bounds
        ])
        
        integral_estimate = result['amplitude'] * domain_volume
        
        return {
            'integral_estimate': float(integral_estimate),
            'amplitude': result['amplitude'],
            'confidence_interval': tuple(
                ci * domain_volume for ci in result['confidence_interval']
            ),
            'num_oracle_calls': result['num_oracle_calls']
        }
        
    def _build_function_encoding(self,
                               num_bits: int) -> QuantumCircuit:
        """Build quantum circuit encoding function values"""
        num_points = 2 ** num_bits
        qc = QuantumCircuit(num_bits + 1)  # +1 for ancilla
        
        # Simplified - would encode actual function values
        # For now, create superposition
        qc.h(range(num_bits))
        
        # Rotate ancilla based on function value
        for i in range(num_points):
            # Get function value at point i
            x = i / num_points
            if self.integrand_function:
                f_val = self.integrand_function(x)
            else:
                f_val = x  # Default to identity
                
            # Controlled rotation
            angle = 2 * np.arcsin(np.sqrt(f_val))
            
            # Would implement controlled rotation here
            # Simplified for illustration
            
        return qc
        
    def _build_threshold_oracle(self,
                              num_bits: int,
                              threshold: float) -> QuantumCircuit:
        """Build oracle marking states above threshold"""
        qc = QuantumCircuit(num_bits + 1)
        
        # Mark states where function value > threshold
        # Simplified implementation
        qc.z(num_bits)  # Mark ancilla
        
        return qc 