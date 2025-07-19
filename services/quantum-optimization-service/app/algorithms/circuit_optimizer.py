"""
Quantum Circuit Optimizer

Optimizes quantum circuits for:
- Gate count reduction
- Circuit depth minimization
- Noise resilience
- Hardware-specific compilation
- Resource efficiency
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import numpy as np
import networkx as nx

try:
    from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
    from qiskit.circuit import Gate, Instruction, Parameter
    from qiskit.transpiler import PassManager, Layout
    from qiskit.transpiler.passes import (
        Optimize1qGates, CommutativeCancellation, CXCancellation,
        OptimizeSwapBeforeMeasure, RemoveDiagonalGatesBeforeMeasure,
        Collect2qBlocks, ConsolidateBlocks, UnitarySynthesis,
        ApplyLayout, SetLayout, TrivialLayout, DenseLayout,
        StochasticSwap, SabreSwap, BasicSwap,
        FullAncillaAllocation, EnlargeWithAncilla,
        FixedPoint, Depth, Size, DAGCircuit
    )
    from qiskit.quantum_info import Operator, process_fidelity
    from qiskit.providers.fake_provider import FakeBackend
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Qiskit not available. Circuit optimization features limited.")

logger = logging.getLogger(__name__)


class OptimizationObjective(str, Enum):
    """Optimization objectives"""
    GATE_COUNT = "gate_count"
    CIRCUIT_DEPTH = "circuit_depth"
    TWO_QUBIT_GATES = "two_qubit_gates"
    EXECUTION_TIME = "execution_time"
    ERROR_RATE = "error_rate"
    BALANCED = "balanced"


class HardwareArchitecture(str, Enum):
    """Quantum hardware architectures"""
    IBM_QUANTUM = "ibm_quantum"
    GOOGLE_SYCAMORE = "google_sycamore"
    RIGETTI = "rigetti"
    IONQ = "ionq"
    XANADU = "xanadu"
    GENERIC = "generic"


@dataclass
class CircuitMetrics:
    """Metrics for quantum circuit"""
    num_qubits: int
    gate_count: int
    depth: int
    two_qubit_gate_count: int
    single_qubit_gate_count: int
    cnot_count: int
    t_gate_count: int
    measurement_count: int
    critical_path_length: int
    parallelism_factor: float
    estimated_error_rate: float = 0.0
    estimated_execution_time_us: float = 0.0


@dataclass
class OptimizationConfig:
    """Configuration for circuit optimization"""
    objective: OptimizationObjective = OptimizationObjective.BALANCED
    hardware_architecture: HardwareArchitecture = HardwareArchitecture.GENERIC
    max_optimization_level: int = 3
    preserve_measurements: bool = True
    allow_approximations: bool = False
    approximation_degree: float = 1e-10
    enable_layout_optimization: bool = True
    enable_routing_optimization: bool = True
    enable_gate_optimization: bool = True
    enable_pulse_optimization: bool = False
    hardware_constraints: Dict[str, Any] = field(default_factory=dict)
    custom_gate_costs: Dict[str, float] = field(default_factory=dict)
    

@dataclass
class OptimizationResult:
    """Result of circuit optimization"""
    optimized_circuit: Optional[Any] = None  # QuantumCircuit
    original_metrics: Optional[CircuitMetrics] = None
    optimized_metrics: Optional[CircuitMetrics] = None
    optimization_ratio: float = 1.0
    fidelity: float = 1.0
    optimization_time_seconds: float = 0.0
    applied_passes: List[str] = field(default_factory=list)
    layout_mapping: Optional[Dict[int, int]] = None
    warnings: List[str] = field(default_factory=list)


class QuantumCircuitOptimizer:
    """Advanced quantum circuit optimizer"""
    
    def __init__(self):
        self.logger = logger
        
        # Hardware specifications
        self.hardware_specs = {
            HardwareArchitecture.IBM_QUANTUM: {
                "native_gates": ["id", "rz", "sx", "x", "cx"],
                "coupling_map": None,  # Device-specific
                "basis_gates": ["id", "rz", "sx", "x", "cx"],
                "max_experiments": 900,
                "max_shots": 20000,
                "t1": 100e-6,  # Average T1 time
                "t2": 100e-6,  # Average T2 time
                "gate_times": {
                    "id": 0.035e-6,
                    "rz": 0.035e-6,
                    "sx": 0.035e-6,
                    "x": 0.035e-6,
                    "cx": 0.300e-6
                },
                "gate_errors": {
                    "id": 1e-4,
                    "rz": 1e-4,
                    "sx": 1e-4,
                    "x": 1e-4,
                    "cx": 1e-3
                }
            },
            HardwareArchitecture.GOOGLE_SYCAMORE: {
                "native_gates": ["sqrt_x", "sqrt_y", "sqrt_w", "cz"],
                "coupling_map": None,  # Grid topology
                "max_circuit_depth": 20,
                "gate_times": {
                    "sqrt_x": 0.025e-6,
                    "sqrt_y": 0.025e-6,
                    "sqrt_w": 0.025e-6,
                    "cz": 0.040e-6
                },
                "gate_errors": {
                    "single_qubit": 1.5e-3,
                    "two_qubit": 6e-3
                }
            },
            HardwareArchitecture.IONQ: {
                "native_gates": ["rx", "ry", "rz", "rxx"],
                "all_to_all_connectivity": True,
                "gate_times": {
                    "single_qubit": 0.010e-3,
                    "two_qubit": 0.200e-3
                },
                "gate_errors": {
                    "single_qubit": 5e-4,
                    "two_qubit": 1e-2
                }
            }
        }
        
        # Optimization strategies
        self.optimization_strategies = {
            OptimizationObjective.GATE_COUNT: self._optimize_gate_count,
            OptimizationObjective.CIRCUIT_DEPTH: self._optimize_depth,
            OptimizationObjective.TWO_QUBIT_GATES: self._optimize_two_qubit_gates,
            OptimizationObjective.EXECUTION_TIME: self._optimize_execution_time,
            OptimizationObjective.ERROR_RATE: self._optimize_error_rate,
            OptimizationObjective.BALANCED: self._optimize_balanced
        }
        
    async def optimize_circuit(self,
                              circuit: Any,  # QuantumCircuit
                              config: OptimizationConfig) -> OptimizationResult:
        """
        Optimize a quantum circuit
        
        Args:
            circuit: Quantum circuit to optimize
            config: Optimization configuration
            
        Returns:
            Optimization result
        """
        if not QISKIT_AVAILABLE:
            return OptimizationResult(
                warnings=["Qiskit not available. Cannot optimize circuit."]
            )
            
        try:
            import time
            start_time = time.time()
            
            # Analyze original circuit
            original_metrics = self._analyze_circuit(circuit)
            
            # Select optimization strategy
            strategy = self.optimization_strategies.get(
                config.objective, 
                self._optimize_balanced
            )
            
            # Apply optimization
            optimized_circuit = await strategy(circuit, config)
            
            # Analyze optimized circuit
            optimized_metrics = self._analyze_circuit(optimized_circuit)
            
            # Calculate optimization ratio
            optimization_ratio = self._calculate_optimization_ratio(
                original_metrics, 
                optimized_metrics,
                config.objective
            )
            
            # Verify fidelity
            fidelity = self._verify_fidelity(circuit, optimized_circuit)
            
            # Create result
            result = OptimizationResult(
                optimized_circuit=optimized_circuit,
                original_metrics=original_metrics,
                optimized_metrics=optimized_metrics,
                optimization_ratio=optimization_ratio,
                fidelity=fidelity,
                optimization_time_seconds=time.time() - start_time
            )
            
            # Add warnings if needed
            if fidelity < 0.99:
                result.warnings.append(
                    f"Circuit fidelity ({fidelity:.4f}) below 0.99"
                )
                
            return result
            
        except Exception as e:
            self.logger.error(f"Circuit optimization failed: {e}")
            return OptimizationResult(
                warnings=[f"Optimization failed: {str(e)}"]
            )
            
    def _analyze_circuit(self, circuit: QuantumCircuit) -> CircuitMetrics:
        """Analyze circuit metrics"""
        # Gate counts
        gate_counts = circuit.count_ops()
        total_gates = sum(gate_counts.values())
        
        # Two-qubit gates
        two_qubit_gates = ["cx", "cy", "cz", "ch", "crx", "cry", "crz", 
                          "cu1", "cu3", "swap", "iswap", "rxx", "ryy", "rzz"]
        two_qubit_count = sum(
            gate_counts.get(gate, 0) for gate in two_qubit_gates
        )
        
        # Single-qubit gates
        single_qubit_count = total_gates - two_qubit_count - gate_counts.get("measure", 0)
        
        # Circuit depth
        depth = circuit.depth()
        
        # Critical path
        dag = circuit_to_dag(circuit)
        critical_path = dag.longest_path()
        critical_path_length = len(critical_path) if critical_path else 0
        
        # Parallelism factor
        parallelism = total_gates / depth if depth > 0 else 0
        
        # Estimate execution time and error rate
        exec_time, error_rate = self._estimate_circuit_cost(circuit)
        
        return CircuitMetrics(
            num_qubits=circuit.num_qubits,
            gate_count=total_gates,
            depth=depth,
            two_qubit_gate_count=two_qubit_count,
            single_qubit_gate_count=single_qubit_count,
            cnot_count=gate_counts.get("cx", 0),
            t_gate_count=gate_counts.get("t", 0) + gate_counts.get("tdg", 0),
            measurement_count=gate_counts.get("measure", 0),
            critical_path_length=critical_path_length,
            parallelism_factor=parallelism,
            estimated_error_rate=error_rate,
            estimated_execution_time_us=exec_time
        )
        
    def _estimate_circuit_cost(self, 
                              circuit: QuantumCircuit,
                              hardware: Optional[HardwareArchitecture] = None) -> Tuple[float, float]:
        """Estimate execution time and error rate"""
        if hardware is None:
            hardware = HardwareArchitecture.GENERIC
            
        specs = self.hardware_specs.get(hardware, {})
        gate_times = specs.get("gate_times", {})
        gate_errors = specs.get("gate_errors", {})
        
        total_time = 0.0
        total_error = 0.0
        
        for gate, count in circuit.count_ops().items():
            # Time estimation
            if gate in gate_times:
                total_time += gate_times[gate] * count * 1e6  # Convert to microseconds
            else:
                # Default times
                if gate in ["cx", "cy", "cz", "swap"]:
                    total_time += 0.3 * count  # Two-qubit gate
                else:
                    total_time += 0.035 * count  # Single-qubit gate
                    
            # Error estimation
            if gate in gate_errors:
                total_error += gate_errors[gate] * count
            else:
                # Default errors
                if gate in ["cx", "cy", "cz", "swap"]:
                    total_error += 1e-3 * count  # Two-qubit error
                else:
                    total_error += 1e-4 * count  # Single-qubit error
                    
        return total_time, total_error
        
    async def _optimize_gate_count(self,
                                  circuit: QuantumCircuit,
                                  config: OptimizationConfig) -> QuantumCircuit:
        """Optimize for minimal gate count"""
        pm = PassManager()
        
        # Gate cancellation passes
        pm.append(Optimize1qGates())
        pm.append(CommutativeCancellation())
        pm.append(CXCancellation())
        
        # Block collection and consolidation
        pm.append(Collect2qBlocks())
        pm.append(ConsolidateBlocks())
        
        # Synthesis
        if config.hardware_architecture != HardwareArchitecture.GENERIC:
            basis_gates = self.hardware_specs[config.hardware_architecture].get(
                "basis_gates", ["u3", "cx"]
            )
            pm.append(UnitarySynthesis(basis_gates=basis_gates))
            
        # Remove redundant operations
        pm.append(OptimizeSwapBeforeMeasure())
        pm.append(RemoveDiagonalGatesBeforeMeasure())
        
        # Fixed point optimization
        pm.append(FixedPoint("gate_count"))
        
        return pm.run(circuit)
        
    async def _optimize_depth(self,
                             circuit: QuantumCircuit,
                             config: OptimizationConfig) -> QuantumCircuit:
        """Optimize for minimal circuit depth"""
        pm = PassManager()
        
        # Layout optimization for parallelism
        if config.enable_layout_optimization:
            pm.append(DenseLayout())
            
        # Commutation for parallelization
        pm.append(CommutativeCancellation())
        
        # Synthesis with depth optimization
        pm.append(UnitarySynthesis(
            basis_gates=["u3", "cx"],
            approximation_degree=config.approximation_degree,
            optimization_level=2  # Prioritize depth
        ))
        
        # Gate optimization
        pm.append(Optimize1qGates())
        
        return pm.run(circuit)
        
    async def _optimize_two_qubit_gates(self,
                                       circuit: QuantumCircuit,
                                       config: OptimizationConfig) -> QuantumCircuit:
        """Optimize for minimal two-qubit gates"""
        pm = PassManager()
        
        # Aggressive two-qubit gate reduction
        pm.append(CXCancellation())
        pm.append(CommutativeCancellation())
        
        # Collect and optimize blocks
        pm.append(Collect2qBlocks())
        pm.append(ConsolidateBlocks())
        
        # KAK decomposition for two-qubit gates
        pm.append(UnitarySynthesis(
            basis_gates=["u3", "cx"],
            approximation_degree=config.approximation_degree,
            optimization_level=3  # Maximum optimization
        ))
        
        # Routing optimization
        if config.enable_routing_optimization:
            pm.append(SabreSwap())  # Heuristic for minimal swaps
            
        return pm.run(circuit)
        
    async def _optimize_execution_time(self,
                                      circuit: QuantumCircuit,
                                      config: OptimizationConfig) -> QuantumCircuit:
        """Optimize for minimal execution time"""
        pm = PassManager()
        
        # Parallelize operations
        pm.append(CommutativeCancellation())
        
        # Minimize critical path
        pm.append(DenseLayout())
        
        # Hardware-specific optimization
        if config.hardware_architecture != HardwareArchitecture.GENERIC:
            specs = self.hardware_specs[config.hardware_architecture]
            gate_times = specs.get("gate_times", {})
            
            # Custom cost function based on gate times
            def time_cost(gate):
                return gate_times.get(gate.name, 0.1)
                
            pm.append(UnitarySynthesis(
                basis_gates=specs.get("basis_gates", ["u3", "cx"]),
                approximation_degree=config.approximation_degree,
                optimization_level=2
            ))
            
        return pm.run(circuit)
        
    async def _optimize_error_rate(self,
                                  circuit: QuantumCircuit,
                                  config: OptimizationConfig) -> QuantumCircuit:
        """Optimize for minimal error rate"""
        pm = PassManager()
        
        # Reduce high-error operations
        pm.append(CXCancellation())  # CNOT has high error
        
        # Error-aware synthesis
        if config.hardware_architecture != HardwareArchitecture.GENERIC:
            specs = self.hardware_specs[config.hardware_architecture]
            gate_errors = specs.get("gate_errors", {})
            
            # Prefer low-error gates
            basis_gates = sorted(
                specs.get("basis_gates", ["u3", "cx"]),
                key=lambda g: gate_errors.get(g, 1e-3)
            )
            
            pm.append(UnitarySynthesis(
                basis_gates=basis_gates,
                approximation_degree=config.approximation_degree
            ))
            
        # Minimize gate count (fewer gates = lower error)
        pm.append(Optimize1qGates())
        pm.append(ConsolidateBlocks())
        
        return pm.run(circuit)
        
    async def _optimize_balanced(self,
                                circuit: QuantumCircuit,
                                config: OptimizationConfig) -> QuantumCircuit:
        """Balanced optimization considering all factors"""
        pm = PassManager()
        
        # Standard optimization sequence
        pm.append(Optimize1qGates())
        pm.append(CommutativeCancellation())
        pm.append(CXCancellation())
        pm.append(OptimizeSwapBeforeMeasure())
        
        # Layout and routing
        if config.enable_layout_optimization:
            pm.append(TrivialLayout())
            
        if config.enable_routing_optimization:
            pm.append(StochasticSwap())
            
        # Synthesis
        pm.append(Collect2qBlocks())
        pm.append(ConsolidateBlocks())
        pm.append(UnitarySynthesis())
        
        # Final cleanup
        pm.append(RemoveDiagonalGatesBeforeMeasure())
        pm.append(FixedPoint("balanced"))
        
        return pm.run(circuit)
        
    def _calculate_optimization_ratio(self,
                                     original: CircuitMetrics,
                                     optimized: CircuitMetrics,
                                     objective: OptimizationObjective) -> float:
        """Calculate optimization improvement ratio"""
        if objective == OptimizationObjective.GATE_COUNT:
            return original.gate_count / max(optimized.gate_count, 1)
        elif objective == OptimizationObjective.CIRCUIT_DEPTH:
            return original.depth / max(optimized.depth, 1)
        elif objective == OptimizationObjective.TWO_QUBIT_GATES:
            return original.two_qubit_gate_count / max(optimized.two_qubit_gate_count, 1)
        elif objective == OptimizationObjective.EXECUTION_TIME:
            return original.estimated_execution_time_us / max(optimized.estimated_execution_time_us, 0.1)
        elif objective == OptimizationObjective.ERROR_RATE:
            return original.estimated_error_rate / max(optimized.estimated_error_rate, 1e-6)
        else:  # BALANCED
            # Weighted average of improvements
            gate_ratio = original.gate_count / max(optimized.gate_count, 1)
            depth_ratio = original.depth / max(optimized.depth, 1)
            two_qubit_ratio = original.two_qubit_gate_count / max(optimized.two_qubit_gate_count, 1)
            
            return (gate_ratio + depth_ratio + two_qubit_ratio) / 3
            
    def _verify_fidelity(self, 
                        original: QuantumCircuit, 
                        optimized: QuantumCircuit) -> float:
        """Verify quantum state fidelity between circuits"""
        try:
            # Convert to operators
            original_op = Operator(original)
            optimized_op = Operator(optimized)
            
            # Calculate process fidelity
            fidelity = process_fidelity(original_op, optimized_op)
            
            return float(fidelity)
            
        except Exception as e:
            self.logger.warning(f"Could not calculate fidelity: {e}")
            return 1.0  # Assume perfect fidelity if cannot calculate
            
    async def compile_for_hardware(self,
                                  circuit: QuantumCircuit,
                                  hardware: HardwareArchitecture,
                                  backend: Optional[Any] = None) -> OptimizationResult:
        """Compile circuit for specific hardware"""
        config = OptimizationConfig(
            objective=OptimizationObjective.BALANCED,
            hardware_architecture=hardware,
            enable_layout_optimization=True,
            enable_routing_optimization=True
        )
        
        # Hardware-specific configuration
        if hardware == HardwareArchitecture.IBM_QUANTUM:
            config.hardware_constraints = {
                "coupling_map": backend.configuration().coupling_map if backend else None,
                "basis_gates": backend.configuration().basis_gates if backend else None
            }
        elif hardware == HardwareArchitecture.IONQ:
            config.hardware_constraints = {
                "all_to_all": True
            }
            
        return await self.optimize_circuit(circuit, config)
        
    def suggest_optimizations(self, 
                             circuit: QuantumCircuit) -> List[str]:
        """Suggest optimization strategies for a circuit"""
        if not QISKIT_AVAILABLE:
            return ["Install Qiskit for circuit optimization features"]
            
        suggestions = []
        metrics = self._analyze_circuit(circuit)
        
        # High gate count
        if metrics.gate_count > 100:
            suggestions.append(
                "High gate count detected. Consider gate count optimization."
            )
            
        # Deep circuit
        if metrics.depth > 50:
            suggestions.append(
                "Deep circuit detected. Consider depth optimization for better coherence."
            )
            
        # Many two-qubit gates
        two_qubit_ratio = metrics.two_qubit_gate_count / max(metrics.gate_count, 1)
        if two_qubit_ratio > 0.3:
            suggestions.append(
                "High ratio of two-qubit gates. Consider two-qubit gate optimization."
            )
            
        # Poor parallelism
        if metrics.parallelism_factor < 1.5:
            suggestions.append(
                "Low parallelism detected. Consider commutation-based optimization."
            )
            
        # T-gate usage
        if metrics.t_gate_count > 10:
            suggestions.append(
                "Many T-gates detected. Consider T-gate optimization for fault tolerance."
            )
            
        return suggestions
        

class CircuitOptimizationService:
    """Service for circuit optimization via API"""
    
    def __init__(self):
        self.optimizer = QuantumCircuitOptimizer()
        
    async def optimize(self,
                      circuit_qasm: str,
                      config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize circuit from QASM string
        
        Args:
            circuit_qasm: OpenQASM circuit string
            config: Optimization configuration
            
        Returns:
            Optimization result as dictionary
        """
        if not QISKIT_AVAILABLE:
            return {
                "status": "error",
                "message": "Qiskit not available"
            }
            
        try:
            # Parse circuit
            circuit = QuantumCircuit.from_qasm_str(circuit_qasm)
            
            # Create config
            opt_config = OptimizationConfig(**config)
            
            # Optimize
            result = await self.optimizer.optimize_circuit(circuit, opt_config)
            
            # Convert to dictionary
            return {
                "status": "success",
                "optimized_qasm": result.optimized_circuit.qasm() if result.optimized_circuit else None,
                "original_metrics": result.original_metrics.__dict__ if result.original_metrics else None,
                "optimized_metrics": result.optimized_metrics.__dict__ if result.optimized_metrics else None,
                "optimization_ratio": result.optimization_ratio,
                "fidelity": result.fidelity,
                "optimization_time": result.optimization_time_seconds,
                "warnings": result.warnings
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
            
    async def analyze(self, circuit_qasm: str) -> Dict[str, Any]:
        """Analyze circuit and provide suggestions"""
        if not QISKIT_AVAILABLE:
            return {
                "status": "error",
                "message": "Qiskit not available"
            }
            
        try:
            circuit = QuantumCircuit.from_qasm_str(circuit_qasm)
            metrics = self.optimizer._analyze_circuit(circuit)
            suggestions = self.optimizer.suggest_optimizations(circuit)
            
            return {
                "status": "success",
                "metrics": metrics.__dict__,
                "suggestions": suggestions
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }


# Helper functions for non-Qiskit environments
def circuit_to_dag(circuit):
    """Convert circuit to DAG (placeholder for non-Qiskit env)"""
    if QISKIT_AVAILABLE:
        from qiskit.converters import circuit_to_dag
        return circuit_to_dag(circuit)
    else:
        # Return mock DAG
        class MockDAG:
            def longest_path(self):
                return []
        return MockDAG() 