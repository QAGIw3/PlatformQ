"""
Quantum Optimization Service Models

SQLAlchemy models for quantum optimization problems and solutions.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class ProblemType(str, enum.Enum):
    """Types of optimization problems"""
    LINEAR_PROGRAMMING = "linear_programming"
    QUADRATIC_PROGRAMMING = "quadratic_programming"
    QUBO = "qubo"  # Quadratic Unconstrained Binary Optimization
    ISING = "ising"
    MAX_CUT = "max_cut"
    TSP = "tsp"  # Traveling Salesman Problem
    VRP = "vrp"  # Vehicle Routing Problem
    PORTFOLIO_OPTIMIZATION = "portfolio_optimization"
    SCHEDULING = "scheduling"
    GRAPH_COLORING = "graph_coloring"
    SAT = "sat"  # Boolean Satisfiability
    MOLECULAR_SIMULATION = "molecular_simulation"
    CUSTOM = "custom"


class SolverType(str, enum.Enum):
    """Types of solvers"""
    QAOA = "qaoa"  # Quantum Approximate Optimization Algorithm
    VQE = "vqe"  # Variational Quantum Eigensolver
    QUANTUM_ANNEALING = "quantum_annealing"
    AMPLITUDE_ESTIMATION = "amplitude_estimation"
    GROVER = "grover"
    HHL = "hhl"  # Harrow-Hassidim-Lloyd algorithm
    HYBRID_CLASSICAL_QUANTUM = "hybrid"
    BENDERS_DECOMPOSITION = "benders"
    NEUROMORPHIC = "neuromorphic"
    CLASSICAL = "classical"


class JobStatus(str, enum.Enum):
    """Optimization job status"""
    PENDING = "pending"
    ENCODING = "encoding"
    RUNNING = "running"
    DECODING = "decoding"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class BackendType(str, enum.Enum):
    """Quantum backend types"""
    SIMULATOR = "simulator"
    IBM_QUANTUM = "ibm_quantum"
    RIGETTI = "rigetti"
    IONQ = "ionq"
    DWAVE = "dwave"
    AZURE_QUANTUM = "azure_quantum"
    AMAZON_BRAKET = "amazon_braket"
    NEUROMORPHIC_CHIP = "neuromorphic"


class OptimizationProblem(Base):
    """Optimization problem definition"""
    __tablename__ = "optimization_problems"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    problem_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Problem metadata
    name = Column(String(255), nullable=False)
    description = Column(Text)
    problem_type = Column(Enum(ProblemType), nullable=False, index=True)
    
    # Problem definition
    objective_function = Column(JSON, nullable=False)  # Coefficients, terms
    constraints = Column(JSON, default=list)  # Linear/quadratic constraints
    variables = Column(JSON, default=dict)  # Variable definitions and bounds
    problem_data = Column(JSON)  # Additional problem-specific data
    
    # Problem characteristics
    num_variables = Column(Integer)
    num_constraints = Column(Integer)
    is_convex = Column(Boolean)
    is_linear = Column(Boolean)
    sparsity = Column(Float)  # 0-1, measure of sparsity
    
    # Configuration
    solver_preferences = Column(JSON)  # Preferred solvers and settings
    accuracy_requirement = Column(Float)  # Required solution accuracy
    time_limit_seconds = Column(Integer)  # Maximum solving time
    
    # Status
    is_template = Column(Boolean, default=False)
    is_public = Column(Boolean, default=False)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    jobs = relationship("OptimizationJob", back_populates="problem")
    
    # Indexes
    __table_args__ = (
        Index('idx_problem_tenant_type', 'tenant_id', 'problem_type'),
        Index('idx_problem_template', 'is_template', 'is_public'),
    )


class OptimizationJob(Base):
    """Optimization job execution"""
    __tablename__ = "optimization_jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    problem_id = Column(String(255), ForeignKey("optimization_problems.problem_id"), nullable=False)
    
    # Job configuration
    solver_type = Column(Enum(SolverType), nullable=False, index=True)
    backend_type = Column(Enum(BackendType), nullable=False)
    backend_config = Column(JSON)  # Backend-specific configuration
    
    # Solver parameters
    solver_params = Column(JSON)  # Algorithm-specific parameters
    max_iterations = Column(Integer)
    convergence_threshold = Column(Float)
    
    # Quantum-specific settings
    num_qubits = Column(Integer)
    circuit_depth = Column(Integer)
    num_shots = Column(Integer)  # For sampling
    optimization_level = Column(Integer)  # Transpiler optimization
    
    # Status and progress
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, index=True)
    progress = Column(Float, default=0)  # 0-100
    current_iteration = Column(Integer, default=0)
    
    # Results
    solution = Column(JSON)  # Optimal variable values
    objective_value = Column(Float)  # Optimal objective function value
    solution_quality = Column(Float)  # Quality metric (0-1)
    
    # Performance metrics
    encoding_time_ms = Column(Integer)
    solving_time_ms = Column(Integer)
    decoding_time_ms = Column(Integer)
    total_time_ms = Column(Integer)
    
    # Resource usage
    quantum_volume_used = Column(Integer)
    classical_cpu_seconds = Column(Float)
    memory_mb_used = Column(Integer)
    
    # Cost tracking
    quantum_credits_used = Column(Float)
    estimated_cost_usd = Column(Float)
    
    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Timing
    submitted_at = Column(DateTime, default=datetime.utcnow, index=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    created_by = Column(String(255))
    
    # Relationships
    problem = relationship("OptimizationProblem", back_populates="jobs")
    iterations = relationship("OptimizationIteration", back_populates="job")
    
    # Indexes
    __table_args__ = (
        Index('idx_job_tenant_status', 'tenant_id', 'status'),
        Index('idx_job_problem', 'problem_id'),
        Index('idx_job_submitted', 'submitted_at'),
    )


class OptimizationIteration(Base):
    """Iteration history for optimization jobs"""
    __tablename__ = "optimization_iterations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(255), ForeignKey("optimization_jobs.job_id"), nullable=False)
    
    # Iteration details
    iteration_number = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Solution state
    current_solution = Column(JSON)
    objective_value = Column(Float)
    constraint_violations = Column(JSON)
    
    # Quantum state (if applicable)
    quantum_state = Column(JSON)  # State vector or density matrix
    measurement_outcomes = Column(JSON)  # Measurement statistics
    
    # Convergence metrics
    gradient_norm = Column(Float)
    step_size = Column(Float)
    improvement = Column(Float)  # Improvement from previous iteration
    
    # Relationships
    job = relationship("OptimizationJob", back_populates="iterations")


class QuantumCircuit(Base):
    """Quantum circuit definitions"""
    __tablename__ = "quantum_circuits"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    circuit_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Circuit metadata
    name = Column(String(255), nullable=False)
    description = Column(Text)
    circuit_type = Column(String(100))  # ansatz, encoding, etc.
    
    # Circuit definition
    qasm_code = Column(Text)  # OpenQASM representation
    circuit_json = Column(JSON)  # JSON representation
    
    # Circuit properties
    num_qubits = Column(Integer, nullable=False)
    depth = Column(Integer)
    gate_count = Column(Integer)
    gate_types = Column(JSON)  # Count of each gate type
    
    # Parameters
    num_parameters = Column(Integer, default=0)
    parameter_names = Column(JSON)
    
    # Usage
    is_template = Column(Boolean, default=False)
    usage_count = Column(Integer, default=0)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_circuit_tenant_type', 'tenant_id', 'circuit_type'),
    )


class OptimizationTemplate(Base):
    """Reusable optimization problem templates"""
    __tablename__ = "optimization_templates"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    template_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Template metadata
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    category = Column(String(100))  # finance, logistics, ml, etc.
    
    # Problem template
    problem_type = Column(Enum(ProblemType), nullable=False)
    template_definition = Column(JSON, nullable=False)  # Parameterized problem
    parameters = Column(JSON)  # Parameter definitions
    
    # Example
    example_data = Column(JSON)
    example_solution = Column(JSON)
    
    # Recommended settings
    recommended_solver = Column(Enum(SolverType))
    recommended_backend = Column(Enum(BackendType))
    default_params = Column(JSON)
    
    # Usage
    is_public = Column(Boolean, default=False)
    usage_count = Column(Integer, default=0)
    average_solving_time = Column(Float)
    success_rate = Column(Float)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_template_category', 'category'),
        Index('idx_template_public', 'is_public'),
    )


class SolverBenchmark(Base):
    """Benchmark results for different solvers"""
    __tablename__ = "solver_benchmarks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    benchmark_id = Column(String(255), unique=True, nullable=False, index=True)
    
    # Problem details
    problem_type = Column(Enum(ProblemType), nullable=False, index=True)
    problem_size = Column(Integer)  # Number of variables
    problem_characteristics = Column(JSON)
    
    # Solver details
    solver_type = Column(Enum(SolverType), nullable=False, index=True)
    backend_type = Column(Enum(BackendType), nullable=False)
    solver_params = Column(JSON)
    
    # Performance metrics
    solving_time_ms = Column(Integer)
    solution_quality = Column(Float)  # 0-1
    iterations_needed = Column(Integer)
    
    # Resource usage
    memory_mb = Column(Integer)
    quantum_volume = Column(Integer)
    quantum_depth = Column(Integer)
    
    # Cost
    estimated_cost_usd = Column(Float)
    
    # Environment
    hardware_specs = Column(JSON)
    software_versions = Column(JSON)
    
    benchmark_date = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_benchmark_problem_solver', 'problem_type', 'solver_type'),
        Index('idx_benchmark_date', 'benchmark_date'),
    )


class QuantumResourceAllocation(Base):
    """Track quantum resource allocations"""
    __tablename__ = "quantum_resource_allocations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    allocation_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    
    # Resource limits
    monthly_quantum_credits = Column(Float, nullable=False)
    daily_job_limit = Column(Integer)
    max_qubits = Column(Integer)
    max_circuit_depth = Column(Integer)
    
    # Usage tracking
    credits_used_this_month = Column(Float, default=0)
    jobs_today = Column(Integer, default=0)
    last_reset_date = Column(DateTime)
    
    # Priority
    priority_level = Column(Integer, default=0)  # Higher = better
    
    # Status
    is_active = Column(Boolean, default=True)
    suspended_until = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_allocation_tenant_user', 'tenant_id', 'user_id'),
    ) 