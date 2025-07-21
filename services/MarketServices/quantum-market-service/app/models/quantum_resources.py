"""
Quantum resource models for the Quantum Market Service.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
import uuid


class QPUProvider(str, Enum):
    """Quantum processor providers."""
    IBM = "ibm"
    GOOGLE = "google"
    IONQ = "ionq"
    RIGETTI = "rigetti"
    DWAVE = "dwave"
    HONEYWELL = "honeywell"
    XANADU = "xanadu"
    PASQAL = "pasqal"
    CUSTOM = "custom"


class QPUTopology(str, Enum):
    """QPU connectivity topologies."""
    LINEAR = "linear"
    RING = "ring"
    GRID = "grid"
    HEXAGONAL = "hexagonal"
    ALL_TO_ALL = "all_to_all"
    CUSTOM = "custom"


class QuantumGateSet(str, Enum):
    """Supported quantum gates."""
    RX = "rx"
    RY = "ry"
    RZ = "rz"
    H = "h"
    X = "x"
    Y = "y"
    Z = "z"
    CX = "cx"
    CZ = "cz"
    SWAP = "swap"
    TOFFOLI = "toffoli"
    MEASURE = "measure"
    RESET = "reset"


class AlgorithmType(str, Enum):
    """Quantum algorithm types."""
    VQE = "vqe"
    QAOA = "qaoa"
    GROVER = "grover"
    SHOR = "shor"
    HHL = "hhl"
    QML = "qml"
    CUSTOM = "custom"


class WindowStatus(str, Enum):
    """Coherence window status."""
    AVAILABLE = "available"
    RESERVED = "reserved"
    ALLOCATED = "allocated"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class QPUSpec(BaseModel):
    """Quantum Processing Unit specification."""
    qpu_id: str = Field(default_factory=lambda: f"qpu_{uuid.uuid4().hex[:8]}")
    provider: QPUProvider
    model: str  # e.g., "ibmq_manhattan", "Sycamore", "Aria-1"
    
    # Quantum properties
    qubit_count: int = Field(..., gt=0, le=1000)
    coherence_time: int = Field(..., gt=0)  # microseconds
    gate_fidelity: Decimal = Field(..., ge=0, le=1)
    measurement_fidelity: Decimal = Field(..., ge=0, le=1)
    
    # Connectivity
    topology: QPUTopology
    connectivity_graph: Dict[int, List[int]] = Field(default_factory=dict)
    coupling_map: Optional[List[List[int]]] = None
    
    # Gate set
    native_gates: List[QuantumGateSet]
    gate_times: Dict[str, int] = Field(default_factory=dict)  # nanoseconds
    
    # Physical properties
    operating_temperature: Decimal  # Kelvin
    readout_error: Decimal = Field(..., ge=0, le=1)
    
    # Error rates
    single_qubit_error: Decimal = Field(..., ge=0, le=1)
    two_qubit_error: Decimal = Field(..., ge=0, le=1)
    
    # Metadata
    location: str
    is_active: bool = True
    last_calibration: datetime
    quality_score: int = Field(default=5000, ge=0, le=10000)  # basis points
    
    # Provider info
    provider_address: str
    token_id: Optional[int] = None  # Resource token ID
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }
    
    @validator('gate_fidelity', 'measurement_fidelity')
    def validate_fidelity(cls, v):
        if v < Decimal('0.9'):
            raise ValueError("Fidelity must be at least 90%")
        return v


class CoherenceWindow(BaseModel):
    """Coherence window for quantum computation."""
    window_id: str = Field(default_factory=lambda: f"cw_{uuid.uuid4().hex[:8]}")
    qpu_id: str
    
    # Time window
    start_time: datetime
    duration_us: int = Field(..., gt=0)  # microseconds
    end_time: datetime
    
    # Resource allocation
    qubit_allocation: int = Field(..., gt=0)
    allocated_qubits: Optional[List[int]] = None  # Specific qubit indices
    
    # User info
    user_address: Optional[str] = None
    token_id: Optional[int] = None
    
    # Algorithm info
    algorithm_hash: Optional[str] = None
    algorithm_type: Optional[AlgorithmType] = None
    expected_gates: Optional[int] = None
    
    # Pricing
    base_price: Decimal
    final_price: Optional[Decimal] = None
    is_auction: bool = False
    reserve_price: Optional[Decimal] = None
    
    # Execution
    status: WindowStatus = WindowStatus.AVAILABLE
    actual_coherence: Optional[int] = None  # Achieved coherence time
    execution_result: Optional[Dict[str, Any]] = None
    result_hash: Optional[str] = None
    
    # Quality metrics
    success: Optional[bool] = None
    error_rate: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }
    
    @validator('end_time', always=True)
    def set_end_time(cls, v, values):
        if 'start_time' in values and 'duration_us' in values:
            return values['start_time'] + timedelta(microseconds=values['duration_us'])
        return v


class EntanglementPair(BaseModel):
    """Quantum entanglement pair."""
    pair_id: str = Field(default_factory=lambda: f"ep_{uuid.uuid4().hex[:8]}")
    
    # QPUs involved
    source_qpu: str
    target_qpu: str
    source_qubit: Optional[int] = None
    target_qubit: Optional[int] = None
    
    # Entanglement properties
    pair_count: int = Field(..., gt=0)
    fidelity: Decimal = Field(..., ge=0, le=1)
    initial_fidelity: Decimal = Field(..., ge=0, le=1)
    
    # Timing
    creation_time: datetime
    expected_lifetime: int  # microseconds
    expiry_time: datetime
    
    # Ownership
    owner_address: str
    is_consumed: bool = False
    consumed_by: Optional[str] = None
    
    # Pricing
    price_per_pair: Decimal
    total_price: Decimal
    
    # Bell state type
    bell_state: str = "phi_plus"  # |Φ+⟩, |Φ-⟩, |Ψ+⟩, |Ψ-⟩
    
    # Network properties
    distance: Optional[int] = None  # km between QPUs
    channel_loss: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }
    
    @validator('fidelity')
    def validate_fidelity(cls, v):
        if v < Decimal('0.5'):
            raise ValueError("Fidelity below classical threshold")
        return v
    
    @validator('expiry_time', always=True)
    def set_expiry_time(cls, v, values):
        if 'creation_time' in values and 'expected_lifetime' in values:
            return values['creation_time'] + timedelta(microseconds=values['expected_lifetime'])
        return v


class QuantumAlgorithm(BaseModel):
    """Quantum algorithm specification."""
    algorithm_id: str = Field(default_factory=lambda: f"qa_{uuid.uuid4().hex[:8]}")
    name: str
    algorithm_type: AlgorithmType
    
    # Resource requirements
    min_qubits: int = Field(..., gt=0)
    max_qubits: Optional[int] = None
    circuit_depth: int = Field(..., gt=0)
    gate_count: int = Field(..., gt=0)
    
    # Gate requirements
    required_gates: List[QuantumGateSet]
    two_qubit_gate_count: int = Field(default=0, ge=0)
    
    # Connectivity requirements
    required_topology: Optional[QPUTopology] = None
    min_connectivity: Optional[int] = None  # Minimum qubit connectivity
    
    # Timing requirements
    min_coherence_time: int  # microseconds
    estimated_runtime: int  # microseconds
    
    # Fidelity requirements
    min_gate_fidelity: Decimal = Field(..., ge=0, le=1)
    min_measurement_fidelity: Decimal = Field(..., ge=0, le=1)
    
    # Special requirements
    requires_mid_circuit_measurement: bool = False
    requires_parametric_compilation: bool = False
    requires_error_mitigation: bool = False
    
    # Circuit info
    circuit_hash: str
    parameterized: bool = False
    parameter_count: Optional[int] = None
    
    # Performance metrics
    success_rate: Optional[Decimal] = None
    average_fidelity: Optional[Decimal] = None
    execution_count: int = 0
    
    # Metadata
    creator: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    description: Optional[str] = None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class QPUAvailability(BaseModel):
    """QPU availability information."""
    qpu_id: str
    available_qubits: int
    
    # Time slots
    available_windows: List[Dict[str, Any]]  # Start time and duration
    next_available: datetime
    
    # Utilization
    current_utilization: Decimal = Field(..., ge=0, le=1)
    daily_utilization: Decimal = Field(..., ge=0, le=1)
    
    # Pricing
    current_spot_price: Decimal  # Per qubit-microsecond
    price_trend: str = "stable"  # "increasing", "decreasing", "stable"
    
    # Queue depth
    pending_jobs: int = 0
    average_wait_time: int = 0  # seconds
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class QuantumPriceQuote(BaseModel):
    """Price quote for quantum computation."""
    quote_id: str = Field(default_factory=lambda: f"pq_{uuid.uuid4().hex[:8]}")
    
    # Resource details
    qpu_id: str
    qubit_count: int
    coherence_time: int  # microseconds
    algorithm_type: Optional[AlgorithmType] = None
    
    # Pricing components
    base_price: Decimal
    coherence_premium: Decimal = Decimal("0")
    fidelity_premium: Decimal = Decimal("0")
    complexity_factor: Decimal = Decimal("1")
    urgency_premium: Decimal = Decimal("0")
    
    # Final price
    total_price: Decimal
    price_per_qubit_us: Decimal
    
    # Validity
    valid_until: datetime
    execution_window: Optional[Dict[str, Any]] = None
    
    # Comparison
    classical_equivalent_cost: Optional[Decimal] = None
    quantum_advantage_factor: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class QuantumArbitrageOpportunity(BaseModel):
    """Quantum-classical arbitrage opportunity."""
    opportunity_id: str = Field(default_factory=lambda: f"arb_{uuid.uuid4().hex[:8]}")
    
    # Algorithm details
    algorithm_id: str
    algorithm_type: AlgorithmType
    problem_size: Dict[str, Any]
    
    # Quantum execution
    quantum_qpu: str
    quantum_cost: Decimal
    quantum_time: int  # microseconds
    quantum_success_probability: Decimal
    
    # Classical execution
    classical_resource: str
    classical_cost: Decimal
    classical_time: int  # milliseconds
    
    # Arbitrage metrics
    cost_advantage: Decimal  # Quantum cost / Classical cost
    speed_advantage: Decimal  # Classical time / Quantum time
    overall_advantage: Decimal
    
    # Risk factors
    coherence_risk: Decimal = Field(..., ge=0, le=1)
    error_risk: Decimal = Field(..., ge=0, le=1)
    
    # Validity
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime
    
    # Execution info
    is_executed: bool = False
    executed_by: Optional[str] = None
    profit: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }
    
    @validator('overall_advantage')
    def validate_advantage(cls, v):
        if v <= Decimal('1'):
            raise ValueError("No quantum advantage")
        return v


# Request/Response models

class RegisterQPURequest(BaseModel):
    """Request to register a new QPU."""
    provider: QPUProvider
    model: str
    qubit_count: int
    coherence_time: int
    gate_fidelity: Decimal
    measurement_fidelity: Decimal
    topology: QPUTopology
    connectivity_graph: Dict[int, List[int]]
    native_gates: List[QuantumGateSet]
    operating_temperature: Decimal
    location: str


class CreateCoherenceWindowRequest(BaseModel):
    """Request to create a coherence window."""
    qpu_id: str
    start_time: datetime
    duration_us: int
    qubit_count: int
    is_auction: bool = False
    start_price: Optional[Decimal] = None
    reserve_price: Optional[Decimal] = None


class AllocateWindowRequest(BaseModel):
    """Request to allocate a coherence window."""
    window_id: str
    algorithm_hash: Optional[str] = None
    expected_gates: Optional[int] = None


class CreateEntanglementRequest(BaseModel):
    """Request to create entanglement pairs."""
    source_qpu: str
    target_qpu: str
    pair_count: int
    bell_state: str = "phi_plus"


class TradeEntanglementRequest(BaseModel):
    """Request to trade entanglement pairs."""
    pair_id: str
    new_owner: str
    price: Decimal


class RegisterAlgorithmRequest(BaseModel):
    """Request to register a quantum algorithm."""
    name: str
    algorithm_type: AlgorithmType
    circuit_hash: str
    min_qubits: int
    circuit_depth: int
    gate_count: int
    required_gates: List[QuantumGateSet]
    min_coherence_time: int
    min_gate_fidelity: Decimal
    min_measurement_fidelity: Decimal
    description: Optional[str] = None


class QuantumAdvantageQuery(BaseModel):
    """Query for quantum advantage calculation."""
    algorithm_id: str
    problem_size: Dict[str, Any]
    classical_time_estimate: int  # milliseconds
    classical_cost_estimate: Decimal
    deadline: Optional[datetime] = None 