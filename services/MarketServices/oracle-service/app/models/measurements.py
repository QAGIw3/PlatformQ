"""
Oracle Measurement Models
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field, validator


# Enums
class MeasurementType(str, Enum):
    """Types of oracle measurements"""
    QUANTUM_FIDELITY = "quantum_fidelity"
    QUANTUM_COHERENCE = "quantum_coherence"
    QUANTUM_ERROR_RATE = "quantum_error_rate"
    AI_BENCHMARK = "ai_benchmark"
    AI_INFERENCE_LATENCY = "ai_inference_latency"
    AI_THERMAL = "ai_thermal"
    AI_POWER = "ai_power"
    NETWORK_LATENCY = "network_latency"
    NETWORK_BANDWIDTH = "network_bandwidth"
    NETWORK_PACKET_LOSS = "network_packet_loss"
    NETWORK_JITTER = "network_jitter"


class QualityStatus(str, Enum):
    """Resource quality status"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    FAILING = "failing"


class OracleSource(str, Enum):
    """Oracle data sources"""
    HARDWARE = "hardware"
    SOFTWARE = "software"
    NETWORK_PROBE = "network_probe"
    BENCHMARK_SUITE = "benchmark_suite"
    EXTERNAL_API = "external_api"
    CONSENSUS = "consensus"


# Base Models
class Measurement(BaseModel):
    """Base measurement model"""
    measurement_id: str
    resource_id: str
    measurement_type: MeasurementType
    value: float
    unit: str
    timestamp: datetime
    source: OracleSource
    confidence: float = Field(..., ge=0, le=1)
    metadata: Dict[str, Any] = {}
    
    @validator('confidence')
    def validate_confidence(cls, v):
        if v < 0.8:
            raise ValueError("Confidence too low for reliable measurement")
        return v


class QuantumMeasurement(Measurement):
    """Quantum-specific measurements"""
    qubit_count: Optional[int] = None
    gate_type: Optional[str] = None
    circuit_depth: Optional[int] = None
    error_mitigation_applied: bool = False
    calibration_timestamp: Optional[datetime] = None
    
    @validator('measurement_type')
    def validate_quantum_type(cls, v):
        if not v.startswith("quantum_"):
            raise ValueError("Must be a quantum measurement type")
        return v


class AIMeasurement(Measurement):
    """AI accelerator measurements"""
    accelerator_type: str  # TPU, GPU, NPU, ASIC
    model_type: Optional[str] = None
    batch_size: Optional[int] = None
    precision: Optional[str] = None  # fp32, fp16, int8
    memory_usage_mb: Optional[int] = None
    
    @validator('measurement_type')
    def validate_ai_type(cls, v):
        if not v.startswith("ai_"):
            raise ValueError("Must be an AI measurement type")
        return v


class NetworkMeasurement(Measurement):
    """Network measurements"""
    source_node: str
    destination_node: str
    path_id: Optional[str] = None
    protocol: Optional[str] = None  # tcp, udp, icmp
    packet_size_bytes: Optional[int] = None
    sample_count: Optional[int] = None
    
    @validator('measurement_type')
    def validate_network_type(cls, v):
        if not v.startswith("network_"):
            raise ValueError("Must be a network measurement type")
        return v


# Aggregated Models
class QualityScore(BaseModel):
    """Aggregated quality score for a resource"""
    resource_id: str
    resource_type: str  # quantum, ai, network
    overall_score: float = Field(..., ge=0, le=100)
    status: QualityStatus
    component_scores: Dict[str, float]
    measurement_count: int
    last_updated: datetime
    confidence_interval: tuple[float, float]
    trend: str  # improving, stable, degrading
    
    @validator('overall_score')
    def determine_status(cls, v, values):
        if 'status' not in values:
            if v >= 90:
                values['status'] = QualityStatus.EXCELLENT
            elif v >= 75:
                values['status'] = QualityStatus.GOOD
            elif v >= 50:
                values['status'] = QualityStatus.FAIR
            elif v >= 25:
                values['status'] = QualityStatus.POOR
            else:
                values['status'] = QualityStatus.FAILING
        return v


class QuantumQualityScore(QualityScore):
    """Quantum resource quality score"""
    fidelity_score: float
    coherence_score: float
    error_rate_score: float
    gate_quality_scores: Dict[str, float]
    readout_fidelity: float
    crosstalk_score: float
    
    def calculate_overall_score(self) -> float:
        """Calculate weighted overall score"""
        weights = {
            'fidelity': 0.3,
            'coherence': 0.25,
            'error_rate': 0.25,
            'readout': 0.1,
            'crosstalk': 0.1
        }
        
        score = (
            self.fidelity_score * weights['fidelity'] +
            self.coherence_score * weights['coherence'] +
            self.error_rate_score * weights['error_rate'] +
            self.readout_fidelity * weights['readout'] +
            self.crosstalk_score * weights['crosstalk']
        )
        
        return score


class AIQualityScore(QualityScore):
    """AI accelerator quality score"""
    performance_score: float  # TFLOPS relative to spec
    thermal_score: float
    power_efficiency_score: float
    memory_bandwidth_score: float
    reliability_score: float
    
    def calculate_overall_score(self) -> float:
        """Calculate weighted overall score"""
        weights = {
            'performance': 0.35,
            'thermal': 0.2,
            'power': 0.2,
            'memory': 0.15,
            'reliability': 0.1
        }
        
        score = (
            self.performance_score * weights['performance'] +
            self.thermal_score * weights['thermal'] +
            self.power_efficiency_score * weights['power'] +
            self.memory_bandwidth_score * weights['memory'] +
            self.reliability_score * weights['reliability']
        )
        
        return score


class NetworkQualityScore(QualityScore):
    """Network path quality score"""
    latency_score: float
    bandwidth_score: float
    packet_loss_score: float
    jitter_score: float
    availability_score: float
    
    def calculate_overall_score(self) -> float:
        """Calculate weighted overall score"""
        weights = {
            'latency': 0.25,
            'bandwidth': 0.25,
            'packet_loss': 0.2,
            'jitter': 0.15,
            'availability': 0.15
        }
        
        score = (
            self.latency_score * weights['latency'] +
            self.bandwidth_score * weights['bandwidth'] +
            self.packet_loss_score * weights['packet_loss'] +
            self.jitter_score * weights['jitter'] +
            self.availability_score * weights['availability']
        )
        
        return score


# Oracle Feed Models
class OracleFeed(BaseModel):
    """Oracle data feed for blockchain submission"""
    feed_id: str
    resource_id: str
    measurement_type: MeasurementType
    aggregated_value: float
    timestamp: datetime
    measurement_count: int
    confidence: float
    signature: Optional[str] = None
    block_number: Optional[int] = None
    tx_hash: Optional[str] = None


class BatchOracleUpdate(BaseModel):
    """Batch oracle update for multiple resources"""
    batch_id: str
    timestamp: datetime
    updates: List[OracleFeed]
    oracle_address: str
    signature: str


# API Request/Response Models
class MeasurementRequest(BaseModel):
    """Request to record a measurement"""
    resource_id: str
    measurement_type: MeasurementType
    value: float
    unit: str
    source: OracleSource
    confidence: float = 0.95
    metadata: Dict[str, Any] = {}


class QualityScoreRequest(BaseModel):
    """Request for quality score calculation"""
    resource_id: str
    resource_type: str
    time_window_hours: int = 24
    include_components: bool = True


class MeasurementQuery(BaseModel):
    """Query for historical measurements"""
    resource_id: Optional[str] = None
    measurement_type: Optional[MeasurementType] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    source: Optional[OracleSource] = None
    min_confidence: float = 0.8
    limit: int = Field(100, le=1000)


class MeasurementResponse(BaseModel):
    """Response containing measurements"""
    measurements: List[Measurement]
    total_count: int
    query_time_ms: float


class QualityScoreResponse(BaseModel):
    """Response containing quality scores"""
    quality_score: QualityScore
    recent_measurements: Optional[List[Measurement]] = None
    recommendations: List[str] = []


class OracleHealthResponse(BaseModel):
    """Oracle service health status"""
    status: str
    measurement_rate: float  # per minute
    active_resources: int
    last_blockchain_update: Optional[datetime] = None
    pending_updates: int 