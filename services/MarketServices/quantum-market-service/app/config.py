"""
Configuration for Quantum Market Service.
"""

from pydantic_settings import BaseSettings
from typing import List, Dict, Any
from decimal import Decimal


class Settings(BaseSettings):
    """Quantum Market Service configuration."""
    
    # Service info
    service_name: str = "quantum-market-service"
    service_version: str = "1.0.0"
    
    # API Configuration
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8024
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_prefix: str = "quantum_market"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_quantum_events_topic: str = "persistent://public/default/quantum-events"
    pulsar_coherence_events_topic: str = "persistent://public/default/coherence-events"
    pulsar_entanglement_events_topic: str = "persistent://public/default/entanglement-events"
    
    # Blockchain
    blockchain_rpc_url: str = "http://localhost:8545"
    resource_token_address: str = "0x0000000000000000000000000000000000000000"
    quantum_manager_address: str = "0x0000000000000000000000000000000000000000"
    private_key: str = ""  # For development only
    
    # Quantum resource limits
    max_qubits_per_user: int = 100
    max_coherence_window_duration: int = 1000  # microseconds
    min_coherence_window_duration: int = 10  # microseconds
    max_advance_booking: int = 86400  # 24 hours in seconds
    
    # Coherence and fidelity
    coherence_decay_rate: Decimal = Decimal("0.01")  # per microsecond
    min_gate_fidelity: Decimal = Decimal("0.9")  # 90%
    min_measurement_fidelity: Decimal = Decimal("0.9")  # 90%
    min_entanglement_fidelity: Decimal = Decimal("0.5")  # Classical threshold
    fidelity_threshold_premium: Decimal = Decimal("0.99")  # 99% for premium pricing
    
    # Entanglement
    entanglement_base_lifetime: int = 50  # microseconds
    max_entanglement_distance: int = 1000  # km
    entanglement_decay_per_km: Decimal = Decimal("0.001")
    
    # Pricing parameters
    base_qubit_price: Decimal = Decimal("0.001")  # ETH per qubit-microsecond
    coherence_premium_multiplier: Decimal = Decimal("1.5")
    fidelity_premium_multiplier: Decimal = Decimal("1.2")
    exclusive_access_multiplier: Decimal = Decimal("2.0")
    burst_pricing_multiplier: Decimal = Decimal("3.0")
    
    # Entanglement pricing
    base_entanglement_price: Decimal = Decimal("0.0001")  # ETH per Bell pair
    entanglement_fidelity_multiplier: Decimal = Decimal("2.0")  # For high fidelity
    
    # Algorithm pricing factors
    algorithm_complexity_factors: Dict[str, Decimal] = {
        "vqe": Decimal("1.5"),
        "qaoa": Decimal("1.3"),
        "grover": Decimal("1.0"),
        "shor": Decimal("2.0"),
        "hhl": Decimal("1.8"),
        "qml": Decimal("1.4"),
        "custom": Decimal("1.0")
    }
    
    # Market parameters
    auction_duration: int = 300  # seconds
    auction_decrement_rate: Decimal = Decimal("0.02")  # 2% per minute
    min_auction_participants: int = 2
    
    # QPU providers configuration
    supported_providers: List[str] = [
        "ibm", "google", "ionq", "rigetti", 
        "dwave", "honeywell", "xanadu", "pasqal"
    ]
    
    # Quality scoring
    quality_score_update_interval: int = 300  # 5 minutes
    min_quality_score: int = 1000  # 10%
    quality_score_decay_rate: Decimal = Decimal("0.001")  # per hour
    
    # Performance thresholds
    min_success_rate: Decimal = Decimal("0.8")  # 80%
    coherence_achievement_threshold: Decimal = Decimal("0.9")  # 90% of expected
    
    # Resource allocation
    max_qubits_per_qpu: int = 1000
    reservation_buffer_time: int = 60  # seconds between windows
    
    # Monitoring and alerts
    metrics_enabled: bool = True
    metrics_port: int = 9024
    alert_low_coherence_threshold: int = 20  # microseconds
    alert_high_error_rate: Decimal = Decimal("0.1")  # 10%
    
    # Background task intervals
    performance_monitor_interval: int = 300  # 5 minutes
    expired_window_check_interval: int = 10  # seconds
    fidelity_update_interval: int = 1  # second
    spot_price_update_interval: int = 60  # seconds
    
    # Circuit validation
    max_circuit_depth: int = 10000
    max_gate_count: int = 100000
    max_two_qubit_gates: int = 50000
    
    # Arbitrage parameters
    arbitrage_scan_interval: int = 30  # seconds
    min_arbitrage_advantage: Decimal = Decimal("1.2")  # 20% minimum
    arbitrage_execution_timeout: int = 300  # seconds
    
    # Development/Testing
    enable_test_qpus: bool = False
    test_qpu_count: int = 3
    simulate_coherence_decay: bool = True
    simulate_errors: bool = False
    
    class Config:
        env_prefix = "QUANTUM_MARKET_"
        case_sensitive = False
        
        # Custom type encoders
        json_encoders = {
            Decimal: str
        }


# Helper functions for configuration

def get_qpu_config(provider: str) -> Dict[str, Any]:
    """Get QPU-specific configuration by provider."""
    configs = {
        "ibm": {
            "default_topology": "hexagonal",
            "typical_coherence": 100,  # microseconds
            "typical_gate_fidelity": 0.9995,
            "typical_measurement_fidelity": 0.997
        },
        "google": {
            "default_topology": "grid",
            "typical_coherence": 80,
            "typical_gate_fidelity": 0.9993,
            "typical_measurement_fidelity": 0.996
        },
        "ionq": {
            "default_topology": "all_to_all",
            "typical_coherence": 150,
            "typical_gate_fidelity": 0.9997,
            "typical_measurement_fidelity": 0.998
        },
        "rigetti": {
            "default_topology": "custom",
            "typical_coherence": 70,
            "typical_gate_fidelity": 0.999,
            "typical_measurement_fidelity": 0.995
        }
    }
    return configs.get(provider, configs["ibm"])


def get_algorithm_requirements(algorithm_type: str) -> Dict[str, Any]:
    """Get typical requirements for algorithm types."""
    requirements = {
        "vqe": {
            "min_qubits": 4,
            "typical_depth": 100,
            "min_coherence": 50,
            "requires_parametric": True
        },
        "qaoa": {
            "min_qubits": 5,
            "typical_depth": 50,
            "min_coherence": 40,
            "requires_parametric": True
        },
        "grover": {
            "min_qubits": 3,
            "typical_depth": 30,
            "min_coherence": 30,
            "requires_parametric": False
        },
        "shor": {
            "min_qubits": 15,
            "typical_depth": 1000,
            "min_coherence": 200,
            "requires_parametric": False
        }
    }
    return requirements.get(algorithm_type, requirements["qaoa"]) 