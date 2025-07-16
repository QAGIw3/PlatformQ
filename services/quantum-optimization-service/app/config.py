"""
Configuration for Quantum Optimization Service
"""

from typing import Optional, List
from pydantic import BaseSettings, Field


class QuantumConfig(BaseSettings):
    """Configuration for Quantum Optimization Service"""
    
    # Service configuration
    service_name: str = Field(default="quantum-optimization-service", env="SERVICE_NAME")
    service_port: int = Field(default=8001, env="SERVICE_PORT")
    grpc_port: int = Field(default=50053, env="GRPC_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Quantum backend configuration
    backend: str = Field(default="qiskit_aer", env="QUANTUM_BACKEND")
    num_shots: int = Field(default=8192, env="NUM_SHOTS")
    gpu_acceleration: bool = Field(default=True, env="GPU_ACCELERATION")
    circuit_optimization_level: int = Field(default=3, env="CIRCUIT_OPTIMIZATION_LEVEL")
    
    # Algorithm defaults
    default_qaoa_layers: int = Field(default=3, env="DEFAULT_QAOA_LAYERS")
    default_vqe_depth: int = Field(default=3, env="DEFAULT_VQE_DEPTH")
    default_optimizer: str = Field(default="COBYLA", env="DEFAULT_OPTIMIZER")
    max_iterations: int = Field(default=100, env="MAX_ITERATIONS")
    
    # Problem size limits
    max_qubits: int = Field(default=20, env="MAX_QUBITS")
    max_variables: int = Field(default=1024, env="MAX_VARIABLES")
    max_constraints: int = Field(default=100, env="MAX_CONSTRAINTS")
    
    # Performance settings
    circuit_cache_enabled: bool = Field(default=True, env="CIRCUIT_CACHE_ENABLED")
    circuit_cache_size: int = Field(default=1000, env="CIRCUIT_CACHE_SIZE")
    parallel_jobs: int = Field(default=4, env="PARALLEL_JOBS")
    job_timeout: int = Field(default=300, env="JOB_TIMEOUT")  # seconds
    
    # Pulsar configuration
    pulsar_url: str = Field(default="pulsar://localhost:6650", env="PULSAR_URL")
    optimization_request_topics: List[str] = Field(
        default=[
            "persistent://public/default/optimization-requests",
            "persistent://public/default/anomaly-triggered-optimization",
            "persistent://public/default/causal-optimization-requests"
        ],
        env="OPTIMIZATION_REQUEST_TOPICS"
    )
    result_topic: str = Field(
        default="persistent://public/default/optimization-results",
        env="RESULT_TOPIC"
    )
    
    # Ignite configuration
    ignite_host: str = Field(default="localhost", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    ignite_cache_jobs: str = Field(default="quantum_jobs", env="IGNITE_CACHE_JOBS")
    ignite_cache_results: str = Field(default="quantum_results", env="IGNITE_CACHE_RESULTS")
    
    # Model persistence
    model_save_path: str = Field(default="/data/quantum_models", env="MODEL_SAVE_PATH")
    checkpoint_interval: int = Field(default=3600, env="CHECKPOINT_INTERVAL")  # seconds
    
    # Monitoring
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    metrics_port: int = Field(default=9091, env="METRICS_PORT")
    trace_enabled: bool = Field(default=True, env="TRACE_ENABLED")
    trace_endpoint: str = Field(default="http://jaeger:14268/api/traces", env="TRACE_ENDPOINT")
    
    # Benchmarking
    benchmark_enabled: bool = Field(default=True, env="BENCHMARK_ENABLED")
    benchmark_problems: List[str] = Field(
        default=["resource_allocation", "route_optimization", "portfolio"],
        env="BENCHMARK_PROBLEMS"
    )
    
    # Security
    api_key_required: bool = Field(default=False, env="API_KEY_REQUIRED")
    api_key_header: str = Field(default="X-API-Key", env="API_KEY_HEADER")
    
    # Advanced quantum settings
    error_mitigation: bool = Field(default=True, env="ERROR_MITIGATION")
    transpiler_seed: Optional[int] = Field(default=42, env="TRANSPILER_SEED")
    coupling_map: Optional[str] = Field(default=None, env="COUPLING_MAP")  # JSON string
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        
    def get_ignite_hosts(self) -> List[str]:
        """Get list of Ignite hosts"""
        return [f"{self.ignite_host}:{self.ignite_port}"]
        
    def get_optimization_topics_list(self) -> List[str]:
        """Get optimization topics as list"""
        if isinstance(self.optimization_request_topics, str):
            return [t.strip() for t in self.optimization_request_topics.split(",")]
        return self.optimization_request_topics
        
    def validate_quantum_config(self):
        """Validate quantum configuration"""
        # Check backend availability
        valid_backends = ["qiskit_aer", "cirq", "pennylane"]
        if self.backend not in valid_backends:
            raise ValueError(f"Invalid backend: {self.backend}. Must be one of {valid_backends}")
            
        # Check qubit limits
        if self.max_qubits > 30:
            logger.warning(f"max_qubits={self.max_qubits} may cause performance issues")
            
        # Validate optimizer
        valid_optimizers = ["COBYLA", "SPSA", "L_BFGS_B", "SLSQP", "NFT"]
        if self.default_optimizer not in valid_optimizers:
            logger.warning(f"Unknown optimizer: {self.default_optimizer}, defaulting to COBYLA")
            self.default_optimizer = "COBYLA"
            
        logger.info(f"Quantum config validated: backend={self.backend}, "
                   f"shots={self.num_shots}, gpu={self.gpu_acceleration}")


# Global config instance
config = QuantumConfig()
config.validate_quantum_config() 