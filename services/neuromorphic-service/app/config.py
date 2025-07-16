"""
Configuration for Neuromorphic Event Processing Service
"""

from platformq.shared.config import Settings
from typing import List, Optional, Dict, Any
from pydantic import Field
import os
import numpy as np

class NeuromorphicConfig(Settings):
    """Configuration for Neuromorphic Service"""
    
    # Service configuration
    service_name: str = Field(default="neuromorphic-service", env="SERVICE_NAME")
    service_port: int = Field(default=8000, env="SERVICE_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # SNN configuration
    encoding_type: str = Field(default="temporal", env="SNN_ENCODING_TYPE")
    input_neurons: int = Field(default=1000, env="SNN_INPUT_NEURONS")
    reservoir_neurons: int = Field(default=10000, env="SNN_RESERVOIR_NEURONS")
    output_neurons: int = Field(default=100, env="SNN_OUTPUT_NEURONS")
    connectivity: float = Field(default=0.1, env="SNN_CONNECTIVITY")
    spectral_radius: float = Field(default=0.9, env="SNN_SPECTRAL_RADIUS")
    
    # Neuron parameters
    tau_membrane: float = Field(default=20.0, env="NEURON_TAU_MEMBRANE")
    tau_synapse: float = Field(default=5.0, env="NEURON_TAU_SYNAPSE")
    v_threshold: float = Field(default=1.0, env="NEURON_V_THRESHOLD")
    v_reset: float = Field(default=0.0, env="NEURON_V_RESET")
    refractory: float = Field(default=2.0, env="NEURON_REFRACTORY")
    
    # Learning configuration
    learning_rate: float = Field(default=0.01, env="LEARNING_RATE")
    stdp_enabled: bool = Field(default=True, env="STDP_ENABLED")
    tau_pre: float = Field(default=20.0, env="STDP_TAU_PRE")
    tau_post: float = Field(default=20.0, env="STDP_TAU_POST")
    
    # Anomaly detection
    anomaly_threshold: float = Field(default=0.7, env="ANOMALY_THRESHOLD")
    detection_window: int = Field(default=100, env="DETECTION_WINDOW")
    detection_sensitivity: float = Field(default=2.0, env="DETECTION_SENSITIVITY")
    
    # Workflow-specific anomaly detection
    workflow_anomaly_threshold: float = Field(default=0.6, env="WORKFLOW_ANOMALY_THRESHOLD")
    workflow_history_buffer_size: int = Field(default=1000, env="WORKFLOW_HISTORY_BUFFER_SIZE")
    workflow_min_history_size: int = Field(default=5, env="WORKFLOW_MIN_HISTORY_SIZE")
    workflow_optimization_auto_trigger: bool = Field(default=True, env="WORKFLOW_OPTIMIZATION_AUTO_TRIGGER")
    workflow_anomaly_severity_threshold: float = Field(default=0.8, env="WORKFLOW_ANOMALY_SEVERITY_THRESHOLD")
    
    # Quantum optimization integration
    quantum_optimization_url: str
    quantum_optimization_timeout: int = Field(default=30, env="QUANTUM_OPTIMIZATION_TIMEOUT")
    
    # Performance tuning
    batch_size: int = Field(default=1000, env="BATCH_SIZE")
    gpu_enabled: bool = Field(default=True, env="GPU_ENABLED")
    gpu_device: str = Field(default="cuda:0", env="GPU_DEVICE")
    max_neurons_per_core: int = Field(default=1000, env="MAX_NEURONS_PER_CORE")
    parallel_simulations: int = Field(default=4, env="PARALLEL_SIMULATIONS")
    
    # Pulsar configuration
    pulsar_topics: List[str] = Field(
        default=[
            "persistent://public/default/platform-events",
            "persistent://public/default/security-events",
            "persistent://public/default/performance-metrics",
            "persistent://public/default/sensor-data"
        ],
        env="PULSAR_TOPICS"
    )
    workflow_topics: List[str] = Field(
        default=[
            "persistent://public/default/workflow-execution-started",
            "persistent://public/default/workflow-execution-completed",
            "persistent://public/default/airflow-dag-events",
            "persistent://public/default/dag-run-metrics"
        ],
        env="WORKFLOW_TOPICS"
    )
    anomaly_topic: str = Field(
        default="persistent://public/default/anomaly-events",
        env="ANOMALY_TOPIC"
    )
    workflow_anomaly_topic: str = Field(
        default="persistent://public/default/workflow-anomaly-events",
        env="WORKFLOW_ANOMALY_TOPIC"
    )
    workflow_optimization_topic: str = Field(
        default="persistent://public/default/workflow-optimization-requests",
        env="WORKFLOW_OPTIMIZATION_TOPIC"
    )
    
    # Ignite configuration
    ignite_host: str
    ignite_port: int
    ignite_cache_events: str = Field(default="neuromorphic_events", env="IGNITE_CACHE_EVENTS")
    ignite_cache_patterns: str = Field(default="anomaly_patterns", env="IGNITE_CACHE_PATTERNS")
    ignite_cache_workflow_patterns: str = Field(default="workflow_patterns", env="IGNITE_CACHE_WORKFLOW_PATTERNS")
    
    # Monitoring
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")
    trace_enabled: bool = Field(default=True, env="TRACE_ENABLED")
    
    # Model persistence
    model_save_interval: int = Field(default=3600, env="MODEL_SAVE_INTERVAL")  # seconds
    model_path: str = Field(default="/data/models", env="MODEL_PATH")
    checkpoint_enabled: bool = Field(default=True, env="CHECKPOINT_ENABLED")
    
    # Resource limits
    max_memory_gb: float = Field(default=8.0, env="MAX_MEMORY_GB")
    max_events_per_second: int = Field(default=100000, env="MAX_EVENTS_PER_SECOND")
    event_buffer_size: int = Field(default=10000, env="EVENT_BUFFER_SIZE")
    
    # Feature extraction
    feature_extraction_timeout: float = Field(default=0.1, env="FEATURE_EXTRACTION_TIMEOUT")  # seconds
    max_features_per_event: int = Field(default=100, env="MAX_FEATURES_PER_EVENT")
    
    # WebSocket configuration
    ws_max_connections: int = Field(default=1000, env="WS_MAX_CONNECTIONS")
    ws_heartbeat_interval: int = Field(default=30, env="WS_HEARTBEAT_INTERVAL")
    
    # Visualization
    spike_visualization_enabled: bool = Field(default=True, env="SPIKE_VISUALIZATION_ENABLED")
    visualization_update_interval: float = Field(default=0.1, env="VISUALIZATION_UPDATE_INTERVAL")  # seconds
    
    # Advanced SNN options
    use_gpu_snn: bool = Field(default=False, env="USE_GPU_SNN")  # Use Norse for GPU acceleration
    reservoir_type: str = Field(default="brian2", env="RESERVOIR_TYPE")  # brian2 or reservoir_computing
    synapse_model: str = Field(default="exponential", env="SYNAPSE_MODEL")
    
    # Debug options
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")
    save_spike_trains: bool = Field(default=False, env="SAVE_SPIKE_TRAINS")
    profile_enabled: bool = Field(default=False, env="PROFILE_ENABLED")
    
    # Workflow optimization specific
    workflow_optimization_algorithms: List[str] = Field(
        default=["qaoa", "vqe", "hybrid", "neuromorphic"],
        env="WORKFLOW_OPTIMIZATION_ALGORITHMS"
    )
    workflow_optimization_max_iterations: int = Field(default=100, env="WORKFLOW_OPTIMIZATION_MAX_ITERATIONS")
    workflow_optimization_convergence_threshold: float = Field(default=0.001, env="WORKFLOW_OPTIMIZATION_CONVERGENCE_THRESHOLD")

    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str
    
    class Config:
        env_prefix = "NEUROMORPHIC_"

    def get_ignite_hosts(self) -> List[str]:
        """Get list of Ignite hosts"""
        return [f"{self.ignite_host}:{self.ignite_port}"]
        
    def get_pulsar_topics_list(self) -> List[str]:
        """Get Pulsar topics as list"""
        if isinstance(self.pulsar_topics, str):
            return [t.strip() for t in self.pulsar_topics.split(",")]
        return self.pulsar_topics
        
    def get_workflow_topics_list(self) -> List[str]:
        """Get workflow topics as list"""
        if isinstance(self.workflow_topics, str):
            return [t.strip() for t in self.workflow_topics.split(",")]
        return self.workflow_topics
        
    def validate_gpu_config(self):
        """Validate GPU configuration"""
        if self.gpu_enabled or self.use_gpu_snn:
            try:
                import torch
                if not torch.cuda.is_available():
                    self.gpu_enabled = False
                    self.use_gpu_snn = False
                    print("Warning: GPU requested but not available, falling back to CPU")
            except ImportError:
                self.gpu_enabled = False
                self.use_gpu_snn = False
                print("Warning: PyTorch not available for GPU support")
                
    def get_snn_config_dict(self) -> dict:
        """Get SNN configuration as dictionary"""
        return {
            "input_size": self.input_neurons,
            "reservoir_size": self.reservoir_neurons,
            "output_size": self.output_neurons,
            "connectivity": self.connectivity,
            "spectral_radius": self.spectral_radius,
            "tau_membrane": self.tau_membrane,
            "tau_synapse": self.tau_synapse,
            "v_threshold": self.v_threshold,
            "v_reset": self.v_reset,
            "refractory": self.refractory,
            "learning_rate": self.learning_rate
        }


# Global config instance
settings = NeuromorphicConfig()
settings.validate_gpu_config() 