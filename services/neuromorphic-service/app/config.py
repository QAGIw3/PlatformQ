"""
Configuration for Neuromorphic Event Processing Service
"""

from typing import Optional, List
from pydantic import BaseSettings, Field
import os


class NeuromorphicConfig(BaseSettings):
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
    
    # Performance tuning
    batch_size: int = Field(default=1000, env="BATCH_SIZE")
    gpu_enabled: bool = Field(default=True, env="GPU_ENABLED")
    gpu_device: str = Field(default="cuda:0", env="GPU_DEVICE")
    max_neurons_per_core: int = Field(default=1000, env="MAX_NEURONS_PER_CORE")
    parallel_simulations: int = Field(default=4, env="PARALLEL_SIMULATIONS")
    
    # Pulsar configuration
    pulsar_url: str = Field(default="pulsar://localhost:6650", env="PULSAR_URL")
    pulsar_topics: List[str] = Field(
        default=[
            "persistent://public/default/platform-events",
            "persistent://public/default/security-events",
            "persistent://public/default/performance-metrics",
            "persistent://public/default/sensor-data"
        ],
        env="PULSAR_TOPICS"
    )
    anomaly_topic: str = Field(
        default="persistent://public/default/anomaly-events",
        env="ANOMALY_TOPIC"
    )
    
    # Ignite configuration
    ignite_host: str = Field(default="localhost", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    ignite_cache_events: str = Field(default="neuromorphic_events", env="IGNITE_CACHE_EVENTS")
    ignite_cache_patterns: str = Field(default="anomaly_patterns", env="IGNITE_CACHE_PATTERNS")
    
    # Monitoring
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")
    trace_enabled: bool = Field(default=True, env="TRACE_ENABLED")
    trace_endpoint: str = Field(default="http://jaeger:14268/api/traces", env="TRACE_ENDPOINT")
    
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
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        
    def get_ignite_hosts(self) -> List[str]:
        """Get list of Ignite hosts"""
        return [f"{self.ignite_host}:{self.ignite_port}"]
        
    def get_pulsar_topics_list(self) -> List[str]:
        """Get Pulsar topics as list"""
        if isinstance(self.pulsar_topics, str):
            return [t.strip() for t in self.pulsar_topics.split(",")]
        return self.pulsar_topics
        
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
config = NeuromorphicConfig()
config.validate_gpu_config() 