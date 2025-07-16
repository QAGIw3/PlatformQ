"""
Workflow Anomaly Detection Module

Specialized anomaly detection for workflow/DAG executions using spiking neural networks.
"""

import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import brian2 as b2
from brian2 import *
import json

from platformq_shared.logging_config import get_logger
from .anomaly_detector import AnomalyDetector, AnomalyType, AnomalyPattern

logger = get_logger(__name__)


@dataclass
class WorkflowMetrics:
    """Metrics extracted from workflow execution"""
    execution_duration_ms: float
    task_count: int
    failed_tasks: int
    retry_count: int
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    parallelism_degree: int = 1
    critical_path_length: int = 1
    resource_wait_time_ms: float = 0.0
    data_volume_bytes: Optional[int] = None


class WorkflowAnomalyDetector(AnomalyDetector):
    """
    Specialized anomaly detector for workflow executions using spiking neural networks.
    
    Detects anomalies such as:
    - Performance degradation
    - Unusual failure patterns
    - Resource usage spikes
    - Task scheduling anomalies
    - Dependency bottlenecks
    """
    
    def __init__(self,
                 threshold: float = 0.7,
                 window_size: int = 100,
                 learning_rate: float = 0.01,
                 historical_buffer_size: int = 1000):
        """
        Initialize workflow anomaly detector
        
        Args:
            threshold: Anomaly detection threshold
            window_size: Size of sliding window for pattern analysis
            learning_rate: Learning rate for STDP
            historical_buffer_size: Size of historical data buffer
        """
        super().__init__(threshold, window_size)
        
        self.learning_rate = learning_rate
        self.historical_buffer_size = historical_buffer_size
        
        # Historical data for each workflow
        self.workflow_history = {}
        
        # Learned patterns for different workflow types
        self.learned_patterns = {}
        
        # Initialize specialized neurons for workflow features
        self._initialize_workflow_neurons()
        
        logger.info("Initialized WorkflowAnomalyDetector with specialized workflow analysis")
        
    def _initialize_workflow_neurons(self):
        """Initialize specialized neuron groups for workflow features"""
        
        # Neuron groups for different aspects of workflow execution
        self.duration_neurons = 50  # Encode execution duration
        self.failure_neurons = 30   # Encode failure patterns
        self.resource_neurons = 40  # Encode resource usage
        self.schedule_neurons = 30  # Encode scheduling patterns
        
        # Total neurons for workflow encoding
        self.total_workflow_neurons = (
            self.duration_neurons + 
            self.failure_neurons + 
            self.resource_neurons + 
            self.schedule_neurons
        )
        
        # Synaptic weights for pattern learning
        self.pattern_weights = np.random.randn(
            self.total_workflow_neurons, 
            self.total_workflow_neurons
        ) * 0.1
        
    def extract_workflow_features(self, workflow_event: Dict[str, Any]) -> WorkflowMetrics:
        """
        Extract relevant features from workflow execution event
        
        Args:
            workflow_event: Workflow execution event data
            
        Returns:
            WorkflowMetrics object
        """
        # Handle both started and completed events
        if 'duration_ms' in workflow_event:
            # Completed event
            duration_ms = workflow_event['duration_ms']
            
            task_metrics = workflow_event.get('task_metrics', {})
            total_tasks = task_metrics.get('total_tasks', 0)
            failed_tasks = task_metrics.get('failed_tasks', 0)
            retry_count = task_metrics.get('retry_count', 0)
            
            resource_usage = workflow_event.get('resource_usage', {})
            cpu_usage = resource_usage.get('cpu_seconds')
            memory_usage = resource_usage.get('memory_mb_seconds')
            
        else:
            # Started event - use expected values
            duration_ms = workflow_event.get('expected_duration_ms', 0)
            total_tasks = 0
            failed_tasks = 0
            retry_count = 0
            cpu_usage = None
            memory_usage = None
            
        # Extract additional features from metadata
        metadata = workflow_event.get('metadata', {})
        parallelism_degree = int(metadata.get('parallelism_degree', '1'))
        critical_path_length = int(metadata.get('critical_path_length', '1'))
        resource_wait_time_ms = float(metadata.get('resource_wait_time_ms', '0'))
        data_volume_bytes = metadata.get('data_volume_bytes')
        if data_volume_bytes:
            data_volume_bytes = int(data_volume_bytes)
            
        return WorkflowMetrics(
            execution_duration_ms=duration_ms,
            task_count=total_tasks,
            failed_tasks=failed_tasks,
            retry_count=retry_count,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            parallelism_degree=parallelism_degree,
            critical_path_length=critical_path_length,
            resource_wait_time_ms=resource_wait_time_ms,
            data_volume_bytes=data_volume_bytes
        )
        
    def encode_workflow_to_spikes(self, metrics: WorkflowMetrics) -> np.ndarray:
        """
        Encode workflow metrics into spike trains
        
        Args:
            metrics: Workflow metrics
            
        Returns:
            Spike train array
        """
        spike_trains = []
        
        # Encode duration (log scale for better representation)
        duration_spikes = self._encode_duration(metrics.execution_duration_ms)
        spike_trains.append(duration_spikes)
        
        # Encode failure patterns
        failure_spikes = self._encode_failures(
            metrics.failed_tasks, 
            metrics.task_count,
            metrics.retry_count
        )
        spike_trains.append(failure_spikes)
        
        # Encode resource usage
        resource_spikes = self._encode_resources(
            metrics.cpu_usage,
            metrics.memory_usage,
            metrics.data_volume_bytes
        )
        spike_trains.append(resource_spikes)
        
        # Encode scheduling patterns
        schedule_spikes = self._encode_scheduling(
            metrics.parallelism_degree,
            metrics.critical_path_length,
            metrics.resource_wait_time_ms
        )
        spike_trains.append(schedule_spikes)
        
        # Combine all spike trains
        return np.concatenate(spike_trains)
        
    def _encode_duration(self, duration_ms: float) -> np.ndarray:
        """Encode execution duration using population coding"""
        # Use log scale for duration
        log_duration = np.log10(max(duration_ms, 1))
        
        # Population coding with Gaussian tuning curves
        preferred_durations = np.logspace(1, 6, self.duration_neurons)  # 10ms to 1000s
        sigma = 0.5  # Width of tuning curves
        
        activations = np.exp(-0.5 * ((np.log10(preferred_durations) - log_duration) / sigma) ** 2)
        
        # Convert to spike rates (0-100 Hz)
        spike_rates = activations * 100
        
        # Generate spike train
        return self._rates_to_spikes(spike_rates, window_ms=100)
        
    def _encode_failures(self, failed_tasks: int, total_tasks: int, retries: int) -> np.ndarray:
        """Encode failure patterns"""
        if total_tasks == 0:
            failure_rate = 0
        else:
            failure_rate = failed_tasks / total_tasks
            
        # Different neuron groups for different failure aspects
        rates = np.zeros(self.failure_neurons)
        
        # Failure rate encoding (first 10 neurons)
        rates[:10] = failure_rate * 100
        
        # Retry pattern encoding (next 10 neurons)
        if retries > 0:
            rates[10:20] = min(retries / 5, 1.0) * 50  # Normalize to max 5 retries
            
        # Failure clustering (last 10 neurons) - would need temporal info
        rates[20:30] = np.random.rand(10) * 20  # Placeholder
        
        return self._rates_to_spikes(rates, window_ms=100)
        
    def _encode_resources(self, cpu: Optional[float], memory: Optional[float], 
                         data_bytes: Optional[int]) -> np.ndarray:
        """Encode resource usage patterns"""
        rates = np.zeros(self.resource_neurons)
        
        # CPU usage encoding (first 15 neurons)
        if cpu is not None:
            cpu_normalized = np.log10(max(cpu, 0.1)) / 3  # Normalize log scale
            rates[:15] = np.clip(cpu_normalized, 0, 1) * 80
            
        # Memory usage encoding (next 15 neurons)
        if memory is not None:
            mem_normalized = np.log10(max(memory, 1)) / 6  # Normalize log scale
            rates[15:30] = np.clip(mem_normalized, 0, 1) * 80
            
        # Data volume encoding (last 10 neurons)
        if data_bytes is not None:
            data_normalized = np.log10(max(data_bytes, 1)) / 12  # Up to TB scale
            rates[30:40] = np.clip(data_normalized, 0, 1) * 60
            
        return self._rates_to_spikes(rates, window_ms=100)
        
    def _encode_scheduling(self, parallelism: int, critical_path: int, 
                          wait_time_ms: float) -> np.ndarray:
        """Encode scheduling and parallelism patterns"""
        rates = np.zeros(self.schedule_neurons)
        
        # Parallelism degree (first 10 neurons)
        parallelism_normalized = min(parallelism / 10, 1.0)  # Normalize to max 10
        rates[:10] = parallelism_normalized * 70
        
        # Critical path length (next 10 neurons)
        path_normalized = min(critical_path / 50, 1.0)  # Normalize to max 50 tasks
        rates[10:20] = path_normalized * 60
        
        # Resource wait time (last 10 neurons)
        wait_normalized = np.log10(max(wait_time_ms, 1)) / 5  # Log scale up to 100s
        rates[20:30] = np.clip(wait_normalized, 0, 1) * 50
        
        return self._rates_to_spikes(rates, window_ms=100)
        
    def _rates_to_spikes(self, rates: np.ndarray, window_ms: float) -> np.ndarray:
        """Convert firing rates to spike trains"""
        # Simple Poisson spike generation
        dt = 1.0  # 1ms time step
        n_steps = int(window_ms / dt)
        
        spikes = np.zeros((len(rates), n_steps))
        for i, rate in enumerate(rates):
            prob = rate * dt / 1000.0  # Convert Hz to probability
            spikes[i, :] = np.random.rand(n_steps) < prob
            
        return spikes
        
    def detect_workflow_anomaly(self, workflow_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect anomalies in workflow execution
        
        Args:
            workflow_event: Workflow execution event
            
        Returns:
            Anomaly detection result or None
        """
        workflow_id = workflow_event.get('workflow_id')
        execution_id = workflow_event.get('execution_id')
        
        # Extract features
        metrics = self.extract_workflow_features(workflow_event)
        
        # Encode to spikes
        spike_train = self.encode_workflow_to_spikes(metrics)
        
        # Get historical data for this workflow
        if workflow_id not in self.workflow_history:
            self.workflow_history[workflow_id] = []
            
        history = self.workflow_history[workflow_id]
        
        # Detect anomaly if we have enough history
        if len(history) >= 5:
            anomaly_score = self._calculate_anomaly_score(metrics, history)
            
            if anomaly_score > self.threshold:
                anomaly_type = self._classify_anomaly(metrics, history)
                
                return {
                    'anomaly_id': f"workflow_anomaly_{datetime.utcnow().timestamp()}",
                    'execution_id': execution_id,
                    'workflow_id': workflow_id,
                    'anomaly_type': anomaly_type,
                    'severity': anomaly_score,
                    'confidence': min(len(history) / 20, 1.0),  # Confidence based on history size
                    'detected_at': int(datetime.utcnow().timestamp() * 1000),
                    'spike_pattern': spike_train.flatten().tolist()[:100],  # First 100 values
                    'metrics': {
                        'duration_ms': metrics.execution_duration_ms,
                        'failed_tasks': metrics.failed_tasks,
                        'retry_count': metrics.retry_count,
                        'cpu_usage': metrics.cpu_usage,
                        'memory_usage': metrics.memory_usage
                    },
                    'contributing_factors': self._identify_contributing_factors(metrics, history),
                    'recommended_actions': self._generate_recommendations(anomaly_type, metrics)
                }
                
        # Update history
        history.append(metrics)
        if len(history) > self.historical_buffer_size:
            history.pop(0)
            
        # Update learned patterns using STDP
        self._update_patterns(spike_train)
        
        return None
        
    def _calculate_anomaly_score(self, current: WorkflowMetrics, 
                                history: List[WorkflowMetrics]) -> float:
        """Calculate anomaly score based on deviation from historical patterns"""
        
        # Calculate historical statistics
        hist_durations = [h.execution_duration_ms for h in history]
        hist_failures = [h.failed_tasks / max(h.task_count, 1) for h in history]
        hist_retries = [h.retry_count for h in history]
        
        # Calculate z-scores for different metrics
        duration_zscore = abs((current.execution_duration_ms - np.mean(hist_durations)) / 
                             (np.std(hist_durations) + 1e-6))
        
        failure_rate = current.failed_tasks / max(current.task_count, 1)
        failure_zscore = abs((failure_rate - np.mean(hist_failures)) / 
                            (np.std(hist_failures) + 1e-6))
        
        retry_zscore = abs((current.retry_count - np.mean(hist_retries)) / 
                          (np.std(hist_retries) + 1e-6))
        
        # Resource usage anomalies (if available)
        resource_score = 0
        if current.cpu_usage is not None:
            hist_cpu = [h.cpu_usage for h in history if h.cpu_usage is not None]
            if hist_cpu:
                cpu_zscore = abs((current.cpu_usage - np.mean(hist_cpu)) / 
                                (np.std(hist_cpu) + 1e-6))
                resource_score = max(resource_score, cpu_zscore)
                
        # Combine scores with weights
        anomaly_score = (
            0.3 * min(duration_zscore / 3, 1.0) +  # Duration anomaly
            0.3 * min(failure_zscore / 2, 1.0) +   # Failure anomaly
            0.2 * min(retry_zscore / 3, 1.0) +     # Retry anomaly
            0.2 * min(resource_score / 3, 1.0)     # Resource anomaly
        )
        
        return anomaly_score
        
    def _classify_anomaly(self, current: WorkflowMetrics, 
                         history: List[WorkflowMetrics]) -> str:
        """Classify the type of anomaly detected"""
        
        # Calculate deviations
        avg_duration = np.mean([h.execution_duration_ms for h in history])
        avg_failure_rate = np.mean([h.failed_tasks / max(h.task_count, 1) for h in history])
        
        duration_deviation = (current.execution_duration_ms - avg_duration) / avg_duration
        failure_deviation = (current.failed_tasks / max(current.task_count, 1)) - avg_failure_rate
        
        # Classify based on primary deviation
        if abs(duration_deviation) > 0.5:
            if duration_deviation > 0:
                return "performance_degradation"
            else:
                return "unusual_speedup"
        elif failure_deviation > 0.2:
            return "failure_pattern"
        elif current.retry_count > np.mean([h.retry_count for h in history]) * 2:
            return "excessive_retries"
        elif current.resource_wait_time_ms > np.mean([h.resource_wait_time_ms for h in history]) * 3:
            return "resource_contention"
        else:
            return "complex_anomaly"
            
    def _identify_contributing_factors(self, current: WorkflowMetrics, 
                                      history: List[WorkflowMetrics]) -> List[str]:
        """Identify factors contributing to the anomaly"""
        factors = []
        
        # Duration factors
        avg_duration = np.mean([h.execution_duration_ms for h in history])
        if current.execution_duration_ms > avg_duration * 1.5:
            factors.append("execution_time_increased_significantly")
            
        # Failure factors
        if current.failed_tasks > 0:
            factors.append(f"task_failures_detected: {current.failed_tasks}")
            
        # Retry factors
        avg_retries = np.mean([h.retry_count for h in history])
        if current.retry_count > avg_retries * 2:
            factors.append("excessive_retry_attempts")
            
        # Resource factors
        if current.resource_wait_time_ms > 1000:
            factors.append("significant_resource_wait_time")
            
        if current.cpu_usage is not None:
            hist_cpu = [h.cpu_usage for h in history if h.cpu_usage is not None]
            if hist_cpu and current.cpu_usage > np.mean(hist_cpu) * 2:
                factors.append("abnormal_cpu_usage")
                
        return factors
        
    def _generate_recommendations(self, anomaly_type: str, 
                                 metrics: WorkflowMetrics) -> List[str]:
        """Generate recommendations based on anomaly type"""
        recommendations = []
        
        if anomaly_type == "performance_degradation":
            recommendations.extend([
                "optimize_task_parallelism",
                "review_resource_allocation",
                "check_for_data_bottlenecks",
                "consider_workflow_restructuring"
            ])
        elif anomaly_type == "failure_pattern":
            recommendations.extend([
                "implement_retry_strategy",
                "add_error_handling",
                "review_task_dependencies",
                "check_external_service_health"
            ])
        elif anomaly_type == "excessive_retries":
            recommendations.extend([
                "investigate_transient_failures",
                "adjust_retry_parameters",
                "implement_circuit_breaker",
                "review_timeout_settings"
            ])
        elif anomaly_type == "resource_contention":
            recommendations.extend([
                "increase_resource_allocation",
                "implement_resource_pooling",
                "optimize_task_scheduling",
                "consider_horizontal_scaling"
            ])
            
        return recommendations
        
    def _update_patterns(self, spike_train: np.ndarray):
        """Update learned patterns using STDP-like learning"""
        # Simplified STDP update for pattern learning
        # In a real implementation, this would involve more complex
        # spike-timing dependent plasticity calculations
        
        flat_spikes = spike_train.flatten()
        if len(flat_spikes) >= self.total_workflow_neurons:
            pattern = flat_spikes[:self.total_workflow_neurons]
            
            # Update synaptic weights based on coincident spikes
            outer_product = np.outer(pattern, pattern)
            self.pattern_weights += self.learning_rate * (outer_product - self.pattern_weights)
            
            # Keep weights bounded
            self.pattern_weights = np.clip(self.pattern_weights, -1, 1) 