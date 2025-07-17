"""
Neuromorphic Event Processor

Real-time event processing using spiking neural networks for:
- Ultra-low latency anomaly detection
- Pattern recognition
- Temporal sequence learning
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Callable
import logging
from dataclasses import dataclass, field
from collections import deque
import time
import asyncio
from datetime import datetime, timedelta

from .spiking_neural_network import SpikingNeuralNetwork, SpikeTrain

logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Represents an event in the system"""
    event_id: str
    event_type: str
    timestamp: datetime
    source: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_vector(self, feature_map: Dict[str, int]) -> np.ndarray:
        """Convert event to feature vector"""
        vector = np.zeros(len(feature_map))
        
        # Event type encoding
        if self.event_type in feature_map:
            vector[feature_map[self.event_type]] = 1.0
            
        # Extract numerical features
        for key, value in self.data.items():
            if key in feature_map and isinstance(value, (int, float)):
                vector[feature_map[key]] = float(value)
                
        # Time-based features
        hour = self.timestamp.hour
        day_of_week = self.timestamp.weekday()
        
        if "hour" in feature_map:
            vector[feature_map["hour"]] = hour / 24.0
        if "day_of_week" in feature_map:
            vector[feature_map["day_of_week"]] = day_of_week / 7.0
            
        return vector


@dataclass
class AnomalyDetection:
    """Results of anomaly detection"""
    is_anomaly: bool
    confidence: float
    anomaly_type: Optional[str] = None
    contributing_factors: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    spike_pattern: Optional[Dict[str, Any]] = None


class NeuromorphicEventProcessor:
    """
    Event processor using spiking neural networks for real-time anomaly detection
    """
    
    def __init__(self,
                 feature_dim: int = 100,
                 hidden_layers: List[int] = [200, 100, 50],
                 anomaly_threshold: float = 0.8,
                 learning_enabled: bool = True):
        """
        Initialize neuromorphic event processor
        
        Args:
            feature_dim: Dimension of feature vectors
            hidden_layers: Hidden layer sizes for SNN
            anomaly_threshold: Threshold for anomaly detection
            learning_enabled: Whether to enable online learning
        """
        self.feature_dim = feature_dim
        self.anomaly_threshold = anomaly_threshold
        self.learning_enabled = learning_enabled
        
        # Initialize spiking neural networks for different tasks
        self.anomaly_detector = SpikingNeuralNetwork(
            input_size=feature_dim,
            hidden_sizes=hidden_layers,
            output_size=10,  # Different anomaly types
            neuron_model="LIF",
            learning_rule="STDP" if learning_enabled else "static"
        )
        
        self.pattern_recognizer = SpikingNeuralNetwork(
            input_size=feature_dim,
            hidden_sizes=[150, 75],
            output_size=20,  # Pattern categories
            neuron_model="Izhikevich",
            learning_rule="R-STDP" if learning_enabled else "static"
        )
        
        self.sequence_predictor = SpikingNeuralNetwork(
            input_size=feature_dim * 5,  # 5 time steps
            hidden_sizes=[300, 150, 75],
            output_size=feature_dim,  # Predict next event features
            neuron_model="AdEx",
            learning_rule="STDP" if learning_enabled else "static"
        )
        
        # Event buffers
        self.event_buffer = deque(maxlen=1000)
        self.anomaly_buffer = deque(maxlen=100)
        self.pattern_buffer = deque(maxlen=100)
        
        # Feature mapping
        self.feature_map = {}
        self._build_feature_map()
        
        # Performance metrics
        self.processing_times = deque(maxlen=1000)
        self.detection_accuracy = {"true_positives": 0, "false_positives": 0, 
                                 "true_negatives": 0, "false_negatives": 0}
        
        # Neuromorphic-specific parameters
        self.spike_encoding = "temporal"  # Fast temporal encoding
        self.simulation_dt = 0.1  # ms - very fine time resolution
        self.max_simulation_time = 10.0  # ms - ultra-fast processing
        
    def _build_feature_map(self):
        """Build feature mapping for event encoding"""
        # Common event types
        event_types = [
            "login", "logout", "api_call", "database_query", "file_access",
            "network_connection", "process_start", "error", "warning", "success"
        ]
        
        # Numerical features
        numerical_features = [
            "response_time", "cpu_usage", "memory_usage", "network_bytes",
            "error_count", "request_rate", "latency", "throughput"
        ]
        
        # Temporal features
        temporal_features = ["hour", "day_of_week", "minute", "is_weekend"]
        
        # Build map
        idx = 0
        for feature in event_types + numerical_features + temporal_features:
            self.feature_map[feature] = idx
            idx += 1
            
        # Add dynamic features
        self.next_feature_idx = idx
        
    async def process_event(self, event: Event) -> AnomalyDetection:
        """
        Process a single event for anomaly detection
        
        Args:
            event: Event to process
            
        Returns:
            AnomalyDetection result
        """
        start_time = time.time()
        
        # Add to buffer
        self.event_buffer.append(event)
        
        # Convert to feature vector
        feature_vector = event.to_vector(self.feature_map)
        
        # Encode as spike train
        spike_train = self.anomaly_detector.encode_data(
            feature_vector,
            encoding=self.spike_encoding
        )
        
        # Process through SNN
        output_spikes = self.anomaly_detector.process(
            spike_train,
            duration=self.max_simulation_time
        )
        
        # Analyze output
        anomaly_result = self._analyze_anomaly_output(output_spikes, event)
        
        # Pattern recognition in parallel
        pattern_result = await self._detect_patterns(event, feature_vector)
        
        # Combine results
        if pattern_result and pattern_result["confidence"] > 0.7:
            anomaly_result.anomaly_type = pattern_result["pattern_type"]
            anomaly_result.confidence = max(anomaly_result.confidence, 
                                           pattern_result["confidence"])
            
        # Online learning if enabled
        if self.learning_enabled and anomaly_result.is_anomaly:
            await self._update_models(event, anomaly_result)
            
        # Record performance
        processing_time = (time.time() - start_time) * 1000  # ms
        self.processing_times.append(processing_time)
        
        # Add to anomaly buffer if detected
        if anomaly_result.is_anomaly:
            self.anomaly_buffer.append((event, anomaly_result))
            
        return anomaly_result
        
    def _analyze_anomaly_output(self, 
                               output_spikes: SpikeTrain,
                               event: Event) -> AnomalyDetection:
        """Analyze SNN output for anomalies"""
        # Count spikes per output neuron
        spike_counts = np.zeros(self.anomaly_detector.output_size)
        
        for neuron_id in output_spikes.neuron_ids:
            if neuron_id < self.anomaly_detector.output_size:
                spike_counts[neuron_id] += 1
                
        # Normalize by simulation time
        spike_rates = spike_counts / (self.max_simulation_time / 1000.0)
        
        # Anomaly detection logic
        max_rate_idx = np.argmax(spike_rates)
        max_rate = spike_rates[max_rate_idx]
        
        # Map neuron index to anomaly type
        anomaly_types = [
            "unusual_pattern", "timing_anomaly", "value_anomaly",
            "sequence_break", "frequency_anomaly", "correlation_anomaly",
            "burst_anomaly", "silence_anomaly", "drift_anomaly", "unknown"
        ]
        
        is_anomaly = max_rate > self.anomaly_threshold * 100  # Hz
        anomaly_type = anomaly_types[max_rate_idx] if is_anomaly else None
        
        # Calculate confidence
        confidence = min(max_rate / 100.0, 1.0) if is_anomaly else 0.0
        
        # Identify contributing factors
        contributing_factors = []
        if is_anomaly:
            # Analyze which input features contributed most
            contributing_factors = self._identify_contributing_factors(event)
            
        # Generate recommendations
        recommendations = []
        if is_anomaly:
            recommendations = self._generate_recommendations(anomaly_type, event)
            
        # Analyze spike pattern
        spike_pattern = {
            "total_spikes": len(output_spikes.spike_times),
            "spike_rates": spike_rates.tolist(),
            "dominant_neuron": int(max_rate_idx),
            "synchrony_index": self._calculate_synchrony(output_spikes)
        }
        
        return AnomalyDetection(
            is_anomaly=is_anomaly,
            confidence=confidence,
            anomaly_type=anomaly_type,
            contributing_factors=contributing_factors,
            recommended_actions=recommendations,
            spike_pattern=spike_pattern
        )
        
    async def _detect_patterns(self, 
                              event: Event,
                              feature_vector: np.ndarray) -> Optional[Dict[str, Any]]:
        """Detect patterns using pattern recognition SNN"""
        # Encode for pattern recognition
        spike_train = self.pattern_recognizer.encode_data(
            feature_vector,
            encoding="rate"  # Different encoding for patterns
        )
        
        # Process
        output_spikes = self.pattern_recognizer.process(
            spike_train,
            duration=self.max_simulation_time * 2  # Longer for patterns
        )
        
        # Analyze patterns
        pattern_types = [
            "periodic", "trending", "clustering", "outlier",
            "seasonal", "cyclic", "random", "structured"
        ]
        
        spike_counts = np.zeros(len(pattern_types))
        for neuron_id in output_spikes.neuron_ids:
            if neuron_id < len(pattern_types):
                spike_counts[neuron_id] += 1
                
        max_pattern_idx = np.argmax(spike_counts)
        confidence = spike_counts[max_pattern_idx] / (len(output_spikes.spike_times) + 1)
        
        if confidence > 0.3:
            return {
                "pattern_type": pattern_types[max_pattern_idx],
                "confidence": float(confidence),
                "spike_distribution": spike_counts.tolist()
            }
            
        return None
        
    async def predict_next_event(self, 
                                context_window: int = 5) -> Dict[str, Any]:
        """Predict next event based on recent history"""
        if len(self.event_buffer) < context_window:
            return {"error": "Insufficient event history"}
            
        # Get recent events
        recent_events = list(self.event_buffer)[-context_window:]
        
        # Create temporal feature vector
        temporal_features = []
        for event in recent_events:
            temporal_features.extend(event.to_vector(self.feature_map))
            
        temporal_vector = np.array(temporal_features)
        
        # Encode and process
        spike_train = self.sequence_predictor.encode_data(
            temporal_vector,
            encoding="delta"  # Delta encoding for sequences
        )
        
        output_spikes = self.sequence_predictor.process(
            spike_train,
            duration=self.max_simulation_time * 3
        )
        
        # Decode prediction
        predicted_features = self._decode_spike_output(output_spikes)
        
        # Convert back to event prediction
        prediction = {
            "predicted_event_type": self._predict_event_type(predicted_features),
            "predicted_features": predicted_features,
            "confidence": self._calculate_prediction_confidence(output_spikes),
            "expected_time": datetime.now() + timedelta(seconds=1)
        }
        
        return prediction
        
    def _identify_contributing_factors(self, event: Event) -> List[str]:
        """Identify factors contributing to anomaly"""
        factors = []
        
        # Check event type
        if event.event_type in ["error", "warning", "failure"]:
            factors.append(f"Event type: {event.event_type}")
            
        # Check numerical anomalies
        for key, value in event.data.items():
            if isinstance(value, (int, float)):
                # Simple threshold check (would be more sophisticated in practice)
                if key == "response_time" and value > 1000:
                    factors.append(f"High {key}: {value}ms")
                elif key == "error_count" and value > 0:
                    factors.append(f"Errors detected: {value}")
                elif key == "cpu_usage" and value > 90:
                    factors.append(f"High CPU: {value}%")
                    
        # Check temporal factors
        if event.timestamp.hour < 6 or event.timestamp.hour > 22:
            factors.append("Unusual time of day")
            
        return factors
        
    def _generate_recommendations(self, 
                                anomaly_type: str,
                                event: Event) -> List[str]:
        """Generate recommendations based on anomaly type"""
        recommendations = []
        
        if anomaly_type == "unusual_pattern":
            recommendations.append("Review recent system changes")
            recommendations.append("Check for unauthorized access")
            
        elif anomaly_type == "timing_anomaly":
            recommendations.append("Investigate performance bottlenecks")
            recommendations.append("Check network latency")
            
        elif anomaly_type == "value_anomaly":
            recommendations.append("Verify input validation")
            recommendations.append("Check for data corruption")
            
        elif anomaly_type == "sequence_break":
            recommendations.append("Review workflow integrity")
            recommendations.append("Check for missing events")
            
        elif anomaly_type == "frequency_anomaly":
            recommendations.append("Monitor for DDoS attacks")
            recommendations.append("Check rate limiting")
            
        elif anomaly_type == "burst_anomaly":
            recommendations.append("Scale resources if needed")
            recommendations.append("Implement backpressure")
            
        return recommendations
        
    def _calculate_synchrony(self, spike_train: SpikeTrain) -> float:
        """Calculate synchrony index of spike train"""
        if len(spike_train.spike_times) < 2:
            return 0.0
            
        # Bin spikes
        bins = np.linspace(
            spike_train.spike_times.min(),
            spike_train.spike_times.max(),
            20
        )
        
        hist, _ = np.histogram(spike_train.spike_times, bins=bins)
        
        # Calculate coefficient of variation
        if hist.mean() > 0:
            cv = hist.std() / hist.mean()
            return float(cv)
        return 0.0
        
    def _decode_spike_output(self, spike_train: SpikeTrain) -> Dict[str, float]:
        """Decode spike train back to feature values"""
        features = {}
        
        # Simple rate-based decoding
        for neuron_id in range(self.sequence_predictor.output_size):
            neuron_spikes = spike_train.spike_times[spike_train.neuron_ids == neuron_id]
            rate = len(neuron_spikes) / (self.max_simulation_time / 1000.0)
            
            # Map back to feature
            for feature_name, feature_idx in self.feature_map.items():
                if feature_idx == neuron_id:
                    features[feature_name] = rate / 100.0  # Normalize
                    
        return features
        
    def _predict_event_type(self, features: Dict[str, float]) -> str:
        """Predict event type from features"""
        # Find highest activation among event type features
        event_types = ["login", "logout", "api_call", "database_query", 
                      "file_access", "network_connection", "process_start",
                      "error", "warning", "success"]
        
        max_activation = 0
        predicted_type = "unknown"
        
        for event_type in event_types:
            if event_type in features and features[event_type] > max_activation:
                max_activation = features[event_type]
                predicted_type = event_type
                
        return predicted_type
        
    def _calculate_prediction_confidence(self, spike_train: SpikeTrain) -> float:
        """Calculate confidence of prediction"""
        if len(spike_train.spike_times) == 0:
            return 0.0
            
        # Based on spike count and distribution
        total_spikes = len(spike_train.spike_times)
        unique_neurons = len(np.unique(spike_train.neuron_ids))
        
        # More spikes and more active neurons = higher confidence
        confidence = min(1.0, (total_spikes / 100.0) * (unique_neurons / 10.0))
        
        return float(confidence)
        
    async def _update_models(self, event: Event, anomaly: AnomalyDetection):
        """Update models with new learning"""
        if not self.learning_enabled:
            return
            
        # Create training sample
        feature_vector = event.to_vector(self.feature_map)
        
        # Create target based on confirmed anomaly
        target = np.zeros(self.anomaly_detector.output_size)
        if anomaly.anomaly_type:
            anomaly_types = [
                "unusual_pattern", "timing_anomaly", "value_anomaly",
                "sequence_break", "frequency_anomaly", "correlation_anomaly",
                "burst_anomaly", "silence_anomaly", "drift_anomaly", "unknown"
            ]
            if anomaly.anomaly_type in anomaly_types:
                idx = anomaly_types.index(anomaly.anomaly_type)
                target[idx] = 1.0
                
        # Convert to spike trains
        input_train = self.anomaly_detector.encode_data(feature_vector)
        target_train = self.anomaly_detector.encode_data(target, encoding="rate")
        
        # Quick online update
        self.anomaly_detector.train(
            [input_train],
            [target_train],
            learning_rate=0.001,
            epochs=1
        )
        
    async def analyze_event_stream(self, 
                                  events: List[Event],
                                  window_size: int = 100) -> Dict[str, Any]:
        """Analyze a stream of events for patterns and anomalies"""
        results = {
            "total_events": len(events),
            "anomalies_detected": 0,
            "patterns_found": [],
            "processing_stats": {},
            "neuromorphic_efficiency": {}
        }
        
        anomalies = []
        patterns = defaultdict(int)
        
        # Process events
        for event in events:
            anomaly = await self.process_event(event)
            
            if anomaly.is_anomaly:
                anomalies.append({
                    "event_id": event.event_id,
                    "anomaly_type": anomaly.anomaly_type,
                    "confidence": anomaly.confidence
                })
                
            # Detect patterns in windows
            if len(self.event_buffer) >= window_size:
                window_pattern = await self._analyze_window_pattern()
                if window_pattern:
                    patterns[window_pattern["pattern"]] += 1
                    
        results["anomalies_detected"] = len(anomalies)
        results["anomaly_details"] = anomalies
        results["patterns_found"] = [
            {"pattern": p, "count": c} for p, c in patterns.items()
        ]
        
        # Calculate processing statistics
        if self.processing_times:
            results["processing_stats"] = {
                "mean_latency_ms": np.mean(self.processing_times),
                "max_latency_ms": np.max(self.processing_times),
                "min_latency_ms": np.min(self.processing_times),
                "p99_latency_ms": np.percentile(self.processing_times, 99)
            }
            
        # Calculate neuromorphic efficiency
        energy_stats = self.anomaly_detector.get_energy_consumption()
        results["neuromorphic_efficiency"] = {
            "total_energy_joules": energy_stats["total_energy_joules"],
            "energy_per_event": energy_stats["total_energy_joules"] / len(events),
            "vs_traditional_factor": energy_stats["energy_efficiency_factor"],
            "average_spikes_per_event": energy_stats.get("total_spikes", 0) / len(events)
        }
        
        return results
        
    async def _analyze_window_pattern(self) -> Optional[Dict[str, str]]:
        """Analyze pattern in current window"""
        recent_events = list(self.event_buffer)[-100:]
        
        # Simple pattern detection
        event_types = [e.event_type for e in recent_events]
        
        # Check for repetitive patterns
        for pattern_len in range(2, 10):
            pattern = event_types[:pattern_len]
            matches = 0
            
            for i in range(pattern_len, len(event_types), pattern_len):
                if event_types[i:i+pattern_len] == pattern:
                    matches += 1
                    
            if matches > 3:
                return {
                    "pattern": f"repetitive_{pattern_len}",
                    "description": f"Repeating sequence of {pattern_len} events"
                }
                
        return None
        
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics of the processor"""
        metrics = {
            "processing_latency": {},
            "detection_accuracy": {},
            "neuromorphic_stats": {},
            "memory_usage": {}
        }
        
        # Latency stats
        if self.processing_times:
            metrics["processing_latency"] = {
                "mean_ms": float(np.mean(self.processing_times)),
                "std_ms": float(np.std(self.processing_times)),
                "min_ms": float(np.min(self.processing_times)),
                "max_ms": float(np.max(self.processing_times)),
                "p50_ms": float(np.percentile(self.processing_times, 50)),
                "p90_ms": float(np.percentile(self.processing_times, 90)),
                "p99_ms": float(np.percentile(self.processing_times, 99))
            }
            
        # Detection accuracy
        total = sum(self.detection_accuracy.values())
        if total > 0:
            tp = self.detection_accuracy["true_positives"]
            fp = self.detection_accuracy["false_positives"]
            tn = self.detection_accuracy["true_negatives"]
            fn = self.detection_accuracy["false_negatives"]
            
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            
            metrics["detection_accuracy"] = {
                "precision": float(precision),
                "recall": float(recall),
                "f1_score": float(f1),
                "accuracy": float((tp + tn) / total)
            }
            
        # Neuromorphic stats
        snn_activity = self.anomaly_detector.analyze_activity()
        metrics["neuromorphic_stats"] = {
            "total_neurons": self.anomaly_detector.input_size + 
                           sum(self.anomaly_detector.hidden_sizes) + 
                           self.anomaly_detector.output_size,
            "average_firing_rate": np.mean(list(snn_activity.get("firing_rates", {}).values())),
            "network_synchrony": snn_activity.get("synchrony", 0),
            "energy_efficiency": self.anomaly_detector.get_energy_consumption()["energy_efficiency_factor"]
        }
        
        # Memory usage
        metrics["memory_usage"] = {
            "event_buffer_size": len(self.event_buffer),
            "anomaly_buffer_size": len(self.anomaly_buffer),
            "pattern_buffer_size": len(self.pattern_buffer)
        }
        
        return metrics
        
    def reset_metrics(self):
        """Reset performance metrics"""
        self.processing_times.clear()
        self.detection_accuracy = {"true_positives": 0, "false_positives": 0,
                                  "true_negatives": 0, "false_negatives": 0}
                                  
    def update_detection_accuracy(self, 
                                 predicted: bool,
                                 actual: bool):
        """Update detection accuracy metrics"""
        if predicted and actual:
            self.detection_accuracy["true_positives"] += 1
        elif predicted and not actual:
            self.detection_accuracy["false_positives"] += 1
        elif not predicted and actual:
            self.detection_accuracy["false_negatives"] += 1
        else:
            self.detection_accuracy["true_negatives"] += 1 