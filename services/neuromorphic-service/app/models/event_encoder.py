"""
Event Encoder for converting platform events to spike trains
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Union
import hashlib
import json
from enum import Enum
from datetime import datetime
import struct

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class EncodingType(Enum):
    """Supported encoding types for events to spikes"""
    RATE = "rate"
    TEMPORAL = "temporal"
    POPULATION = "population"
    LATENCY = "latency"
    PHASE = "phase"


class EventEncoder:
    """
    Converts various event types into spike trains for SNN processing
    """
    
    def __init__(self, 
                 encoding_type: Union[EncodingType, str] = EncodingType.TEMPORAL,
                 num_neurons: int = 1000,
                 time_window: float = 100.0):  # ms
        """
        Initialize event encoder
        
        Args:
            encoding_type: How to encode events (rate, temporal, population)
            num_neurons: Number of neurons for encoding
            time_window: Time window for encoding in milliseconds
        """
        if isinstance(encoding_type, str):
            encoding_type = EncodingType(encoding_type)
            
        self.encoding_type = encoding_type
        self.num_neurons = num_neurons
        self.time_window = time_window
        
        # Feature extractors for different event types
        self.feature_extractors = {
            'security': self._extract_security_features,
            'performance': self._extract_performance_features,
            'business': self._extract_business_features,
            'sensor': self._extract_sensor_features,
            'generic': self._extract_generic_features
        }
        
        # Encoding functions
        self.encoders = {
            EncodingType.RATE: self._rate_encoding,
            EncodingType.TEMPORAL: self._temporal_encoding,
            EncodingType.POPULATION: self._population_encoding,
            EncodingType.LATENCY: self._latency_encoding,
            EncodingType.PHASE: self._phase_encoding
        }
        
        logger.info(f"Initialized EventEncoder with {encoding_type.value} encoding, "
                   f"{num_neurons} neurons")
        
    def encode(self, event: Dict) -> np.ndarray:
        """
        Encode an event into spike trains
        
        Args:
            event: Event dictionary with type, data, timestamp, etc.
            
        Returns:
            Spike train array of shape (num_neurons,)
        """
        # Determine event type
        event_type = self._determine_event_type(event)
        
        # Extract features
        features = self.feature_extractors.get(
            event_type, 
            self._extract_generic_features
        )(event)
        
        # Encode features to spikes
        spike_train = self.encoders[self.encoding_type](features)
        
        return spike_train
        
    def _determine_event_type(self, event: Dict) -> str:
        """Determine the type of event for appropriate feature extraction"""
        event_type = event.get('event_type', '').lower()
        
        if any(keyword in event_type for keyword in ['auth', 'access', 'security', 'intrusion']):
            return 'security'
        elif any(keyword in event_type for keyword in ['latency', 'cpu', 'memory', 'performance']):
            return 'performance'
        elif any(keyword in event_type for keyword in ['transaction', 'order', 'payment', 'user']):
            return 'business'
        elif any(keyword in event_type for keyword in ['sensor', 'iot', 'telemetry', 'measurement']):
            return 'sensor'
        else:
            return 'generic'
            
    def _extract_security_features(self, event: Dict) -> np.ndarray:
        """Extract features from security-related events"""
        features = []
        
        # Time-based features
        timestamp = event.get('timestamp', datetime.utcnow().timestamp())
        hour = datetime.fromtimestamp(timestamp).hour
        features.append(hour / 24.0)  # Normalized hour
        
        # Access pattern features
        if 'source_ip' in event:
            ip_hash = int(hashlib.md5(event['source_ip'].encode()).hexdigest()[:8], 16)
            features.append((ip_hash % 1000) / 1000.0)
            
        if 'user_id' in event:
            user_hash = int(hashlib.md5(str(event['user_id']).encode()).hexdigest()[:8], 16)
            features.append((user_hash % 1000) / 1000.0)
            
        # Request features
        features.append(event.get('request_size', 0) / 10000.0)  # Normalized
        features.append(event.get('failed_attempts', 0) / 10.0)
        features.append(1.0 if event.get('is_authenticated', False) else 0.0)
        
        # Anomaly indicators
        features.append(event.get('geo_anomaly_score', 0.0))
        features.append(event.get('time_anomaly_score', 0.0))
        features.append(event.get('behavior_anomaly_score', 0.0))
        
        return np.array(features)
        
    def _extract_performance_features(self, event: Dict) -> np.ndarray:
        """Extract features from performance-related events"""
        features = []
        
        # Resource metrics
        features.append(event.get('cpu_usage', 0.0) / 100.0)
        features.append(event.get('memory_usage', 0.0) / 100.0)
        features.append(event.get('disk_io', 0.0) / 1000.0)
        features.append(event.get('network_io', 0.0) / 1000.0)
        
        # Latency metrics
        features.append(min(event.get('response_time', 0.0) / 1000.0, 1.0))
        features.append(min(event.get('queue_depth', 0) / 100.0, 1.0))
        
        # Error metrics
        features.append(event.get('error_rate', 0.0))
        features.append(event.get('retry_count', 0) / 10.0)
        
        # Service health
        features.append(1.0 if event.get('is_healthy', True) else 0.0)
        features.append(event.get('health_score', 1.0))
        
        return np.array(features)
        
    def _extract_business_features(self, event: Dict) -> np.ndarray:
        """Extract features from business process events"""
        features = []
        
        # Transaction features
        features.append(np.log1p(event.get('amount', 0.0)) / 10.0)
        features.append(event.get('item_count', 0) / 100.0)
        
        # User behavior
        features.append(event.get('session_duration', 0) / 3600.0)  # Hours
        features.append(event.get('page_views', 0) / 50.0)
        features.append(event.get('cart_abandonment_risk', 0.0))
        
        # Business metrics
        features.append(event.get('conversion_probability', 0.0))
        features.append(event.get('customer_lifetime_value', 0.0) / 10000.0)
        
        # Temporal patterns
        timestamp = event.get('timestamp', datetime.utcnow().timestamp())
        dt = datetime.fromtimestamp(timestamp)
        features.append(dt.weekday() / 6.0)  # Day of week
        features.append(dt.hour / 24.0)  # Hour of day
        
        return np.array(features)
        
    def _extract_sensor_features(self, event: Dict) -> np.ndarray:
        """Extract features from sensor/IoT events"""
        features = []
        
        # Sensor readings
        features.append(event.get('temperature', 20.0) / 100.0)
        features.append(event.get('humidity', 50.0) / 100.0)
        features.append(event.get('pressure', 1013.0) / 2000.0)
        
        # Motion/vibration
        features.append(event.get('acceleration_x', 0.0) / 10.0)
        features.append(event.get('acceleration_y', 0.0) / 10.0)
        features.append(event.get('acceleration_z', 0.0) / 10.0)
        
        # Device health
        features.append(event.get('battery_level', 100.0) / 100.0)
        features.append(event.get('signal_strength', -50.0) / -100.0)
        
        # Anomaly scores
        features.append(event.get('deviation_score', 0.0))
        features.append(event.get('trend_anomaly', 0.0))
        
        return np.array(features)
        
    def _extract_generic_features(self, event: Dict) -> np.ndarray:
        """Extract features from generic events"""
        features = []
        
        # Basic event properties
        event_str = json.dumps(event, sort_keys=True)
        event_hash = int(hashlib.sha256(event_str.encode()).hexdigest()[:16], 16)
        
        # Extract numeric values
        for key, value in event.items():
            if isinstance(value, (int, float)):
                features.append(float(value))
            elif isinstance(value, bool):
                features.append(1.0 if value else 0.0)
                
        # If too few features, pad with hash-based pseudo-random values
        if len(features) < 10:
            np.random.seed(event_hash % 2**32)
            features.extend(np.random.rand(10 - len(features)).tolist())
            
        return np.array(features[:20])  # Limit to 20 features
        
    def _rate_encoding(self, features: np.ndarray) -> np.ndarray:
        """
        Rate encoding: feature values determine spike rates
        """
        # Normalize features to [0, 1]
        features = np.clip(features, 0, 1)
        
        # Distribute features across neurons
        spike_rates = np.zeros(self.num_neurons)
        neurons_per_feature = self.num_neurons // len(features)
        
        for i, feature in enumerate(features):
            start_idx = i * neurons_per_feature
            end_idx = min((i + 1) * neurons_per_feature, self.num_neurons)
            spike_rates[start_idx:end_idx] = feature
            
        # Add noise for better encoding
        spike_rates += np.random.normal(0, 0.1, self.num_neurons)
        spike_rates = np.clip(spike_rates, 0, 1)
        
        return spike_rates
        
    def _temporal_encoding(self, features: np.ndarray) -> np.ndarray:
        """
        Temporal encoding: feature values determine spike timing
        """
        spike_times = np.zeros(self.num_neurons)
        neurons_per_feature = self.num_neurons // len(features)
        
        for i, feature in enumerate(features):
            # Convert feature to spike time (inverse relationship)
            spike_time = (1.0 - np.clip(feature, 0, 1)) * self.time_window
            
            start_idx = i * neurons_per_feature
            end_idx = min((i + 1) * neurons_per_feature, self.num_neurons)
            
            # Add temporal jitter
            jitter = np.random.normal(0, 5, end_idx - start_idx)  # 5ms std
            spike_times[start_idx:end_idx] = spike_time + jitter
            
        # Convert to spike probability based on timing
        spike_prob = np.exp(-spike_times / (self.time_window / 4))
        
        return spike_prob
        
    def _population_encoding(self, features: np.ndarray) -> np.ndarray:
        """
        Population encoding: feature values encoded by population of neurons
        """
        spike_pattern = np.zeros(self.num_neurons)
        neurons_per_feature = self.num_neurons // len(features)
        
        for i, feature in enumerate(features):
            # Each feature encoded by gaussian population
            start_idx = i * neurons_per_feature
            end_idx = min((i + 1) * neurons_per_feature, self.num_neurons)
            
            # Create gaussian receptive fields
            neuron_positions = np.linspace(0, 1, end_idx - start_idx)
            responses = np.exp(-((neuron_positions - feature) ** 2) / (2 * 0.1 ** 2))
            
            spike_pattern[start_idx:end_idx] = responses
            
        return spike_pattern
        
    def _latency_encoding(self, features: np.ndarray) -> np.ndarray:
        """
        Latency encoding: stronger inputs spike earlier
        """
        # Similar to temporal but with different distribution
        latencies = (1.0 - np.clip(features, 0, 1)) * self.time_window
        
        # Repeat features to fill neurons
        repeated_features = np.tile(features, self.num_neurons // len(features) + 1)
        repeated_features = repeated_features[:self.num_neurons]
        
        # Convert to spike probability
        spike_prob = 1.0 / (1.0 + np.exp((latencies.mean() - 50) / 10))
        
        return repeated_features * spike_prob
        
    def _phase_encoding(self, features: np.ndarray) -> np.ndarray:
        """
        Phase encoding: encode features as phase relationships
        """
        spike_pattern = np.zeros(self.num_neurons)
        neurons_per_feature = self.num_neurons // len(features)
        
        base_frequency = 40.0  # Hz (gamma band)
        
        for i, feature in enumerate(features):
            start_idx = i * neurons_per_feature
            end_idx = min((i + 1) * neurons_per_feature, self.num_neurons)
            
            # Phase based on feature value
            phase = feature * 2 * np.pi
            
            # Create oscillatory pattern
            t = np.linspace(0, self.time_window / 1000, end_idx - start_idx)
            oscillation = np.sin(2 * np.pi * base_frequency * t + phase)
            
            # Convert to spike probability
            spike_pattern[start_idx:end_idx] = (oscillation + 1) / 2
            
        return spike_pattern
        
    def encode_batch(self, events: List[Dict]) -> np.ndarray:
        """
        Encode multiple events in batch
        
        Args:
            events: List of event dictionaries
            
        Returns:
            Array of shape (batch_size, num_neurons)
        """
        encoded = []
        for event in events:
            encoded.append(self.encode(event))
            
        return np.array(encoded)
        
    def decode_spike_pattern(self, spike_pattern: np.ndarray) -> Dict:
        """
        Attempt to decode information from spike pattern
        
        Args:
            spike_pattern: Spike pattern array
            
        Returns:
            Dictionary with decoded information
        """
        # Basic statistics
        stats = {
            'mean_rate': np.mean(spike_pattern),
            'max_rate': np.max(spike_pattern),
            'active_fraction': np.sum(spike_pattern > 0.1) / len(spike_pattern),
            'sparsity': 1.0 - np.mean(spike_pattern > 0.1)
        }
        
        # Pattern analysis
        neurons_per_feature = self.num_neurons // 10  # Assume 10 features
        feature_activations = []
        
        for i in range(10):
            start_idx = i * neurons_per_feature
            end_idx = min((i + 1) * neurons_per_feature, self.num_neurons)
            
            if end_idx > start_idx:
                activation = np.mean(spike_pattern[start_idx:end_idx])
                feature_activations.append(activation)
                
        stats['feature_activations'] = feature_activations
        
        # Temporal structure (if temporal encoding)
        if self.encoding_type == EncodingType.TEMPORAL:
            # Estimate timing from spike probabilities
            spike_times = -np.log(spike_pattern + 1e-10) * (self.time_window / 4)
            stats['estimated_latencies'] = spike_times.tolist()[:10]  # First 10
            
        return stats 