"""
Anomaly Detector for analyzing spike patterns from SNN
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import deque
from datetime import datetime
import json
from dataclasses import dataclass
from enum import Enum
import uuid

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class AnomalyType(Enum):
    """Types of anomalies that can be detected"""
    SECURITY_THREAT = "security_threat"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    BUSINESS_ANOMALY = "business_anomaly"
    SENSOR_PATTERN = "sensor_pattern"
    UNKNOWN = "unknown"


@dataclass
class AnomalyPattern:
    """Represents a learned anomaly pattern"""
    pattern_id: str
    pattern_type: AnomalyType
    spike_signature: np.ndarray
    threshold: float
    occurrence_count: int = 0
    last_seen: Optional[datetime] = None
    metadata: Dict = None


class AnomalyDetector:
    """
    Detects anomalies from spike patterns using multiple techniques
    """
    
    def __init__(self,
                 threshold: float = 0.7,
                 window_size: int = 100,
                 sensitivity: float = 2.0):
        """
        Initialize anomaly detector
        
        Args:
            threshold: Base threshold for anomaly detection
            window_size: Number of recent patterns to consider
            sensitivity: Multiplier for dynamic threshold adjustment
        """
        self.threshold = threshold
        self.window_size = window_size
        self.sensitivity = sensitivity
        
        # Pattern memory
        self.known_patterns: Dict[str, AnomalyPattern] = {}
        self.pattern_history = deque(maxlen=window_size)
        
        # Statistics tracking
        self.baseline_stats = {
            'mean': 0.5,
            'std': 0.1,
            'min': 0.0,
            'max': 1.0
        }
        
        # Detection methods
        self.detection_methods = {
            'statistical': self._statistical_detection,
            'pattern_matching': self._pattern_matching_detection,
            'temporal': self._temporal_anomaly_detection,
            'clustering': self._clustering_detection,
            'entropy': self._entropy_detection
        }
        
        # Anomaly tracking
        self.total_anomalies = 0
        self.anomaly_buffer = deque(maxlen=1000)
        
        logger.info(f"Initialized AnomalyDetector with threshold={threshold}, "
                   f"window_size={window_size}")
        
    def detect(self, spike_pattern: np.ndarray, event: Dict) -> Optional[Dict]:
        """
        Detect anomalies in spike pattern
        
        Args:
            spike_pattern: Output spike pattern from SNN
            event: Original event that generated the pattern
            
        Returns:
            Anomaly dictionary if detected, None otherwise
        """
        # Update pattern history
        self.pattern_history.append(spike_pattern)
        
        # Run multiple detection methods
        anomaly_scores = {}
        for method_name, method_func in self.detection_methods.items():
            score = method_func(spike_pattern)
            anomaly_scores[method_name] = score
            
        # Combine scores
        combined_score = self._combine_anomaly_scores(anomaly_scores)
        
        # Determine if anomaly
        if combined_score > self.threshold:
            anomaly = self._create_anomaly_event(
                spike_pattern,
                event,
                combined_score,
                anomaly_scores
            )
            
            self.total_anomalies += 1
            self.anomaly_buffer.append(anomaly)
            
            # Learn from anomaly
            self._learn_pattern(spike_pattern, anomaly)
            
            return anomaly
            
        # Update baseline if normal
        self._update_baseline(spike_pattern)
        
        return None
        
    def _statistical_detection(self, spike_pattern: np.ndarray) -> float:
        """Statistical anomaly detection based on deviation from baseline"""
        if len(self.pattern_history) < 10:
            return 0.0
            
        # Calculate statistics from history
        history_array = np.array(self.pattern_history)
        mean_pattern = np.mean(history_array, axis=0)
        std_pattern = np.std(history_array, axis=0)
        
        # Calculate z-scores
        z_scores = np.abs((spike_pattern - mean_pattern) / (std_pattern + 1e-6))
        
        # Anomaly score based on extreme deviations
        anomaly_score = np.mean(z_scores > self.sensitivity)
        
        return anomaly_score
        
    def _pattern_matching_detection(self, spike_pattern: np.ndarray) -> float:
        """Pattern matching against known anomaly patterns"""
        if not self.known_patterns:
            return 0.0
            
        max_similarity = 0.0
        
        for pattern_id, known_pattern in self.known_patterns.items():
            # Cosine similarity
            similarity = np.dot(spike_pattern, known_pattern.spike_signature) / (
                np.linalg.norm(spike_pattern) * np.linalg.norm(known_pattern.spike_signature) + 1e-6
            )
            
            if similarity > max_similarity:
                max_similarity = similarity
                
        return max_similarity
        
    def _temporal_anomaly_detection(self, spike_pattern: np.ndarray) -> float:
        """Detect temporal anomalies in spike timing patterns"""
        if len(self.pattern_history) < 3:
            return 0.0
            
        # Analyze temporal evolution
        recent_patterns = list(self.pattern_history)[-3:]
        
        # Calculate temporal derivatives
        diff1 = spike_pattern - recent_patterns[-1]
        diff2 = recent_patterns[-1] - recent_patterns[-2]
        
        # Sudden changes indicate anomaly
        acceleration = np.abs(diff1 - diff2)
        anomaly_score = np.mean(acceleration) * 5  # Scale factor
        
        return min(anomaly_score, 1.0)
        
    def _clustering_detection(self, spike_pattern: np.ndarray) -> float:
        """Clustering-based anomaly detection"""
        if len(self.pattern_history) < 20:
            return 0.0
            
        # Simple distance-based clustering
        history_array = np.array(self.pattern_history)
        
        # Calculate distances to all historical patterns
        distances = np.linalg.norm(history_array - spike_pattern, axis=1)
        
        # Find k-nearest neighbors
        k = min(5, len(distances))
        k_nearest_distances = np.sort(distances)[:k]
        
        # Anomaly score based on distance to nearest neighbors
        avg_distance = np.mean(k_nearest_distances)
        anomaly_score = 1 - np.exp(-avg_distance)
        
        return anomaly_score
        
    def _entropy_detection(self, spike_pattern: np.ndarray) -> float:
        """Entropy-based anomaly detection"""
        # Calculate entropy of spike pattern
        # Discretize into bins
        bins = np.histogram(spike_pattern, bins=10)[0]
        probabilities = bins / (np.sum(bins) + 1e-6)
        
        # Shannon entropy
        entropy = -np.sum(probabilities * np.log2(probabilities + 1e-10))
        
        # Normalize entropy
        max_entropy = np.log2(10)  # Maximum for 10 bins
        normalized_entropy = entropy / max_entropy
        
        # Very high or very low entropy indicates anomaly
        if normalized_entropy < 0.3 or normalized_entropy > 0.9:
            return 1.0 - abs(normalized_entropy - 0.6) / 0.4
        
        return 0.0
        
    def _combine_anomaly_scores(self, scores: Dict[str, float]) -> float:
        """Combine multiple anomaly scores into single score"""
        # Weighted combination
        weights = {
            'statistical': 0.3,
            'pattern_matching': 0.25,
            'temporal': 0.2,
            'clustering': 0.15,
            'entropy': 0.1
        }
        
        combined_score = 0.0
        for method, score in scores.items():
            combined_score += weights.get(method, 0.1) * score
            
        return combined_score
        
    def _create_anomaly_event(self,
                            spike_pattern: np.ndarray,
                            event: Dict,
                            score: float,
                            method_scores: Dict) -> Dict:
        """Create anomaly event dictionary"""
        # Determine anomaly type
        anomaly_type = self._classify_anomaly(event, spike_pattern)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(anomaly_type, score, event)
        
        anomaly = {
            'anomaly_id': str(uuid.uuid4()),
            'detected_at': int(datetime.utcnow().timestamp() * 1000),
            'anomaly_type': anomaly_type.value,
            'severity': float(score),
            'confidence': float(self._calculate_confidence(method_scores)),
            'spike_pattern': spike_pattern.tolist(),
            'affected_component': event.get('component', event.get('service', None)),
            'contributing_events': [event.get('event_id', '')],
            'neural_metrics': {
                'spike_rate_hz': float(np.mean(spike_pattern) * 100),
                'active_neurons': int(np.sum(spike_pattern > 0.1)),
                'processing_latency_us': 500  # Placeholder
            },
            'recommended_actions': recommendations,
            'metadata': {
                'detection_methods': method_scores,
                'event_type': event.get('event_type', 'unknown'),
                'original_event': json.dumps(event)
            }
        }
        
        return anomaly
        
    def _classify_anomaly(self, event: Dict, spike_pattern: np.ndarray) -> AnomalyType:
        """Classify the type of anomaly based on event and pattern"""
        event_type = event.get('event_type', '').lower()
        
        # Classification based on event type
        if any(keyword in event_type for keyword in ['auth', 'access', 'security']):
            return AnomalyType.SECURITY_THREAT
        elif any(keyword in event_type for keyword in ['performance', 'latency', 'cpu']):
            return AnomalyType.PERFORMANCE_DEGRADATION
        elif any(keyword in event_type for keyword in ['transaction', 'business', 'order']):
            return AnomalyType.BUSINESS_ANOMALY
        elif any(keyword in event_type for keyword in ['sensor', 'iot', 'telemetry']):
            return AnomalyType.SENSOR_PATTERN
            
        # Classification based on spike pattern characteristics
        spike_density = np.mean(spike_pattern)
        if spike_density > 0.8:
            return AnomalyType.PERFORMANCE_DEGRADATION
        elif spike_density < 0.2:
            return AnomalyType.SECURITY_THREAT
            
        return AnomalyType.UNKNOWN
        
    def _generate_recommendations(self,
                                anomaly_type: AnomalyType,
                                severity: float,
                                event: Dict) -> List[Dict]:
        """Generate recommended actions based on anomaly"""
        recommendations = []
        
        # Determine urgency
        if severity > 0.9:
            urgency = "IMMEDIATE"
        elif severity > 0.7:
            urgency = "HIGH"
        elif severity > 0.5:
            urgency = "MEDIUM"
        else:
            urgency = "LOW"
            
        # Type-specific recommendations
        if anomaly_type == AnomalyType.SECURITY_THREAT:
            recommendations.extend([
                {
                    'action_type': 'isolate_component',
                    'urgency': urgency,
                    'description': f"Isolate affected component {event.get('component', 'unknown')}"
                },
                {
                    'action_type': 'increase_monitoring',
                    'urgency': 'HIGH',
                    'description': 'Increase security monitoring and logging'
                },
                {
                    'action_type': 'notify_security',
                    'urgency': urgency,
                    'description': 'Notify security team for investigation'
                }
            ])
            
        elif anomaly_type == AnomalyType.PERFORMANCE_DEGRADATION:
            recommendations.extend([
                {
                    'action_type': 'scale_resources',
                    'urgency': urgency,
                    'description': 'Scale up compute resources'
                },
                {
                    'action_type': 'redistribute_load',
                    'urgency': 'MEDIUM',
                    'description': 'Redistribute workload across available nodes'
                },
                {
                    'action_type': 'clear_cache',
                    'urgency': 'LOW',
                    'description': 'Clear caches and restart services if needed'
                }
            ])
            
        elif anomaly_type == AnomalyType.BUSINESS_ANOMALY:
            recommendations.extend([
                {
                    'action_type': 'review_transactions',
                    'urgency': urgency,
                    'description': 'Review recent transactions for fraud'
                },
                {
                    'action_type': 'adjust_limits',
                    'urgency': 'MEDIUM',
                    'description': 'Temporarily adjust transaction limits'
                }
            ])
            
        elif anomaly_type == AnomalyType.SENSOR_PATTERN:
            recommendations.extend([
                {
                    'action_type': 'calibrate_sensors',
                    'urgency': urgency,
                    'description': 'Recalibrate affected sensors'
                },
                {
                    'action_type': 'physical_inspection',
                    'urgency': 'MEDIUM',
                    'description': 'Schedule physical inspection of equipment'
                }
            ])
            
        # Generic recommendations
        recommendations.append({
            'action_type': 'create_incident',
            'urgency': 'MEDIUM',
            'description': f'Create incident ticket for {anomaly_type.value}'
        })
        
        return recommendations
        
    def _calculate_confidence(self, method_scores: Dict[str, float]) -> float:
        """Calculate confidence in anomaly detection"""
        # Confidence based on agreement between methods
        scores = list(method_scores.values())
        
        # High confidence if methods agree
        std_dev = np.std(scores)
        mean_score = np.mean(scores)
        
        # Lower confidence if high variance
        confidence = mean_score * (1 - std_dev)
        
        return max(0.0, min(1.0, confidence))
        
    def _learn_pattern(self, spike_pattern: np.ndarray, anomaly: Dict):
        """Learn from detected anomaly pattern"""
        pattern_id = anomaly['anomaly_id']
        
        # Create or update pattern
        if pattern_id not in self.known_patterns:
            self.known_patterns[pattern_id] = AnomalyPattern(
                pattern_id=pattern_id,
                pattern_type=AnomalyType(anomaly['anomaly_type']),
                spike_signature=spike_pattern,
                threshold=anomaly['severity'],
                occurrence_count=1,
                last_seen=datetime.utcnow(),
                metadata=anomaly['metadata']
            )
        else:
            # Update existing pattern
            pattern = self.known_patterns[pattern_id]
            pattern.occurrence_count += 1
            pattern.last_seen = datetime.utcnow()
            
            # Update signature with exponential moving average
            alpha = 0.1
            pattern.spike_signature = (1 - alpha) * pattern.spike_signature + alpha * spike_pattern
            
        # Limit pattern memory
        if len(self.known_patterns) > 1000:
            # Remove oldest patterns
            oldest_id = min(self.known_patterns.keys(),
                          key=lambda k: self.known_patterns[k].last_seen)
            del self.known_patterns[oldest_id]
            
    def _update_baseline(self, spike_pattern: np.ndarray):
        """Update baseline statistics with normal pattern"""
        # Exponential moving average update
        alpha = 0.01
        
        self.baseline_stats['mean'] = (1 - alpha) * self.baseline_stats['mean'] + alpha * np.mean(spike_pattern)
        self.baseline_stats['std'] = (1 - alpha) * self.baseline_stats['std'] + alpha * np.std(spike_pattern)
        self.baseline_stats['min'] = min(self.baseline_stats['min'], np.min(spike_pattern))
        self.baseline_stats['max'] = max(self.baseline_stats['max'], np.max(spike_pattern))
        
    def get_detection_stats(self) -> Dict:
        """Get current detection statistics"""
        return {
            'total_anomalies': self.total_anomalies,
            'known_patterns': len(self.known_patterns),
            'baseline_stats': self.baseline_stats,
            'recent_anomalies': len(self.anomaly_buffer),
            'detection_threshold': self.threshold
        }
        
    def adjust_sensitivity(self, factor: float):
        """Adjust detection sensitivity"""
        self.sensitivity *= factor
        self.threshold *= factor
        logger.info(f"Adjusted sensitivity to {self.sensitivity}, threshold to {self.threshold}") 