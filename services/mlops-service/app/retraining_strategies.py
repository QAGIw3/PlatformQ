"""
Retraining Strategies for ML Models

Different strategies to determine when a model should be retrained.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timedelta
import statistics


class RetrainingStrategy(ABC):
    """Abstract base class for retraining strategies"""
    
    @abstractmethod
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Determine if model should be retrained based on alerts"""
        pass


class DriftBasedStrategy(RetrainingStrategy):
    """Trigger retraining based on data drift"""
    
    def __init__(self, severity_threshold: str = "MEDIUM", min_features_affected: int = 3):
        self.severity_threshold = severity_threshold
        self.min_features_affected = min_features_affected
        self.severity_levels = ["LOW", "MEDIUM", "HIGH"]
        
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Check if drift is significant enough for retraining"""
        # Filter drift alerts
        drift_alerts = [a for a in alerts if a.get("alert_type") == "DATA_DRIFT"]
        
        if not drift_alerts:
            return False
            
        # Check severity
        max_severity = max(
            self.severity_levels.index(a.get("severity", "LOW")) 
            for a in drift_alerts
        )
        
        if max_severity >= self.severity_levels.index(self.severity_threshold):
            # Check number of affected features
            all_affected_features = set()
            for alert in drift_alerts:
                drift_results = alert.get("drift_results", {})
                all_affected_features.update(drift_results.keys())
                
            return len(all_affected_features) >= self.min_features_affected
            
        return False


class PerformanceBasedStrategy(RetrainingStrategy):
    """Trigger retraining based on performance degradation"""
    
    def __init__(self, accuracy_threshold: float = 0.8, degradation_duration: int = 3):
        self.accuracy_threshold = accuracy_threshold
        self.degradation_duration = degradation_duration  # Number of consecutive alerts
        
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Check if performance has degraded enough for retraining"""
        # Filter performance alerts
        perf_alerts = [
            a for a in alerts 
            if a.get("alert_type") == "PERFORMANCE_DEGRADATION"
        ]
        
        if len(perf_alerts) < self.degradation_duration:
            return False
            
        # Check if accuracy is consistently below threshold
        accuracies = [
            a.get("current_accuracy", 1.0) 
            for a in perf_alerts[-self.degradation_duration:]
        ]
        
        return all(acc < self.accuracy_threshold for acc in accuracies)


class ScheduledStrategy(RetrainingStrategy):
    """Trigger retraining on a schedule"""
    
    def __init__(self, retraining_interval_days: int = 30):
        self.retraining_interval = timedelta(days=retraining_interval_days)
        self.last_retrained = {}
        
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Check if it's time for scheduled retraining"""
        model_key = f"{current_alert['model_name']}_{current_alert['model_version']}"
        
        # For simplicity, we'd need to track last retrained time externally
        # This is a simplified version
        return False  # Would check against last retrained timestamp


class CombinedStrategy(RetrainingStrategy):
    """Combine multiple strategies"""
    
    def __init__(self):
        self.strategies = [
            DriftBasedStrategy(severity_threshold="HIGH", min_features_affected=2),
            PerformanceBasedStrategy(accuracy_threshold=0.75, degradation_duration=2)
        ]
        
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Retrain if any strategy triggers"""
        return any(
            strategy.should_retrain(alerts, current_alert) 
            for strategy in self.strategies
        )


class AdaptiveStrategy(RetrainingStrategy):
    """Adaptive strategy that learns from past retraining results"""
    
    def __init__(self):
        self.retraining_history = []
        self.success_threshold = 0.7
        
    def should_retrain(self, alerts: List[Dict[str, Any]], current_alert: Dict[str, Any]) -> bool:
        """Use historical data to decide on retraining"""
        # Calculate alert severity score
        severity_score = self._calculate_severity_score(alerts)
        
        # Compare with historical successful retrainings
        if self.retraining_history:
            similar_situations = [
                h for h in self.retraining_history
                if abs(h["severity_score"] - severity_score) < 0.1
            ]
            
            if similar_situations:
                success_rate = sum(
                    1 for s in similar_situations if s["improved"]
                ) / len(similar_situations)
                
                return success_rate >= self.success_threshold
                
        # Default to retraining if severity is high
        return severity_score > 0.7
        
    def _calculate_severity_score(self, alerts: List[Dict[str, Any]]) -> float:
        """Calculate overall severity score from alerts"""
        if not alerts:
            return 0.0
            
        severity_map = {"LOW": 0.3, "MEDIUM": 0.6, "HIGH": 1.0}
        scores = [
            severity_map.get(a.get("severity", "LOW"), 0.3)
            for a in alerts
        ]
        
        return statistics.mean(scores) 