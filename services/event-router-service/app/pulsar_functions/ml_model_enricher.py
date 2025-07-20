#!/usr/bin/env python3
"""
ML Model Event Enricher Pulsar Function

Enriches ML model events with:
- Model lineage information
- Resource utilization metrics
- Performance comparisons
- Cost analysis
- Compliance metadata
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from decimal import Decimal

# Pulsar function interface
from pulsar import Function

logger = logging.getLogger(__name__)


class MLModelEnricher(Function):
    """Enriches ML model events with additional context"""
    
    def __init__(self):
        super().__init__()
        self.model_registry = {}  # In production, connect to actual registry
        self.resource_pricing = {
            "gpu_v100": 2.5,  # $/hour
            "gpu_a100": 4.0,  # $/hour
            "gpu_t4": 0.5,    # $/hour
            "cpu": 0.1,       # $/hour
            "memory_gb": 0.01  # $/GB/hour
        }
        
    def process(self, input_data, context):
        """Process incoming ML event"""
        try:
            # Parse event
            event = json.loads(input_data) if isinstance(input_data, str) else input_data
            event_type = event.get("event_type", "")
            
            # Route to appropriate enrichment method
            if "training" in event_type:
                return self._enrich_training_event(event, context)
            elif "inference" in event_type:
                return self._enrich_inference_event(event, context)
            elif "drift" in event_type:
                return self._enrich_drift_event(event, context)
            elif "experiment" in event_type:
                return self._enrich_experiment_event(event, context)
            else:
                # Pass through without enrichment
                return json.dumps(event)
                
        except Exception as e:
            logger.error(f"Error enriching ML event: {e}")
            # Return original event on error
            return input_data if isinstance(input_data, str) else json.dumps(input_data)
            
    def _enrich_training_event(self, event: Dict[str, Any], context) -> str:
        """Enrich training events"""
        enriched = event.copy()
        
        # Add model lineage
        model_metadata = event.get("model_metadata", {})
        if model_metadata:
            enriched["lineage"] = {
                "parent_model_id": self._get_parent_model(model_metadata.get("model_id")),
                "dataset_version": self._get_dataset_version(model_metadata.get("dataset_id")),
                "code_version": self._get_code_version(model_metadata),
                "environment": self._get_training_environment(event)
            }
            
        # Calculate resource costs
        resource_usage = event.get("resource_usage", {})
        if resource_usage:
            enriched["cost_analysis"] = self._calculate_training_cost(resource_usage)
            
        # Add performance comparison
        metrics = model_metadata.get("metrics", {})
        if metrics:
            enriched["performance_comparison"] = self._compare_with_baseline(
                model_metadata.get("model_name"),
                metrics
            )
            
        # Add compliance metadata
        enriched["compliance"] = {
            "data_privacy": self._check_data_privacy(model_metadata.get("dataset_id")),
            "bias_assessment": self._assess_model_bias(metrics),
            "explainability_score": self._calculate_explainability(model_metadata)
        }
        
        # Add timestamp
        enriched["enriched_at"] = datetime.utcnow().isoformat()
        
        return json.dumps(enriched)
        
    def _enrich_inference_event(self, event: Dict[str, Any], context) -> str:
        """Enrich inference events"""
        enriched = event.copy()
        
        model_id = event.get("model_id")
        model_version = event.get("model_version")
        
        # Add model metadata
        enriched["model_details"] = self._get_model_details(model_id, model_version)
        
        # Add performance metrics
        latency_ms = event.get("latency_ms", 0)
        enriched["performance_analysis"] = {
            "latency_percentile": self._calculate_latency_percentile(model_id, latency_ms),
            "throughput_estimate": 1000 / latency_ms if latency_ms > 0 else 0,
            "sla_compliance": latency_ms < 100  # Example SLA
        }
        
        # Add cost per inference
        enriched["inference_cost"] = self._calculate_inference_cost(model_id)
        
        # Add monitoring alerts
        if latency_ms > 200:  # High latency threshold
            enriched["alerts"] = [{
                "type": "high_latency",
                "severity": "warning",
                "message": f"Inference latency {latency_ms}ms exceeds threshold"
            }]
            
        return json.dumps(enriched)
        
    def _enrich_drift_event(self, event: Dict[str, Any], context) -> str:
        """Enrich drift detection events"""
        enriched = event.copy()
        
        drift_score = event.get("drift_score", 0)
        drift_type = event.get("drift_type", "")
        
        # Add drift analysis
        enriched["drift_analysis"] = {
            "severity": self._calculate_drift_severity(drift_score),
            "estimated_impact": self._estimate_drift_impact(drift_score, drift_type),
            "recommended_actions": self._get_drift_recommendations(drift_score, drift_type)
        }
        
        # Add historical context
        model_id = event.get("model_id")
        enriched["drift_history"] = {
            "previous_drifts": self._get_drift_history(model_id),
            "drift_trend": self._calculate_drift_trend(model_id, drift_score),
            "time_since_last_retrain": self._get_time_since_retrain(model_id)
        }
        
        # Add retraining estimate
        if drift_score > 0.5:
            enriched["retraining_estimate"] = {
                "estimated_duration": "2 hours",
                "estimated_cost": 150.0,
                "priority": "high" if drift_score > 0.7 else "medium"
            }
            
        return json.dumps(enriched)
        
    def _enrich_experiment_event(self, event: Dict[str, Any], context) -> str:
        """Enrich experiment events"""
        enriched = event.copy()
        
        # Add experiment context
        experiment_id = event.get("experiment_id")
        enriched["experiment_context"] = {
            "experiment_type": self._classify_experiment(event.get("parameters", {})),
            "hypothesis": self._extract_hypothesis(experiment_id),
            "related_experiments": self._find_related_experiments(experiment_id)
        }
        
        # Add resource allocation
        enriched["resource_allocation"] = {
            "compute_budget": self._get_experiment_budget(experiment_id),
            "time_budget": self._get_time_budget(experiment_id),
            "priority_score": self._calculate_priority(event)
        }
        
        # Add success probability
        if event.get("status") == "running":
            enriched["success_prediction"] = {
                "probability": self._predict_experiment_success(event),
                "estimated_completion": self._estimate_completion_time(event)
            }
            
        return json.dumps(enriched)
        
    # Helper methods
    def _get_parent_model(self, model_id: str) -> Optional[str]:
        """Get parent model ID for lineage tracking"""
        # In production, query model registry
        return f"parent-{model_id}" if model_id else None
        
    def _get_dataset_version(self, dataset_id: str) -> str:
        """Get dataset version information"""
        # In production, query data catalog
        return f"v1.0-{dataset_id}" if dataset_id else "unknown"
        
    def _get_code_version(self, model_metadata: Dict) -> str:
        """Get code version from model metadata"""
        return model_metadata.get("code_version", "unknown")
        
    def _get_training_environment(self, event: Dict) -> Dict:
        """Extract training environment details"""
        return {
            "framework": event.get("model_metadata", {}).get("framework", "unknown"),
            "python_version": "3.8",  # Would be extracted from actual environment
            "cuda_version": "11.0"    # Would be extracted from actual environment
        }
        
    def _calculate_training_cost(self, resource_usage: Dict) -> Dict:
        """Calculate training cost based on resource usage"""
        total_cost = 0.0
        breakdown = {}
        
        # GPU costs
        for gpu_type in ["gpu_v100", "gpu_a100", "gpu_t4"]:
            hours = resource_usage.get(f"{gpu_type}_hours", 0)
            if hours > 0:
                cost = hours * self.resource_pricing[gpu_type]
                breakdown[gpu_type] = cost
                total_cost += cost
                
        # CPU costs
        cpu_hours = resource_usage.get("cpu_hours", 0)
        if cpu_hours > 0:
            cpu_cost = cpu_hours * self.resource_pricing["cpu"]
            breakdown["cpu"] = cpu_cost
            total_cost += cpu_cost
            
        # Memory costs
        memory_gb_hours = resource_usage.get("memory_gb_hours", 0)
        if memory_gb_hours > 0:
            memory_cost = memory_gb_hours * self.resource_pricing["memory_gb"]
            breakdown["memory"] = memory_cost
            total_cost += memory_cost
            
        return {
            "total_cost": round(total_cost, 2),
            "breakdown": breakdown,
            "currency": "USD"
        }
        
    def _compare_with_baseline(self, model_name: str, metrics: Dict) -> Dict:
        """Compare model performance with baseline"""
        # In production, fetch baseline from model registry
        baseline_accuracy = 0.85
        current_accuracy = metrics.get("accuracy", 0)
        
        return {
            "baseline_accuracy": baseline_accuracy,
            "current_accuracy": current_accuracy,
            "improvement": round((current_accuracy - baseline_accuracy) * 100, 2),
            "beats_baseline": current_accuracy > baseline_accuracy
        }
        
    def _check_data_privacy(self, dataset_id: str) -> Dict:
        """Check data privacy compliance"""
        # In production, query data governance service
        return {
            "contains_pii": False,
            "encryption_status": "encrypted",
            "compliance_frameworks": ["GDPR", "CCPA"]
        }
        
    def _assess_model_bias(self, metrics: Dict) -> Dict:
        """Assess model bias based on metrics"""
        # Simplified bias assessment
        return {
            "bias_detected": False,
            "fairness_score": 0.95,
            "protected_attributes_tested": ["gender", "race", "age"]
        }
        
    def _calculate_explainability(self, model_metadata: Dict) -> float:
        """Calculate model explainability score"""
        # Simplified scoring based on model type
        algorithm = model_metadata.get("algorithm", "")
        if algorithm in ["linear_regression", "decision_tree"]:
            return 0.9
        elif algorithm in ["random_forest", "xgboost"]:
            return 0.7
        elif algorithm in ["neural_network", "deep_learning"]:
            return 0.4
        return 0.5
        
    def _get_model_details(self, model_id: str, version: str) -> Dict:
        """Get detailed model information"""
        # In production, query model registry
        return {
            "framework": "tensorflow",
            "size_mb": 150,
            "input_shape": [224, 224, 3],
            "output_classes": 10
        }
        
    def _calculate_latency_percentile(self, model_id: str, latency_ms: int) -> Dict:
        """Calculate latency percentile for the model"""
        # In production, query metrics store
        return {
            "p50": 45,
            "p90": 85,
            "p99": 150,
            "current_percentile": 75 if latency_ms < 85 else 95
        }
        
    def _calculate_inference_cost(self, model_id: str) -> float:
        """Calculate cost per inference"""
        # Simplified calculation
        return 0.0001  # $0.0001 per inference
        
    def _calculate_drift_severity(self, drift_score: float) -> str:
        """Determine drift severity level"""
        if drift_score > 0.8:
            return "critical"
        elif drift_score > 0.6:
            return "high"
        elif drift_score > 0.4:
            return "medium"
        return "low"
        
    def _estimate_drift_impact(self, drift_score: float, drift_type: str) -> Dict:
        """Estimate impact of detected drift"""
        impact_factor = drift_score * 0.2  # Simplified calculation
        return {
            "accuracy_degradation": round(impact_factor * 100, 2),
            "affected_predictions_percent": round(drift_score * 30, 2),
            "business_impact": "high" if drift_score > 0.7 else "medium"
        }
        
    def _get_drift_recommendations(self, drift_score: float, drift_type: str) -> list:
        """Get recommendations based on drift detection"""
        recommendations = []
        
        if drift_score > 0.7:
            recommendations.append("Immediate model retraining recommended")
            recommendations.append("Consider rolling back to previous model version")
        elif drift_score > 0.5:
            recommendations.append("Schedule model retraining within 24 hours")
            recommendations.append("Increase monitoring frequency")
        else:
            recommendations.append("Continue monitoring")
            recommendations.append("Review feature distributions")
            
        if drift_type == "data_drift":
            recommendations.append("Investigate data source changes")
        elif drift_type == "concept_drift":
            recommendations.append("Review business logic changes")
            
        return recommendations
        
    def _get_drift_history(self, model_id: str) -> list:
        """Get historical drift events for model"""
        # In production, query from storage
        return [
            {"date": "2024-01-15", "score": 0.3, "type": "data_drift"},
            {"date": "2024-01-20", "score": 0.5, "type": "concept_drift"}
        ]
        
    def _calculate_drift_trend(self, model_id: str, current_score: float) -> str:
        """Calculate drift trend (increasing/decreasing/stable)"""
        # Simplified trend calculation
        return "increasing" if current_score > 0.5 else "stable"
        
    def _get_time_since_retrain(self, model_id: str) -> str:
        """Get time since last model retrain"""
        # In production, query model registry
        return "5 days"
        
    def _classify_experiment(self, parameters: Dict) -> str:
        """Classify experiment type based on parameters"""
        if "hyperparameter" in str(parameters):
            return "hyperparameter_optimization"
        elif "architecture" in str(parameters):
            return "architecture_search"
        elif "feature" in str(parameters):
            return "feature_engineering"
        return "general_experiment"
        
    def _extract_hypothesis(self, experiment_id: str) -> str:
        """Extract experiment hypothesis"""
        # In production, query experiment tracking system
        return "Increasing learning rate will improve convergence speed"
        
    def _find_related_experiments(self, experiment_id: str) -> list:
        """Find related experiments"""
        # In production, query experiment tracking system
        return ["exp-123", "exp-456"]
        
    def _get_experiment_budget(self, experiment_id: str) -> Dict:
        """Get compute budget for experiment"""
        return {
            "gpu_hours": 10,
            "max_cost": 50.0,
            "allocated_resources": "2x V100 GPU"
        }
        
    def _get_time_budget(self, experiment_id: str) -> str:
        """Get time budget for experiment"""
        return "4 hours"
        
    def _calculate_priority(self, event: Dict) -> float:
        """Calculate experiment priority score"""
        # Simplified priority calculation
        return 0.8
        
    def _predict_experiment_success(self, event: Dict) -> float:
        """Predict probability of experiment success"""
        # Simplified prediction
        return 0.75
        
    def _estimate_completion_time(self, event: Dict) -> str:
        """Estimate experiment completion time"""
        # Simplified estimation
        return "2024-01-30T15:00:00Z" 