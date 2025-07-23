"""
Enhanced Quality Engine extending common library
"""
import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import uuid
from dataclasses import dataclass

# Import from common library
from data_intelligence_common.core.processing.quality_processor import (
    QualityProcessor,
    QualityCheckType,
    QualityCheckResult,
    DataQualityProfile
)
from data_intelligence_common.models.data_models import (
    DataQualityMetric,
    DataQualityDimension,
    ValidationResult
)
from data_intelligence_common.core.events.event_bus import EventBus
from data_intelligence_common.core.caching.cache_manager import CacheManager
from data_intelligence_common.monitoring.metrics import MetricsCollector

# Import domain models
from ..domain.models.quality import (
    QualityRuleDefinition,
    QualityCheckRequest,
    EnhancedQualityProfile,
    QualityIncident,
    RemediationAction,
    QualityRuleType,
    RemediationStrategy
)


class EnhancedQualityEngine(QualityProcessor):
    """Enhanced quality engine with advanced features"""
    
    def __init__(
        self,
        config,
        quality_processor: QualityProcessor,
        cache_manager: CacheManager,
        event_bus: EventBus,
        ignite_client,
        elasticsearch_client,
        minio_client,
        seatunnel_client,
        great_expectations_client,
        soda_core_client,
        ml_service_client,
        metrics_collector: MetricsCollector
    ):
        # Initialize parent class
        super().__init__(
            cache_manager=cache_manager,
            event_bus=event_bus,
            metrics_collector=metrics_collector
        )
        
        self.config = config
        self.ignite_client = ignite_client
        self.elasticsearch_client = elasticsearch_client
        self.minio_client = minio_client
        self.seatunnel_client = seatunnel_client
        self.great_expectations_client = great_expectations_client
        self.soda_core_client = soda_core_client
        self.ml_service_client = ml_service_client
        
        # Rule registry
        self.rule_registry: Dict[str, QualityRuleDefinition] = {}
        
        # Active incidents
        self.active_incidents: Dict[str, QualityIncident] = {}
        
        # ML models cache
        self.ml_models_cache: Dict[str, Any] = {}
    
    async def initialize(self):
        """Initialize quality engine"""
        await super().initialize()
        
        # Load rules from storage
        await self._load_rules()
        
        # Initialize ML models
        if self.config.ml_quality_enabled:
            await self._initialize_ml_models()
        
        # Start background tasks
        asyncio.create_task(self._rule_evaluation_loop())
        asyncio.create_task(self._incident_monitoring_loop())
    
    async def check_quality(
        self,
        request: QualityCheckRequest
    ) -> EnhancedQualityProfile:
        """Run quality checks with enhanced features"""
        start_time = datetime.utcnow()
        
        # Get base quality profile
        base_result = await super().check_quality(
            entity_id=request.entity_id,
            entity_type=request.entity_type,
            check_types=request.check_types
        )
        
        # Enhance with governance features
        enhanced_profile = EnhancedQualityProfile(
            entity_id=request.entity_id,
            entity_type=request.entity_type,
            timestamp=datetime.utcnow(),
            overall_score=base_result.overall_score,
            dimension_scores=base_result.dimension_scores,
            field_profiles=base_result.field_profiles,
            validation_results=base_result.validation_results,
            recommendations=base_result.recommendations
        )
        
        # Add governance metadata
        await self._add_governance_metadata(enhanced_profile)
        
        # Run ML-based quality checks
        if self.config.ml_quality_enabled:
            ml_results = await self._run_ml_quality_checks(request, enhanced_profile)
            enhanced_profile.anomaly_scores = ml_results.get("anomaly_scores", {})
            enhanced_profile.predicted_quality_score = ml_results.get("predicted_score")
            enhanced_profile.quality_trend = ml_results.get("trend")
            enhanced_profile.risk_indicators = ml_results.get("risk_indicators", [])
        
        # Run custom rules
        if request.rule_ids:
            custom_results = await self._run_custom_rules(request, enhanced_profile)
            enhanced_profile.validation_results.extend(custom_results)
        
        # Check for incidents
        incidents = await self._check_for_incidents(enhanced_profile)
        if incidents:
            await self._handle_incidents(incidents, request)
        
        # Generate improvement suggestions
        enhanced_profile.improvement_suggestions = await self._generate_improvements(enhanced_profile)
        enhanced_profile.optimization_opportunities = await self._identify_optimizations(enhanced_profile)
        
        # Store results
        await self._store_quality_results(enhanced_profile)
        
        # Emit event
        await self.event_bus.publish(
            "quality.check.completed",
            {
                "entity_id": request.entity_id,
                "overall_score": enhanced_profile.overall_score,
                "duration_ms": (datetime.utcnow() - start_time).total_seconds() * 1000
            }
        )
        
        return enhanced_profile
    
    async def create_rule(
        self,
        rule: QualityRuleDefinition
    ) -> str:
        """Create a new quality rule"""
        rule_id = str(uuid.uuid4())
        rule.rule_id = rule_id
        rule.created_at = datetime.utcnow()
        
        # Validate rule
        await self._validate_rule(rule)
        
        # Store rule
        self.rule_registry[rule_id] = rule
        await self._save_rule(rule)
        
        # Schedule if needed
        if rule.schedule_cron:
            await self._schedule_rule(rule)
        
        return rule_id
    
    async def trigger_remediation(
        self,
        incident_id: str,
        strategy: Optional[RemediationStrategy] = None
    ) -> RemediationAction:
        """Trigger remediation for an incident"""
        incident = self.active_incidents.get(incident_id)
        if not incident:
            raise ValueError(f"Incident {incident_id} not found")
        
        # Determine strategy
        if not strategy:
            rule = self.rule_registry.get(incident.rule_id)
            strategy = rule.remediation_strategy if rule else RemediationStrategy.ALERT_ONLY
        
        # Create remediation action
        action = RemediationAction(
            action_id=str(uuid.uuid4()),
            incident_id=incident_id,
            action_type=strategy,
            description=f"Remediation for {incident.entity_id}",
            automated=strategy != RemediationStrategy.MANUAL_REVIEW
        )
        
        # Execute remediation
        if action.automated and self.config.auto_remediation_enabled:
            await self._execute_remediation(action, incident)
        else:
            action.status = "pending_approval"
        
        # Store action
        await self._save_remediation_action(action)
        
        return action
    
    async def _run_ml_quality_checks(
        self,
        request: QualityCheckRequest,
        profile: EnhancedQualityProfile
    ) -> Dict[str, Any]:
        """Run ML-based quality checks"""
        results = {}
        
        try:
            # Get historical data
            history = await self._get_quality_history(request.entity_id)
            
            # Predict quality score
            if history and len(history) > 10:
                prediction = await self.ml_service_client.predict(
                    model_id="quality_predictor",
                    data={
                        "history": history,
                        "current_scores": profile.dimension_scores
                    }
                )
                results["predicted_score"] = prediction.predictions[0]
                results["trend"] = self._determine_trend(history, prediction.predictions[0])
            
            # Anomaly detection
            anomaly_scores = {}
            for dim, score in profile.dimension_scores.items():
                anomaly_result = await self.ml_service_client.predict(
                    model_id=f"anomaly_detector_{dim}",
                    data={"score": score, "history": history}
                )
                anomaly_scores[dim] = anomaly_result.predictions[0]
            
            results["anomaly_scores"] = anomaly_scores
            
            # Risk indicators
            risk_indicators = []
            if any(score > self.config.anomaly_detection_threshold for score in anomaly_scores.values()):
                risk_indicators.append("High anomaly detected")
            
            if results.get("trend") == "degrading":
                risk_indicators.append("Quality degradation trend")
            
            results["risk_indicators"] = risk_indicators
            
        except Exception as e:
            self.logger.error(f"ML quality check failed: {e}")
        
        return results
    
    async def _run_custom_rules(
        self,
        request: QualityCheckRequest,
        profile: EnhancedQualityProfile
    ) -> List[ValidationResult]:
        """Run custom quality rules"""
        results = []
        
        for rule_id in request.rule_ids:
            rule = self.rule_registry.get(rule_id)
            if not rule:
                continue
            
            try:
                if rule.rule_type == QualityRuleType.CUSTOM_SQL:
                    result = await self._execute_sql_rule(rule, request.entity_id)
                elif rule.rule_type == QualityRuleType.CUSTOM_PYTHON:
                    result = await self._execute_python_rule(rule, profile)
                elif rule.rule_type == QualityRuleType.ML_BASED:
                    result = await self._execute_ml_rule(rule, profile)
                else:
                    result = await self._execute_standard_rule(rule, profile)
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Rule {rule_id} execution failed: {e}")
                results.append(ValidationResult(
                    rule_id=rule_id,
                    passed=False,
                    message=f"Rule execution failed: {str(e)}",
                    severity="error"
                ))
        
        return results
    
    async def _check_for_incidents(
        self,
        profile: EnhancedQualityProfile
    ) -> List[QualityIncident]:
        """Check for quality incidents"""
        incidents = []
        
        # Check dimension scores
        for dimension, score in profile.dimension_scores.items():
            if score < 0.7:  # Configurable threshold
                incident = QualityIncident(
                    incident_id=str(uuid.uuid4()),
                    entity_id=profile.entity_id,
                    rule_id=f"dimension_{dimension}",
                    severity="high" if score < 0.5 else "medium",
                    impact_score=1.0 - score,
                    affected_records=profile.record_count or 0,
                    detection_method="threshold"
                )
                incidents.append(incident)
        
        # Check validation results
        for result in profile.validation_results:
            if not result.passed and result.severity in ["error", "critical"]:
                incident = QualityIncident(
                    incident_id=str(uuid.uuid4()),
                    entity_id=profile.entity_id,
                    rule_id=result.rule_id,
                    severity=result.severity,
                    impact_score=0.8,
                    affected_records=result.failed_count or 0,
                    detection_method="rule_based"
                )
                incidents.append(incident)
        
        # Check ML anomalies
        if hasattr(profile, "anomaly_scores"):
            for dim, score in profile.anomaly_scores.items():
                if score > self.config.anomaly_detection_threshold:
                    incident = QualityIncident(
                        incident_id=str(uuid.uuid4()),
                        entity_id=profile.entity_id,
                        rule_id=f"anomaly_{dim}",
                        severity="high",
                        impact_score=score,
                        affected_records=profile.record_count or 0,
                        detection_method="ml_based"
                    )
                    incidents.append(incident)
        
        return incidents
    
    async def _handle_incidents(
        self,
        incidents: List[QualityIncident],
        request: QualityCheckRequest
    ):
        """Handle quality incidents"""
        for incident in incidents:
            # Store incident
            self.active_incidents[incident.incident_id] = incident
            await self._save_incident(incident)
            
            # Check if auto-remediation should trigger
            rule = self.rule_registry.get(incident.rule_id)
            if rule and rule.auto_fix_enabled and self.config.auto_remediation_enabled:
                await self.trigger_remediation(
                    incident.incident_id,
                    rule.remediation_strategy
                )
            
            # Send notifications
            await self._send_incident_notification(incident)
            
            # Emit event
            await self.event_bus.publish(
                "quality.incident.detected",
                {
                    "incident_id": incident.incident_id,
                    "entity_id": incident.entity_id,
                    "severity": incident.severity,
                    "triggered_by": request.triggered_by
                }
            )
    
    async def _add_governance_metadata(self, profile: EnhancedQualityProfile):
        """Add governance metadata to quality profile"""
        # Get catalog metadata
        try:
            from ..core.container import Container
            container = Container()
            catalog_client = await container.catalog_service_client()
            
            entity = await catalog_client.get_entity(profile.entity_id)
            if entity:
                profile.data_classification = entity.metadata.get("classification")
                profile.sensitivity_level = entity.metadata.get("sensitivity")
                profile.retention_period_days = entity.metadata.get("retention_days")
        except Exception as e:
            self.logger.warning(f"Failed to get governance metadata: {e}")
    
    async def _generate_improvements(
        self,
        profile: EnhancedQualityProfile
    ) -> List[Dict[str, Any]]:
        """Generate improvement suggestions"""
        suggestions = []
        
        # Check for low dimension scores
        for dimension, score in profile.dimension_scores.items():
            if score < 0.8:
                suggestions.append({
                    "type": "dimension_improvement",
                    "dimension": dimension,
                    "current_score": score,
                    "target_score": 0.9,
                    "priority": "high" if score < 0.6 else "medium",
                    "actions": self._get_dimension_improvements(dimension, profile)
                })
        
        # Check for validation failures
        failed_rules = [r for r in profile.validation_results if not r.passed]
        if failed_rules:
            suggestions.append({
                "type": "validation_fixes",
                "failed_rules": len(failed_rules),
                "priority": "high",
                "actions": [
                    f"Fix validation rule: {r.rule_id} - {r.message}"
                    for r in failed_rules[:5]  # Top 5
                ]
            })
        
        return suggestions
    
    async def _identify_optimizations(
        self,
        profile: EnhancedQualityProfile
    ) -> List[Dict[str, Any]]:
        """Identify optimization opportunities"""
        optimizations = []
        
        # Check for redundant rules
        rule_overlap = await self._analyze_rule_overlap(profile.entity_id)
        if rule_overlap:
            optimizations.append({
                "type": "rule_consolidation",
                "description": "Consolidate overlapping quality rules",
                "impact": "Reduce processing time by 20%",
                "rules": rule_overlap
            })
        
        # Check for expensive operations
        if profile.processing_time_ms > 5000:
            optimizations.append({
                "type": "performance",
                "description": "Enable caching for quality checks",
                "impact": "Reduce latency by 50%",
                "config": {
                    "cache_ttl": 3600,
                    "cache_key": f"quality:{profile.entity_id}"
                }
            })
        
        return optimizations
    
    def _get_dimension_improvements(
        self,
        dimension: str,
        profile: EnhancedQualityProfile
    ) -> List[str]:
        """Get improvement actions for a dimension"""
        improvements = {
            "completeness": [
                "Identify and fill missing values",
                "Set up data validation at source",
                "Implement default value strategies"
            ],
            "accuracy": [
                "Validate against reference data",
                "Implement data entry validation",
                "Set up automated accuracy checks"
            ],
            "consistency": [
                "Standardize data formats",
                "Implement referential integrity checks",
                "Create data normalization rules"
            ],
            "timeliness": [
                "Optimize data pipeline latency",
                "Set up real-time data feeds",
                "Monitor data freshness SLAs"
            ],
            "validity": [
                "Define comprehensive validation rules",
                "Implement schema enforcement",
                "Set up data type constraints"
            ],
            "uniqueness": [
                "Create unique constraints",
                "Implement deduplication logic",
                "Set up duplicate detection alerts"
            ]
        }
        
        return improvements.get(dimension, ["Review and improve data quality"])
    
    def _determine_trend(self, history: List[float], predicted: float) -> str:
        """Determine quality trend"""
        if len(history) < 3:
            return "stable"
        
        recent_avg = sum(history[-3:]) / 3
        older_avg = sum(history[-6:-3]) / 3 if len(history) >= 6 else recent_avg
        
        if predicted > recent_avg * 1.05:
            return "improving"
        elif predicted < recent_avg * 0.95:
            return "degrading"
        else:
            return "stable" 