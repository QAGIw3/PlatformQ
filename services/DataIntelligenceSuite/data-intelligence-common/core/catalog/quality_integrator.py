"""
Data quality integration for catalog.

Provides comprehensive data quality management and monitoring.
"""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import asyncio
import statistics

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"
    CONFORMITY = "conformity"


class QualityStatus(str, Enum):
    """Quality check status"""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


class RuleType(str, Enum):
    """Quality rule types"""
    NULL_CHECK = "null_check"
    RANGE_CHECK = "range_check"
    PATTERN_CHECK = "pattern_check"
    UNIQUENESS_CHECK = "uniqueness_check"
    REFERENTIAL_CHECK = "referential_check"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_FUNCTION = "custom_function"
    STATISTICAL = "statistical"
    SCHEMA_VALIDATION = "schema_validation"


@dataclass
class QualityRule:
    """Data quality rule"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    dimension: QualityDimension = QualityDimension.VALIDITY
    rule_type: RuleType = RuleType.CUSTOM_SQL
    config: Dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # error, warning, info
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "dimension": self.dimension.value,
            "rule_type": self.rule_type.value,
            "config": self.config,
            "severity": self.severity,
            "enabled": self.enabled,
            "tags": self.tags
        }


@dataclass
class QualityMetric:
    """Quality metric result"""
    dimension: QualityDimension
    value: float
    threshold: Optional[float] = None
    passed: bool = True
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "dimension": self.dimension.value,
            "value": self.value,
            "threshold": self.threshold,
            "passed": self.passed,
            "details": self.details
        }


@dataclass
class QualityScore:
    """Overall quality score"""
    entity_id: str
    overall_score: float
    dimension_scores: Dict[QualityDimension, float] = field(default_factory=dict)
    passed: bool = True
    issues_count: int = 0
    critical_issues: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "entity_id": self.entity_id,
            "overall_score": self.overall_score,
            "dimension_scores": {k.value: v for k, v in self.dimension_scores.items()},
            "passed": self.passed,
            "issues_count": self.issues_count,
            "critical_issues": self.critical_issues,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class QualityIssue:
    """Quality issue found"""
    rule_id: str
    rule_name: str
    severity: str
    dimension: QualityDimension
    description: str
    affected_records: Optional[int] = None
    sample_values: Optional[List[Any]] = None
    location: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "dimension": self.dimension.value,
            "description": self.description,
            "affected_records": self.affected_records,
            "sample_values": self.sample_values,
            "location": self.location
        }


@dataclass
class QualityProfile:
    """Quality profile for entity type"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    entity_type: str = ""
    description: Optional[str] = None
    rules: List[str] = field(default_factory=list)  # Rule IDs
    thresholds: Dict[QualityDimension, float] = field(default_factory=dict)
    schedule: Optional[str] = None  # Cron expression
    notification_config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    
    def get_default_thresholds(self) -> Dict[QualityDimension, float]:
        """Get default thresholds"""
        defaults = {
            QualityDimension.COMPLETENESS: 0.95,
            QualityDimension.ACCURACY: 0.98,
            QualityDimension.CONSISTENCY: 0.95,
            QualityDimension.VALIDITY: 0.99,
            QualityDimension.UNIQUENESS: 1.0,
            QualityDimension.TIMELINESS: 0.90,
            QualityDimension.INTEGRITY: 0.99,
            QualityDimension.CONFORMITY: 0.95
        }
        return {**defaults, **self.thresholds}


@dataclass
class QualityCheckResult:
    """Result of quality check execution"""
    check_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    entity_id: str = ""
    profile_id: Optional[str] = None
    status: QualityStatus = QualityStatus.PENDING
    score: Optional[QualityScore] = None
    metrics: List[QualityMetric] = field(default_factory=list)
    issues: List[QualityIssue] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "check_id": self.check_id,
            "entity_id": self.entity_id,
            "profile_id": self.profile_id,
            "status": self.status.value,
            "score": self.score.to_dict() if self.score else None,
            "metrics": [m.to_dict() for m in self.metrics],
            "issues": [i.to_dict() for i in self.issues],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration": self.duration,
            "metadata": self.metadata
        }


class BaseQualityChecker(ABC):
    """Base class for quality checkers"""
    
    @abstractmethod
    async def check(
        self,
        entity_id: str,
        rule: QualityRule,
        context: Dict[str, Any]
    ) -> tuple[bool, Optional[QualityIssue], Dict[str, Any]]:
        """Execute quality check"""
        pass


class NullChecker(BaseQualityChecker):
    """Check for null values"""
    
    async def check(
        self,
        entity_id: str,
        rule: QualityRule,
        context: Dict[str, Any]
    ) -> tuple[bool, Optional[QualityIssue], Dict[str, Any]]:
        """Check for null values"""
        config = rule.config
        column = config.get("column")
        threshold = config.get("threshold", 0.0)
        
        # Get data accessor
        data_accessor = context.get("data_accessor")
        if not data_accessor:
            return False, None, {"error": "No data accessor provided"}
            
        # Count nulls
        total_count = await data_accessor.count(entity_id)
        null_count = await data_accessor.count_nulls(entity_id, column)
        
        if total_count == 0:
            return True, None, {"message": "No data to check"}
            
        null_ratio = null_count / total_count
        passed = null_ratio <= threshold
        
        issue = None
        if not passed:
            issue = QualityIssue(
                rule_id=rule.id,
                rule_name=rule.name,
                severity=rule.severity,
                dimension=rule.dimension,
                description=f"Column '{column}' has {null_ratio:.2%} null values, exceeding threshold of {threshold:.2%}",
                affected_records=null_count,
                location=f"column:{column}"
            )
            
        return passed, issue, {
            "null_count": null_count,
            "total_count": total_count,
            "null_ratio": null_ratio
        }


class RangeChecker(BaseQualityChecker):
    """Check value ranges"""
    
    async def check(
        self,
        entity_id: str,
        rule: QualityRule,
        context: Dict[str, Any]
    ) -> tuple[bool, Optional[QualityIssue], Dict[str, Any]]:
        """Check value ranges"""
        config = rule.config
        column = config.get("column")
        min_value = config.get("min_value")
        max_value = config.get("max_value")
        
        # Get data accessor
        data_accessor = context.get("data_accessor")
        if not data_accessor:
            return False, None, {"error": "No data accessor provided"}
            
        # Get out of range count
        out_of_range = await data_accessor.count_out_of_range(
            entity_id, column, min_value, max_value
        )
        
        passed = out_of_range == 0
        
        issue = None
        if not passed:
            # Get sample values
            samples = await data_accessor.get_out_of_range_samples(
                entity_id, column, min_value, max_value, limit=5
            )
            
            issue = QualityIssue(
                rule_id=rule.id,
                rule_name=rule.name,
                severity=rule.severity,
                dimension=rule.dimension,
                description=f"Column '{column}' has {out_of_range} values outside range [{min_value}, {max_value}]",
                affected_records=out_of_range,
                sample_values=samples,
                location=f"column:{column}"
            )
            
        return passed, issue, {
            "out_of_range_count": out_of_range,
            "min_value": min_value,
            "max_value": max_value
        }


class QualityIntegrator:
    """
    Integrates data quality management with catalog.
    
    Features:
    - Quality rule management
    - Profile-based quality checks
    - Multi-dimensional scoring
    - Issue tracking
    - Trend analysis
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._rules: Dict[str, QualityRule] = {}
        self._profiles: Dict[str, QualityProfile] = {}
        self._results: Dict[str, QualityCheckResult] = {}
        self._checkers: Dict[RuleType, BaseQualityChecker] = {
            RuleType.NULL_CHECK: NullChecker(),
            RuleType.RANGE_CHECK: RangeChecker(),
            # Add more checkers as needed
        }
        
        # Initialize default rules
        self._initialize_default_rules()
        
    def _initialize_default_rules(self):
        """Initialize default quality rules"""
        # Completeness rule
        self.register_rule(QualityRule(
            name="null_check_critical",
            description="Check for null values in critical columns",
            dimension=QualityDimension.COMPLETENESS,
            rule_type=RuleType.NULL_CHECK,
            config={"threshold": 0.0},
            severity="error"
        ))
        
        # Validity rule
        self.register_rule(QualityRule(
            name="range_check_numeric",
            description="Check numeric values are within expected range",
            dimension=QualityDimension.VALIDITY,
            rule_type=RuleType.RANGE_CHECK,
            config={},
            severity="warning"
        ))
        
    def register_rule(self, rule: QualityRule) -> str:
        """Register quality rule"""
        self._rules[rule.id] = rule
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="quality.rule.registered",
                source="quality_integrator",
                data={"rule_id": rule.id, "rule_name": rule.name}
            ))
            
        logger.info(f"Registered quality rule: {rule.name}")
        return rule.id
        
    def register_profile(self, profile: QualityProfile) -> str:
        """Register quality profile"""
        self._profiles[profile.id] = profile
        
        # Validate rules exist
        for rule_id in profile.rules:
            if rule_id not in self._rules:
                logger.warning(f"Rule {rule_id} not found for profile {profile.name}")
                
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="quality.profile.registered",
                source="quality_integrator",
                data={"profile_id": profile.id, "profile_name": profile.name}
            ))
            
        logger.info(f"Registered quality profile: {profile.name}")
        return profile.id
        
    def register_checker(self, rule_type: RuleType, checker: BaseQualityChecker):
        """Register custom quality checker"""
        self._checkers[rule_type] = checker
        logger.info(f"Registered quality checker for: {rule_type.value}")
        
    async def check_quality(
        self,
        entity_id: str,
        profile_id: Optional[str] = None,
        rules: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> QualityCheckResult:
        """Run quality check on entity"""
        check_id = str(uuid.uuid4())
        context = context or {}
        
        result = QualityCheckResult(
            check_id=check_id,
            entity_id=entity_id,
            profile_id=profile_id,
            status=QualityStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        # Store result
        self._results[check_id] = result
        
        # Publish start event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="quality.check.started",
                source="quality_integrator",
                data={"check_id": check_id, "entity_id": entity_id}
            ))
            
        try:
            # Get rules to execute
            rules_to_execute = []
            
            if profile_id and profile_id in self._profiles:
                profile = self._profiles[profile_id]
                rules_to_execute = [
                    self._rules[rid] for rid in profile.rules 
                    if rid in self._rules and self._rules[rid].enabled
                ]
            elif rules:
                rules_to_execute = [
                    self._rules[rid] for rid in rules 
                    if rid in self._rules and self._rules[rid].enabled
                ]
            else:
                # Use all enabled rules
                rules_to_execute = [r for r in self._rules.values() if r.enabled]
                
            # Execute rules
            dimension_results: Dict[QualityDimension, List[float]] = {}
            
            for rule in rules_to_execute:
                try:
                    # Get checker
                    checker = self._checkers.get(rule.rule_type)
                    if not checker:
                        logger.warning(f"No checker for rule type: {rule.rule_type}")
                        continue
                        
                    # Execute check
                    passed, issue, metrics = await checker.check(entity_id, rule, context)
                    
                    # Create metric
                    metric = QualityMetric(
                        dimension=rule.dimension,
                        value=1.0 if passed else 0.0,
                        passed=passed,
                        details=metrics
                    )
                    result.metrics.append(metric)
                    
                    # Track dimension results
                    if rule.dimension not in dimension_results:
                        dimension_results[rule.dimension] = []
                    dimension_results[rule.dimension].append(metric.value)
                    
                    # Add issue if failed
                    if issue:
                        result.issues.append(issue)
                        result.issues_count += 1
                        if issue.severity == "error":
                            result.critical_issues += 1
                            
                except Exception as e:
                    logger.error(f"Rule {rule.name} failed: {e}")
                    result.issues.append(QualityIssue(
                        rule_id=rule.id,
                        rule_name=rule.name,
                        severity="error",
                        dimension=rule.dimension,
                        description=f"Rule execution failed: {str(e)}"
                    ))
                    
            # Calculate scores
            dimension_scores = {}
            for dimension, values in dimension_results.items():
                if values:
                    dimension_scores[dimension] = statistics.mean(values)
                    
            # Get thresholds
            thresholds = {}
            if profile_id and profile_id in self._profiles:
                thresholds = self._profiles[profile_id].get_default_thresholds()
                
            # Calculate overall score
            if dimension_scores:
                overall_score = statistics.mean(dimension_scores.values())
            else:
                overall_score = 0.0
                
            # Check if passed
            passed = True
            for dimension, score in dimension_scores.items():
                threshold = thresholds.get(dimension, 0.8)
                if score < threshold:
                    passed = False
                    break
                    
            # Create score
            result.score = QualityScore(
                entity_id=entity_id,
                overall_score=overall_score,
                dimension_scores=dimension_scores,
                passed=passed,
                issues_count=result.issues_count,
                critical_issues=result.critical_issues
            )
            
            result.status = QualityStatus.PASSED if passed else QualityStatus.FAILED
            
        except Exception as e:
            result.status = QualityStatus.ERROR
            logger.error(f"Quality check failed: {e}")
            
        finally:
            result.completed_at = datetime.utcnow()
            result.duration = (result.completed_at - result.started_at).total_seconds()
            
            # Cache result
            if self.cache:
                cache_key = f"quality:check:{check_id}"
                self.cache.set(cache_key, result.to_dict(), ttl=3600)
                
            # Publish complete event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="quality.check.completed",
                    source="quality_integrator",
                    data={
                        "check_id": check_id,
                        "entity_id": entity_id,
                        "status": result.status.value,
                        "score": result.score.overall_score if result.score else None
                    }
                ))
                
        return result
        
    async def get_quality_history(
        self,
        entity_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[QualityCheckResult]:
        """Get quality check history for entity"""
        results = []
        
        for result in self._results.values():
            if result.entity_id != entity_id:
                continue
                
            if start_date and result.started_at and result.started_at < start_date:
                continue
                
            if end_date and result.started_at and result.started_at > end_date:
                continue
                
            results.append(result)
            
        # Sort by date descending
        results.sort(key=lambda r: r.started_at or datetime.min, reverse=True)
        
        return results[:limit]
        
    async def get_quality_trends(
        self,
        entity_id: str,
        dimension: Optional[QualityDimension] = None,
        period: int = 30  # days
    ) -> Dict[str, Any]:
        """Get quality trends for entity"""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period)
        
        history = await self.get_quality_history(entity_id, start_date, end_date)
        
        if not history:
            return {
                "entity_id": entity_id,
                "period_days": period,
                "data_points": 0,
                "trends": {}
            }
            
        # Calculate trends
        if dimension:
            # Single dimension trend
            values = []
            dates = []
            
            for result in history:
                if result.score and dimension in result.score.dimension_scores:
                    values.append(result.score.dimension_scores[dimension])
                    dates.append(result.started_at)
                    
            trend = self._calculate_trend(values)
            
            return {
                "entity_id": entity_id,
                "dimension": dimension.value,
                "period_days": period,
                "data_points": len(values),
                "current_score": values[0] if values else None,
                "average_score": statistics.mean(values) if values else None,
                "trend": trend,
                "values": list(zip(dates, values))
            }
        else:
            # Overall trend
            values = []
            dates = []
            
            for result in history:
                if result.score:
                    values.append(result.score.overall_score)
                    dates.append(result.started_at)
                    
            trend = self._calculate_trend(values)
            
            return {
                "entity_id": entity_id,
                "period_days": period,
                "data_points": len(values),
                "current_score": values[0] if values else None,
                "average_score": statistics.mean(values) if values else None,
                "trend": trend,
                "values": list(zip(dates, values))
            }
            
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend from values"""
        if len(values) < 2:
            return "stable"
            
        # Simple trend calculation
        recent = statistics.mean(values[:len(values)//2])
        older = statistics.mean(values[len(values)//2:])
        
        if recent > older * 1.05:
            return "improving"
        elif recent < older * 0.95:
            return "declining"
        else:
            return "stable"
            
    def get_rule(self, rule_id: str) -> Optional[QualityRule]:
        """Get rule by ID"""
        return self._rules.get(rule_id)
        
    def list_rules(
        self,
        dimension: Optional[QualityDimension] = None,
        rule_type: Optional[RuleType] = None,
        enabled: Optional[bool] = None
    ) -> List[QualityRule]:
        """List quality rules"""
        rules = list(self._rules.values())
        
        if dimension:
            rules = [r for r in rules if r.dimension == dimension]
        if rule_type:
            rules = [r for r in rules if r.rule_type == rule_type]
        if enabled is not None:
            rules = [r for r in rules if r.enabled == enabled]
            
        return rules
        
    def get_profile(self, profile_id: str) -> Optional[QualityProfile]:
        """Get profile by ID"""
        return self._profiles.get(profile_id)
        
    def list_profiles(
        self,
        entity_type: Optional[str] = None,
        enabled: Optional[bool] = None
    ) -> List[QualityProfile]:
        """List quality profiles"""
        profiles = list(self._profiles.values())
        
        if entity_type:
            profiles = [p for p in profiles if p.entity_type == entity_type]
        if enabled is not None:
            profiles = [p for p in profiles if p.enabled == enabled]
            
        return profiles
        
    def get_check_result(self, check_id: str) -> Optional[QualityCheckResult]:
        """Get check result by ID"""
        # Check cache first
        if self.cache:
            cache_key = f"quality:check:{check_id}"
            cached = self.cache.get(cache_key)
            if cached:
                # Reconstruct from dict
                return self._dict_to_result(cached)
                
        return self._results.get(check_id)
        
    def _dict_to_result(self, data: Dict[str, Any]) -> QualityCheckResult:
        """Convert dictionary to QualityCheckResult"""
        # Implementation would reconstruct the full object
        # For now, return None
        return None 