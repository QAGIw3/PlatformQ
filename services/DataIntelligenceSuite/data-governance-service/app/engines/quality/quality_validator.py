"""
Data Quality Validator for comprehensive quality checks.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from collections import defaultdict

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class QualityDimension(str, Enum):
    """Quality dimensions for assessment."""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    INTEGRITY = "integrity"


class RuleType(str, Enum):
    """Types of quality rules."""
    SQL = "sql"
    PYTHON = "python"
    REGEX = "regex"
    STATISTICAL = "statistical"
    BUSINESS = "business"
    SCHEMA = "schema"
    REFERENTIAL = "referential"


class SeverityLevel(str, Enum):
    """Severity levels for quality issues."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class QualityRule:
    """Represents a quality validation rule."""
    rule_id: str
    name: str
    description: str
    dimension: QualityDimension
    rule_type: RuleType
    expression: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: SeverityLevel = SeverityLevel.MEDIUM
    threshold: float = 0.95  # 95% pass rate by default
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationIssue:
    """Represents a quality validation issue."""
    rule_id: str
    dimension: QualityDimension
    severity: SeverityLevel
    description: str
    affected_records: int
    sample_data: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of quality validation."""
    dataset_id: str
    validation_id: str
    timestamp: datetime
    total_records: int
    passed_records: int
    failed_records: int
    quality_score: float
    dimensions: Dict[QualityDimension, float]
    issues: List[ValidationIssue]
    execution_time_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class QualityValidator:
    """
    Main quality validation engine for data quality checks.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[IgniteClient] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        
        # Rule storage
        self.rules: Dict[str, QualityRule] = {}
        self.rule_sets: Dict[str, List[str]] = {}  # rule set name -> rule IDs
        
        # Validation history
        self.validation_history: List[ValidationResult] = []
        
        # Custom validators
        self.custom_validators: Dict[str, Callable] = {}
        
        # Statistics
        self.validation_stats = defaultdict(int)
        
        logger.info("Quality Validator initialized")
        
    async def initialize(self):
        """Initialize quality validator."""
        # Load default rules
        await self._load_default_rules()
        
        # Subscribe to events
        await self.event_bus.subscribe("quality.validate", self._handle_validation_request)
        await self.event_bus.subscribe("quality.rule.update", self._handle_rule_update)
        
        logger.info("Quality Validator initialized with default rules")
        
    async def register_rule(self, rule: QualityRule):
        """Register a quality rule."""
        self.rules[rule.rule_id] = rule
        
        # Cache rule
        await self.cache_manager.set(f"quality:rule:{rule.rule_id}", rule.__dict__)
        
        # Publish event
        await self.event_bus.publish("quality.rule.registered", {
            "rule_id": rule.rule_id,
            "name": rule.name,
            "dimension": rule.dimension.value
        })
        
        logger.info(f"Registered quality rule: {rule.name}")
        
    async def create_rule_set(self, name: str, rule_ids: List[str]):
        """Create a rule set."""
        # Validate rule IDs
        invalid_rules = [rid for rid in rule_ids if rid not in self.rules]
        if invalid_rules:
            raise ValueError(f"Invalid rule IDs: {invalid_rules}")
        
        self.rule_sets[name] = rule_ids
        
        # Cache rule set
        await self.cache_manager.set(f"quality:ruleset:{name}", rule_ids)
        
        logger.info(f"Created rule set '{name}' with {len(rule_ids)} rules")
        
    async def validate_data(
        self,
        data: Union[pd.DataFrame, Dict[str, Any]],
        dataset_id: str,
        rule_set: Optional[str] = None,
        rules: Optional[List[str]] = None,
        sample_size: int = 100
    ) -> ValidationResult:
        """
        Validate data quality using specified rules.
        
        Args:
            data: Data to validate (DataFrame or dict)
            dataset_id: Dataset identifier
            rule_set: Name of rule set to use
            rules: Specific rule IDs to apply
            sample_size: Number of failed records to sample
            
        Returns:
            ValidationResult with quality scores and issues
        """
        start_time = datetime.utcnow()
        validation_id = f"val_{dataset_id}_{start_time.timestamp()}"
        
        # Convert data to DataFrame if needed
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        
        total_records = len(data)
        
        # Determine rules to apply
        if rule_set:
            rule_ids = self.rule_sets.get(rule_set, [])
        elif rules:
            rule_ids = rules
        else:
            # Apply all enabled rules
            rule_ids = [rid for rid, rule in self.rules.items() if rule.enabled]
        
        # Apply rules
        issues = []
        dimension_scores = defaultdict(list)
        failed_records_total = 0
        
        for rule_id in rule_ids:
            rule = self.rules.get(rule_id)
            if not rule or not rule.enabled:
                continue
            
            try:
                issue = await self._apply_rule(data, rule, sample_size)
                if issue:
                    issues.append(issue)
                    failed_records_total += issue.affected_records
                    dimension_scores[rule.dimension].append(0.0)
                else:
                    dimension_scores[rule.dimension].append(1.0)
                    
            except Exception as e:
                logger.error(f"Error applying rule {rule_id}: {e}")
                issues.append(ValidationIssue(
                    rule_id=rule_id,
                    dimension=rule.dimension,
                    severity=SeverityLevel.HIGH,
                    description=f"Rule execution error: {str(e)}",
                    affected_records=0
                ))
        
        # Calculate scores
        passed_records = total_records - min(failed_records_total, total_records)
        quality_score = passed_records / total_records if total_records > 0 else 1.0
        
        # Calculate dimension scores
        dimension_results = {}
        for dimension, scores in dimension_scores.items():
            dimension_results[dimension] = sum(scores) / len(scores) if scores else 1.0
        
        # Create result
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        result = ValidationResult(
            dataset_id=dataset_id,
            validation_id=validation_id,
            timestamp=start_time,
            total_records=total_records,
            passed_records=passed_records,
            failed_records=failed_records_total,
            quality_score=quality_score,
            dimensions=dimension_results,
            issues=issues,
            execution_time_ms=execution_time,
            metadata={
                "rule_set": rule_set,
                "rules_applied": len(rule_ids)
            }
        )
        
        # Store result
        self.validation_history.append(result)
        self.validation_stats["total_validations"] += 1
        self.validation_stats["total_records_validated"] += total_records
        
        # Cache result
        await self.cache_manager.set(
            f"quality:validation:{validation_id}",
            result.__dict__,
            ttl=86400  # 24 hours
        )
        
        # Publish event
        await self.event_bus.publish("quality.validation.complete", {
            "validation_id": validation_id,
            "dataset_id": dataset_id,
            "quality_score": quality_score,
            "issues_count": len(issues)
        })
        
        logger.info(f"Validation complete for {dataset_id}: score={quality_score:.2f}")
        
        return result
        
    async def _apply_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply a single rule to data."""
        if rule.rule_type == RuleType.SQL:
            return await self._apply_sql_rule(data, rule, sample_size)
        elif rule.rule_type == RuleType.PYTHON:
            return await self._apply_python_rule(data, rule, sample_size)
        elif rule.rule_type == RuleType.REGEX:
            return await self._apply_regex_rule(data, rule, sample_size)
        elif rule.rule_type == RuleType.STATISTICAL:
            return await self._apply_statistical_rule(data, rule, sample_size)
        elif rule.rule_type == RuleType.SCHEMA:
            return await self._apply_schema_rule(data, rule, sample_size)
        else:
            logger.warning(f"Unsupported rule type: {rule.rule_type}")
            return None
            
    async def _apply_python_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply Python expression rule."""
        try:
            # Create safe evaluation context
            context = {
                'df': data,
                'pd': pd,
                'np': np,
                'len': len,
                'sum': sum,
                'min': min,
                'max': max,
                **rule.parameters
            }
            
            # Evaluate expression
            mask = eval(rule.expression, {"__builtins__": {}}, context)
            
            if isinstance(mask, pd.Series):
                failed_mask = ~mask
                failed_count = failed_mask.sum()
            else:
                # Single boolean result
                failed_count = 0 if mask else len(data)
                failed_mask = pd.Series([not mask] * len(data))
            
            if failed_count > 0:
                # Sample failed records
                failed_data = data[failed_mask].head(sample_size)
                sample = failed_data.to_dict('records')
                
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    description=f"{rule.name}: {failed_count} records failed",
                    affected_records=failed_count,
                    sample_data=sample
                )
                
        except Exception as e:
            logger.error(f"Error in Python rule {rule.rule_id}: {e}")
            raise
            
        return None
        
    async def _apply_regex_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply regex pattern rule."""
        try:
            column = rule.parameters.get("column")
            pattern = rule.expression
            
            if column not in data.columns:
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=SeverityLevel.HIGH,
                    description=f"Column '{column}' not found",
                    affected_records=len(data)
                )
            
            # Apply regex
            mask = data[column].astype(str).str.match(pattern, na=False)
            failed_mask = ~mask
            failed_count = failed_mask.sum()
            
            if failed_count > 0:
                failed_data = data[failed_mask].head(sample_size)
                sample = failed_data.to_dict('records')
                
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    description=f"{rule.name}: {failed_count} records don't match pattern",
                    affected_records=failed_count,
                    sample_data=sample
                )
                
        except Exception as e:
            logger.error(f"Error in regex rule {rule.rule_id}: {e}")
            raise
            
        return None
        
    async def _apply_statistical_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply statistical rule."""
        try:
            column = rule.parameters.get("column")
            stat_type = rule.parameters.get("statistic", "zscore")
            threshold = rule.parameters.get("threshold", 3.0)
            
            if column not in data.columns:
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=SeverityLevel.HIGH,
                    description=f"Column '{column}' not found",
                    affected_records=len(data)
                )
            
            # Calculate statistic
            if stat_type == "zscore":
                mean = data[column].mean()
                std = data[column].std()
                if std > 0:
                    zscore = np.abs((data[column] - mean) / std)
                    failed_mask = zscore > threshold
                else:
                    failed_mask = pd.Series([False] * len(data))
                    
            elif stat_type == "iqr":
                q1 = data[column].quantile(0.25)
                q3 = data[column].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                failed_mask = (data[column] < lower) | (data[column] > upper)
                
            else:
                raise ValueError(f"Unknown statistic type: {stat_type}")
            
            failed_count = failed_mask.sum()
            
            if failed_count > 0:
                failed_data = data[failed_mask].head(sample_size)
                sample = failed_data.to_dict('records')
                
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    description=f"{rule.name}: {failed_count} statistical outliers detected",
                    affected_records=failed_count,
                    sample_data=sample,
                    metadata={"statistic": stat_type, "threshold": threshold}
                )
                
        except Exception as e:
            logger.error(f"Error in statistical rule {rule.rule_id}: {e}")
            raise
            
        return None
        
    async def _apply_schema_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply schema validation rule."""
        try:
            expected_schema = rule.parameters.get("schema", {})
            issues = []
            
            # Check columns
            expected_columns = set(expected_schema.keys())
            actual_columns = set(data.columns)
            
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            
            if missing_columns:
                issues.append(f"Missing columns: {missing_columns}")
            if extra_columns and rule.parameters.get("strict", False):
                issues.append(f"Extra columns: {extra_columns}")
            
            # Check data types
            for column, expected_type in expected_schema.items():
                if column in data.columns:
                    actual_type = str(data[column].dtype)
                    if not self._compatible_types(actual_type, expected_type):
                        issues.append(f"Column '{column}': expected {expected_type}, got {actual_type}")
            
            if issues:
                return ValidationIssue(
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    description=f"{rule.name}: Schema validation failed - {'; '.join(issues)}",
                    affected_records=len(data),
                    metadata={"schema_issues": issues}
                )
                
        except Exception as e:
            logger.error(f"Error in schema rule {rule.rule_id}: {e}")
            raise
            
        return None
        
    def _compatible_types(self, actual: str, expected: str) -> bool:
        """Check if data types are compatible."""
        # Type mapping
        type_groups = {
            "numeric": ["int", "float", "number", "decimal"],
            "string": ["str", "object", "string", "text"],
            "datetime": ["datetime", "timestamp", "date"],
            "boolean": ["bool", "boolean"]
        }
        
        # Find groups
        actual_group = None
        expected_group = None
        
        for group, types in type_groups.items():
            if any(t in actual.lower() for t in types):
                actual_group = group
            if any(t in expected.lower() for t in types):
                expected_group = group
        
        return actual_group == expected_group
        
    async def _apply_sql_rule(
        self,
        data: pd.DataFrame,
        rule: QualityRule,
        sample_size: int
    ) -> Optional[ValidationIssue]:
        """Apply SQL-based rule (requires database connection)."""
        # This would require database configuration
        # For now, log warning
        logger.warning(f"SQL rules not implemented yet: {rule.rule_id}")
        return None
        
    async def _load_default_rules(self):
        """Load default quality rules."""
        default_rules = [
            # Completeness rules
            QualityRule(
                rule_id="null_check",
                name="Null Value Check",
                description="Check for null values in required columns",
                dimension=QualityDimension.COMPLETENESS,
                rule_type=RuleType.PYTHON,
                expression="df[column].notna()",
                parameters={"column": "id"},
                severity=SeverityLevel.HIGH
            ),
            
            # Validity rules
            QualityRule(
                rule_id="email_format",
                name="Email Format Validation",
                description="Validate email format",
                dimension=QualityDimension.VALIDITY,
                rule_type=RuleType.REGEX,
                expression=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                parameters={"column": "email"},
                severity=SeverityLevel.MEDIUM
            ),
            
            # Uniqueness rules
            QualityRule(
                rule_id="duplicate_check",
                name="Duplicate Record Check",
                description="Check for duplicate records",
                dimension=QualityDimension.UNIQUENESS,
                rule_type=RuleType.PYTHON,
                expression="~df.duplicated(subset=columns)",
                parameters={"columns": ["id"]},
                severity=SeverityLevel.HIGH
            ),
            
            # Statistical rules
            QualityRule(
                rule_id="outlier_detection",
                name="Statistical Outlier Detection",
                description="Detect statistical outliers using z-score",
                dimension=QualityDimension.ACCURACY,
                rule_type=RuleType.STATISTICAL,
                expression="zscore",
                parameters={"column": "value", "statistic": "zscore", "threshold": 3.0},
                severity=SeverityLevel.MEDIUM
            )
        ]
        
        for rule in default_rules:
            await self.register_rule(rule)
            
    async def _handle_validation_request(self, event_data: Dict[str, Any]):
        """Handle validation request event."""
        try:
            dataset_id = event_data["dataset_id"]
            data = event_data["data"]
            rule_set = event_data.get("rule_set")
            
            result = await self.validate_data(data, dataset_id, rule_set)
            
            # Publish result
            await self.event_bus.publish("quality.validation.result", result.__dict__)
            
        except Exception as e:
            logger.error(f"Error handling validation request: {e}")
            await self.event_bus.publish("quality.validation.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def _handle_rule_update(self, event_data: Dict[str, Any]):
        """Handle rule update event."""
        try:
            rule_data = event_data["rule"]
            rule = QualityRule(**rule_data)
            await self.register_rule(rule)
            
        except Exception as e:
            logger.error(f"Error handling rule update: {e}")
            
    def register_custom_validator(self, name: str, validator: Callable):
        """Register a custom validator function."""
        self.custom_validators[name] = validator
        logger.info(f"Registered custom validator: {name}")
        
    async def get_validation_history(
        self,
        dataset_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ValidationResult]:
        """Get validation history."""
        history = self.validation_history
        
        if dataset_id:
            history = [v for v in history if v.dataset_id == dataset_id]
        
        return history[-limit:]
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return {
            "total_validations": self.validation_stats["total_validations"],
            "total_records_validated": self.validation_stats["total_records_validated"],
            "total_rules": len(self.rules),
            "total_rule_sets": len(self.rule_sets),
            "rules_by_dimension": self._count_rules_by_dimension(),
            "recent_validations": len(self.validation_history)
        }
        
    def _count_rules_by_dimension(self) -> Dict[str, int]:
        """Count rules by dimension."""
        counts = defaultdict(int)
        for rule in self.rules.values():
            counts[rule.dimension.value] += 1
        return dict(counts) 