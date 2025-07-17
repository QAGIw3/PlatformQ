"""
Data Quality Framework

Implements comprehensive data quality checks, profiling, and monitoring
for the medallion architecture data lake.
"""

import logging
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
import json
import hashlib
from collections import defaultdict

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.dataset import SparkDFDataset
import deequ
from pydeequ import VerificationSuite, Check, AnalysisRunner, ConstraintSuggestionRunner
from pydeequ.analyzers import Size, Completeness, Mean, StandardDeviation, Entropy, Uniqueness
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import DEFAULT

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Data quality dimensions"""
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


@dataclass
class QualityMetric:
    """Individual quality metric"""
    name: str
    dimension: QualityDimension
    value: float
    threshold: float
    passed: bool
    details: Optional[Dict[str, Any]] = None


@dataclass
class QualityProfile:
    """Data quality profile for a dataset"""
    dataset_id: str
    timestamp: datetime
    row_count: int
    column_count: int
    metrics: List[QualityMetric]
    column_profiles: Dict[str, Dict[str, Any]]
    anomalies: List[Dict[str, Any]]
    suggestions: List[Dict[str, Any]]
    overall_score: float


@dataclass
class QualityRule:
    """Data quality rule definition"""
    rule_id: str
    name: str
    description: str
    dimension: QualityDimension
    check_type: str  # constraint, expectation, statistical
    parameters: Dict[str, Any]
    severity: str = "warning"  # info, warning, error, critical
    active: bool = True
    tags: List[str] = field(default_factory=list)


@dataclass
class AnomalyDetection:
    """Anomaly detection configuration"""
    method: str  # statistical, ml_based, rule_based
    sensitivity: float = 0.95
    lookback_periods: int = 7
    min_samples: int = 100


class DataQualityFramework:
    """
    Comprehensive data quality framework integrating multiple quality tools
    """
    
    def __init__(self,
                 spark: SparkSession,
                 ge_context: Optional[DataContext] = None):
        """
        Initialize data quality framework
        
        Args:
            spark: Spark session
            ge_context: Great Expectations data context
        """
        self.spark = spark
        self.ge_context = ge_context or self._init_ge_context()
        
        # Quality rules registry
        self.quality_rules: Dict[str, QualityRule] = {}
        
        # Historical metrics for trend analysis
        self.metrics_history: Dict[str, List[QualityProfile]] = defaultdict(list)
        
        # Anomaly detection configs
        self.anomaly_configs: Dict[str, AnomalyDetection] = {}
        
        # Initialize default rules
        self._init_default_rules()
        
    def _init_ge_context(self) -> DataContext:
        """Initialize Great Expectations context"""
        # In production, this would connect to a proper GE backend
        context_config = {
            "datasources": {
                "spark_datasource": {
                    "class_name": "SparkDFDatasource",
                    "batch_kwargs_generators": {},
                    "module_name": "great_expectations.datasource"
                }
            },
            "stores": {},
            "validations_store_name": "validations_store",
            "expectations_store_name": "expectations_store"
        }
        
        return DataContext(project_config=context_config)
        
    def _init_default_rules(self):
        """Initialize default quality rules"""
        # Completeness rules
        self.add_rule(QualityRule(
            rule_id="completeness_critical_fields",
            name="Critical Fields Completeness",
            description="Ensure critical fields have no nulls",
            dimension=QualityDimension.COMPLETENESS,
            check_type="constraint",
            parameters={
                "fields": ["id", "timestamp", "source"],
                "threshold": 1.0
            },
            severity="critical"
        ))
        
        # Uniqueness rules
        self.add_rule(QualityRule(
            rule_id="uniqueness_primary_key",
            name="Primary Key Uniqueness",
            description="Ensure primary keys are unique",
            dimension=QualityDimension.UNIQUENESS,
            check_type="constraint",
            parameters={
                "key_columns": ["id"],
                "threshold": 1.0
            },
            severity="error"
        ))
        
        # Validity rules
        self.add_rule(QualityRule(
            rule_id="validity_timestamp_range",
            name="Timestamp Range Validity",
            description="Ensure timestamps are within valid range",
            dimension=QualityDimension.VALIDITY,
            check_type="expectation",
            parameters={
                "column": "timestamp",
                "min_value": "2020-01-01",
                "max_value": "2030-12-31"
            },
            severity="warning"
        ))
        
        # Consistency rules
        self.add_rule(QualityRule(
            rule_id="consistency_referential_integrity",
            name="Referential Integrity",
            description="Ensure foreign key references are valid",
            dimension=QualityDimension.CONSISTENCY,
            check_type="constraint",
            parameters={
                "foreign_keys": {},  # Will be populated per dataset
                "threshold": 0.99
            },
            severity="error"
        ))
        
        # Statistical rules
        self.add_rule(QualityRule(
            rule_id="statistical_outlier_detection",
            name="Statistical Outlier Detection",
            description="Detect statistical outliers in numeric columns",
            dimension=QualityDimension.ACCURACY,
            check_type="statistical",
            parameters={
                "method": "z_score",
                "threshold": 3.0,
                "columns": []  # Auto-detect numeric columns
            },
            severity="info"
        ))
        
    def add_rule(self, rule: QualityRule):
        """Add a quality rule"""
        self.quality_rules[rule.rule_id] = rule
        logger.info(f"Added quality rule: {rule.rule_id}")
        
    def profile_dataset(self,
                       df: DataFrame,
                       dataset_id: str,
                       rules: Optional[List[str]] = None) -> QualityProfile:
        """
        Profile a dataset and run quality checks
        
        Args:
            df: Spark DataFrame to profile
            dataset_id: Unique identifier for the dataset
            rules: List of rule IDs to apply (None = all active rules)
            
        Returns:
            Quality profile with metrics and findings
        """
        start_time = datetime.now(timezone.utc)
        
        # Basic statistics
        row_count = df.count()
        column_count = len(df.columns)
        
        # Run column profiling
        column_profiles = self._profile_columns(df)
        
        # Run quality checks
        metrics = []
        
        # Deequ-based checks
        deequ_metrics = self._run_deequ_checks(df, dataset_id, rules)
        metrics.extend(deequ_metrics)
        
        # Great Expectations checks
        if self.ge_context:
            ge_metrics = self._run_ge_checks(df, dataset_id, rules)
            metrics.extend(ge_metrics)
            
        # Custom statistical checks
        stat_metrics = self._run_statistical_checks(df, dataset_id, rules)
        metrics.extend(stat_metrics)
        
        # Detect anomalies
        anomalies = self._detect_anomalies(df, dataset_id)
        
        # Generate improvement suggestions
        suggestions = self._generate_suggestions(df, metrics)
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(metrics)
        
        # Create profile
        profile = QualityProfile(
            dataset_id=dataset_id,
            timestamp=start_time,
            row_count=row_count,
            column_count=column_count,
            metrics=metrics,
            column_profiles=column_profiles,
            anomalies=anomalies,
            suggestions=suggestions,
            overall_score=overall_score
        )
        
        # Store in history
        self.metrics_history[dataset_id].append(profile)
        
        logger.info(f"Completed profiling for {dataset_id}: score={overall_score:.2f}")
        return profile
        
    def _profile_columns(self, df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """Profile individual columns"""
        profiles = {}
        
        for col in df.columns:
            col_df = df.select(col)
            dtype = dict(df.dtypes)[col]
            
            profile = {
                "data_type": dtype,
                "null_count": col_df.filter(F.col(col).isNull()).count(),
                "distinct_count": col_df.select(col).distinct().count()
            }
            
            # Numeric column statistics
            if dtype in ["int", "bigint", "float", "double", "decimal"]:
                stats = col_df.select(
                    F.min(col).alias("min"),
                    F.max(col).alias("max"),
                    F.mean(col).alias("mean"),
                    F.stddev(col).alias("stddev"),
                    F.expr(f"percentile_approx({col}, 0.25)").alias("q1"),
                    F.expr(f"percentile_approx({col}, 0.50)").alias("median"),
                    F.expr(f"percentile_approx({col}, 0.75)").alias("q3")
                ).collect()[0]
                
                profile.update({
                    "min": stats["min"],
                    "max": stats["max"],
                    "mean": stats["mean"],
                    "stddev": stats["stddev"],
                    "q1": stats["q1"],
                    "median": stats["median"],
                    "q3": stats["q3"]
                })
                
            # String column statistics
            elif dtype == "string":
                stats = col_df.select(
                    F.min(F.length(col)).alias("min_length"),
                    F.max(F.length(col)).alias("max_length"),
                    F.avg(F.length(col)).alias("avg_length")
                ).collect()[0]
                
                profile.update({
                    "min_length": stats["min_length"],
                    "max_length": stats["max_length"],
                    "avg_length": stats["avg_length"]
                })
                
                # Sample values
                sample = col_df.filter(F.col(col).isNotNull()).limit(10).collect()
                profile["sample_values"] = [row[col] for row in sample]
                
            profiles[col] = profile
            
        return profiles
        
    def _run_deequ_checks(self,
                         df: DataFrame,
                         dataset_id: str,
                         rules: Optional[List[str]] = None) -> List[QualityMetric]:
        """Run Deequ-based quality checks"""
        metrics = []
        
        # Build verification suite
        verification = VerificationSuite(self.spark).onData(df)
        
        # Add checks based on rules
        for rule_id, rule in self.quality_rules.items():
            if rules and rule_id not in rules:
                continue
                
            if not rule.active or rule.check_type != "constraint":
                continue
                
            check = Check(self.spark, Check.Level.Error, rule.name)
            
            # Completeness checks
            if rule.dimension == QualityDimension.COMPLETENESS:
                for field in rule.parameters.get("fields", []):
                    if field in df.columns:
                        check = check.isComplete(field)
                        
            # Uniqueness checks
            elif rule.dimension == QualityDimension.UNIQUENESS:
                key_cols = rule.parameters.get("key_columns", [])
                if all(col in df.columns for col in key_cols):
                    check = check.hasUniqueness(key_cols, lambda x: x >= rule.parameters.get("threshold", 1.0))
                    
            verification = verification.addCheck(check)
            
        # Run verification
        result = verification.run()
        
        # Convert results to metrics
        if result.status == "Success":
            for check_result in result.checkResults:
                metric = QualityMetric(
                    name=check_result.check.description,
                    dimension=QualityDimension.COMPLETENESS,  # Map appropriately
                    value=1.0 if check_result.status == "Success" else 0.0,
                    threshold=1.0,
                    passed=check_result.status == "Success",
                    details={"constraint": check_result.constraint.toString()}
                )
                metrics.append(metric)
                
        # Run analyzers for additional metrics
        analysis_result = AnalysisRunner(self.spark) \
            .onData(df) \
            .addAnalyzer(Size()) \
            .addAnalyzer(Completeness("*")) \
            .run()
            
        # Extract analyzer metrics
        for analyzer_name, value in analysis_result.metricMap.items():
            if value.value.isSuccess:
                metric = QualityMetric(
                    name=f"Analyzer: {analyzer_name}",
                    dimension=QualityDimension.COMPLETENESS,
                    value=value.value.get(),
                    threshold=0.0,  # Informational
                    passed=True,
                    details={"analyzer": analyzer_name}
                )
                metrics.append(metric)
                
        return metrics
        
    def _run_ge_checks(self,
                      df: DataFrame,
                      dataset_id: str,
                      rules: Optional[List[str]] = None) -> List[QualityMetric]:
        """Run Great Expectations checks"""
        metrics = []
        
        # Convert to GE dataset
        ge_df = SparkDFDataset(df)
        
        # Build expectation suite
        suite = ExpectationSuite(expectation_suite_name=f"{dataset_id}_suite")
        
        for rule_id, rule in self.quality_rules.items():
            if rules and rule_id not in rules:
                continue
                
            if not rule.active or rule.check_type != "expectation":
                continue
                
            # Validity checks
            if rule.dimension == QualityDimension.VALIDITY:
                if rule.rule_id == "validity_timestamp_range":
                    col = rule.parameters.get("column")
                    if col in df.columns:
                        expectation = ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_between",
                            kwargs={
                                "column": col,
                                "min_value": rule.parameters.get("min_value"),
                                "max_value": rule.parameters.get("max_value")
                            }
                        )
                        suite.add_expectation(expectation)
                        
        # Validate
        results = ge_df.validate(expectation_suite=suite)
        
        # Convert results to metrics
        for result in results["results"]:
            metric = QualityMetric(
                name=result["expectation_config"]["expectation_type"],
                dimension=QualityDimension.VALIDITY,
                value=1.0 if result["success"] else 0.0,
                threshold=1.0,
                passed=result["success"],
                details=result.get("result", {})
            )
            metrics.append(metric)
            
        return metrics
        
    def _run_statistical_checks(self,
                              df: DataFrame,
                              dataset_id: str,
                              rules: Optional[List[str]] = None) -> List[QualityMetric]:
        """Run statistical quality checks"""
        metrics = []
        
        for rule_id, rule in self.quality_rules.items():
            if rules and rule_id not in rules:
                continue
                
            if not rule.active or rule.check_type != "statistical":
                continue
                
            if rule.rule_id == "statistical_outlier_detection":
                # Detect numeric columns
                numeric_cols = [col for col, dtype in df.dtypes 
                              if dtype in ["int", "bigint", "float", "double"]]
                
                for col in numeric_cols:
                    # Calculate z-scores
                    stats = df.select(
                        F.mean(col).alias("mean"),
                        F.stddev(col).alias("stddev")
                    ).collect()[0]
                    
                    if stats["stddev"] and stats["stddev"] > 0:
                        outlier_threshold = rule.parameters.get("threshold", 3.0)
                        
                        outliers = df.filter(
                            F.abs((F.col(col) - stats["mean"]) / stats["stddev"]) > outlier_threshold
                        ).count()
                        
                        outlier_ratio = outliers / df.count() if df.count() > 0 else 0
                        
                        metric = QualityMetric(
                            name=f"Outliers in {col}",
                            dimension=QualityDimension.ACCURACY,
                            value=1.0 - outlier_ratio,
                            threshold=0.95,  # Max 5% outliers
                            passed=outlier_ratio <= 0.05,
                            details={
                                "column": col,
                                "outlier_count": outliers,
                                "outlier_ratio": outlier_ratio,
                                "method": "z_score",
                                "threshold": outlier_threshold
                            }
                        )
                        metrics.append(metric)
                        
        return metrics
        
    def _detect_anomalies(self, df: DataFrame, dataset_id: str) -> List[Dict[str, Any]]:
        """Detect anomalies in the dataset"""
        anomalies = []
        
        # Get anomaly detection config
        config = self.anomaly_configs.get(dataset_id, AnomalyDetection(method="statistical"))
        
        # Compare with historical data if available
        if dataset_id in self.metrics_history and len(self.metrics_history[dataset_id]) > config.min_samples:
            history = self.metrics_history[dataset_id][-config.lookback_periods:]
            
            # Check row count anomalies
            historical_counts = [p.row_count for p in history]
            mean_count = np.mean(historical_counts)
            std_count = np.std(historical_counts)
            
            current_count = df.count()
            if std_count > 0:
                z_score = abs((current_count - mean_count) / std_count)
                
                if z_score > 3:
                    anomalies.append({
                        "type": "row_count_anomaly",
                        "severity": "warning",
                        "current_value": current_count,
                        "expected_range": (mean_count - 3*std_count, mean_count + 3*std_count),
                        "z_score": z_score
                    })
                    
            # Check column cardinality anomalies
            for col in df.columns:
                current_cardinality = df.select(col).distinct().count()
                historical_cardinalities = []
                
                for profile in history:
                    if col in profile.column_profiles:
                        historical_cardinalities.append(profile.column_profiles[col].get("distinct_count", 0))
                        
                if historical_cardinalities:
                    mean_card = np.mean(historical_cardinalities)
                    std_card = np.std(historical_cardinalities)
                    
                    if std_card > 0:
                        z_score = abs((current_cardinality - mean_card) / std_card)
                        
                        if z_score > 3:
                            anomalies.append({
                                "type": "cardinality_anomaly",
                                "column": col,
                                "severity": "info",
                                "current_value": current_cardinality,
                                "expected_range": (mean_card - 3*std_card, mean_card + 3*std_card),
                                "z_score": z_score
                            })
                            
        # Pattern-based anomaly detection
        # Check for sudden appearance of nulls
        for col in df.columns:
            null_ratio = df.filter(F.col(col).isNull()).count() / df.count()
            
            if null_ratio > 0.1:  # More than 10% nulls
                historical_null_ratios = []
                
                for profile in self.metrics_history.get(dataset_id, [])[-5:]:
                    if col in profile.column_profiles:
                        hist_null_count = profile.column_profiles[col].get("null_count", 0)
                        hist_total = profile.row_count
                        historical_null_ratios.append(hist_null_count / hist_total if hist_total > 0 else 0)
                        
                if historical_null_ratios and max(historical_null_ratios) < 0.01:
                    anomalies.append({
                        "type": "null_spike_anomaly",
                        "column": col,
                        "severity": "warning",
                        "current_null_ratio": null_ratio,
                        "historical_max_null_ratio": max(historical_null_ratios)
                    })
                    
        return anomalies
        
    def _generate_suggestions(self,
                            df: DataFrame,
                            metrics: List[QualityMetric]) -> List[Dict[str, Any]]:
        """Generate improvement suggestions based on quality findings"""
        suggestions = []
        
        # Use Deequ constraint suggestions
        suggestion_result = ConstraintSuggestionRunner(self.spark) \
            .onData(df) \
            .addConstraintRule(DEFAULT()) \
            .run()
            
        for suggestion in suggestion_result.constraintSuggestions:
            suggestions.append({
                "type": "constraint_suggestion",
                "description": f"Add constraint: {suggestion.description}",
                "constraint": suggestion.constraint.toString(),
                "code_for_constraint": suggestion.codeForConstraint
            })
            
        # Custom suggestions based on metrics
        failed_metrics = [m for m in metrics if not m.passed]
        
        for metric in failed_metrics:
            if metric.dimension == QualityDimension.COMPLETENESS:
                suggestions.append({
                    "type": "completeness_improvement",
                    "description": f"Improve completeness for {metric.name}",
                    "recommendation": "Consider implementing data validation at source or adding default values"
                })
                
            elif metric.dimension == QualityDimension.ACCURACY and "outlier" in metric.name.lower():
                suggestions.append({
                    "type": "outlier_handling",
                    "description": f"Handle outliers in {metric.details.get('column', 'unknown')}",
                    "recommendation": "Review outlier detection rules or implement outlier treatment strategy"
                })
                
        # Schema optimization suggestions
        for col, profile in self._profile_columns(df).items():
            # Suggest type optimization
            if profile["data_type"] == "string" and profile.get("distinct_count", 0) < 100:
                suggestions.append({
                    "type": "schema_optimization",
                    "description": f"Consider categorizing column {col}",
                    "recommendation": f"Column has only {profile['distinct_count']} distinct values, consider using categorical type"
                })
                
        return suggestions
        
    def _calculate_overall_score(self, metrics: List[QualityMetric]) -> float:
        """Calculate overall quality score"""
        if not metrics:
            return 1.0
            
        # Weight by dimension
        dimension_weights = {
            QualityDimension.ACCURACY: 0.25,
            QualityDimension.COMPLETENESS: 0.25,
            QualityDimension.CONSISTENCY: 0.20,
            QualityDimension.VALIDITY: 0.15,
            QualityDimension.UNIQUENESS: 0.10,
            QualityDimension.TIMELINESS: 0.05
        }
        
        dimension_scores = defaultdict(list)
        
        for metric in metrics:
            dimension_scores[metric.dimension].append(metric.value)
            
        weighted_score = 0.0
        total_weight = 0.0
        
        for dimension, scores in dimension_scores.items():
            if scores:
                avg_score = np.mean(scores)
                weight = dimension_weights.get(dimension, 0.1)
                weighted_score += avg_score * weight
                total_weight += weight
                
        return weighted_score / total_weight if total_weight > 0 else 0.0
        
    def validate_schema_evolution(self,
                                old_schema: StructType,
                                new_schema: StructType) -> Dict[str, Any]:
        """Validate schema evolution compatibility"""
        validation_result = {
            "compatible": True,
            "breaking_changes": [],
            "warnings": [],
            "additions": [],
            "removals": []
        }
        
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Check for removed fields (breaking change)
        for field_name, field in old_fields.items():
            if field_name not in new_fields:
                validation_result["removals"].append(field_name)
                if not field.nullable:
                    validation_result["breaking_changes"].append(
                        f"Non-nullable field '{field_name}' was removed"
                    )
                    validation_result["compatible"] = False
                    
        # Check for added fields
        for field_name, field in new_fields.items():
            if field_name not in old_fields:
                validation_result["additions"].append(field_name)
                if not field.nullable:
                    validation_result["warnings"].append(
                        f"New non-nullable field '{field_name}' may cause issues with existing data"
                    )
                    
        # Check for type changes (breaking change)
        for field_name, new_field in new_fields.items():
            if field_name in old_fields:
                old_field = old_fields[field_name]
                if old_field.dataType != new_field.dataType:
                    validation_result["breaking_changes"].append(
                        f"Field '{field_name}' type changed from {old_field.dataType} to {new_field.dataType}"
                    )
                    validation_result["compatible"] = False
                    
        return validation_result
        
    def generate_quality_report(self,
                              dataset_id: str,
                              profile: QualityProfile) -> Dict[str, Any]:
        """Generate comprehensive quality report"""
        report = {
            "dataset_id": dataset_id,
            "report_timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "overall_score": profile.overall_score,
                "row_count": profile.row_count,
                "column_count": profile.column_count,
                "passed_checks": sum(1 for m in profile.metrics if m.passed),
                "failed_checks": sum(1 for m in profile.metrics if not m.passed),
                "anomalies_detected": len(profile.anomalies)
            },
            "dimension_scores": {},
            "failed_checks": [],
            "anomalies": profile.anomalies,
            "suggestions": profile.suggestions,
            "trend_analysis": {}
        }
        
        # Calculate dimension scores
        for dimension in QualityDimension:
            dimension_metrics = [m for m in profile.metrics if m.dimension == dimension]
            if dimension_metrics:
                report["dimension_scores"][dimension.value] = {
                    "score": np.mean([m.value for m in dimension_metrics]),
                    "check_count": len(dimension_metrics),
                    "passed_count": sum(1 for m in dimension_metrics if m.passed)
                }
                
        # List failed checks
        for metric in profile.metrics:
            if not metric.passed:
                report["failed_checks"].append({
                    "name": metric.name,
                    "dimension": metric.dimension.value,
                    "value": metric.value,
                    "threshold": metric.threshold,
                    "details": metric.details
                })
                
        # Trend analysis
        if dataset_id in self.metrics_history and len(self.metrics_history[dataset_id]) > 1:
            history = self.metrics_history[dataset_id][-10:]  # Last 10 profiles
            
            report["trend_analysis"] = {
                "score_trend": [p.overall_score for p in history],
                "row_count_trend": [p.row_count for p in history],
                "anomaly_trend": [len(p.anomalies) for p in history],
                "timestamps": [p.timestamp.isoformat() for p in history]
            }
            
        return report
        
    def export_metrics_to_monitoring(self,
                                   profile: QualityProfile,
                                   monitoring_backend: str = "prometheus") -> Dict[str, Any]:
        """Export quality metrics to monitoring systems"""
        metrics_export = {}
        
        if monitoring_backend == "prometheus":
            # Format metrics for Prometheus
            for metric in profile.metrics:
                metric_name = f"data_quality_{metric.dimension.value}_{metric.name.lower().replace(' ', '_')}"
                metrics_export[metric_name] = {
                    "value": metric.value,
                    "labels": {
                        "dataset": profile.dataset_id,
                        "dimension": metric.dimension.value,
                        "passed": str(metric.passed).lower()
                    }
                }
                
            # Add summary metrics
            metrics_export["data_quality_overall_score"] = {
                "value": profile.overall_score,
                "labels": {"dataset": profile.dataset_id}
            }
            
            metrics_export["data_quality_row_count"] = {
                "value": profile.row_count,
                "labels": {"dataset": profile.dataset_id}
            }
            
            metrics_export["data_quality_anomaly_count"] = {
                "value": len(profile.anomalies),
                "labels": {"dataset": profile.dataset_id}
            }
            
        return metrics_export 