"""
Quality Processing Implementation for DataIntelligenceSuite

Provides data quality validation and monitoring capabilities.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from collections import defaultdict

from .base_processor import BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus
from ...monitoring import MetricsCollector

logger = logging.getLogger(__name__)


class DataQualityDimension(Enum):
    """Data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


class QualityCheckType(Enum):
    """Types of quality checks"""
    NULL_CHECK = "null_check"
    RANGE_CHECK = "range_check"
    FORMAT_CHECK = "format_check"
    REFERENCE_CHECK = "reference_check"
    BUSINESS_RULE = "business_rule"
    STATISTICAL = "statistical"
    DUPLICATE_CHECK = "duplicate_check"
    OUTLIER_CHECK = "outlier_check"


@dataclass
class QualityRule:
    """Quality validation rule"""
    rule_id: str
    name: str
    check_type: QualityCheckType
    dimension: DataQualityDimension
    column: Optional[str] = None
    condition: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: str = "warning"  # info, warning, error, critical
    enabled: bool = True
    
    def apply(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Apply rule to data and return results"""
        # This would be implemented based on check_type
        pass


@dataclass
class QualityConfig(ProcessorConfig):
    """Configuration for quality processing"""
    # Rules configuration
    rules: List[QualityRule] = field(default_factory=list)
    custom_rules_path: Optional[str] = None
    
    # Thresholds
    completeness_threshold: float = 0.95
    accuracy_threshold: float = 0.98
    uniqueness_threshold: float = 0.99
    
    # Sampling
    enable_sampling: bool = True
    sample_size: Optional[int] = 10000
    sample_percentage: Optional[float] = None
    
    # Profiling
    enable_profiling: bool = True
    profile_columns: Optional[List[str]] = None
    
    # Anomaly detection
    enable_anomaly_detection: bool = True
    anomaly_threshold: float = 3.0  # standard deviations
    
    # Reporting
    generate_report: bool = True
    report_format: str = "json"  # json, html, pdf
    report_path: Optional[str] = None
    
    # Actions
    fail_on_critical: bool = True
    quarantine_invalid: bool = False
    auto_correct: bool = False


@dataclass
class QualityCheck:
    """Individual quality check result"""
    check_id: str
    rule: QualityRule
    status: str  # passed, failed, skipped
    score: float  # 0.0 to 1.0
    records_checked: int
    records_failed: int
    execution_time_ms: float
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    
    @property
    def pass_rate(self) -> float:
        """Calculate pass rate"""
        if self.records_checked == 0:
            return 1.0
        return (self.records_checked - self.records_failed) / self.records_checked


@dataclass
class QualityResult(ProcessingResult):
    """Result of quality processing"""
    # Overall quality scores
    overall_score: float = 0.0
    dimension_scores: Dict[DataQualityDimension, float] = field(default_factory=dict)
    
    # Check results
    checks_performed: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    check_results: List[QualityCheck] = field(default_factory=list)
    
    # Data profile
    data_profile: Optional[Dict[str, Any]] = None
    
    # Issues found
    quality_issues: List[Dict[str, Any]] = field(default_factory=list)
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    
    # Report location
    report_path: Optional[str] = None


class QualityProcessor(BaseProcessor):
    """
    Quality processor for data validation and monitoring.
    
    Features:
    - Multi-dimensional quality assessment
    - Configurable quality rules
    - Data profiling
    - Anomaly detection
    - Quality reporting
    - Automated remediation
    """
    
    def __init__(
        self,
        config: QualityConfig,
        **kwargs
    ):
        super().__init__(config, **kwargs)
        self.config: QualityConfig = config
        self._rules_registry: Dict[str, QualityRule] = {}
        self._custom_validators: Dict[str, Callable] = {}
        
    async def initialize(self):
        """Initialize quality processor"""
        logger.info(f"Initializing quality processor: {self.config.name}")
        
        # Load rules
        await self._load_rules()
        
        # Register built-in validators
        self._register_builtin_validators()
        
    async def _load_rules(self):
        """Load quality rules"""
        # Add configured rules
        for rule in self.config.rules:
            self._rules_registry[rule.rule_id] = rule
            
        # Load custom rules if path provided
        if self.config.custom_rules_path:
            custom_rules = await self._load_custom_rules(self.config.custom_rules_path)
            for rule in custom_rules:
                self._rules_registry[rule.rule_id] = rule
                
        logger.info(f"Loaded {len(self._rules_registry)} quality rules")
        
    async def _load_custom_rules(self, path: str) -> List[QualityRule]:
        """Load custom rules from file"""
        # Simplified - actual implementation would load from JSON/YAML
        return []
        
    def _register_builtin_validators(self):
        """Register built-in validators"""
        self._custom_validators["null_check"] = self._null_check
        self._custom_validators["range_check"] = self._range_check
        self._custom_validators["format_check"] = self._format_check
        self._custom_validators["duplicate_check"] = self._duplicate_check
        self._custom_validators["outlier_check"] = self._outlier_check
        
    async def process(self, data: Any, job_id: Optional[str] = None) -> ProcessingResult:
        """Process data quality"""
        job_id = job_id or f"quality_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        result = QualityResult(
            job_id=job_id,
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Convert data to DataFrame if needed
            df = self._to_dataframe(data)
            result.records_processed = len(df)
            
            # Apply sampling if configured
            if self.config.enable_sampling:
                df_sample = self._sample_data(df)
            else:
                df_sample = df
                
            # Profile data if enabled
            if self.config.enable_profiling:
                result.data_profile = await self._profile_data(df_sample)
                
            # Run quality checks
            check_results = await self._run_quality_checks(df_sample)
            result.check_results = check_results
            result.checks_performed = len(check_results)
            result.checks_passed = sum(1 for c in check_results if c.status == "passed")
            result.checks_failed = sum(1 for c in check_results if c.status == "failed")
            
            # Calculate quality scores
            result.overall_score = self._calculate_overall_score(check_results)
            result.dimension_scores = self._calculate_dimension_scores(check_results)
            
            # Identify quality issues
            result.quality_issues = self._identify_issues(check_results)
            
            # Generate recommendations
            result.recommendations = self._generate_recommendations(result)
            
            # Generate report if configured
            if self.config.generate_report:
                result.report_path = await self._generate_report(result)
                
            # Determine final status
            if self.config.fail_on_critical and any(
                issue["severity"] == "critical" for issue in result.quality_issues
            ):
                result.status = ProcessingStatus.FAILED
                result.errors.append({"error": "Critical quality issues found"})
            else:
                result.status = ProcessingStatus.COMPLETED
                
            result.completed_at = datetime.utcnow()
            result.processing_time_ms = (result.completed_at - result.started_at).total_seconds() * 1000
            
            # Record metrics
            self._record_quality_metrics(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Quality processing failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.completed_at = datetime.utcnow()
            result.errors.append({"error": str(e), "type": type(e).__name__})
            return result
            
    def _to_dataframe(self, data: Any) -> pd.DataFrame:
        """Convert data to pandas DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, str):
            # Assume file path
            if data.endswith('.csv'):
                return pd.read_csv(data)
            elif data.endswith('.parquet'):
                return pd.read_parquet(data)
            elif data.endswith('.json'):
                return pd.read_json(data)
            else:
                raise ValueError(f"Unsupported file format: {data}")
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
            
    def _sample_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sample data for quality checks"""
        if self.config.sample_size and len(df) > self.config.sample_size:
            return df.sample(n=self.config.sample_size, random_state=42)
        elif self.config.sample_percentage:
            return df.sample(frac=self.config.sample_percentage, random_state=42)
        else:
            return df
            
    async def _profile_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Profile data to understand its characteristics"""
        profile = {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
            "memory_usage": df.memory_usage(deep=True).to_dict(),
            "column_stats": {}
        }
        
        # Profile each column
        columns_to_profile = self.config.profile_columns or df.columns
        
        for col in columns_to_profile:
            if col not in df.columns:
                continue
                
            col_profile = {
                "dtype": str(df[col].dtype),
                "null_count": df[col].isnull().sum(),
                "null_percentage": df[col].isnull().sum() / len(df) * 100,
                "unique_count": df[col].nunique(),
                "unique_percentage": df[col].nunique() / len(df) * 100
            }
            
            # Numeric column stats
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    "mean": df[col].mean(),
                    "std": df[col].std(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "25%": df[col].quantile(0.25),
                    "50%": df[col].quantile(0.50),
                    "75%": df[col].quantile(0.75)
                })
                
            # String column stats
            elif pd.api.types.is_string_dtype(df[col]):
                col_profile.update({
                    "min_length": df[col].str.len().min(),
                    "max_length": df[col].str.len().max(),
                    "avg_length": df[col].str.len().mean()
                })
                
            profile["column_stats"][col] = col_profile
            
        return profile
        
    async def _run_quality_checks(self, df: pd.DataFrame) -> List[QualityCheck]:
        """Run all quality checks"""
        check_results = []
        
        for rule_id, rule in self._rules_registry.items():
            if not rule.enabled:
                continue
                
            check_start = datetime.utcnow()
            
            try:
                # Get validator for check type
                validator = self._custom_validators.get(rule.check_type.value)
                
                if validator:
                    # Run validation
                    validation_result = await validator(df, rule)
                    
                    check = QualityCheck(
                        check_id=f"{rule_id}_{check_start.timestamp()}",
                        rule=rule,
                        status="passed" if validation_result["passed"] else "failed",
                        score=validation_result.get("score", 0.0),
                        records_checked=validation_result.get("records_checked", len(df)),
                        records_failed=validation_result.get("records_failed", 0),
                        execution_time_ms=(datetime.utcnow() - check_start).total_seconds() * 1000,
                        details=validation_result.get("details", {})
                    )
                else:
                    check = QualityCheck(
                        check_id=f"{rule_id}_{check_start.timestamp()}",
                        rule=rule,
                        status="skipped",
                        score=0.0,
                        records_checked=0,
                        records_failed=0,
                        execution_time_ms=0,
                        errors=[f"No validator for check type: {rule.check_type.value}"]
                    )
                    
                check_results.append(check)
                
            except Exception as e:
                logger.error(f"Error running check {rule_id}: {e}")
                check = QualityCheck(
                    check_id=f"{rule_id}_{check_start.timestamp()}",
                    rule=rule,
                    status="failed",
                    score=0.0,
                    records_checked=len(df),
                    records_failed=len(df),
                    execution_time_ms=(datetime.utcnow() - check_start).total_seconds() * 1000,
                    errors=[str(e)]
                )
                check_results.append(check)
                
        return check_results
        
    async def _null_check(self, df: pd.DataFrame, rule: QualityRule) -> Dict[str, Any]:
        """Check for null values"""
        column = rule.column
        
        if column not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "records_checked": 0,
                "records_failed": 0,
                "details": {"error": f"Column {column} not found"}
            }
            
        null_count = df[column].isnull().sum()
        total_count = len(df)
        completeness = (total_count - null_count) / total_count if total_count > 0 else 1.0
        
        threshold = rule.parameters.get("threshold", self.config.completeness_threshold)
        passed = completeness >= threshold
        
        return {
            "passed": passed,
            "score": completeness,
            "records_checked": total_count,
            "records_failed": null_count,
            "details": {
                "completeness": completeness,
                "threshold": threshold,
                "null_count": null_count
            }
        }
        
    async def _range_check(self, df: pd.DataFrame, rule: QualityRule) -> Dict[str, Any]:
        """Check if values are within specified range"""
        column = rule.column
        min_value = rule.parameters.get("min")
        max_value = rule.parameters.get("max")
        
        if column not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "records_checked": 0,
                "records_failed": 0,
                "details": {"error": f"Column {column} not found"}
            }
            
        # Filter out nulls
        non_null_df = df[df[column].notna()]
        
        violations = 0
        if min_value is not None:
            violations += (non_null_df[column] < min_value).sum()
        if max_value is not None:
            violations += (non_null_df[column] > max_value).sum()
            
        total_checked = len(non_null_df)
        pass_rate = (total_checked - violations) / total_checked if total_checked > 0 else 1.0
        
        return {
            "passed": pass_rate >= rule.parameters.get("threshold", 0.95),
            "score": pass_rate,
            "records_checked": total_checked,
            "records_failed": violations,
            "details": {
                "min_value": min_value,
                "max_value": max_value,
                "violations": violations
            }
        }
        
    async def _format_check(self, df: pd.DataFrame, rule: QualityRule) -> Dict[str, Any]:
        """Check if values match expected format"""
        column = rule.column
        pattern = rule.parameters.get("pattern")
        
        if not pattern:
            return {
                "passed": False,
                "score": 0.0,
                "records_checked": 0,
                "records_failed": 0,
                "details": {"error": "No pattern specified"}
            }
            
        # Filter out nulls
        non_null_df = df[df[column].notna()]
        
        # Check pattern matches
        matches = non_null_df[column].astype(str).str.match(pattern)
        violations = (~matches).sum()
        
        total_checked = len(non_null_df)
        pass_rate = (total_checked - violations) / total_checked if total_checked > 0 else 1.0
        
        return {
            "passed": pass_rate >= rule.parameters.get("threshold", 0.95),
            "score": pass_rate,
            "records_checked": total_checked,
            "records_failed": violations,
            "details": {
                "pattern": pattern,
                "violations": violations
            }
        }
        
    async def _duplicate_check(self, df: pd.DataFrame, rule: QualityRule) -> Dict[str, Any]:
        """Check for duplicate values"""
        columns = rule.parameters.get("columns", [rule.column] if rule.column else df.columns)
        
        # Check duplicates
        duplicates = df.duplicated(subset=columns, keep=False)
        duplicate_count = duplicates.sum()
        
        total_count = len(df)
        uniqueness = (total_count - duplicate_count) / total_count if total_count > 0 else 1.0
        
        threshold = rule.parameters.get("threshold", self.config.uniqueness_threshold)
        passed = uniqueness >= threshold
        
        return {
            "passed": passed,
            "score": uniqueness,
            "records_checked": total_count,
            "records_failed": duplicate_count,
            "details": {
                "uniqueness": uniqueness,
                "threshold": threshold,
                "duplicate_count": duplicate_count,
                "columns_checked": columns
            }
        }
        
    async def _outlier_check(self, df: pd.DataFrame, rule: QualityRule) -> Dict[str, Any]:
        """Check for statistical outliers"""
        column = rule.column
        method = rule.parameters.get("method", "zscore")
        threshold = rule.parameters.get("threshold", self.config.anomaly_threshold)
        
        if column not in df.columns:
            return {
                "passed": False,
                "score": 0.0,
                "records_checked": 0,
                "records_failed": 0,
                "details": {"error": f"Column {column} not found"}
            }
            
        # Filter numeric values
        numeric_df = df[pd.to_numeric(df[column], errors='coerce').notna()]
        
        if method == "zscore":
            # Z-score method
            mean = numeric_df[column].mean()
            std = numeric_df[column].std()
            z_scores = np.abs((numeric_df[column] - mean) / std)
            outliers = z_scores > threshold
        elif method == "iqr":
            # IQR method
            Q1 = numeric_df[column].quantile(0.25)
            Q3 = numeric_df[column].quantile(0.75)
            IQR = Q3 - Q1
            outliers = (numeric_df[column] < (Q1 - 1.5 * IQR)) | (numeric_df[column] > (Q3 + 1.5 * IQR))
        else:
            outliers = pd.Series([False] * len(numeric_df))
            
        outlier_count = outliers.sum()
        total_checked = len(numeric_df)
        pass_rate = (total_checked - outlier_count) / total_checked if total_checked > 0 else 1.0
        
        return {
            "passed": outlier_count == 0 or pass_rate >= rule.parameters.get("tolerance", 0.99),
            "score": pass_rate,
            "records_checked": total_checked,
            "records_failed": outlier_count,
            "details": {
                "method": method,
                "threshold": threshold,
                "outlier_count": outlier_count
            }
        }
        
    def _calculate_overall_score(self, check_results: List[QualityCheck]) -> float:
        """Calculate overall quality score"""
        if not check_results:
            return 1.0
            
        # Weight by severity
        severity_weights = {
            "info": 0.1,
            "warning": 0.3,
            "error": 0.6,
            "critical": 1.0
        }
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for check in check_results:
            weight = severity_weights.get(check.rule.severity, 0.5)
            weighted_sum += check.score * weight
            total_weight += weight
            
        return weighted_sum / total_weight if total_weight > 0 else 0.0
        
    def _calculate_dimension_scores(self, check_results: List[QualityCheck]) -> Dict[DataQualityDimension, float]:
        """Calculate quality scores by dimension"""
        dimension_scores = defaultdict(list)
        
        for check in check_results:
            dimension_scores[check.rule.dimension].append(check.score)
            
        return {
            dimension: np.mean(scores) if scores else 1.0
            for dimension, scores in dimension_scores.items()
        }
        
    def _identify_issues(self, check_results: List[QualityCheck]) -> List[Dict[str, Any]]:
        """Identify quality issues from check results"""
        issues = []
        
        for check in check_results:
            if check.status == "failed":
                issue = {
                    "rule_id": check.rule.rule_id,
                    "rule_name": check.rule.name,
                    "dimension": check.rule.dimension.value,
                    "severity": check.rule.severity,
                    "column": check.rule.column,
                    "score": check.score,
                    "records_affected": check.records_failed,
                    "details": check.details
                }
                issues.append(issue)
                
        # Sort by severity
        severity_order = {"critical": 0, "error": 1, "warning": 2, "info": 3}
        issues.sort(key=lambda x: severity_order.get(x["severity"], 99))
        
        return issues
        
    def _generate_recommendations(self, result: QualityResult) -> List[str]:
        """Generate recommendations based on quality results"""
        recommendations = []
        
        # Completeness recommendations
        completeness_score = result.dimension_scores.get(DataQualityDimension.COMPLETENESS, 1.0)
        if completeness_score < 0.9:
            recommendations.append(
                "Consider implementing data collection improvements to reduce missing values"
            )
            
        # Accuracy recommendations
        accuracy_score = result.dimension_scores.get(DataQualityDimension.ACCURACY, 1.0)
        if accuracy_score < 0.95:
            recommendations.append(
                "Review data validation rules at the point of entry to improve accuracy"
            )
            
        # Uniqueness recommendations
        uniqueness_score = result.dimension_scores.get(DataQualityDimension.UNIQUENESS, 1.0)
        if uniqueness_score < 0.99:
            recommendations.append(
                "Implement deduplication processes to handle duplicate records"
            )
            
        # Critical issues
        critical_issues = [i for i in result.quality_issues if i["severity"] == "critical"]
        if critical_issues:
            recommendations.append(
                f"Address {len(critical_issues)} critical quality issues immediately"
            )
            
        return recommendations
        
    async def _generate_report(self, result: QualityResult) -> str:
        """Generate quality report"""
        report_path = self.config.report_path or f"/tmp/quality_report_{result.job_id}.{self.config.report_format}"
        
        if self.config.report_format == "json":
            # Generate JSON report
            import json
            report_data = {
                "job_id": result.job_id,
                "timestamp": result.completed_at.isoformat() if result.completed_at else None,
                "overall_score": result.overall_score,
                "dimension_scores": {k.value: v for k, v in result.dimension_scores.items()},
                "summary": {
                    "records_processed": result.records_processed,
                    "checks_performed": result.checks_performed,
                    "checks_passed": result.checks_passed,
                    "checks_failed": result.checks_failed
                },
                "issues": result.quality_issues,
                "recommendations": result.recommendations,
                "check_details": [
                    {
                        "rule_id": check.rule.rule_id,
                        "rule_name": check.rule.name,
                        "status": check.status,
                        "score": check.score,
                        "pass_rate": check.pass_rate,
                        "details": check.details
                    }
                    for check in result.check_results
                ]
            }
            
            with open(report_path, 'w') as f:
                json.dump(report_data, f, indent=2)
                
        elif self.config.report_format == "html":
            # Generate HTML report (simplified)
            html_content = f"""
            <html>
            <head><title>Data Quality Report - {result.job_id}</title></head>
            <body>
                <h1>Data Quality Report</h1>
                <h2>Overall Score: {result.overall_score:.2%}</h2>
                <h3>Summary</h3>
                <ul>
                    <li>Records Processed: {result.records_processed}</li>
                    <li>Checks Performed: {result.checks_performed}</li>
                    <li>Checks Passed: {result.checks_passed}</li>
                    <li>Checks Failed: {result.checks_failed}</li>
                </ul>
                <h3>Quality Issues</h3>
                <ul>
                    {"".join(f"<li>{issue['rule_name']} ({issue['severity']}): {issue['records_affected']} records affected</li>" for issue in result.quality_issues)}
                </ul>
                <h3>Recommendations</h3>
                <ul>
                    {"".join(f"<li>{rec}</li>" for rec in result.recommendations)}
                </ul>
            </body>
            </html>
            """
            
            with open(report_path, 'w') as f:
                f.write(html_content)
                
        logger.info(f"Generated quality report: {report_path}")
        return report_path
        
    def _record_quality_metrics(self, result: QualityResult):
        """Record quality metrics"""
        if self.metrics:
            # Overall score
            self.metrics.set_gauge("data_quality_score", result.overall_score, {"job_id": result.job_id})
            
            # Dimension scores
            for dimension, score in result.dimension_scores.items():
                self.metrics.set_gauge(
                    "data_quality_dimension_score",
                    score,
                    {"dimension": dimension.value, "job_id": result.job_id}
                )
                
            # Check results
            self.metrics.increment_counter("quality_checks_total", {"status": "passed"}, result.checks_passed)
            self.metrics.increment_counter("quality_checks_total", {"status": "failed"}, result.checks_failed)
            
            # Issues by severity
            severity_counts = defaultdict(int)
            for issue in result.quality_issues:
                severity_counts[issue["severity"]] += 1
                
            for severity, count in severity_counts.items():
                self.metrics.set_gauge(
                    "quality_issues_count",
                    count,
                    {"severity": severity, "job_id": result.job_id}
                )
                
    async def create_quality_profile(self, data: Any) -> Dict[str, Any]:
        """Create a comprehensive quality profile for the data"""
        df = self._to_dataframe(data)
        
        # Run profiling
        profile = await self._profile_data(df)
        
        # Run all quality checks
        check_results = await self._run_quality_checks(df)
        
        # Calculate scores
        overall_score = self._calculate_overall_score(check_results)
        dimension_scores = self._calculate_dimension_scores(check_results)
        
        return {
            "profile": profile,
            "quality_score": overall_score,
            "dimension_scores": {k.value: v for k, v in dimension_scores.items()},
            "check_summary": {
                "total": len(check_results),
                "passed": sum(1 for c in check_results if c.status == "passed"),
                "failed": sum(1 for c in check_results if c.status == "failed")
            }
        }
        
    def add_custom_rule(self, rule: QualityRule):
        """Add a custom quality rule"""
        self._rules_registry[rule.rule_id] = rule
        
    def add_custom_validator(self, check_type: str, validator: Callable):
        """Add a custom validator function"""
        self._custom_validators[check_type] = validator 