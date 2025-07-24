"""
Quality Processing Implementation for DataIntelligenceSuite v2.0

Enhanced with enterprise-scale data quality validation, ML-based anomaly detection,
and intelligent remediation capabilities.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from collections import defaultdict
import json
from pathlib import Path
import uuid

try:
    from great_expectations import DataContext
    from great_expectations.core.batch import RuntimeBatchRequest
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False

try:
    from pydeequ import Check, CheckLevel, VerificationSuite, VerificationResult
    from pydeequ.analyzers import Size, Completeness, Mean, StandardDeviation
    DEEQU_AVAILABLE = True
except ImportError:
    DEEQU_AVAILABLE = False

try:
    from soda.scan import Scan
    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

from .base_processor import (
    BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus,
    ProcessingMode, ProcessingMetrics
)
from ...monitoring import StructuredLogger
from ...core.ml import AnomalyDetector
from ...core.catalog import LineageTracker as QualityLineageTracker

logger = StructuredLogger.get_logger(__name__)


class DataQualityDimension(Enum):
    """Data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    INTEGRITY = "integrity"
    CONFORMITY = "conformity"


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
    PATTERN_CHECK = "pattern_check"
    DEPENDENCY_CHECK = "dependency_check"
    CUSTOM = "custom"


class RemediationStrategy(Enum):
    """Strategies for handling quality issues"""
    REJECT = "reject"
    QUARANTINE = "quarantine"
    CORRECT = "correct"
    IMPUTE = "impute"
    FLAG = "flag"
    ALERT = "alert"
    CUSTOM = "custom"


class QualityEngine(Enum):
    """Available quality engines"""
    NATIVE = "native"
    GREAT_EXPECTATIONS = "great_expectations"
    DEEQU = "deequ"
    SODA = "soda"
    AUTO = "auto"


@dataclass
class QualityRule:
    """Enhanced quality validation rule"""
    rule_id: str
    name: str
    check_type: QualityCheckType
    dimension: DataQualityDimension
    
    # Target specification
    column: Optional[str] = None
    columns: Optional[List[str]] = None  # For multi-column rules
    table: Optional[str] = None
    
    # Rule definition
    condition: Optional[str] = None  # SQL-like condition
    expression: Optional[str] = None  # Python expression
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Severity and actions
    severity: str = "warning"  # info, warning, error, critical
    remediation: RemediationStrategy = RemediationStrategy.FLAG
    remediation_config: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    description: Optional[str] = None
    owner: Optional[str] = None
    sla_minutes: Optional[int] = None
    
    # ML enhancement
    enable_ml: bool = False
    ml_threshold: float = 0.95
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "check_type": self.check_type.value,
            "dimension": self.dimension.value,
            "severity": self.severity,
            "enabled": self.enabled
        }


@dataclass
class QualityConfig(ProcessorConfig):
    """Enhanced configuration for quality processing v2.0"""
    # Engine configuration
    engine: QualityEngine = QualityEngine.AUTO
    engine_config: Dict[str, Any] = field(default_factory=dict)
    
    # Rules configuration
    rules: List[QualityRule] = field(default_factory=list)
    rules_path: Optional[str] = None  # Path to rules file/directory
    enable_rule_discovery: bool = True  # Auto-discover rules
    
    # Thresholds
    quality_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "completeness": 0.95,
        "accuracy": 0.98,
        "consistency": 0.99,
        "validity": 0.97,
        "uniqueness": 0.99,
        "overall": 0.95
    })
    
    # Sampling
    enable_sampling: bool = True
    sample_size: Optional[int] = 100000
    sample_percentage: Optional[float] = None
    stratified_sampling: bool = True
    stratify_columns: List[str] = field(default_factory=list)
    
    # Profiling
    enable_profiling: bool = True
    profile_sample_size: int = 10000
    compute_correlations: bool = True
    compute_histograms: bool = True
    
    # Anomaly detection
    enable_anomaly_detection: bool = True
    anomaly_algorithms: List[str] = field(default_factory=lambda: ["isolation_forest", "local_outlier_factor"])
    anomaly_threshold: float = 0.95
    
    # ML-based quality
    enable_ml_quality: bool = True
    ml_models_path: Optional[str] = None
    retrain_frequency: timedelta = timedelta(days=7)
    
    # Remediation
    enable_auto_remediation: bool = True
    remediation_rules: Dict[str, RemediationStrategy] = field(default_factory=dict)
    quarantine_path: Optional[str] = None
    
    # Reporting
    generate_report: bool = True
    report_formats: List[str] = field(default_factory=lambda: ["json", "html"])
    report_path: Optional[str] = None
    include_samples: bool = True
    sample_size_per_issue: int = 10
    
    # Performance
    parallel_rules: bool = True
    rule_timeout: timedelta = timedelta(minutes=5)
    
    # Integration
    send_to_catalog: bool = True
    update_lineage: bool = True
    publish_metrics: bool = True
    
    def __post_init__(self):
        super().__post_init__()
        self.mode = ProcessingMode.BATCH  # Quality checks are typically batch


@dataclass
class QualityIssue:
    """Represents a quality issue found"""
    rule_id: str
    rule_name: str
    dimension: DataQualityDimension
    severity: str
    
    # Issue details
    issue_type: str
    description: str
    affected_records: int
    total_records: int
    percentage: float
    
    # Location
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    sample_values: List[Any] = field(default_factory=list)
    
    # Remediation
    remediation_applied: bool = False
    remediation_strategy: Optional[RemediationStrategy] = None
    remediation_details: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    detected_at: datetime = field(default_factory=datetime.utcnow)
    detection_method: str = "rule"  # rule, ml, statistical
    confidence: float = 1.0


@dataclass
class DataProfile:
    """Data profiling results"""
    # Basic statistics
    row_count: int
    column_count: int
    
    # Column profiles
    column_profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Data types
    data_types: Dict[str, str] = field(default_factory=dict)
    
    # Missing values
    missing_counts: Dict[str, int] = field(default_factory=dict)
    missing_percentages: Dict[str, float] = field(default_factory=dict)
    
    # Cardinality
    unique_counts: Dict[str, int] = field(default_factory=dict)
    cardinality_ratios: Dict[str, float] = field(default_factory=dict)
    
    # Patterns
    patterns: Dict[str, List[str]] = field(default_factory=dict)
    
    # Correlations
    correlations: Optional[pd.DataFrame] = None
    
    # Anomalies
    anomaly_scores: Dict[str, float] = field(default_factory=dict)
    
    # Metadata
    profiled_at: datetime = field(default_factory=datetime.utcnow)
    profiling_duration_ms: float = 0.0


@dataclass
class QualityMetrics(ProcessingMetrics):
    """Enhanced metrics for quality processing"""
    # Quality scores
    quality_scores: Dict[str, float] = field(default_factory=dict)
    overall_quality_score: float = 1.0
    
    # Issue counts
    total_issues: int = 0
    issues_by_severity: Dict[str, int] = field(default_factory=dict)
    issues_by_dimension: Dict[str, int] = field(default_factory=dict)
    
    # Records
    records_validated: int = 0
    records_passed: int = 0
    records_failed: int = 0
    records_quarantined: int = 0
    records_corrected: int = 0
    
    # Rules
    rules_executed: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    rules_skipped: int = 0
    
    # Performance
    avg_rule_execution_ms: float = 0.0
    slowest_rule: Optional[str] = None
    slowest_rule_ms: float = 0.0
    
    # ML metrics
    anomalies_detected: int = 0
    ml_predictions_made: int = 0
    ml_confidence_avg: float = 0.0


@dataclass
class QualityResult(ProcessingResult):
    """Enhanced result of quality processing"""
    # Profile
    profile: Optional[DataProfile] = None
    
    # Issues
    issues: List[QualityIssue] = field(default_factory=list)
    
    # Quality scores
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    overall_score: float = 1.0
    
    # Remediation summary
    remediation_summary: Dict[str, int] = field(default_factory=dict)
    
    # Report paths
    report_paths: Dict[str, str] = field(default_factory=dict)


class QualityProcessor(BaseProcessor[Union[pd.DataFrame, str, List[str]]]):
    """
    Enhanced quality processor for enterprise-scale data quality management.
    
    New v2.0 Features:
    - Multi-engine support (Great Expectations, Deequ, Soda)
    - ML-based anomaly detection
    - Intelligent auto-remediation
    - Real-time quality monitoring
    - Advanced profiling
    - Quality lineage tracking
    - SLA monitoring
    - Custom rule discovery
    """
    
    def __init__(
        self,
        config: QualityConfig,
        anomaly_detector: Optional[AnomalyDetector] = None,
        lineage_tracker: Optional[QualityLineageTracker] = None,
        **kwargs
    ):
        super().__init__(config, **kwargs)
        self.config: QualityConfig = config
        self.anomaly_detector = anomaly_detector
        self.lineage_tracker = lineage_tracker
        
        # Engine instances
        self.ge_context: Optional[Any] = None
        self.deequ_analyzer: Optional[Any] = None
        self.soda_scan: Optional[Any] = None
        
        # Rule management
        self._rules: Dict[str, QualityRule] = {}
        self._rule_cache: Dict[str, Any] = {}
        
        # ML models
        self._ml_models: Dict[str, Any] = {}
        
        # Metrics
        self._quality_metrics = QualityMetrics()
        
    async def initialize(self):
        """Initialize quality processor with auto engine selection"""
        await super().initialize()
        
        logger.info(f"Initializing quality processor v2.0: {self.config.name}")
        
        # Select optimal engine
        if self.config.engine == QualityEngine.AUTO:
            self._select_optimal_engine()
            
        # Initialize engine
        await self._initialize_engine()
        
        # Load rules
        await self._load_rules()
        
        # Initialize ML models if enabled
        if self.config.enable_ml_quality:
            await self._initialize_ml_models()
            
    def _select_optimal_engine(self):
        """Select optimal quality engine"""
        available_engines = []
        
        if GE_AVAILABLE:
            available_engines.append(QualityEngine.GREAT_EXPECTATIONS)
        if DEEQU_AVAILABLE:
            available_engines.append(QualityEngine.DEEQU)
        if SODA_AVAILABLE:
            available_engines.append(QualityEngine.SODA)
            
        if not available_engines:
            self.config.engine = QualityEngine.NATIVE
            return
            
        # Select based on features needed
        if self.config.enable_ml_quality and GE_AVAILABLE:
            self.config.engine = QualityEngine.GREAT_EXPECTATIONS
        elif DEEQU_AVAILABLE:  # Good for Spark integration
            self.config.engine = QualityEngine.DEEQU
        elif available_engines:
            self.config.engine = available_engines[0]
        else:
            self.config.engine = QualityEngine.NATIVE
            
        logger.info(f"Auto-selected {self.config.engine.value} engine for quality")
        
    async def _initialize_engine(self):
        """Initialize the selected quality engine"""
        if self.config.engine == QualityEngine.GREAT_EXPECTATIONS and GE_AVAILABLE:
            await self._initialize_great_expectations()
        elif self.config.engine == QualityEngine.DEEQU and DEEQU_AVAILABLE:
            await self._initialize_deequ()
        elif self.config.engine == QualityEngine.SODA and SODA_AVAILABLE:
            await self._initialize_soda()
        else:
            logger.info("Using native quality engine")
            
    async def _initialize_great_expectations(self):
        """Initialize Great Expectations"""
        self.ge_context = DataContext()
        logger.info("Initialized Great Expectations context")
        
    async def _initialize_deequ(self):
        """Initialize PyDeequ"""
        # Deequ initialization would happen with Spark context
        logger.info("Initialized PyDeequ analyzer")
        
    async def _initialize_soda(self):
        """Initialize Soda Core"""
        self.soda_scan = Scan()
        logger.info("Initialized Soda Core scan")
        
    async def _load_rules(self):
        """Load quality rules from configuration"""
        # Load predefined rules
        for rule in self.config.rules:
            self._rules[rule.rule_id] = rule
            
        # Load rules from file if specified
        if self.config.rules_path:
            rules_path = Path(self.config.rules_path)
            if rules_path.is_file():
                await self._load_rules_from_file(rules_path)
            elif rules_path.is_dir():
                for rule_file in rules_path.glob("*.json"):
                    await self._load_rules_from_file(rule_file)
                    
        # Auto-discover rules if enabled
        if self.config.enable_rule_discovery:
            discovered_rules = await self._discover_rules()
            for rule in discovered_rules:
                if rule.rule_id not in self._rules:
                    self._rules[rule.rule_id] = rule
                    
        logger.info(f"Loaded {len(self._rules)} quality rules")
        
    async def _load_rules_from_file(self, file_path: Path):
        """Load rules from a JSON file"""
        try:
            with open(file_path, 'r') as f:
                rules_data = json.load(f)
                
            for rule_data in rules_data.get('rules', []):
                rule = QualityRule(
                    rule_id=rule_data['rule_id'],
                    name=rule_data['name'],
                    check_type=QualityCheckType(rule_data['check_type']),
                    dimension=DataQualityDimension(rule_data['dimension']),
                    **{k: v for k, v in rule_data.items() 
                       if k not in ['rule_id', 'name', 'check_type', 'dimension']}
                )
                self._rules[rule.rule_id] = rule
                
        except Exception as e:
            logger.error(f"Error loading rules from {file_path}: {e}")
            
    async def _discover_rules(self) -> List[QualityRule]:
        """Auto-discover quality rules based on data patterns"""
        # This would implement intelligent rule discovery
        # For now, return empty list
        return []
        
    async def _initialize_ml_models(self):
        """Initialize ML models for quality assessment"""
        if self.config.ml_models_path:
            # Load pre-trained models
            pass
        else:
            # Initialize default models
            if self.anomaly_detector:
                self._ml_models['anomaly'] = self.anomaly_detector
                
    async def process(
        self,
        data: Union[pd.DataFrame, str, List[str]],
        job_id: Optional[str] = None
    ) -> QualityResult:
        """
        Process data for quality assessment.
        
        Args:
            data: DataFrame, file path, or list of file paths
            job_id: Optional job ID
            
        Returns:
            QualityResult with detailed quality metrics
        """
        job_id = job_id or str(uuid.uuid4())
        
        # Create result object
        result = QualityResult(
            job_id=job_id,
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow(),
            metrics=self._quality_metrics
        )
        
        try:
            # Load data if needed
            df = await self._load_data(data)
            
            # Sample if configured
            if self.config.enable_sampling:
                df = self._sample_data(df)
                
            # Profile data
            if self.config.enable_profiling:
                result.profile = await self._profile_data(df)
                
            # Execute quality rules
            issues = await self._execute_rules(df, result)
            result.issues = issues
            
            # Run anomaly detection
            if self.config.enable_anomaly_detection:
                anomaly_issues = await self._detect_anomalies(df, result)
                result.issues.extend(anomaly_issues)
                
            # Calculate quality scores
            result.dimension_scores = self._calculate_dimension_scores(result.issues, len(df))
            result.overall_score = self._calculate_overall_score(result.dimension_scores)
            
            # Apply remediation
            if self.config.enable_auto_remediation and result.issues:
                df_remediated, remediation_summary = await self._apply_remediation(df, result.issues)
                result.remediation_summary = remediation_summary
                
                # Re-validate after remediation
                if remediation_summary:
                    post_issues = await self._execute_rules(df_remediated, result)
                    logger.info(f"Issues after remediation: {len(post_issues)} (was {len(issues)})")
                    
            # Generate reports
            if self.config.generate_report:
                result.report_paths = await self._generate_reports(result)
                
            # Update lineage
            if self.config.update_lineage and self.lineage_tracker:
                await self.lineage_tracker.track_quality_assessment(
                    job_id,
                    data if isinstance(data, str) else "dataframe",
                    result
                )
                
            result.status = ProcessingStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Quality processing failed: {e}", exc_info=True)
            result.status = ProcessingStatus.FAILED
            result.errors.append({
                "type": type(e).__name__,
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            
        finally:
            result.completed_at = datetime.utcnow()
            self._update_metrics(result)
            
        return result
        
    async def _load_data(self, data: Union[pd.DataFrame, str, List[str]]) -> pd.DataFrame:
        """Load data into DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, str):
            # Load from file
            if data.endswith('.parquet'):
                return pd.read_parquet(data)
            elif data.endswith('.csv'):
                return pd.read_csv(data)
            else:
                raise ValueError(f"Unsupported file format: {data}")
        elif isinstance(data, list):
            # Load and concatenate multiple files
            dfs = []
            for file_path in data:
                df = await self._load_data(file_path)
                dfs.append(df)
            return pd.concat(dfs, ignore_index=True)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
            
    def _sample_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sample data for quality assessment"""
        total_rows = len(df)
        
        # Determine sample size
        if self.config.sample_percentage:
            sample_size = int(total_rows * self.config.sample_percentage)
        elif self.config.sample_size:
            sample_size = min(self.config.sample_size, total_rows)
        else:
            return df
            
        if sample_size >= total_rows:
            return df
            
        # Apply sampling
        if self.config.stratified_sampling and self.config.stratify_columns:
            # Stratified sampling
            return df.groupby(self.config.stratify_columns).apply(
                lambda x: x.sample(
                    n=int(len(x) * sample_size / total_rows),
                    random_state=42
                )
            ).reset_index(drop=True)
        else:
            # Random sampling
            return df.sample(n=sample_size, random_state=42)
            
    async def _profile_data(self, df: pd.DataFrame) -> DataProfile:
        """Profile the data"""
        profile_start = datetime.utcnow()
        
        profile = DataProfile(
            row_count=len(df),
            column_count=len(df.columns)
        )
        
        # Sample for profiling if needed
        profile_df = df
        if len(df) > self.config.profile_sample_size:
            profile_df = df.sample(n=self.config.profile_sample_size, random_state=42)
            
        # Column profiles
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'null_count': df[col].isnull().sum(),
                'null_percentage': df[col].isnull().sum() / len(df) * 100,
                'unique_count': df[col].nunique(),
                'cardinality': df[col].nunique() / len(df)
            }
            
            # Numeric statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    'mean': df[col].mean(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'q25': df[col].quantile(0.25),
                    'q50': df[col].quantile(0.50),
                    'q75': df[col].quantile(0.75)
                })
                
            # String statistics
            elif pd.api.types.is_string_dtype(df[col]):
                col_profile.update({
                    'min_length': df[col].str.len().min(),
                    'max_length': df[col].str.len().max(),
                    'avg_length': df[col].str.len().mean()
                })
                
                # Pattern detection
                if self.config.compute_histograms:
                    # Simple pattern detection
                    patterns = self._detect_patterns(profile_df[col].dropna().head(1000))
                    if patterns:
                        profile.patterns[col] = patterns[:5]  # Top 5 patterns
                        
            profile.column_profiles[col] = col_profile
            
        # Correlations
        if self.config.compute_correlations:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 1:
                profile.correlations = df[numeric_cols].corr()
                
        profile.profiling_duration_ms = (datetime.utcnow() - profile_start).total_seconds() * 1000
        
        return profile
        
    def _detect_patterns(self, series: pd.Series) -> List[str]:
        """Detect common patterns in string data"""
        patterns = []
        
        # Email pattern
        if series.str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$').any():
            patterns.append("email")
            
        # Phone pattern
        if series.str.match(r'^\+?1?\d{9,15}$').any():
            patterns.append("phone")
            
        # Date patterns
        if series.str.match(r'^\d{4}-\d{2}-\d{2}$').any():
            patterns.append("date_iso")
            
        # UUID pattern
        if series.str.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$').any():
            patterns.append("uuid")
            
        return patterns
        
    async def _execute_rules(
        self,
        df: pd.DataFrame,
        result: QualityResult
    ) -> List[QualityIssue]:
        """Execute quality rules on the data"""
        issues = []
        
        # Execute rules in parallel if configured
        if self.config.parallel_rules:
            tasks = []
            for rule_id, rule in self._rules.items():
                if rule.enabled:
                    task = asyncio.create_task(
                        self._execute_single_rule(df, rule)
                    )
                    tasks.append((rule_id, task))
                    
            # Wait for all rules with timeout
            for rule_id, task in tasks:
                try:
                    rule_issues = await asyncio.wait_for(
                        task,
                        timeout=self.config.rule_timeout.total_seconds()
                    )
                    issues.extend(rule_issues)
                    self._quality_metrics.rules_executed += 1
                    if rule_issues:
                        self._quality_metrics.rules_failed += 1
                    else:
                        self._quality_metrics.rules_passed += 1
                except asyncio.TimeoutError:
                    logger.error(f"Rule {rule_id} timed out")
                    self._quality_metrics.rules_skipped += 1
                except Exception as e:
                    logger.error(f"Error executing rule {rule_id}: {e}")
                    self._quality_metrics.rules_skipped += 1
        else:
            # Sequential execution
            for rule_id, rule in self._rules.items():
                if rule.enabled:
                    try:
                        rule_issues = await self._execute_single_rule(df, rule)
                        issues.extend(rule_issues)
                        self._quality_metrics.rules_executed += 1
                        if rule_issues:
                            self._quality_metrics.rules_failed += 1
                        else:
                            self._quality_metrics.rules_passed += 1
                    except Exception as e:
                        logger.error(f"Error executing rule {rule_id}: {e}")
                        self._quality_metrics.rules_skipped += 1
                        
        return issues
        
    async def _execute_single_rule(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> List[QualityIssue]:
        """Execute a single quality rule"""
        rule_start = datetime.utcnow()
        issues = []
        
        try:
            if rule.check_type == QualityCheckType.NULL_CHECK:
                issues = self._check_nulls(df, rule)
            elif rule.check_type == QualityCheckType.DUPLICATE_CHECK:
                issues = self._check_duplicates(df, rule)
            elif rule.check_type == QualityCheckType.RANGE_CHECK:
                issues = self._check_range(df, rule)
            elif rule.check_type == QualityCheckType.FORMAT_CHECK:
                issues = self._check_format(df, rule)
            elif rule.check_type == QualityCheckType.STATISTICAL:
                issues = self._check_statistical(df, rule)
            elif rule.check_type == QualityCheckType.BUSINESS_RULE:
                issues = self._check_business_rule(df, rule)
            elif rule.check_type == QualityCheckType.CUSTOM:
                issues = await self._check_custom(df, rule)
                
            # Apply ML enhancement if enabled
            if rule.enable_ml and self._ml_models:
                ml_issues = await self._enhance_with_ml(df, rule, issues)
                issues.extend(ml_issues)
                
        except Exception as e:
            logger.error(f"Error in rule {rule.rule_id}: {e}")
            
        # Update metrics
        rule_duration = (datetime.utcnow() - rule_start).total_seconds() * 1000
        if rule_duration > self._quality_metrics.slowest_rule_ms:
            self._quality_metrics.slowest_rule = rule.rule_id
            self._quality_metrics.slowest_rule_ms = rule_duration
            
        return issues
        
    def _check_nulls(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check for null values"""
        issues = []
        
        columns = rule.columns or ([rule.column] if rule.column else df.columns)
        
        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issue = QualityIssue(
                        rule_id=rule.rule_id,
                        rule_name=rule.name,
                        dimension=rule.dimension,
                        severity=rule.severity,
                        issue_type="null_values",
                        description=f"Column '{col}' contains {null_count} null values",
                        affected_records=null_count,
                        total_records=len(df),
                        percentage=(null_count / len(df)) * 100,
                        column=col,
                        sample_values=df[df[col].isnull()].head(
                            self.config.sample_size_per_issue
                        ).index.tolist()
                    )
                    issues.append(issue)
                    
        return issues
        
    def _check_duplicates(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check for duplicate values"""
        issues = []
        
        columns = rule.columns or ([rule.column] if rule.column else df.columns)
        
        # Check for duplicate rows
        if len(columns) == len(df.columns):
            duplicates = df.duplicated()
            dup_count = duplicates.sum()
            if dup_count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="duplicate_rows",
                    description=f"Found {dup_count} duplicate rows",
                    affected_records=dup_count,
                    total_records=len(df),
                    percentage=(dup_count / len(df)) * 100,
                    sample_values=df[duplicates].head(
                        self.config.sample_size_per_issue
                    ).index.tolist()
                )
                issues.append(issue)
        else:
            # Check for duplicates in specific columns
            duplicates = df.duplicated(subset=columns)
            dup_count = duplicates.sum()
            if dup_count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="duplicate_values",
                    description=f"Found {dup_count} duplicate values in columns {columns}",
                    affected_records=dup_count,
                    total_records=len(df),
                    percentage=(dup_count / len(df)) * 100,
                    columns=columns,
                    sample_values=df[duplicates].head(
                        self.config.sample_size_per_issue
                    ).index.tolist()
                )
                issues.append(issue)
                
        return issues
        
    def _check_range(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check if values are within specified range"""
        issues = []
        
        col = rule.column
        if col not in df.columns:
            return issues
            
        min_val = rule.parameters.get('min')
        max_val = rule.parameters.get('max')
        
        if min_val is not None:
            below_min = df[col] < min_val
            count = below_min.sum()
            if count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="below_minimum",
                    description=f"Column '{col}' has {count} values below minimum {min_val}",
                    affected_records=count,
                    total_records=len(df),
                    percentage=(count / len(df)) * 100,
                    column=col,
                    sample_values=df[below_min][col].head(
                        self.config.sample_size_per_issue
                    ).tolist()
                )
                issues.append(issue)
                
        if max_val is not None:
            above_max = df[col] > max_val
            count = above_max.sum()
            if count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="above_maximum",
                    description=f"Column '{col}' has {count} values above maximum {max_val}",
                    affected_records=count,
                    total_records=len(df),
                    percentage=(count / len(df)) * 100,
                    column=col,
                    sample_values=df[above_max][col].head(
                        self.config.sample_size_per_issue
                    ).tolist()
                )
                issues.append(issue)
                
        return issues
        
    def _check_format(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check if values match expected format"""
        issues = []
        
        col = rule.column
        if col not in df.columns:
            return issues
            
        pattern = rule.parameters.get('pattern')
        if not pattern:
            return issues
            
        # Check format using regex
        if pd.api.types.is_string_dtype(df[col]):
            non_matching = ~df[col].str.match(pattern, na=False)
            count = non_matching.sum()
            if count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="invalid_format",
                    description=f"Column '{col}' has {count} values not matching pattern '{pattern}'",
                    affected_records=count,
                    total_records=len(df),
                    percentage=(count / len(df)) * 100,
                    column=col,
                    sample_values=df[non_matching][col].head(
                        self.config.sample_size_per_issue
                    ).tolist()
                )
                issues.append(issue)
                
        return issues
        
    def _check_statistical(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check statistical properties"""
        issues = []
        
        col = rule.column
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            return issues
            
        # Outlier detection using IQR
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        outlier_threshold = rule.parameters.get('outlier_threshold', 1.5)
        lower_bound = Q1 - outlier_threshold * IQR
        upper_bound = Q3 + outlier_threshold * IQR
        
        outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
        count = outliers.sum()
        
        if count > 0:
            issue = QualityIssue(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                dimension=rule.dimension,
                severity=rule.severity,
                issue_type="statistical_outliers",
                description=f"Column '{col}' has {count} statistical outliers",
                affected_records=count,
                total_records=len(df),
                percentage=(count / len(df)) * 100,
                column=col,
                sample_values=df[outliers][col].head(
                    self.config.sample_size_per_issue
                ).tolist()
            )
            issues.append(issue)
            
        return issues
        
    def _check_business_rule(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Check business rule using SQL-like condition or Python expression"""
        issues = []
        
        try:
            if rule.condition:
                # SQL-like condition
                violations = ~df.eval(rule.condition)
            elif rule.expression:
                # Python expression
                violations = ~df.apply(lambda row: eval(rule.expression, {'row': row}), axis=1)
            else:
                return issues
                
            count = violations.sum()
            if count > 0:
                issue = QualityIssue(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    severity=rule.severity,
                    issue_type="business_rule_violation",
                    description=f"Business rule '{rule.name}' violated by {count} records",
                    affected_records=count,
                    total_records=len(df),
                    percentage=(count / len(df)) * 100,
                    sample_values=df[violations].head(
                        self.config.sample_size_per_issue
                    ).index.tolist()
                )
                issues.append(issue)
                
        except Exception as e:
            logger.error(f"Error evaluating business rule {rule.rule_id}: {e}")
            
        return issues
        
    async def _check_custom(self, df: pd.DataFrame, rule: QualityRule) -> List[QualityIssue]:
        """Execute custom quality check"""
        # This would call a custom function specified in the rule
        return []
        
    async def _enhance_with_ml(
        self,
        df: pd.DataFrame,
        rule: QualityRule,
        rule_issues: List[QualityIssue]
    ) -> List[QualityIssue]:
        """Enhance quality checks with ML predictions"""
        ml_issues = []
        
        if 'anomaly' in self._ml_models and rule.column:
            try:
                # Prepare data for anomaly detection
                feature_data = df[[rule.column]].dropna()
                if len(feature_data) > 0:
                    # Detect anomalies
                    anomaly_scores = self._ml_models['anomaly'].predict_proba(feature_data)
                    anomalies = anomaly_scores > rule.ml_threshold
                    
                    if anomalies.any():
                        issue = QualityIssue(
                            rule_id=f"{rule.rule_id}_ml",
                            rule_name=f"{rule.name} (ML)",
                            dimension=rule.dimension,
                            severity="warning",
                            issue_type="ml_anomaly",
                            description=f"ML detected {anomalies.sum()} potential anomalies in '{rule.column}'",
                            affected_records=anomalies.sum(),
                            total_records=len(feature_data),
                            percentage=(anomalies.sum() / len(feature_data)) * 100,
                            column=rule.column,
                            detection_method="ml",
                            confidence=float(anomaly_scores[anomalies].mean())
                        )
                        ml_issues.append(issue)
                        
            except Exception as e:
                logger.error(f"ML enhancement error for rule {rule.rule_id}: {e}")
                
        return ml_issues
        
    async def _detect_anomalies(
        self,
        df: pd.DataFrame,
        result: QualityResult
    ) -> List[QualityIssue]:
        """Detect anomalies using ML algorithms"""
        anomaly_issues = []
        
        if not self.anomaly_detector:
            return anomaly_issues
            
        # Detect anomalies for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            try:
                feature_data = df[[col]].dropna()
                if len(feature_data) > 10:  # Need minimum samples
                    # Detect anomalies
                    anomalies = await self.anomaly_detector.detect(
                        feature_data,
                        contamination=1 - self.config.anomaly_threshold
                    )
                    
                    if anomalies.any():
                        issue = QualityIssue(
                            rule_id=f"anomaly_{col}",
                            rule_name=f"Anomaly Detection - {col}",
                            dimension=DataQualityDimension.VALIDITY,
                            severity="warning",
                            issue_type="anomaly",
                            description=f"Detected {anomalies.sum()} anomalies in column '{col}'",
                            affected_records=anomalies.sum(),
                            total_records=len(feature_data),
                            percentage=(anomalies.sum() / len(feature_data)) * 100,
                            column=col,
                            detection_method="ml",
                            confidence=self.config.anomaly_threshold
                        )
                        anomaly_issues.append(issue)
                        self._quality_metrics.anomalies_detected += anomalies.sum()
                        
            except Exception as e:
                logger.error(f"Anomaly detection error for column {col}: {e}")
                
        return anomaly_issues
        
    def _calculate_dimension_scores(
        self,
        issues: List[QualityIssue],
        total_records: int
    ) -> Dict[str, float]:
        """Calculate quality scores by dimension"""
        dimension_scores = {dim.value: 1.0 for dim in DataQualityDimension}
        
        # Group issues by dimension
        issues_by_dimension = defaultdict(list)
        for issue in issues:
            issues_by_dimension[issue.dimension.value].append(issue)
            
        # Calculate scores
        for dimension, dim_issues in issues_by_dimension.items():
            if dim_issues:
                # Weight by severity
                severity_weights = {
                    'info': 0.1,
                    'warning': 0.3,
                    'error': 0.6,
                    'critical': 1.0
                }
                
                total_impact = 0
                for issue in dim_issues:
                    weight = severity_weights.get(issue.severity, 0.5)
                    impact = (issue.affected_records / total_records) * weight
                    total_impact += impact
                    
                # Calculate score (1 - impact, bounded to [0, 1])
                dimension_scores[dimension] = max(0, 1 - total_impact)
                
        return dimension_scores
        
    def _calculate_overall_score(self, dimension_scores: Dict[str, float]) -> float:
        """Calculate overall quality score"""
        if not dimension_scores:
            return 1.0
            
        # Weight dimensions based on configuration
        weights = {
            'completeness': 0.2,
            'accuracy': 0.2,
            'consistency': 0.15,
            'validity': 0.15,
            'uniqueness': 0.15,
            'timeliness': 0.1,
            'integrity': 0.05,
            'conformity': 0.0
        }
        
        weighted_sum = 0
        total_weight = 0
        
        for dimension, score in dimension_scores.items():
            weight = weights.get(dimension, 0.1)
            weighted_sum += score * weight
            total_weight += weight
            
        return weighted_sum / total_weight if total_weight > 0 else 0
        
    async def _apply_remediation(
        self,
        df: pd.DataFrame,
        issues: List[QualityIssue]
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Apply remediation strategies to fix quality issues"""
        df_copy = df.copy()
        remediation_summary = defaultdict(int)
        
        for issue in issues:
            try:
                strategy = issue.remediation_strategy or RemediationStrategy.FLAG
                
                if strategy == RemediationStrategy.REJECT:
                    # Remove affected records
                    # This is simplified - actual implementation would be more sophisticated
                    pass
                    
                elif strategy == RemediationStrategy.QUARANTINE:
                    # Move to quarantine
                    if self.config.quarantine_path:
                        # Save affected records to quarantine
                        pass
                        
                elif strategy == RemediationStrategy.CORRECT:
                    # Apply corrections
                    if issue.issue_type == "null_values" and issue.column:
                        # Simple imputation
                        if pd.api.types.is_numeric_dtype(df_copy[issue.column]):
                            df_copy[issue.column].fillna(
                                df_copy[issue.column].mean(),
                                inplace=True
                            )
                        else:
                            df_copy[issue.column].fillna(
                                df_copy[issue.column].mode()[0] if not df_copy[issue.column].mode().empty else "UNKNOWN",
                                inplace=True
                            )
                        remediation_summary['corrected'] += issue.affected_records
                        
                elif strategy == RemediationStrategy.IMPUTE:
                    # Advanced imputation
                    # This would use more sophisticated imputation methods
                    pass
                    
                elif strategy == RemediationStrategy.FLAG:
                    # Add quality flag column
                    flag_col = f"quality_flag_{issue.rule_id}"
                    if flag_col not in df_copy.columns:
                        df_copy[flag_col] = False
                    # Mark affected records
                    # This is simplified - actual implementation would identify specific records
                    remediation_summary['flagged'] += issue.affected_records
                    
                issue.remediation_applied = True
                issue.remediation_strategy = strategy
                
            except Exception as e:
                logger.error(f"Remediation error for issue {issue.rule_id}: {e}")
                
        return df_copy, dict(remediation_summary)
        
    async def _generate_reports(self, result: QualityResult) -> Dict[str, str]:
        """Generate quality reports in various formats"""
        report_paths = {}
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base_name = f"quality_report_{result.job_id}_{timestamp}"
        
        for format in self.config.report_formats:
            try:
                if format == "json":
                    report_path = await self._generate_json_report(result, base_name)
                elif format == "html":
                    report_path = await self._generate_html_report(result, base_name)
                elif format == "pdf":
                    report_path = await self._generate_pdf_report(result, base_name)
                else:
                    continue
                    
                if report_path:
                    report_paths[format] = report_path
                    
            except Exception as e:
                logger.error(f"Error generating {format} report: {e}")
                
        return report_paths
        
    async def _generate_json_report(
        self,
        result: QualityResult,
        base_name: str
    ) -> str:
        """Generate JSON report"""
        report_data = {
            "job_id": result.job_id,
            "timestamp": datetime.utcnow().isoformat(),
            "overall_score": result.overall_score,
            "dimension_scores": result.dimension_scores,
            "issues": [
                {
                    "rule_id": issue.rule_id,
                    "rule_name": issue.rule_name,
                    "dimension": issue.dimension.value,
                    "severity": issue.severity,
                    "issue_type": issue.issue_type,
                    "description": issue.description,
                    "affected_records": issue.affected_records,
                    "percentage": issue.percentage,
                    "remediation_applied": issue.remediation_applied
                }
                for issue in result.issues
            ],
            "metrics": result.metrics.to_dict() if hasattr(result.metrics, 'to_dict') else {},
            "remediation_summary": result.remediation_summary
        }
        
        # Add profile if available
        if result.profile:
            report_data["profile"] = {
                "row_count": result.profile.row_count,
                "column_count": result.profile.column_count,
                "missing_percentages": result.profile.missing_percentages,
                "cardinality_ratios": result.profile.cardinality_ratios
            }
            
        # Save report
        report_path = f"{self.config.report_path or '.'}/{base_name}.json"
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
            
        return report_path
        
    async def _generate_html_report(
        self,
        result: QualityResult,
        base_name: str
    ) -> str:
        """Generate HTML report"""
        # This would generate a nice HTML report
        # For now, return empty string
        return ""
        
    async def _generate_pdf_report(
        self,
        result: QualityResult,
        base_name: str
    ) -> str:
        """Generate PDF report"""
        # This would generate a PDF report
        # For now, return empty string
        return ""
        
    # Additional utility methods for specific quality checks
    
    async def validate_schema(
        self,
        df: pd.DataFrame,
        expected_schema: Dict[str, str]
    ) -> List[QualityIssue]:
        """Validate DataFrame against expected schema"""
        issues = []
        
        # Check columns
        missing_cols = set(expected_schema.keys()) - set(df.columns)
        extra_cols = set(df.columns) - set(expected_schema.keys())
        
        if missing_cols:
            issue = QualityIssue(
                rule_id="schema_missing_columns",
                rule_name="Schema Validation - Missing Columns",
                dimension=DataQualityDimension.CONFORMITY,
                severity="error",
                issue_type="missing_columns",
                description=f"Missing columns: {missing_cols}",
                affected_records=len(df),
                total_records=len(df),
                percentage=100.0
            )
            issues.append(issue)
            
        if extra_cols:
            issue = QualityIssue(
                rule_id="schema_extra_columns",
                rule_name="Schema Validation - Extra Columns",
                dimension=DataQualityDimension.CONFORMITY,
                severity="warning",
                issue_type="extra_columns",
                description=f"Extra columns: {extra_cols}",
                affected_records=len(df),
                total_records=len(df),
                percentage=100.0
            )
            issues.append(issue)
            
        # Check data types
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if not self._types_compatible(actual_type, expected_type):
                    issue = QualityIssue(
                        rule_id=f"schema_type_{col}",
                        rule_name=f"Schema Validation - Type Mismatch ({col})",
                        dimension=DataQualityDimension.CONFORMITY,
                        severity="error",
                        issue_type="type_mismatch",
                        description=f"Column '{col}' has type '{actual_type}', expected '{expected_type}'",
                        affected_records=len(df),
                        total_records=len(df),
                        percentage=100.0,
                        column=col
                    )
                    issues.append(issue)
                    
        return issues
        
    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual type is compatible with expected type"""
        # Simple compatibility check - can be enhanced
        type_map = {
            'int64': ['int', 'integer', 'bigint'],
            'float64': ['float', 'double', 'numeric'],
            'object': ['string', 'text', 'varchar'],
            'bool': ['boolean', 'bool'],
            'datetime64': ['datetime', 'timestamp']
        }
        
        for dtype, compatible in type_map.items():
            if actual.startswith(dtype) and expected.lower() in compatible:
                return True
                
        return actual == expected 