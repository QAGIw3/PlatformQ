"""
Apache Deequ Client Integration

Provides data quality unit testing using Apache Deequ.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
from pyspark.sql import SparkSession, DataFrame
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.suggestions import *
from pydeequ.profiles import *
from pydeequ.repository import *
from pydeequ.metrics import *

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class CheckLevel(str, Enum):
    """Deequ check levels"""
    ERROR = "Error"
    WARNING = "Warning"


class ConstraintStatus(str, Enum):
    """Constraint status"""
    SUCCESS = "Success"
    FAILURE = "Failure"


@dataclass
class DeequConfig(ClientConfig):
    """Configuration for Deequ client"""
    spark_master: str = "local[*]"
    app_name: str = "DeequDataQuality"
    
    # Repository settings
    enable_repository: bool = True
    metrics_repository_path: str = "s3://datalake/deequ/metrics"
    
    # Analysis settings
    enable_profiling: bool = True
    enable_suggestions: bool = True
    suggestion_rules: Dict[str, Any] = field(default_factory=dict)
    
    # Performance
    parallelism: int = 4
    cache_data: bool = True
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "deequ"


@dataclass
class QualityCheck:
    """Data quality check definition"""
    name: str
    level: CheckLevel
    constraints: List[Any]
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "level": self.level.value,
            "constraints": [str(c) for c in self.constraints],
            "description": self.description
        }


@dataclass
class VerificationResult:
    """Verification result"""
    status: ConstraintStatus
    check_results: List[Dict[str, Any]]
    metrics: Dict[str, float]
    row_count: int
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "status": self.status.value,
            "check_results": self.check_results,
            "metrics": self.metrics,
            "row_count": self.row_count,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class DataProfile:
    """Data profiling result"""
    column_profiles: Dict[str, Dict[str, Any]]
    dataset_metrics: Dict[str, float]
    suggestions: List[Dict[str, Any]]
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "column_profiles": self.column_profiles,
            "dataset_metrics": self.dataset_metrics,
            "suggestions": self.suggestions,
            "timestamp": self.timestamp.isoformat()
        }


class DeequClient(BaseServiceClient):
    """
    Apache Deequ client for data quality unit testing.
    
    Features:
    - Data quality verification
    - Constraint suggestions
    - Data profiling
    - Metrics repository
    - Anomaly detection
    - Quality evolution tracking
    """
    
    def __init__(
        self,
        config: Optional[DeequConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = DeequConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: DeequConfig = config
        self._spark: Optional[SparkSession] = None
        self._metrics_repository: Optional[MetricsRepository] = None
        
    async def connect(self):
        """Connect to Spark and initialize Deequ"""
        await super().connect()
        
        try:
            # Create Spark session
            builder = SparkSession.builder \
                .appName(self.config.app_name) \
                .master(self.config.spark_master) \
                .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.0-spark-3.1")
            
            # Add additional Spark configurations
            if self.config.parallelism:
                builder = builder.config("spark.default.parallelism", self.config.parallelism)
            
            self._spark = builder.getOrCreate()
            
            # Initialize metrics repository if enabled
            if self.config.enable_repository:
                self._init_metrics_repository()
            
            logger.info(f"Connected to Spark with Deequ: {self.config.spark_master}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Spark: {e}")
            raise
    
    async def verify_data(
        self,
        df: DataFrame,
        checks: List[QualityCheck],
        save_metrics: bool = True
    ) -> VerificationResult:
        """
        Verify data quality with defined checks.
        
        Args:
            df: Spark DataFrame to verify
            checks: List of quality checks
            save_metrics: Whether to save metrics to repository
            
        Returns:
            Verification result
        """
        try:
            # Build verification suite
            verification_suite = VerificationSuite(self._spark)
            
            # Add checks
            for check in checks:
                check_builder = Check(self._spark, check.level.value, check.name)
                
                for constraint in check.constraints:
                    check_builder = constraint(check_builder)
                
                verification_suite = verification_suite.addCheck(check_builder)
            
            # Run verification
            if save_metrics and self._metrics_repository:
                verification_result = verification_suite \
                    .onData(df) \
                    .useRepository(self._metrics_repository) \
                    .saveOrAppendResult(ResultKey(datetime.now().timestamp())) \
                    .run()
            else:
                verification_result = verification_suite \
                    .onData(df) \
                    .run()
            
            # Process results
            check_results = []
            all_success = True
            
            for check_result in verification_result.checkResults:
                check_info = {
                    "check": check_result.check.description,
                    "level": check_result.check.level,
                    "status": check_result.status,
                    "constraint_results": []
                }
                
                for constraint_result in check_result.constraintResults:
                    constraint_info = {
                        "constraint": str(constraint_result.constraint),
                        "status": constraint_result.status,
                        "message": constraint_result.message,
                        "metric": constraint_result.metric.value if constraint_result.metric else None
                    }
                    check_info["constraint_results"].append(constraint_info)
                    
                    if constraint_result.status != "Success":
                        all_success = False
                
                check_results.append(check_info)
            
            # Extract metrics
            metrics = {}
            for analyzer, metric in verification_result.metrics.items():
                if metric.value is not None:
                    metrics[str(analyzer)] = metric.value
            
            return VerificationResult(
                status=ConstraintStatus.SUCCESS if all_success else ConstraintStatus.FAILURE,
                check_results=check_results,
                metrics=metrics,
                row_count=df.count()
            )
            
        except Exception as e:
            logger.error(f"Failed to verify data: {e}")
            raise
    
    async def profile_data(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None
    ) -> DataProfile:
        """
        Profile data to understand its characteristics.
        
        Args:
            df: Spark DataFrame to profile
            columns: Specific columns to profile (None for all)
            
        Returns:
            Data profile
        """
        try:
            # Run column profiling
            column_profiles = ColumnProfilerRunner() \
                .onData(df) \
                .run()
            
            # Convert profiles to dict
            profile_dict = {}
            for col_name, profile in column_profiles.profiles.items():
                if columns is None or col_name in columns:
                    profile_dict[col_name] = {
                        "dataType": profile.dataType,
                        "completeness": profile.completeness,
                        "approximateNumDistinctValues": profile.approximateNumDistinctValues,
                        "mean": profile.mean if hasattr(profile, 'mean') else None,
                        "stdDev": profile.stdDev if hasattr(profile, 'stdDev') else None,
                        "min": profile.minimum if hasattr(profile, 'minimum') else None,
                        "max": profile.maximum if hasattr(profile, 'maximum') else None,
                        "histogram": profile.histogram if hasattr(profile, 'histogram') else None
                    }
            
            # Run analyzers for dataset metrics
            analysis_result = AnalysisRunner(self._spark) \
                .onData(df) \
                .addAnalyzer(Size()) \
                .addAnalyzer(Completeness("*")) \
                .run()
            
            # Extract dataset metrics
            dataset_metrics = {}
            for analyzer, metric in analysis_result.metrics.items():
                if metric.value is not None:
                    dataset_metrics[str(analyzer)] = metric.value
            
            # Get constraint suggestions if enabled
            suggestions = []
            if self.config.enable_suggestions:
                suggestion_result = ConstraintSuggestionRunner() \
                    .onData(df) \
                    .run()
                
                for suggestion in suggestion_result.constraintSuggestions:
                    suggestions.append({
                        "column": suggestion.columnName,
                        "description": suggestion.description,
                        "code": suggestion.codeForConstraint
                    })
            
            return DataProfile(
                column_profiles=profile_dict,
                dataset_metrics=dataset_metrics,
                suggestions=suggestions
            )
            
        except Exception as e:
            logger.error(f"Failed to profile data: {e}")
            raise
    
    async def suggest_constraints(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Suggest data quality constraints based on data.
        
        Args:
            df: Spark DataFrame
            columns: Specific columns to analyze
            
        Returns:
            List of constraint suggestions
        """
        try:
            runner = ConstraintSuggestionRunner() \
                .onData(df)
            
            # Add rules from config
            for rule_name, rule_config in self.config.suggestion_rules.items():
                # Apply custom rules if defined
                pass
            
            suggestion_result = runner.run()
            
            suggestions = []
            for suggestion in suggestion_result.constraintSuggestions:
                if columns is None or suggestion.columnName in columns:
                    suggestions.append({
                        "column": suggestion.columnName,
                        "description": suggestion.description,
                        "code": suggestion.codeForConstraint,
                        "constraint_type": suggestion.constraint.__class__.__name__
                    })
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Failed to suggest constraints: {e}")
            raise
    
    async def analyze_metrics(
        self,
        df: DataFrame,
        analyzers: List[Any]
    ) -> Dict[str, float]:
        """
        Run custom analyzers on data.
        
        Args:
            df: Spark DataFrame
            analyzers: List of Deequ analyzers
            
        Returns:
            Metrics dictionary
        """
        try:
            runner = AnalysisRunner(self._spark) \
                .onData(df)
            
            for analyzer in analyzers:
                runner = runner.addAnalyzer(analyzer)
            
            if self._metrics_repository:
                runner = runner \
                    .useRepository(self._metrics_repository) \
                    .saveOrAppendResult(ResultKey(datetime.now().timestamp()))
            
            analysis_result = runner.run()
            
            # Extract metrics
            metrics = {}
            for analyzer, metric in analysis_result.metrics.items():
                if metric.value is not None:
                    metrics[str(analyzer)] = metric.value
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to analyze metrics: {e}")
            raise
    
    async def detect_anomalies(
        self,
        df: DataFrame,
        reference_metrics: Dict[str, float],
        analyzers: List[Any],
        threshold: float = 0.1
    ) -> Dict[str, Any]:
        """
        Detect anomalies by comparing with reference metrics.
        
        Args:
            df: Spark DataFrame to analyze
            reference_metrics: Reference metrics to compare against
            analyzers: List of analyzers to run
            threshold: Anomaly threshold (relative change)
            
        Returns:
            Anomaly detection results
        """
        try:
            # Run current analysis
            current_metrics = await self.analyze_metrics(df, analyzers)
            
            # Compare metrics
            anomalies = []
            for metric_name, current_value in current_metrics.items():
                if metric_name in reference_metrics:
                    reference_value = reference_metrics[metric_name]
                    
                    if reference_value > 0:
                        relative_change = abs(current_value - reference_value) / reference_value
                        
                        if relative_change > threshold:
                            anomalies.append({
                                "metric": metric_name,
                                "current_value": current_value,
                                "reference_value": reference_value,
                                "relative_change": relative_change,
                                "is_anomaly": True
                            })
            
            return {
                "anomalies": anomalies,
                "current_metrics": current_metrics,
                "reference_metrics": reference_metrics,
                "threshold": threshold,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            raise
    
    async def create_quality_checks(
        self,
        name: str,
        level: CheckLevel = CheckLevel.ERROR
    ) -> CheckBuilder:
        """
        Create a quality check builder.
        
        Args:
            name: Check name
            level: Check level
            
        Returns:
            Check builder for chaining constraints
        """
        return CheckBuilder(name, level)
    
    def _init_metrics_repository(self):
        """Initialize metrics repository"""
        try:
            # Create file system repository
            self._metrics_repository = FileSystemMetricsRepository(
                self._spark,
                self.config.metrics_repository_path
            )
            
            logger.info(f"Initialized metrics repository: {self.config.metrics_repository_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize metrics repository: {e}")
    
    async def get_metrics_history(
        self,
        analyzer: Any,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get historical metrics from repository.
        
        Args:
            analyzer: Analyzer to get metrics for
            start_time: Start time filter
            end_time: End time filter
            
        Returns:
            List of historical metrics
        """
        try:
            if not self._metrics_repository:
                return []
            
            # Load metrics from repository
            metrics_df = self._metrics_repository \
                .load() \
                .forAnalyzers([analyzer])
            
            # Apply time filters if provided
            if start_time:
                metrics_df = metrics_df.filter(
                    metrics_df.timestamp >= start_time.timestamp()
                )
            if end_time:
                metrics_df = metrics_df.filter(
                    metrics_df.timestamp <= end_time.timestamp()
                )
            
            # Convert to list of dicts
            metrics = []
            for row in metrics_df.collect():
                metrics.append({
                    "timestamp": datetime.fromtimestamp(row.timestamp),
                    "analyzer": str(row.analyzer),
                    "value": row.value
                })
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics history: {e}")
            return []
    
    async def close(self):
        """Close Spark session"""
        if self._spark:
            self._spark.stop()
            self._spark = None
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Deequ specific configuration"""
        return {
            "spark_master": self.config.spark_master,
            "app_name": self.config.app_name,
            "enable_repository": self.config.enable_repository,
            "enable_profiling": self.config.enable_profiling,
            "enable_suggestions": self.config.enable_suggestions
        }


class CheckBuilder:
    """Builder for creating quality checks with constraints"""
    
    def __init__(self, name: str, level: CheckLevel):
        self.name = name
        self.level = level
        self.constraints = []
        self.description = None
    
    def has_size(self, assertion: Any) -> "CheckBuilder":
        """Check dataset size"""
        self.constraints.append(lambda check: check.hasSize(assertion))
        return self
    
    def is_complete(self, column: str) -> "CheckBuilder":
        """Check column completeness"""
        self.constraints.append(lambda check: check.isComplete(column))
        return self
    
    def is_unique(self, column: str) -> "CheckBuilder":
        """Check column uniqueness"""
        self.constraints.append(lambda check: check.isUnique(column))
        return self
    
    def is_non_negative(self, column: str) -> "CheckBuilder":
        """Check column has no negative values"""
        self.constraints.append(lambda check: check.isNonNegative(column))
        return self
    
    def is_contained_in(self, column: str, allowed_values: List[Any]) -> "CheckBuilder":
        """Check column values are in allowed set"""
        self.constraints.append(
            lambda check: check.isContainedIn(column, allowed_values)
        )
        return self
    
    def satisfies(self, column: str, assertion: str, hint: Optional[str] = None) -> "CheckBuilder":
        """Check custom condition"""
        self.constraints.append(
            lambda check: check.satisfies(column, assertion, hint)
        )
        return self
    
    def has_pattern(self, column: str, pattern: str) -> "CheckBuilder":
        """Check column matches pattern"""
        self.constraints.append(
            lambda check: check.hasPattern(column, pattern)
        )
        return self
    
    def with_description(self, description: str) -> "CheckBuilder":
        """Add description to check"""
        self.description = description
        return self
    
    def build(self) -> QualityCheck:
        """Build the quality check"""
        return QualityCheck(
            name=self.name,
            level=self.level,
            constraints=self.constraints,
            description=self.description
        ) 