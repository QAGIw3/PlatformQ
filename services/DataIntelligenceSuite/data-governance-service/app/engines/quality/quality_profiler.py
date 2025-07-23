"""
Data Quality Profiler for comprehensive data analysis.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict

import pandas as pd
import numpy as np
from scipy import stats

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ProfileType(str, Enum):
    """Types of data profiles."""
    BASIC = "basic"
    STATISTICAL = "statistical"
    DISTRIBUTION = "distribution"
    PATTERN = "pattern"
    RELATIONSHIP = "relationship"
    FULL = "full"


@dataclass
class ColumnProfile:
    """Profile for a single column."""
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: int
    completeness: float
    
    # Statistical metrics
    mean: Optional[float] = None
    median: Optional[float] = None
    mode: Optional[Any] = None
    std_dev: Optional[float] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    quartiles: Optional[Dict[str, float]] = None
    
    # Distribution metrics
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    distribution_type: Optional[str] = None
    
    # Pattern metrics
    common_patterns: Optional[List[Dict[str, Any]]] = None
    format_distribution: Optional[Dict[str, int]] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProfile:
    """Complete data profile."""
    dataset_id: str
    profile_id: str
    timestamp: datetime
    row_count: int
    column_count: int
    
    # Column profiles
    columns: Dict[str, ColumnProfile]
    
    # Dataset-level metrics
    memory_usage_mb: float
    estimated_quality_score: float
    
    # Relationships
    correlations: Optional[Dict[Tuple[str, str], float]] = None
    dependencies: Optional[List[Dict[str, Any]]] = None
    
    # Metadata
    profile_type: ProfileType = ProfileType.BASIC
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProfileMetrics:
    """Aggregated profile metrics."""
    total_profiles: int = 0
    total_datasets_profiled: int = 0
    average_quality_score: float = 0.0
    common_issues: List[Dict[str, Any]] = field(default_factory=list)


class QualityProfiler:
    """
    Comprehensive data profiler for quality analysis.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        
        # Profile storage
        self.profiles: Dict[str, DataProfile] = {}
        self.profile_history: List[DataProfile] = []
        
        # Configuration
        self.sample_size = 10000  # For pattern analysis
        self.correlation_threshold = 0.7
        
        # Metrics
        self.metrics = ProfileMetrics()
        
        logger.info("Quality Profiler initialized")
        
    async def initialize(self):
        """Initialize profiler."""
        # Subscribe to events
        await self.event_bus.subscribe("quality.profile.request", self._handle_profile_request)
        
        logger.info("Quality Profiler ready")
        
    async def profile_data(
        self,
        data: Union[pd.DataFrame, Dict[str, Any]],
        dataset_id: str,
        profile_type: ProfileType = ProfileType.BASIC,
        columns: Optional[List[str]] = None
    ) -> DataProfile:
        """
        Profile data to analyze quality characteristics.
        
        Args:
            data: Data to profile
            dataset_id: Dataset identifier
            profile_type: Type of profiling to perform
            columns: Specific columns to profile (None for all)
            
        Returns:
            DataProfile with comprehensive analysis
        """
        start_time = datetime.utcnow()
        profile_id = f"profile_{dataset_id}_{start_time.timestamp()}"
        
        # Convert to DataFrame if needed
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        
        # Filter columns if specified
        if columns:
            data = data[columns]
        
        # Basic profiling
        column_profiles = {}
        
        for column in data.columns:
            col_profile = await self._profile_column(data[column], column, profile_type)
            column_profiles[column] = col_profile
        
        # Calculate dataset-level metrics
        memory_usage_mb = data.memory_usage(deep=True).sum() / 1024 / 1024
        
        # Estimate quality score based on completeness and validity
        quality_scores = [col.completeness for col in column_profiles.values()]
        estimated_quality_score = np.mean(quality_scores) if quality_scores else 0.0
        
        # Advanced profiling based on type
        correlations = None
        dependencies = None
        
        if profile_type in [ProfileType.STATISTICAL, ProfileType.FULL]:
            correlations = await self._analyze_correlations(data)
            
        if profile_type in [ProfileType.RELATIONSHIP, ProfileType.FULL]:
            dependencies = await self._analyze_dependencies(data)
        
        # Create profile
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        profile = DataProfile(
            dataset_id=dataset_id,
            profile_id=profile_id,
            timestamp=start_time,
            row_count=len(data),
            column_count=len(data.columns),
            columns=column_profiles,
            memory_usage_mb=memory_usage_mb,
            estimated_quality_score=estimated_quality_score,
            correlations=correlations,
            dependencies=dependencies,
            profile_type=profile_type,
            execution_time_ms=execution_time,
            metadata={
                "columns_analyzed": len(column_profiles),
                "profile_depth": profile_type.value
            }
        )
        
        # Store profile
        self.profiles[profile_id] = profile
        self.profile_history.append(profile)
        
        # Update metrics
        self.metrics.total_profiles += 1
        self.metrics.total_datasets_profiled = len(set(p.dataset_id for p in self.profile_history))
        self.metrics.average_quality_score = np.mean([p.estimated_quality_score for p in self.profile_history])
        
        # Cache profile
        await self.cache_manager.set(
            f"quality:profile:{profile_id}",
            self._serialize_profile(profile),
            ttl=86400  # 24 hours
        )
        
        # Publish event
        await self.event_bus.publish("quality.profile.complete", {
            "profile_id": profile_id,
            "dataset_id": dataset_id,
            "quality_score": estimated_quality_score,
            "issues_found": self._identify_issues(profile)
        })
        
        logger.info(f"Profile complete for {dataset_id}: quality_score={estimated_quality_score:.2f}")
        
        return profile
        
    async def _profile_column(
        self,
        series: pd.Series,
        column_name: str,
        profile_type: ProfileType
    ) -> ColumnProfile:
        """Profile a single column."""
        # Basic metrics
        total_count = len(series)
        null_count = series.isna().sum()
        unique_count = series.nunique()
        completeness = 1.0 - (null_count / total_count) if total_count > 0 else 0.0
        
        # Determine data type
        if pd.api.types.is_numeric_dtype(series):
            data_type = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            data_type = "datetime"
        elif pd.api.types.is_bool_dtype(series):
            data_type = "boolean"
        else:
            data_type = "string"
        
        profile = ColumnProfile(
            column_name=column_name,
            data_type=data_type,
            total_count=total_count,
            null_count=null_count,
            unique_count=unique_count,
            completeness=completeness
        )
        
        # Statistical profiling
        if profile_type in [ProfileType.STATISTICAL, ProfileType.FULL] and data_type == "numeric":
            non_null = series.dropna()
            if len(non_null) > 0:
                profile.mean = float(non_null.mean())
                profile.median = float(non_null.median())
                profile.std_dev = float(non_null.std())
                profile.min_value = float(non_null.min())
                profile.max_value = float(non_null.max())
                profile.quartiles = {
                    "q1": float(non_null.quantile(0.25)),
                    "q2": float(non_null.quantile(0.50)),
                    "q3": float(non_null.quantile(0.75))
                }
                
                # Mode (handle multiple modes)
                mode_result = non_null.mode()
                if len(mode_result) > 0:
                    profile.mode = float(mode_result.iloc[0])
        
        # Distribution analysis
        if profile_type in [ProfileType.DISTRIBUTION, ProfileType.FULL] and data_type == "numeric":
            non_null = series.dropna()
            if len(non_null) > 3:  # Need at least 4 values for skewness/kurtosis
                profile.skewness = float(stats.skew(non_null))
                profile.kurtosis = float(stats.kurtosis(non_null))
                
                # Determine distribution type
                profile.distribution_type = self._identify_distribution(non_null)
        
        # Pattern analysis
        if profile_type in [ProfileType.PATTERN, ProfileType.FULL] and data_type == "string":
            profile.common_patterns = await self._analyze_patterns(series)
            profile.format_distribution = await self._analyze_formats(series)
        
        return profile
        
    async def _analyze_correlations(self, data: pd.DataFrame) -> Dict[Tuple[str, str], float]:
        """Analyze correlations between numeric columns."""
        correlations = {}
        
        # Get numeric columns
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) > 1:
            # Calculate correlation matrix
            corr_matrix = data[numeric_cols].corr()
            
            # Extract significant correlations
            for i, col1 in enumerate(numeric_cols):
                for j, col2 in enumerate(numeric_cols):
                    if i < j:  # Upper triangle only
                        corr_value = corr_matrix.loc[col1, col2]
                        if abs(corr_value) >= self.correlation_threshold:
                            correlations[(col1, col2)] = float(corr_value)
        
        return correlations
        
    async def _analyze_dependencies(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze functional dependencies between columns."""
        dependencies = []
        
        # Simple dependency detection based on unique value ratios
        for col1 in data.columns:
            for col2 in data.columns:
                if col1 != col2:
                    # Group by col1 and check uniqueness of col2
                    grouped = data.groupby(col1)[col2].nunique()
                    
                    # If each value of col1 maps to exactly one value of col2
                    if (grouped == 1).all():
                        dependencies.append({
                            "determinant": col1,
                            "dependent": col2,
                            "type": "functional",
                            "confidence": 1.0
                        })
        
        return dependencies
        
    async def _analyze_patterns(self, series: pd.Series) -> List[Dict[str, Any]]:
        """Analyze common patterns in string data."""
        patterns = []
        
        # Sample data for efficiency
        sample = series.dropna().sample(min(len(series), self.sample_size), replace=True)
        
        # Common patterns to check
        pattern_checks = [
            ("email", r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            ("phone", r'^\+?1?\d{9,15}$'),
            ("url", r'^https?://[^\s]+$'),
            ("date", r'^\d{4}-\d{2}-\d{2}$'),
            ("uuid", r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
            ("numeric", r'^\d+$'),
            ("alphanumeric", r'^[a-zA-Z0-9]+$')
        ]
        
        for pattern_name, pattern_regex in pattern_checks:
            matches = sample.str.match(pattern_regex, na=False).sum()
            if matches > 0:
                patterns.append({
                    "pattern": pattern_name,
                    "matches": int(matches),
                    "percentage": float(matches / len(sample) * 100)
                })
        
        # Sort by frequency
        patterns.sort(key=lambda x: x["matches"], reverse=True)
        
        return patterns[:5]  # Top 5 patterns
        
    async def _analyze_formats(self, series: pd.Series) -> Dict[str, int]:
        """Analyze format distribution in string data."""
        format_counts = defaultdict(int)
        
        # Sample data
        sample = series.dropna().sample(min(len(series), self.sample_size), replace=True)
        
        for value in sample:
            # Create format signature
            format_sig = ""
            for char in str(value):
                if char.isalpha():
                    format_sig += "A"
                elif char.isdigit():
                    format_sig += "9"
                elif char.isspace():
                    format_sig += "_"
                else:
                    format_sig += char
            
            format_counts[format_sig] += 1
        
        # Get top formats
        top_formats = dict(sorted(format_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        
        return top_formats
        
    def _identify_distribution(self, data: pd.Series) -> str:
        """Identify the distribution type of numeric data."""
        # Simple distribution identification based on skewness and kurtosis
        skew = stats.skew(data)
        kurt = stats.kurtosis(data)
        
        if abs(skew) < 0.5 and abs(kurt) < 0.5:
            return "normal"
        elif skew > 1:
            return "right_skewed"
        elif skew < -1:
            return "left_skewed"
        elif kurt > 3:
            return "leptokurtic"
        elif kurt < -1:
            return "platykurtic"
        else:
            return "unknown"
            
    def _identify_issues(self, profile: DataProfile) -> List[Dict[str, Any]]:
        """Identify quality issues from profile."""
        issues = []
        
        for col_name, col_profile in profile.columns.items():
            # Completeness issues
            if col_profile.completeness < 0.9:
                issues.append({
                    "column": col_name,
                    "issue": "low_completeness",
                    "severity": "high" if col_profile.completeness < 0.5 else "medium",
                    "value": col_profile.completeness
                })
            
            # Cardinality issues
            if col_profile.unique_count == col_profile.total_count and col_profile.total_count > 100:
                issues.append({
                    "column": col_name,
                    "issue": "all_unique",
                    "severity": "info",
                    "value": col_profile.unique_count
                })
            elif col_profile.unique_count == 1:
                issues.append({
                    "column": col_name,
                    "issue": "single_value",
                    "severity": "medium",
                    "value": col_profile.unique_count
                })
            
            # Distribution issues
            if col_profile.skewness and abs(col_profile.skewness) > 2:
                issues.append({
                    "column": col_name,
                    "issue": "high_skewness",
                    "severity": "info",
                    "value": col_profile.skewness
                })
        
        return issues
        
    def _serialize_profile(self, profile: DataProfile) -> Dict[str, Any]:
        """Serialize profile for caching."""
        return {
            "dataset_id": profile.dataset_id,
            "profile_id": profile.profile_id,
            "timestamp": profile.timestamp.isoformat(),
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "columns": {
                name: {
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "completeness": col.completeness,
                    "unique_count": col.unique_count,
                    "null_count": col.null_count
                }
                for name, col in profile.columns.items()
            },
            "memory_usage_mb": profile.memory_usage_mb,
            "estimated_quality_score": profile.estimated_quality_score,
            "profile_type": profile.profile_type.value,
            "execution_time_ms": profile.execution_time_ms
        }
        
    async def _handle_profile_request(self, event_data: Dict[str, Any]):
        """Handle profile request event."""
        try:
            dataset_id = event_data["dataset_id"]
            data = event_data["data"]
            profile_type = ProfileType(event_data.get("profile_type", "basic"))
            
            profile = await self.profile_data(data, dataset_id, profile_type)
            
            # Publish result
            await self.event_bus.publish("quality.profile.result", self._serialize_profile(profile))
            
        except Exception as e:
            logger.error(f"Error handling profile request: {e}")
            await self.event_bus.publish("quality.profile.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def compare_profiles(
        self,
        profile_id1: str,
        profile_id2: str
    ) -> Dict[str, Any]:
        """Compare two data profiles."""
        profile1 = self.profiles.get(profile_id1)
        profile2 = self.profiles.get(profile_id2)
        
        if not profile1 or not profile2:
            raise ValueError("Profile not found")
        
        comparison = {
            "profile_ids": [profile_id1, profile_id2],
            "dataset_ids": [profile1.dataset_id, profile2.dataset_id],
            "quality_score_diff": profile2.estimated_quality_score - profile1.estimated_quality_score,
            "row_count_diff": profile2.row_count - profile1.row_count,
            "column_changes": self._compare_columns(profile1, profile2)
        }
        
        return comparison
        
    def _compare_columns(self, profile1: DataProfile, profile2: DataProfile) -> Dict[str, Any]:
        """Compare columns between profiles."""
        cols1 = set(profile1.columns.keys())
        cols2 = set(profile2.columns.keys())
        
        changes = {
            "added": list(cols2 - cols1),
            "removed": list(cols1 - cols2),
            "common": list(cols1 & cols2),
            "quality_changes": {}
        }
        
        # Compare common columns
        for col in changes["common"]:
            col1 = profile1.columns[col]
            col2 = profile2.columns[col]
            
            quality_diff = col2.completeness - col1.completeness
            if abs(quality_diff) > 0.05:  # 5% threshold
                changes["quality_changes"][col] = {
                    "completeness_diff": quality_diff,
                    "null_count_diff": col2.null_count - col1.null_count
                }
        
        return changes
        
    def get_profile_summary(self, profile_id: str) -> Dict[str, Any]:
        """Get profile summary."""
        profile = self.profiles.get(profile_id)
        if not profile:
            return None
        
        return {
            "profile_id": profile.profile_id,
            "dataset_id": profile.dataset_id,
            "timestamp": profile.timestamp.isoformat(),
            "quality_score": profile.estimated_quality_score,
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "issues": self._identify_issues(profile),
            "top_quality_columns": sorted(
                [(name, col.completeness) for name, col in profile.columns.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]
        } 