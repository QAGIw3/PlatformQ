"""Quality Profiler for comprehensive data analysis and profiling"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
import json

from app.core.config import Settings
from app.core.quality_engine import QualityEngine


logger = logging.getLogger(__name__)


class ProfileType:
    """Types of profiling operations"""
    BASIC = "basic"
    STATISTICAL = "statistical"
    PATTERN = "pattern"
    CORRELATION = "correlation"
    DISTRIBUTION = "distribution"
    ANOMALY = "anomaly"


class QualityProfiler:
    """Comprehensive data profiler for quality assessment"""
    
    def __init__(self, settings: Settings, quality_engine: QualityEngine):
        self.settings = settings
        self.quality_engine = quality_engine
        self.profile_cache: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize the profiler"""
        logger.info("Initializing quality profiler")
        
    async def profile_dataset(self, dataset_id: str, data: pd.DataFrame,
                            profile_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Perform comprehensive data profiling"""
        logger.info(f"Profiling dataset {dataset_id}")
        
        # Check cache
        cache_key = f"profile:{dataset_id}"
        if cache_key in self.profile_cache:
            cached = self.profile_cache[cache_key]
            if (datetime.utcnow() - cached['timestamp']).seconds < self.settings.cache_ttl:
                return cached['profile']
                
        # Initialize profile result
        profile = {
            "dataset_id": dataset_id,
            "timestamp": datetime.utcnow().isoformat(),
            "row_count": len(data),
            "column_count": len(data.columns),
            "memory_usage": data.memory_usage(deep=True).sum() / 1024 / 1024,  # MB
            "columns": {},
            "correlations": {},
            "patterns": {},
            "recommendations": []
        }
        
        # Determine which profiles to run
        if not profile_types:
            profile_types = [ProfileType.BASIC, ProfileType.STATISTICAL, ProfileType.PATTERN]
            
        # Run profiling
        if ProfileType.BASIC in profile_types:
            await self._basic_profiling(data, profile)
            
        if ProfileType.STATISTICAL in profile_types:
            await self._statistical_profiling(data, profile)
            
        if ProfileType.PATTERN in profile_types:
            await self._pattern_profiling(data, profile)
            
        if ProfileType.CORRELATION in profile_types:
            await self._correlation_profiling(data, profile)
            
        if ProfileType.DISTRIBUTION in profile_types:
            await self._distribution_profiling(data, profile)
            
        # Generate recommendations
        profile["recommendations"] = await self._generate_recommendations(profile)
        
        # Cache result
        self.profile_cache[cache_key] = {
            'profile': profile,
            'timestamp': datetime.utcnow()
        }
        
        return profile
        
    async def _basic_profiling(self, data: pd.DataFrame, profile: Dict[str, Any]):
        """Perform basic profiling for each column"""
        for column in data.columns:
            col_profile = {
                "name": column,
                "dtype": str(data[column].dtype),
                "null_count": int(data[column].isnull().sum()),
                "null_percentage": float(data[column].isnull().sum() / len(data) * 100),
                "unique_count": int(data[column].nunique()),
                "unique_percentage": float(data[column].nunique() / len(data) * 100)
            }
            
            # Add sample values
            col_profile["sample_values"] = data[column].dropna().head(5).tolist()
            
            # Memory usage
            col_profile["memory_usage"] = float(data[column].memory_usage(deep=True) / 1024)  # KB
            
            profile["columns"][column] = col_profile
            
    async def _statistical_profiling(self, data: pd.DataFrame, profile: Dict[str, Any]):
        """Perform statistical profiling for numeric columns"""
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            col_data = data[column].dropna()
            
            if len(col_data) == 0:
                continue
                
            stats = {
                "mean": float(col_data.mean()),
                "median": float(col_data.median()),
                "std": float(col_data.std()),
                "min": float(col_data.min()),
                "max": float(col_data.max()),
                "q1": float(col_data.quantile(0.25)),
                "q3": float(col_data.quantile(0.75)),
                "iqr": float(col_data.quantile(0.75) - col_data.quantile(0.25)),
                "skewness": float(stats.skew(col_data)),
                "kurtosis": float(stats.kurtosis(col_data))
            }
            
            # Detect outliers using IQR method
            q1, q3 = stats["q1"], stats["q3"]
            iqr = stats["iqr"]
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
            
            stats["outlier_count"] = len(outliers)
            stats["outlier_percentage"] = float(len(outliers) / len(col_data) * 100)
            
            # Add to profile
            if column in profile["columns"]:
                profile["columns"][column].update({"statistics": stats})
            else:
                profile["columns"][column] = {"statistics": stats}
                
    async def _pattern_profiling(self, data: pd.DataFrame, profile: Dict[str, Any]):
        """Detect patterns in string columns"""
        string_columns = data.select_dtypes(include=['object']).columns
        
        for column in string_columns:
            col_data = data[column].dropna()
            
            if len(col_data) == 0:
                continue
                
            patterns = {
                "detected_patterns": [],
                "format_distribution": {}
            }
            
            # Email pattern
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            email_matches = col_data.str.match(email_pattern).sum()
            if email_matches > len(col_data) * 0.8:
                patterns["detected_patterns"].append({
                    "type": "email",
                    "confidence": float(email_matches / len(col_data))
                })
                
            # Phone pattern
            phone_pattern = r'^\+?1?\d{9,15}$'
            phone_matches = col_data.str.match(phone_pattern).sum()
            if phone_matches > len(col_data) * 0.8:
                patterns["detected_patterns"].append({
                    "type": "phone",
                    "confidence": float(phone_matches / len(col_data))
                })
                
            # Date patterns
            date_patterns = {
                "ISO": r'^\d{4}-\d{2}-\d{2}',
                "US": r'^\d{2}/\d{2}/\d{4}',
                "EU": r'^\d{2}\.\d{2}\.\d{4}'
            }
            
            for format_name, pattern in date_patterns.items():
                matches = col_data.str.match(pattern).sum()
                if matches > 0:
                    patterns["format_distribution"][format_name] = int(matches)
                    
            # Length analysis
            lengths = col_data.str.len()
            patterns["length_stats"] = {
                "min": int(lengths.min()),
                "max": int(lengths.max()),
                "mean": float(lengths.mean()),
                "std": float(lengths.std())
            }
            
            # Add to profile
            if column in profile["columns"]:
                profile["columns"][column].update({"patterns": patterns})
            else:
                profile["columns"][column] = {"patterns": patterns}
                
    async def _correlation_profiling(self, data: pd.DataFrame, profile: Dict[str, Any]):
        """Calculate correlations between numeric columns"""
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) < 2:
            return
            
        # Calculate correlation matrix
        corr_matrix = data[numeric_columns].corr()
        
        # Find strong correlations
        strong_correlations = []
        threshold = 0.7
        
        for i in range(len(numeric_columns)):
            for j in range(i + 1, len(numeric_columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > threshold:
                    strong_correlations.append({
                        "column1": numeric_columns[i],
                        "column2": numeric_columns[j],
                        "correlation": float(corr_value),
                        "strength": "strong" if abs(corr_value) > 0.9 else "moderate"
                    })
                    
        profile["correlations"] = {
            "matrix": corr_matrix.to_dict(),
            "strong_correlations": strong_correlations
        }
        
    async def _distribution_profiling(self, data: pd.DataFrame, profile: Dict[str, Any]):
        """Analyze data distributions"""
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            col_data = data[column].dropna()
            
            if len(col_data) < 10:
                continue
                
            distribution = {
                "histogram": {},
                "normality_test": {},
                "distribution_type": "unknown"
            }
            
            # Create histogram bins
            hist, bins = np.histogram(col_data, bins=20)
            distribution["histogram"] = {
                "counts": hist.tolist(),
                "bins": bins.tolist()
            }
            
            # Normality test (Shapiro-Wilk)
            if len(col_data) < 5000:  # Shapiro-Wilk has sample size limitations
                statistic, p_value = stats.shapiro(col_data)
                distribution["normality_test"] = {
                    "statistic": float(statistic),
                    "p_value": float(p_value),
                    "is_normal": p_value > 0.05
                }
                
                if p_value > 0.05:
                    distribution["distribution_type"] = "normal"
                    
            # Check for other distributions
            # Uniform distribution check
            if len(col_data.unique()) / len(col_data) > 0.9:
                distribution["distribution_type"] = "uniform"
                
            # Exponential distribution check (positive skew)
            if stats.skew(col_data) > 2:
                distribution["distribution_type"] = "exponential"
                
            # Add to profile
            if column in profile["columns"]:
                profile["columns"][column].update({"distribution": distribution})
            else:
                profile["columns"][column] = {"distribution": distribution}
                
    async def detect_data_types(self, data: pd.DataFrame) -> Dict[str, str]:
        """Detect semantic data types beyond pandas dtypes"""
        semantic_types = {}
        
        for column in data.columns:
            col_data = data[column].dropna()
            
            if len(col_data) == 0:
                semantic_types[column] = "empty"
                continue
                
            # Check various semantic types
            if data[column].dtype in ['int64', 'float64']:
                # Check if it's an ID column
                if 'id' in column.lower() or data[column].nunique() == len(data):
                    semantic_types[column] = "identifier"
                # Check if it's a year
                elif col_data.min() > 1900 and col_data.max() < 2100 and col_data.dtype == 'int64':
                    semantic_types[column] = "year"
                # Check if it's a percentage
                elif col_data.min() >= 0 and col_data.max() <= 100:
                    semantic_types[column] = "percentage"
                # Check if it's currency
                elif 'price' in column.lower() or 'amount' in column.lower() or 'cost' in column.lower():
                    semantic_types[column] = "currency"
                else:
                    semantic_types[column] = "numeric"
                    
            elif data[column].dtype == 'object':
                # Sample for pattern detection
                sample = col_data.head(100)
                
                # Email detection
                if sample.str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$').sum() > len(sample) * 0.8:
                    semantic_types[column] = "email"
                # URL detection
                elif sample.str.match(r'^https?://').sum() > len(sample) * 0.8:
                    semantic_types[column] = "url"
                # Phone detection
                elif sample.str.match(r'^\+?1?\d{9,15}$').sum() > len(sample) * 0.8:
                    semantic_types[column] = "phone"
                # Date detection
                elif self._is_date_column(sample):
                    semantic_types[column] = "date"
                # Category detection
                elif data[column].nunique() < len(data) * 0.1:
                    semantic_types[column] = "category"
                else:
                    semantic_types[column] = "text"
                    
            elif 'datetime' in str(data[column].dtype):
                semantic_types[column] = "datetime"
            elif data[column].dtype == 'bool':
                semantic_types[column] = "boolean"
            else:
                semantic_types[column] = "unknown"
                
        return semantic_types
        
    async def _generate_recommendations(self, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate quality improvement recommendations based on profile"""
        recommendations = []
        
        # Check for high null percentages
        for col_name, col_profile in profile["columns"].items():
            null_percentage = col_profile.get("null_percentage", 0)
            
            if null_percentage > 20:
                recommendations.append({
                    "type": "missing_data",
                    "severity": "high",
                    "column": col_name,
                    "message": f"Column '{col_name}' has {null_percentage:.1f}% missing values",
                    "action": "Consider imputation strategies or investigate data collection issues"
                })
                
            # Check for low cardinality in supposed ID columns
            if 'id' in col_name.lower():
                unique_percentage = col_profile.get("unique_percentage", 0)
                if unique_percentage < 95:
                    recommendations.append({
                        "type": "duplicate_ids",
                        "severity": "critical",
                        "column": col_name,
                        "message": f"ID column '{col_name}' has only {unique_percentage:.1f}% unique values",
                        "action": "Investigate duplicate IDs and ensure data integrity"
                    })
                    
            # Check for outliers in numeric columns
            if "statistics" in col_profile:
                outlier_percentage = col_profile["statistics"].get("outlier_percentage", 0)
                if outlier_percentage > 5:
                    recommendations.append({
                        "type": "outliers",
                        "severity": "medium",
                        "column": col_name,
                        "message": f"Column '{col_name}' has {outlier_percentage:.1f}% outliers",
                        "action": "Review outliers for data quality issues or legitimate extreme values"
                    })
                    
        # Check for strong correlations
        if "correlations" in profile and "strong_correlations" in profile["correlations"]:
            for corr in profile["correlations"]["strong_correlations"]:
                if corr["correlation"] > 0.95:
                    recommendations.append({
                        "type": "redundancy",
                        "severity": "low",
                        "columns": [corr["column1"], corr["column2"]],
                        "message": f"Columns '{corr['column1']}' and '{corr['column2']}' are highly correlated ({corr['correlation']:.2f})",
                        "action": "Consider removing one column to reduce redundancy"
                    })
                    
        return recommendations
        
    def _is_date_column(self, sample: pd.Series) -> bool:
        """Check if a string column contains dates"""
        try:
            # Try to parse as dates
            pd.to_datetime(sample.head(10))
            return True
        except:
            return False
            
    async def generate_profile_report(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a human-readable profile report"""
        report = {
            "summary": {
                "dataset_id": profile["dataset_id"],
                "profiled_at": profile["timestamp"],
                "total_rows": profile["row_count"],
                "total_columns": profile["column_count"],
                "memory_usage_mb": profile["memory_usage"],
                "quality_score": 0.0  # Will be calculated
            },
            "column_summary": [],
            "data_quality_issues": [],
            "recommendations": profile["recommendations"]
        }
        
        # Calculate overall quality score
        quality_scores = []
        
        for col_name, col_profile in profile["columns"].items():
            # Column summary
            col_summary = {
                "name": col_name,
                "type": col_profile.get("dtype", "unknown"),
                "completeness": 100 - col_profile.get("null_percentage", 0),
                "cardinality": col_profile.get("unique_count", 0)
            }
            
            # Calculate column quality score
            col_score = 1.0
            
            # Penalize for missing data
            null_percentage = col_profile.get("null_percentage", 0)
            col_score *= (1 - null_percentage / 100)
            
            # Penalize for outliers
            if "statistics" in col_profile:
                outlier_percentage = col_profile["statistics"].get("outlier_percentage", 0)
                col_score *= (1 - outlier_percentage / 200)  # Less penalty than missing data
                
            quality_scores.append(col_score)
            col_summary["quality_score"] = col_score
            
            report["column_summary"].append(col_summary)
            
            # Identify issues
            if null_percentage > 5:
                report["data_quality_issues"].append({
                    "column": col_name,
                    "issue": "missing_data",
                    "severity": "high" if null_percentage > 20 else "medium",
                    "details": f"{null_percentage:.1f}% missing values"
                })
                
        # Calculate overall quality score
        report["summary"]["quality_score"] = np.mean(quality_scores) if quality_scores else 0.0
        
        return report
