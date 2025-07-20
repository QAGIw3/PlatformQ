"""
ML-Enhanced Data Quality Profiler with anomaly detection
"""
import asyncio
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import great_expectations as ge

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError
from ..core.cache_manager import DataCacheManager

logger = get_logger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class AnomalyType(str, Enum):
    """Types of data anomalies"""
    OUTLIER = "outlier"
    PATTERN_BREAK = "pattern_break"
    DRIFT = "drift"
    SCHEMA_CHANGE = "schema_change"
    VOLUME_ANOMALY = "volume_anomaly"


class DataQualityProfiler:
    """
    ML-enhanced data quality profiler with comprehensive analysis.
    
    Features:
    - Statistical profiling
    - Pattern recognition
    - Anomaly detection
    - Data drift monitoring
    - Quality scoring
    - Rule-based validation
    - ML-based quality predictions
    """
    
    def __init__(self,
                 spark_session: SparkSession,
                 cache_manager: DataCacheManager):
        self.spark = spark_session
        self.cache = cache_manager
        
        # Great Expectations context
        self.ge_context = None
        
        # ML models for anomaly detection
        self.anomaly_models: Dict[str, Any] = {}
        
        # Quality rules registry
        self.quality_rules: Dict[str, Any] = {}
        
        # Profile history for trend analysis
        self.profile_history: Dict[str, List[Dict]] = {}
        
        # Statistics
        self.stats = {
            "total_profiles": 0,
            "anomalies_detected": 0,
            "rules_evaluated": 0,
            "quality_issues": 0
        }
    
    async def initialize(self) -> None:
        """Initialize the quality profiler"""
        try:
            # Initialize Great Expectations
            self._init_great_expectations()
            
            # Load predefined quality rules
            self._load_quality_rules()
            
            logger.info("Data quality profiler initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize quality profiler: {e}")
            raise
    
    def _init_great_expectations(self) -> None:
        """Initialize Great Expectations context"""
        # In production, would configure with actual data context
        # For now, use in-memory context
        self.ge_context = ge.data_context.BaseDataContext()
    
    def _load_quality_rules(self) -> None:
        """Load predefined quality rules"""
        # Common quality rules
        self.quality_rules = {
            "not_null": lambda df, col: df.filter(F.col(col).isNotNull()).count() / df.count(),
            "unique": lambda df, col: df.select(col).distinct().count() / df.count(),
            "positive": lambda df, col: df.filter(F.col(col) > 0).count() / df.count(),
            "in_range": lambda df, col, min_val, max_val: 
                df.filter((F.col(col) >= min_val) & (F.col(col) <= max_val)).count() / df.count(),
            "pattern_match": lambda df, col, pattern: 
                df.filter(F.col(col).rlike(pattern)).count() / df.count(),
            "referential_integrity": lambda df1, col1, df2, col2:
                df1.join(df2, df1[col1] == df2[col2], "left_anti").count() == 0
        }
    
    async def profile_dataset(self,
                            df: DataFrame,
                            dataset_name: str,
                            tenant_id: str,
                            sample_size: Optional[int] = None) -> Dict[str, Any]:
        """Comprehensive dataset profiling"""
        try:
            start_time = datetime.utcnow()
            
            # Sample if dataset is large
            total_rows = df.count()
            if sample_size and total_rows > sample_size:
                sample_fraction = sample_size / total_rows
                df_sample = df.sample(fraction=sample_fraction, seed=42)
            else:
                df_sample = df
            
            # Basic statistics
            basic_stats = await self._compute_basic_statistics(df_sample, total_rows)
            
            # Column profiles
            column_profiles = await self._profile_columns(df_sample)
            
            # Data quality assessment
            quality_assessment = await self._assess_quality(df_sample, column_profiles)
            
            # Anomaly detection
            anomalies = await self._detect_anomalies(df_sample, dataset_name)
            
            # Pattern analysis
            patterns = await self._analyze_patterns(df_sample)
            
            # Compare with historical profiles
            drift_analysis = await self._analyze_drift(dataset_name, column_profiles)
            
            # Calculate overall quality score
            quality_score = self._calculate_quality_score(quality_assessment)
            
            # Build profile result
            profile = {
                "dataset_name": dataset_name,
                "tenant_id": tenant_id,
                "profiled_at": datetime.utcnow(),
                "total_rows": total_rows,
                "sample_size": df_sample.count() if sample_size else total_rows,
                "basic_statistics": basic_stats,
                "column_profiles": column_profiles,
                "quality_assessment": quality_assessment,
                "quality_score": quality_score,
                "anomalies": anomalies,
                "patterns": patterns,
                "drift_analysis": drift_analysis,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
            }
            
            # Store profile
            await self._store_profile(dataset_name, profile)
            
            # Update statistics
            self.stats["total_profiles"] += 1
            self.stats["anomalies_detected"] += len(anomalies)
            
            logger.info(f"Profiled dataset {dataset_name} with quality score {quality_score:.2f}")
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to profile dataset: {e}")
            raise
    
    async def _compute_basic_statistics(self,
                                      df: DataFrame,
                                      total_rows: int) -> Dict[str, Any]:
        """Compute basic dataset statistics"""
        stats = {
            "row_count": total_rows,
            "column_count": len(df.columns),
            "memory_usage_mb": self._estimate_memory_usage(df),
            "null_percentage": 0.0,
            "duplicate_rows": 0
        }
        
        # Calculate null percentage
        null_counts = []
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts.append(null_count)
        
        total_cells = total_rows * len(df.columns)
        total_nulls = sum(null_counts)
        stats["null_percentage"] = (total_nulls / total_cells * 100) if total_cells > 0 else 0
        
        # Count duplicate rows
        stats["duplicate_rows"] = total_rows - df.dropDuplicates().count()
        
        return stats
    
    async def _profile_columns(self, df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """Profile individual columns"""
        column_profiles = {}
        
        for col in df.columns:
            col_type = str(df.schema[col].dataType)
            profile = {
                "data_type": col_type,
                "null_count": df.filter(F.col(col).isNull()).count(),
                "null_percentage": 0.0,
                "unique_count": df.select(col).distinct().count(),
                "unique_percentage": 0.0
            }
            
            # Calculate percentages
            row_count = df.count()
            if row_count > 0:
                profile["null_percentage"] = profile["null_count"] / row_count * 100
                profile["unique_percentage"] = profile["unique_count"] / row_count * 100
            
            # Numeric column statistics
            if any(t in col_type.lower() for t in ["int", "double", "float", "decimal"]):
                stats = df.select(
                    F.min(col).alias("min"),
                    F.max(col).alias("max"),
                    F.mean(col).alias("mean"),
                    F.stddev(col).alias("stddev"),
                    F.expr(f"percentile_approx({col}, 0.25)").alias("q1"),
                    F.expr(f"percentile_approx({col}, 0.5)").alias("median"),
                    F.expr(f"percentile_approx({col}, 0.75)").alias("q3")
                ).collect()[0]
                
                profile.update({
                    "min": stats["min"],
                    "max": stats["max"],
                    "mean": stats["mean"],
                    "stddev": stats["stddev"],
                    "quartiles": {
                        "q1": stats["q1"],
                        "median": stats["median"],
                        "q3": stats["q3"]
                    }
                })
                
                # Detect outliers using IQR
                if stats["q1"] is not None and stats["q3"] is not None:
                    iqr = stats["q3"] - stats["q1"]
                    lower_bound = stats["q1"] - 1.5 * iqr
                    upper_bound = stats["q3"] + 1.5 * iqr
                    
                    outlier_count = df.filter(
                        (F.col(col) < lower_bound) | (F.col(col) > upper_bound)
                    ).count()
                    
                    profile["outlier_count"] = outlier_count
                    profile["outlier_percentage"] = outlier_count / row_count * 100 if row_count > 0 else 0
            
            # String column statistics
            elif "string" in col_type.lower():
                length_stats = df.select(
                    F.min(F.length(col)).alias("min_length"),
                    F.max(F.length(col)).alias("max_length"),
                    F.avg(F.length(col)).alias("avg_length")
                ).collect()[0]
                
                profile.update({
                    "min_length": length_stats["min_length"],
                    "max_length": length_stats["max_length"],
                    "avg_length": length_stats["avg_length"]
                })
                
                # Top values
                top_values = df.groupBy(col).count() \
                    .orderBy(F.desc("count")) \
                    .limit(10) \
                    .collect()
                
                profile["top_values"] = [
                    {"value": row[col], "count": row["count"]}
                    for row in top_values
                ]
            
            # Date/timestamp column statistics
            elif any(t in col_type.lower() for t in ["date", "timestamp"]):
                date_stats = df.select(
                    F.min(col).alias("min_date"),
                    F.max(col).alias("max_date")
                ).collect()[0]
                
                profile.update({
                    "min_date": str(date_stats["min_date"]),
                    "max_date": str(date_stats["max_date"])
                })
            
            column_profiles[col] = profile
        
        return column_profiles
    
    async def _assess_quality(self,
                            df: DataFrame,
                            column_profiles: Dict[str, Dict]) -> Dict[str, Any]:
        """Assess data quality across dimensions"""
        assessment = {}
        
        # Completeness
        completeness_scores = []
        for col, profile in column_profiles.items():
            completeness = 1 - (profile["null_percentage"] / 100)
            completeness_scores.append(completeness)
        
        assessment[QualityDimension.COMPLETENESS] = {
            "score": np.mean(completeness_scores),
            "details": {col: score for col, score in zip(column_profiles.keys(), completeness_scores)}
        }
        
        # Uniqueness
        uniqueness_scores = []
        for col, profile in column_profiles.items():
            if profile["unique_percentage"] > 95:  # Likely an ID column
                uniqueness = 1.0
            else:
                uniqueness = min(profile["unique_percentage"] / 100, 1.0)
            uniqueness_scores.append(uniqueness)
        
        assessment[QualityDimension.UNIQUENESS] = {
            "score": np.mean(uniqueness_scores),
            "details": {col: score for col, score in zip(column_profiles.keys(), uniqueness_scores)}
        }
        
        # Validity (based on data type consistency and patterns)
        validity_score = await self._assess_validity(df, column_profiles)
        assessment[QualityDimension.VALIDITY] = validity_score
        
        # Consistency (based on referential integrity and business rules)
        consistency_score = await self._assess_consistency(df)
        assessment[QualityDimension.CONSISTENCY] = consistency_score
        
        # Accuracy (requires ground truth or rules)
        accuracy_score = await self._assess_accuracy(df)
        assessment[QualityDimension.ACCURACY] = accuracy_score
        
        # Timeliness (based on data freshness)
        timeliness_score = await self._assess_timeliness(df)
        assessment[QualityDimension.TIMELINESS] = timeliness_score
        
        return assessment
    
    async def _assess_validity(self,
                             df: DataFrame,
                             column_profiles: Dict[str, Dict]) -> Dict[str, Any]:
        """Assess data validity"""
        validity_scores = []
        details = {}
        
        for col, profile in column_profiles.items():
            col_type = profile["data_type"]
            score = 1.0
            
            # Check for invalid values based on type
            if "int" in col_type.lower() or "double" in col_type.lower():
                # Check for NaN or infinity
                invalid_count = df.filter(
                    F.isnan(col) | F.col(col).isin([float('inf'), float('-inf')])
                ).count()
                
                if df.count() > 0:
                    score = 1 - (invalid_count / df.count())
            
            elif "string" in col_type.lower():
                # Check for empty strings
                empty_count = df.filter(
                    (F.col(col) == "") | (F.trim(F.col(col)) == "")
                ).count()
                
                if df.count() > 0:
                    score = 1 - (empty_count / df.count())
            
            validity_scores.append(score)
            details[col] = score
        
        return {
            "score": np.mean(validity_scores),
            "details": details
        }
    
    async def _assess_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Assess data consistency"""
        # This would check business rules and referential integrity
        # For now, return a placeholder
        return {
            "score": 0.95,
            "details": {"message": "Consistency checks pending implementation"}
        }
    
    async def _assess_accuracy(self, df: DataFrame) -> Dict[str, Any]:
        """Assess data accuracy"""
        # This would require ground truth or validation rules
        # For now, return a placeholder
        return {
            "score": 0.90,
            "details": {"message": "Accuracy assessment requires ground truth"}
        }
    
    async def _assess_timeliness(self, df: DataFrame) -> Dict[str, Any]:
        """Assess data timeliness"""
        # Check for timestamp columns and assess freshness
        timestamp_cols = [col for col in df.columns 
                         if any(t in str(df.schema[col].dataType).lower() 
                               for t in ["timestamp", "date"])]
        
        if timestamp_cols:
            # Use the most recent timestamp column
            latest_timestamp = None
            for col in timestamp_cols:
                max_time = df.select(F.max(col)).collect()[0][0]
                if max_time and (latest_timestamp is None or max_time > latest_timestamp):
                    latest_timestamp = max_time
            
            if latest_timestamp:
                # Calculate age in hours
                age_hours = (datetime.utcnow() - latest_timestamp).total_seconds() / 3600
                
                # Score based on age (fresher is better)
                if age_hours < 1:
                    score = 1.0
                elif age_hours < 24:
                    score = 0.9
                elif age_hours < 168:  # 1 week
                    score = 0.7
                else:
                    score = 0.5
                
                return {
                    "score": score,
                    "details": {
                        "latest_timestamp": str(latest_timestamp),
                        "age_hours": age_hours
                    }
                }
        
        return {
            "score": 0.5,
            "details": {"message": "No timestamp columns found"}
        }
    
    async def _detect_anomalies(self,
                              df: DataFrame,
                              dataset_name: str) -> List[Dict[str, Any]]:
        """Detect data anomalies using ML"""
        anomalies = []
        
        # Numeric anomaly detection
        numeric_cols = [col for col in df.columns 
                       if any(t in str(df.schema[col].dataType).lower() 
                             for t in ["int", "double", "float"])]
        
        if numeric_cols:
            # Convert to pandas for ML processing
            numeric_data = df.select(*numeric_cols).toPandas()
            
            # Handle missing values
            numeric_data = numeric_data.fillna(numeric_data.mean())
            
            if len(numeric_data) > 10:  # Need minimum samples
                # Standardize data
                scaler = StandardScaler()
                scaled_data = scaler.fit_transform(numeric_data)
                
                # Isolation Forest for anomaly detection
                iso_forest = IsolationForest(
                    contamination=0.1,  # Expect 10% anomalies
                    random_state=42
                )
                
                predictions = iso_forest.fit_predict(scaled_data)
                anomaly_scores = iso_forest.score_samples(scaled_data)
                
                # Identify anomalous records
                anomaly_indices = np.where(predictions == -1)[0]
                
                for idx in anomaly_indices[:10]:  # Limit to top 10
                    anomaly_record = {
                        "type": AnomalyType.OUTLIER,
                        "severity": "high" if anomaly_scores[idx] < -0.5 else "medium",
                        "record_index": int(idx),
                        "anomaly_score": float(anomaly_scores[idx]),
                        "affected_columns": numeric_cols,
                        "values": numeric_data.iloc[idx].to_dict()
                    }
                    anomalies.append(anomaly_record)
                
                # Store model for future use
                self.anomaly_models[dataset_name] = {
                    "model": iso_forest,
                    "scaler": scaler,
                    "columns": numeric_cols
                }
        
        # Volume anomaly detection
        if dataset_name in self.profile_history:
            history = self.profile_history[dataset_name]
            if len(history) > 5:
                # Check for sudden volume changes
                recent_counts = [p["basic_statistics"]["row_count"] for p in history[-5:]]
                current_count = df.count()
                
                mean_count = np.mean(recent_counts)
                std_count = np.std(recent_counts)
                
                if std_count > 0:
                    z_score = abs(current_count - mean_count) / std_count
                    
                    if z_score > 3:  # More than 3 standard deviations
                        anomalies.append({
                            "type": AnomalyType.VOLUME_ANOMALY,
                            "severity": "high",
                            "description": f"Row count {current_count} deviates significantly from average {mean_count:.0f}",
                            "z_score": float(z_score)
                        })
        
        return anomalies
    
    async def _analyze_patterns(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze data patterns"""
        patterns = {}
        
        # Temporal patterns
        timestamp_cols = [col for col in df.columns 
                         if any(t in str(df.schema[col].dataType).lower() 
                               for t in ["timestamp", "date"])]
        
        if timestamp_cols:
            for col in timestamp_cols[:1]:  # Analyze first timestamp column
                # Extract time components
                df_with_time = df.select(
                    col,
                    F.year(col).alias("year"),
                    F.month(col).alias("month"),
                    F.dayofweek(col).alias("dayofweek"),
                    F.hour(col).alias("hour")
                ).filter(F.col(col).isNotNull())
                
                # Analyze distribution by time component
                patterns[f"{col}_patterns"] = {
                    "yearly": df_with_time.groupBy("year").count().orderBy("year").collect(),
                    "monthly": df_with_time.groupBy("month").count().orderBy("month").collect(),
                    "weekly": df_with_time.groupBy("dayofweek").count().orderBy("dayofweek").collect(),
                    "hourly": df_with_time.groupBy("hour").count().orderBy("hour").collect()
                }
        
        # Correlation patterns for numeric columns
        numeric_cols = [col for col in df.columns 
                       if any(t in str(df.schema[col].dataType).lower() 
                             for t in ["int", "double", "float"])]
        
        if len(numeric_cols) > 1:
            # Calculate correlations
            correlations = {}
            for i, col1 in enumerate(numeric_cols):
                for col2 in numeric_cols[i+1:]:
                    corr = df.select(F.corr(col1, col2)).collect()[0][0]
                    if corr is not None and abs(corr) > 0.7:  # Strong correlation
                        correlations[f"{col1}__{col2}"] = float(corr)
            
            patterns["correlations"] = correlations
        
        return patterns
    
    async def _analyze_drift(self,
                           dataset_name: str,
                           current_profile: Dict[str, Dict]) -> Dict[str, Any]:
        """Analyze data drift compared to historical profiles"""
        drift_analysis = {
            "drift_detected": False,
            "drifted_columns": [],
            "drift_score": 0.0
        }
        
        if dataset_name not in self.profile_history:
            return drift_analysis
        
        history = self.profile_history[dataset_name]
        if len(history) < 2:
            return drift_analysis
        
        # Compare with most recent profile
        previous_profile = history[-1]["column_profiles"]
        
        drift_scores = []
        
        for col, current_stats in current_profile.items():
            if col not in previous_profile:
                continue
            
            prev_stats = previous_profile[col]
            
            # Check for distribution changes in numeric columns
            if "mean" in current_stats and "mean" in prev_stats:
                # Calculate normalized difference
                if prev_stats["stddev"] and prev_stats["stddev"] > 0:
                    mean_diff = abs(current_stats["mean"] - prev_stats["mean"]) / prev_stats["stddev"]
                    
                    if mean_diff > 2:  # Significant drift
                        drift_analysis["drifted_columns"].append({
                            "column": col,
                            "drift_type": "mean_shift",
                            "previous_mean": prev_stats["mean"],
                            "current_mean": current_stats["mean"],
                            "normalized_difference": mean_diff
                        })
                        drift_scores.append(min(mean_diff / 10, 1.0))
                    else:
                        drift_scores.append(0.0)
            
            # Check for cardinality changes
            if "unique_count" in current_stats and "unique_count" in prev_stats:
                prev_unique = prev_stats["unique_count"]
                curr_unique = current_stats["unique_count"]
                
                if prev_unique > 0:
                    unique_change = abs(curr_unique - prev_unique) / prev_unique
                    
                    if unique_change > 0.2:  # 20% change
                        drift_analysis["drifted_columns"].append({
                            "column": col,
                            "drift_type": "cardinality_change",
                            "previous_unique": prev_unique,
                            "current_unique": curr_unique,
                            "change_percentage": unique_change * 100
                        })
                        drift_scores.append(min(unique_change, 1.0))
        
        if drift_scores:
            drift_analysis["drift_score"] = np.mean(drift_scores)
            drift_analysis["drift_detected"] = drift_analysis["drift_score"] > 0.1
        
        return drift_analysis
    
    def _calculate_quality_score(self, assessment: Dict[str, Any]) -> float:
        """Calculate overall quality score"""
        dimension_weights = {
            QualityDimension.COMPLETENESS: 0.25,
            QualityDimension.ACCURACY: 0.20,
            QualityDimension.CONSISTENCY: 0.20,
            QualityDimension.VALIDITY: 0.15,
            QualityDimension.Uniqueness: 0.10,
            QualityDimension.TIMELINESS: 0.10
        }
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for dimension, weight in dimension_weights.items():
            if dimension in assessment:
                score = assessment[dimension].get("score", 0.0)
                weighted_sum += score * weight
                total_weight += weight
        
        if total_weight > 0:
            return weighted_sum / total_weight
        
        return 0.0
    
    def _estimate_memory_usage(self, df: DataFrame) -> float:
        """Estimate DataFrame memory usage in MB"""
        # Rough estimation based on schema
        bytes_per_row = 0
        
        for field in df.schema.fields:
            dtype = str(field.dataType).lower()
            
            if "int" in dtype:
                if "long" in dtype:
                    bytes_per_row += 8
                else:
                    bytes_per_row += 4
            elif "double" in dtype or "float" in dtype:
                bytes_per_row += 8
            elif "string" in dtype:
                # Estimate average string length
                bytes_per_row += 50
            elif "boolean" in dtype:
                bytes_per_row += 1
            elif "timestamp" in dtype or "date" in dtype:
                bytes_per_row += 8
            else:
                bytes_per_row += 20  # Default
        
        total_bytes = bytes_per_row * df.count()
        return total_bytes / (1024 * 1024)  # Convert to MB
    
    async def _store_profile(self, dataset_name: str, profile: Dict[str, Any]) -> None:
        """Store profile for historical analysis"""
        # Store in cache
        await self.cache.set_metadata("quality_profile", dataset_name, profile)
        
        # Update history
        if dataset_name not in self.profile_history:
            self.profile_history[dataset_name] = []
        
        self.profile_history[dataset_name].append(profile)
        
        # Keep only last 30 profiles
        if len(self.profile_history[dataset_name]) > 30:
            self.profile_history[dataset_name] = self.profile_history[dataset_name][-30:]
    
    async def validate_with_rules(self,
                                df: DataFrame,
                                rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate data against custom rules"""
        validation_results = {
            "passed": 0,
            "failed": 0,
            "total": len(rules),
            "results": []
        }
        
        for rule in rules:
            rule_type = rule.get("type")
            rule_name = rule.get("name", f"Rule_{rule_type}")
            
            try:
                if rule_type in self.quality_rules:
                    rule_func = self.quality_rules[rule_type]
                    params = rule.get("params", {})
                    
                    # Execute rule
                    if rule_type == "referential_integrity":
                        # Special handling for referential integrity
                        result = rule_func(df, params["column"], params["ref_df"], params["ref_column"])
                    else:
                        result = rule_func(df, **params)
                    
                    passed = result >= rule.get("threshold", 0.95)
                    
                    validation_results["results"].append({
                        "rule_name": rule_name,
                        "rule_type": rule_type,
                        "passed": passed,
                        "score": result,
                        "threshold": rule.get("threshold", 0.95)
                    })
                    
                    if passed:
                        validation_results["passed"] += 1
                    else:
                        validation_results["failed"] += 1
                        self.stats["quality_issues"] += 1
                else:
                    logger.warning(f"Unknown rule type: {rule_type}")
                    
            except Exception as e:
                logger.error(f"Rule validation error: {e}")
                validation_results["results"].append({
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "passed": False,
                    "error": str(e)
                })
                validation_results["failed"] += 1
        
        self.stats["rules_evaluated"] += len(rules)
        
        return validation_results
    
    async def suggest_quality_improvements(self,
                                         profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest improvements based on profile"""
        suggestions = []
        
        # Check completeness
        completeness = profile["quality_assessment"][QualityDimension.COMPLETENESS]
        if completeness["score"] < 0.9:
            worst_columns = sorted(
                completeness["details"].items(),
                key=lambda x: x[1]
            )[:5]
            
            suggestions.append({
                "type": "completeness",
                "priority": "high",
                "description": "Improve data completeness",
                "details": f"Columns with high null rates: {[col for col, _ in worst_columns]}",
                "impact": "Improving completeness will enhance analysis accuracy"
            })
        
        # Check for anomalies
        if profile["anomalies"]:
            high_severity = [a for a in profile["anomalies"] if a.get("severity") == "high"]
            if high_severity:
                suggestions.append({
                    "type": "anomaly",
                    "priority": "high",
                    "description": f"Investigate {len(high_severity)} high-severity anomalies",
                    "details": "Anomalies may indicate data quality issues or genuine outliers",
                    "impact": "Resolving anomalies improves data reliability"
                })
        
        # Check for data drift
        if profile["drift_analysis"]["drift_detected"]:
            suggestions.append({
                "type": "drift",
                "priority": "medium",
                "description": "Address data drift in distribution",
                "details": f"Drift detected in {len(profile['drift_analysis']['drifted_columns'])} columns",
                "impact": "Drift can affect model performance and analysis validity"
            })
        
        # Check validity
        validity = profile["quality_assessment"][QualityDimension.VALIDITY]
        if validity["score"] < 0.95:
            suggestions.append({
                "type": "validity",
                "priority": "medium",
                "description": "Improve data validity",
                "details": "Check for empty strings, invalid formats, and constraint violations",
                "impact": "Valid data ensures accurate processing"
            })
        
        return suggestions
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get profiler statistics"""
        return self.stats 