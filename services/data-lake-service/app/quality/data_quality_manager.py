"""
Data Quality Manager

Manages data quality monitoring, validation, and remediation
in the data lake service.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum
import pandas as pd
import numpy as np

from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import pulsar
from minio import Minio
from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


class DataQualityLevel(Enum):
    """Data quality levels"""
    BRONZE = "bronze"  # Raw data
    SILVER = "silver"  # Cleaned and validated
    GOLD = "gold"     # Aggregated and enriched


class QualityRule:
    """Base class for quality rules"""
    
    def __init__(self, rule_id: str, name: str, severity: str = "medium"):
        self.rule_id = rule_id
        self.name = name
        self.severity = severity  # low, medium, high
        
    def check(self, data: Any) -> Tuple[bool, Optional[str]]:
        """Check if data passes the rule"""
        raise NotImplementedError
        
    def fix(self, data: Any) -> Optional[Any]:
        """Attempt to fix data that fails the rule"""
        return None


class CompletenessRule(QualityRule):
    """Check for missing values"""
    
    def __init__(self, columns: List[str], threshold: float = 0.95):
        super().__init__(
            "completeness",
            "Completeness Check",
            "high"
        )
        self.columns = columns
        self.threshold = threshold
        
    def check(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Check completeness of specified columns"""
        for col in self.columns:
            if col in df.columns:
                completeness = 1 - (df[col].isna().sum() / len(df))
                if completeness < self.threshold:
                    return False, f"Column {col} is only {completeness:.2%} complete"
                    
        return True, None
        
    def fix(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Fill missing values with appropriate defaults"""
        df_fixed = df.copy()
        
        for col in self.columns:
            if col in df_fixed.columns:
                if df_fixed[col].dtype in ['float64', 'int64']:
                    # Fill numeric columns with median
                    df_fixed[col].fillna(df_fixed[col].median(), inplace=True)
                else:
                    # Fill other columns with mode or 'unknown'
                    mode = df_fixed[col].mode()
                    if len(mode) > 0:
                        df_fixed[col].fillna(mode[0], inplace=True)
                    else:
                        df_fixed[col].fillna('unknown', inplace=True)
                        
        return df_fixed


class UniquenessRule(QualityRule):
    """Check for duplicate values"""
    
    def __init__(self, columns: List[str]):
        super().__init__(
            "uniqueness",
            "Uniqueness Check",
            "high"
        )
        self.columns = columns
        
    def check(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Check for duplicates in specified columns"""
        duplicates = df.duplicated(subset=self.columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            return False, f"Found {duplicate_count} duplicate records"
            
        return True, None
        
    def fix(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Remove duplicates, keeping the most recent"""
        if 'timestamp' in df.columns:
            # Sort by timestamp and keep last
            df_sorted = df.sort_values('timestamp')
            return df_sorted.drop_duplicates(subset=self.columns, keep='last')
        else:
            # Keep first occurrence
            return df.drop_duplicates(subset=self.columns, keep='first')


class ValidityRule(QualityRule):
    """Check data validity based on custom validation"""
    
    def __init__(self, validation_func: Callable, fix_func: Optional[Callable] = None):
        super().__init__(
            "validity",
            "Validity Check",
            "medium"
        )
        self.validation_func = validation_func
        self.fix_func = fix_func
        
    def check(self, data: Any) -> Tuple[bool, Optional[str]]:
        """Check if data is valid"""
        try:
            is_valid, message = self.validation_func(data)
            return is_valid, message
        except Exception as e:
            return False, f"Validation error: {str(e)}"
            
    def fix(self, data: Any) -> Optional[Any]:
        """Apply fix function if available"""
        if self.fix_func:
            try:
                return self.fix_func(data)
            except:
                return None
        return None


class DataQualityManager:
    """Manages data quality across the data lake"""
    
    def __init__(self,
                 minio_client: Minio,
                 ignite_client: IgniteClient,
                 event_publisher: EventPublisher):
        self.minio = minio_client
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        
        # Quality rules by data type
        self.rules = {
            "simulation_metrics": [
                CompletenessRule(["simulation_id", "timestamp", "convergence_rate"]),
                UniquenessRule(["simulation_id", "timestamp"]),
                ValidityRule(
                    lambda df: (
                        (df["convergence_rate"].between(0, 1).all(), None)
                        if "convergence_rate" in df.columns
                        else (True, None)
                    ),
                    lambda df: df.assign(
                        convergence_rate=df["convergence_rate"].clip(0, 1)
                    ) if "convergence_rate" in df.columns else df
                )
            ],
            "asset_metadata": [
                CompletenessRule(["asset_id", "asset_type", "size"]),
                UniquenessRule(["asset_id"]),
                ValidityRule(
                    lambda df: (
                        (df["size"] > 0).all() if "size" in df.columns else True,
                        "Invalid asset size found"
                    )
                )
            ],
            "workflow_logs": [
                CompletenessRule(["workflow_id", "timestamp", "status"]),
                ValidityRule(
                    lambda df: (
                        df["status"].isin(["pending", "running", "completed", "failed", "cancelled"]).all()
                        if "status" in df.columns else True,
                        "Invalid workflow status found"
                    )
                )
            ]
        }
        
        # Quality metrics cache
        self.quality_cache = self.ignite.get_or_create_cache("data_quality_metrics")
        
        # Quarantine configuration
        self.quarantine_bucket = "data-quarantine"
        self._ensure_bucket_exists(self.quarantine_bucket)
        
    def _ensure_bucket_exists(self, bucket_name: str):
        """Ensure bucket exists in MinIO"""
        try:
            if not self.minio.bucket_exists(bucket_name):
                self.minio.make_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            
    async def validate_data(self,
                          data: pd.DataFrame,
                          data_type: str,
                          source_path: str) -> Dict[str, Any]:
        """Validate data against quality rules"""
        try:
            rules = self.rules.get(data_type, [])
            
            validation_results = {
                "data_type": data_type,
                "source_path": source_path,
                "timestamp": datetime.utcnow().isoformat(),
                "total_records": len(data),
                "rules_checked": len(rules),
                "passed_rules": 0,
                "violations": [],
                "quality_score": 0.0
            }
            
            # Check each rule
            for rule in rules:
                passed, message = rule.check(data)
                
                if passed:
                    validation_results["passed_rules"] += 1
                else:
                    validation_results["violations"].append({
                        "rule_id": rule.rule_id,
                        "rule_name": rule.name,
                        "severity": rule.severity,
                        "message": message
                    })
                    
            # Calculate quality score
            if rules:
                validation_results["quality_score"] = (
                    validation_results["passed_rules"] / len(rules)
                )
                
            # Publish quality event
            await self._publish_quality_event(validation_results)
            
            # Update quality metrics cache
            self._update_quality_metrics(data_type, validation_results)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            raise
            
    async def process_data_with_quality(self,
                                      data: pd.DataFrame,
                                      data_type: str,
                                      source_path: str,
                                      target_quality: DataQualityLevel) -> Dict[str, Any]:
        """Process data through quality pipeline"""
        try:
            # Validate data
            validation_results = await self.validate_data(data, data_type, source_path)
            
            # Determine action based on quality score and target level
            quality_score = validation_results["quality_score"]
            
            if target_quality == DataQualityLevel.BRONZE:
                # Bronze accepts all data
                action = "accept"
                processed_data = data
                
            elif target_quality == DataQualityLevel.SILVER:
                # Silver requires cleaning or high quality
                if quality_score >= 0.8:
                    action = "accept"
                    processed_data = data
                elif quality_score >= 0.5:
                    # Attempt to clean
                    action = "clean"
                    processed_data = await self._clean_data(data, data_type, validation_results)
                else:
                    # Too poor quality for silver
                    action = "quarantine"
                    processed_data = None
                    
            else:  # GOLD
                # Gold requires perfect quality
                if quality_score == 1.0:
                    action = "accept"
                    processed_data = data
                else:
                    action = "reject"
                    processed_data = None
                    
            # Handle based on action
            result = {
                "action": action,
                "validation_results": validation_results,
                "target_quality": target_quality.value
            }
            
            if action == "accept":
                result["output_path"] = await self._store_validated_data(
                    processed_data,
                    data_type,
                    target_quality
                )
                
            elif action == "clean":
                # Re-validate cleaned data
                clean_validation = await self.validate_data(
                    processed_data,
                    data_type,
                    source_path
                )
                
                if clean_validation["quality_score"] >= 0.8:
                    result["output_path"] = await self._store_validated_data(
                        processed_data,
                        data_type,
                        target_quality
                    )
                    result["cleaning_applied"] = True
                else:
                    # Cleaning didn't help enough
                    action = "quarantine"
                    result["action"] = action
                    
            if action == "quarantine":
                result["quarantine_path"] = await self._quarantine_data(
                    data,
                    data_type,
                    validation_results
                )
                
            return result
            
        except Exception as e:
            logger.error(f"Error processing data with quality: {e}")
            raise
            
    async def _clean_data(self,
                        data: pd.DataFrame,
                        data_type: str,
                        validation_results: Dict[str, Any]) -> pd.DataFrame:
        """Attempt to clean data based on violations"""
        try:
            cleaned_data = data.copy()
            rules = self.rules.get(data_type, [])
            
            # Apply fixes for violated rules
            for violation in validation_results["violations"]:
                rule_id = violation["rule_id"]
                
                # Find the rule
                rule = next((r for r in rules if r.rule_id == rule_id), None)
                
                if rule and hasattr(rule, 'fix'):
                    fixed_data = rule.fix(cleaned_data)
                    if fixed_data is not None:
                        cleaned_data = fixed_data
                        
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            return data
            
    async def _store_validated_data(self,
                                  data: pd.DataFrame,
                                  data_type: str,
                                  quality_level: DataQualityLevel) -> str:
        """Store validated data in appropriate location"""
        try:
            # Generate path based on quality level
            timestamp = datetime.utcnow()
            path = f"{quality_level.value}/{data_type}/{timestamp.strftime('%Y/%m/%d')}/data_{timestamp.timestamp()}.parquet"
            
            # Convert to parquet and store
            parquet_buffer = data.to_parquet(index=False)
            
            bucket_name = f"{data_type}-data"
            self._ensure_bucket_exists(bucket_name)
            
            self.minio.put_object(
                bucket_name,
                path,
                parquet_buffer,
                len(parquet_buffer)
            )
            
            return f"{bucket_name}/{path}"
            
        except Exception as e:
            logger.error(f"Error storing validated data: {e}")
            raise
            
    async def _quarantine_data(self,
                             data: pd.DataFrame,
                             data_type: str,
                             validation_results: Dict[str, Any]) -> str:
        """Move data to quarantine for manual review"""
        try:
            timestamp = datetime.utcnow()
            
            # Store data
            data_path = f"{data_type}/{timestamp.strftime('%Y/%m/%d')}/data_{timestamp.timestamp()}.parquet"
            parquet_buffer = data.to_parquet(index=False)
            
            self.minio.put_object(
                self.quarantine_bucket,
                data_path,
                parquet_buffer,
                len(parquet_buffer)
            )
            
            # Store validation results
            results_path = f"{data_type}/{timestamp.strftime('%Y/%m/%d')}/validation_{timestamp.timestamp()}.json"
            results_json = json.dumps(validation_results, indent=2).encode()
            
            self.minio.put_object(
                self.quarantine_bucket,
                results_path,
                results_json,
                len(results_json)
            )
            
            # Publish quarantine event
            await self.event_publisher.publish_event(
                "DATA_QUARANTINED",
                {
                    "data_type": data_type,
                    "quarantine_path": f"{self.quarantine_bucket}/{data_path}",
                    "validation_results": validation_results,
                    "timestamp": timestamp.isoformat()
                }
            )
            
            return f"{self.quarantine_bucket}/{data_path}"
            
        except Exception as e:
            logger.error(f"Error quarantining data: {e}")
            raise
            
    async def _publish_quality_event(self, validation_results: Dict[str, Any]):
        """Publish data quality event"""
        try:
            event_type = "DATA_QUALITY_CHECK"
            
            if validation_results["quality_score"] < 0.5:
                event_type = "DATA_QUALITY_ALERT"
                
            await self.event_publisher.publish_event(
                event_type,
                validation_results
            )
            
        except Exception as e:
            logger.error(f"Error publishing quality event: {e}")
            
    def _update_quality_metrics(self, data_type: str, validation_results: Dict[str, Any]):
        """Update quality metrics in cache"""
        try:
            key = f"quality_metrics:{data_type}"
            
            # Get existing metrics
            existing = self.quality_cache.get(key) or {
                "total_validations": 0,
                "total_records": 0,
                "average_score": 0.0,
                "violation_counts": {}
            }
            
            # Update metrics
            existing["total_validations"] += 1
            existing["total_records"] += validation_results["total_records"]
            
            # Update average score
            existing["average_score"] = (
                (existing["average_score"] * (existing["total_validations"] - 1) +
                 validation_results["quality_score"]) /
                existing["total_validations"]
            )
            
            # Update violation counts
            for violation in validation_results["violations"]:
                rule_id = violation["rule_id"]
                existing["violation_counts"][rule_id] = \
                    existing["violation_counts"].get(rule_id, 0) + 1
                    
            # Store updated metrics
            self.quality_cache.put(key, existing)
            
        except Exception as e:
            logger.error(f"Error updating quality metrics: {e}")
            
    async def get_quality_report(self,
                               data_type: Optional[str] = None,
                               time_range: str = "24h") -> Dict[str, Any]:
        """Generate quality report"""
        try:
            report = {
                "timestamp": datetime.utcnow().isoformat(),
                "time_range": time_range,
                "data_types": {}
            }
            
            # Get metrics for specified data type or all
            data_types = [data_type] if data_type else list(self.rules.keys())
            
            for dt in data_types:
                key = f"quality_metrics:{dt}"
                metrics = self.quality_cache.get(key)
                
                if metrics:
                    report["data_types"][dt] = {
                        "total_validations": metrics["total_validations"],
                        "total_records": metrics["total_records"],
                        "average_quality_score": metrics["average_score"],
                        "top_violations": sorted(
                            metrics["violation_counts"].items(),
                            key=lambda x: x[1],
                            reverse=True
                        )[:5]
                    }
                    
            return report
            
        except Exception as e:
            logger.error(f"Error generating quality report: {e}")
            raise 