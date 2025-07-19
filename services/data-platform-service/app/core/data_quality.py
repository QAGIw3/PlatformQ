"""
Data Quality for profiling, validation, and monitoring
"""
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import numpy as np
import pandas as pd
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError

logger = get_logger(__name__)


@dataclass
class QualityRule:
    """Data quality rule definition"""
    rule_id: str
    name: str
    rule_type: str  # completeness, uniqueness, validity, consistency, accuracy, timeliness
    condition: str
    severity: str  # critical, warning, info
    threshold: float


@dataclass
class QualityMetric:
    """Data quality metric result"""
    metric_name: str
    value: float
    status: str  # pass, fail, warning
    details: Dict[str, Any]


class DataQuality:
    """Data quality engine for profiling and monitoring"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.quality_rules: Dict[str, QualityRule] = {}
        self.profiles: Dict[str, Dict] = {}
        
    async def profile_dataset(self, 
                            dataset_id: str,
                            sample_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Generate comprehensive data profile"""
        try:
            profile_id = f"profile_{dataset_id}_{datetime.utcnow().timestamp()}"
            
            # If no sample provided, fetch from data lake
            if sample_data is None:
                sample_data = await self._fetch_sample_data(dataset_id)
            
            # Generate profile
            profile = {
                "profile_id": profile_id,
                "dataset_id": dataset_id,
                "profiled_at": datetime.utcnow().isoformat(),
                "row_count": len(sample_data),
                "column_count": len(sample_data.columns),
                "columns": {}
            }
            
            # Profile each column
            for column in sample_data.columns:
                col_profile = await self._profile_column(sample_data[column])
                profile["columns"][column] = col_profile
            
            # Calculate overall quality score
            profile["quality_score"] = self._calculate_quality_score(profile)
            
            # Store profile
            self.profiles[profile_id] = profile
            await self.cache.put(f"quality:profile:{profile_id}", profile)
            
            # Publish event
            await self.event_publisher.publish("quality.profile.completed", {
                "profile_id": profile_id,
                "dataset_id": dataset_id,
                "quality_score": profile["quality_score"]
            })
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to profile dataset: {str(e)}")
            raise
    
    async def _profile_column(self, column: pd.Series) -> Dict[str, Any]:
        """Profile a single column"""
        profile = {
            "name": column.name,
            "dtype": str(column.dtype),
            "null_count": int(column.isnull().sum()),
            "null_ratio": float(column.isnull().mean()),
            "unique_count": int(column.nunique()),
            "unique_ratio": float(column.nunique() / len(column)) if len(column) > 0 else 0
        }
        
        # Numeric columns
        if pd.api.types.is_numeric_dtype(column):
            profile.update({
                "min": float(column.min()) if not column.empty else None,
                "max": float(column.max()) if not column.empty else None,
                "mean": float(column.mean()) if not column.empty else None,
                "median": float(column.median()) if not column.empty else None,
                "std": float(column.std()) if not column.empty else None,
                "percentiles": {
                    "25": float(column.quantile(0.25)) if not column.empty else None,
                    "50": float(column.quantile(0.50)) if not column.empty else None,
                    "75": float(column.quantile(0.75)) if not column.empty else None,
                    "95": float(column.quantile(0.95)) if not column.empty else None
                }
            })
        
        # String columns
        elif pd.api.types.is_string_dtype(column):
            non_null = column.dropna()
            if len(non_null) > 0:
                profile.update({
                    "min_length": int(non_null.str.len().min()),
                    "max_length": int(non_null.str.len().max()),
                    "avg_length": float(non_null.str.len().mean()),
                    "pattern_samples": list(non_null.sample(min(5, len(non_null))))
                })
        
        # Date columns
        elif pd.api.types.is_datetime64_dtype(column):
            profile.update({
                "min_date": column.min().isoformat() if not column.empty else None,
                "max_date": column.max().isoformat() if not column.empty else None,
                "date_range_days": (column.max() - column.min()).days if not column.empty else None
            })
        
        return profile
    
    async def create_quality_check(self, check_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a data quality check"""
        try:
            # Validate check definition
            required_fields = ["name", "dataset_id", "rules"]
            for field in required_fields:
                if field not in check_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            check_id = f"check_{datetime.utcnow().timestamp()}"
            
            # Create check
            quality_check = {
                "check_id": check_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "active",
                **check_data
            }
            
            # Store check
            await self.cache.put(f"quality:check:{check_id}", quality_check)
            
            # Schedule check if requested
            if check_data.get("schedule"):
                await self._schedule_check(check_id, check_data["schedule"])
            
            return quality_check
            
        except Exception as e:
            logger.error(f"Failed to create quality check: {str(e)}")
            raise
    
    async def run_quality_check(self, 
                              check_id: str,
                              dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """Run a quality check"""
        try:
            # Get check definition
            check = await self.cache.get(f"quality:check:{check_id}")
            if not check:
                raise ValidationError(f"Check {check_id} not found")
            
            if not dataset_id:
                dataset_id = check["dataset_id"]
            
            # Fetch sample data
            sample_data = await self._fetch_sample_data(dataset_id)
            
            # Run rules
            results = []
            overall_status = "pass"
            
            for rule in check["rules"]:
                result = await self._evaluate_rule(rule, sample_data)
                results.append(result)
                
                if result.status == "fail" and rule.get("severity") == "critical":
                    overall_status = "fail"
                elif result.status == "fail" and overall_status != "fail":
                    overall_status = "warning"
            
            # Create check result
            check_result = {
                "check_id": check_id,
                "dataset_id": dataset_id,
                "executed_at": datetime.utcnow().isoformat(),
                "status": overall_status,
                "results": [self._metric_to_dict(r) for r in results],
                "summary": {
                    "total_rules": len(results),
                    "passed": sum(1 for r in results if r.status == "pass"),
                    "failed": sum(1 for r in results if r.status == "fail"),
                    "warnings": sum(1 for r in results if r.status == "warning")
                }
            }
            
            # Store result
            await self.cache.put(
                f"quality:result:{check_id}:{datetime.utcnow().timestamp()}", 
                check_result,
                ttl=86400 * 7  # Keep for 7 days
            )
            
            # Publish event if failed
            if overall_status != "pass":
                await self.event_publisher.publish("quality.check.failed", check_result)
            
            return check_result
            
        except Exception as e:
            logger.error(f"Failed to run quality check: {str(e)}")
            raise
    
    async def _evaluate_rule(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Evaluate a single quality rule"""
        rule_type = rule["type"]
        
        if rule_type == "completeness":
            return self._check_completeness(rule, data)
        elif rule_type == "uniqueness":
            return self._check_uniqueness(rule, data)
        elif rule_type == "validity":
            return self._check_validity(rule, data)
        elif rule_type == "consistency":
            return self._check_consistency(rule, data)
        elif rule_type == "timeliness":
            return self._check_timeliness(rule, data)
        else:
            raise ValidationError(f"Unknown rule type: {rule_type}")
    
    def _check_completeness(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Check data completeness"""
        column = rule["column"]
        threshold = rule.get("threshold", 0.95)
        
        if column not in data.columns:
            return QualityMetric(
                metric_name=f"completeness_{column}",
                value=0.0,
                status="fail",
                details={"error": f"Column {column} not found"}
            )
        
        completeness = 1 - data[column].isnull().mean()
        status = "pass" if completeness >= threshold else "fail"
        
        return QualityMetric(
            metric_name=f"completeness_{column}",
            value=completeness,
            status=status,
            details={
                "column": column,
                "threshold": threshold,
                "null_count": int(data[column].isnull().sum())
            }
        )
    
    def _check_uniqueness(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Check data uniqueness"""
        columns = rule["columns"] if isinstance(rule["columns"], list) else [rule["columns"]]
        
        duplicate_count = data.duplicated(subset=columns).sum()
        uniqueness = 1 - (duplicate_count / len(data)) if len(data) > 0 else 1.0
        
        threshold = rule.get("threshold", 1.0)
        status = "pass" if uniqueness >= threshold else "fail"
        
        return QualityMetric(
            metric_name=f"uniqueness_{','.join(columns)}",
            value=uniqueness,
            status=status,
            details={
                "columns": columns,
                "threshold": threshold,
                "duplicate_count": int(duplicate_count)
            }
        )
    
    def _check_validity(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Check data validity"""
        column = rule["column"]
        
        if column not in data.columns:
            return QualityMetric(
                metric_name=f"validity_{column}",
                value=0.0,
                status="fail",
                details={"error": f"Column {column} not found"}
            )
        
        # Apply validity condition
        if "pattern" in rule:
            valid_mask = data[column].astype(str).str.match(rule["pattern"])
        elif "values" in rule:
            valid_mask = data[column].isin(rule["values"])
        elif "range" in rule:
            valid_mask = data[column].between(rule["range"][0], rule["range"][1])
        else:
            valid_mask = pd.Series([True] * len(data))
        
        validity = valid_mask.mean()
        threshold = rule.get("threshold", 0.95)
        status = "pass" if validity >= threshold else "fail"
        
        return QualityMetric(
            metric_name=f"validity_{column}",
            value=validity,
            status=status,
            details={
                "column": column,
                "threshold": threshold,
                "invalid_count": int((~valid_mask).sum())
            }
        )
    
    def _check_consistency(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Check data consistency across columns"""
        # Example: Check if date columns are consistent
        if "date_columns" in rule:
            col1, col2 = rule["date_columns"]
            if col1 in data.columns and col2 in data.columns:
                consistent = (pd.to_datetime(data[col1]) <= pd.to_datetime(data[col2])).all()
                return QualityMetric(
                    metric_name=f"consistency_{col1}_{col2}",
                    value=1.0 if consistent else 0.0,
                    status="pass" if consistent else "fail",
                    details={"columns": [col1, col2]}
                )
        
        return QualityMetric(
            metric_name="consistency",
            value=1.0,
            status="pass",
            details={}
        )
    
    def _check_timeliness(self, rule: Dict, data: pd.DataFrame) -> QualityMetric:
        """Check data timeliness"""
        date_column = rule.get("date_column")
        max_age_days = rule.get("max_age_days", 30)
        
        if date_column and date_column in data.columns:
            latest_date = pd.to_datetime(data[date_column]).max()
            age_days = (datetime.utcnow() - latest_date).days
            
            status = "pass" if age_days <= max_age_days else "fail"
            
            return QualityMetric(
                metric_name=f"timeliness_{date_column}",
                value=1.0 if status == "pass" else 0.0,
                status=status,
                details={
                    "latest_date": latest_date.isoformat(),
                    "age_days": age_days,
                    "max_age_days": max_age_days
                }
            )
        
        return QualityMetric(
            metric_name="timeliness",
            value=1.0,
            status="pass",
            details={}
        )
    
    def _calculate_quality_score(self, profile: Dict) -> float:
        """Calculate overall quality score from profile"""
        scores = []
        
        for column, col_profile in profile["columns"].items():
            # Completeness score
            completeness = 1 - col_profile.get("null_ratio", 0)
            scores.append(completeness)
            
            # Uniqueness score (if applicable)
            if col_profile.get("unique_ratio", 0) > 0.9:
                scores.append(1.0)
            elif col_profile.get("unique_ratio", 0) < 0.1:
                scores.append(0.5)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    async def _fetch_sample_data(self, dataset_id: str) -> pd.DataFrame:
        """Fetch sample data for analysis"""
        # In production, fetch from data lake
        # For now, return mock data
        return pd.DataFrame({
            "id": range(1000),
            "value": np.random.randn(1000),
            "category": np.random.choice(["A", "B", "C"], 1000),
            "date": pd.date_range("2024-01-01", periods=1000, freq="H")
        })
    
    def _metric_to_dict(self, metric: QualityMetric) -> Dict:
        """Convert metric to dictionary"""
        return {
            "metric_name": metric.metric_name,
            "value": metric.value,
            "status": metric.status,
            "details": metric.details
        }
    
    async def _schedule_check(self, check_id: str, schedule: str):
        """Schedule a quality check"""
        # In production, integrate with scheduler
        logger.info(f"Scheduled check {check_id} with schedule: {schedule}") 