"""Quality Engine implementation for comprehensive data quality management"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from enum import Enum
import json
import re

import pandas as pd
import numpy as np
from pyignite import Client as IgniteClient

from app.core.config import Settings


logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Quality dimensions for assessment"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


class RuleType(Enum):
    """Types of quality rules"""
    SQL = "sql"
    PYTHON = "python"
    REGEX = "regex"
    STATISTICAL = "statistical"
    BUSINESS = "business"


class QualityEngine:
    """Main quality engine coordinating all quality operations"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.rules_cache: Dict[str, Any] = {}
        self.ignite_client: Optional[IgniteClient] = None
        self.quality_cache: Optional[Any] = None
        self._monitor_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize quality engine"""
        logger.info("Initializing quality engine")
        
        # Connect to Ignite for caching
        try:
            self.ignite_client = IgniteClient()
            self.ignite_client.connect(self.settings.ignite_host, self.settings.ignite_port)
            self.quality_cache = self.ignite_client.get_or_create_cache(self.settings.ignite_cache_name)
            logger.info("Connected to Ignite cache")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            
        # Load rules from storage
        await self._load_rules()
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_quality_trends())
        
        logger.info("Quality engine initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up quality engine")
        
        if self._monitor_task:
            self._monitor_task.cancel()
            
        if self.ignite_client:
            self.ignite_client.close()
            
        logger.info("Quality engine cleaned up")
        
    async def is_healthy(self) -> bool:
        """Check if engine is healthy"""
        try:
            # Check Ignite connection
            if self.ignite_client and self.quality_cache:
                # Try a simple operation
                self.quality_cache.get("health_check")
                return True
        except:
            pass
        return False
        
    async def validate_quality(self, dataset_id: str, data: pd.DataFrame, 
                             rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Comprehensive quality validation"""
        logger.info(f"Validating quality for dataset {dataset_id}")
        
        # Check cache first
        cache_key = f"quality_result:{dataset_id}"
        cached_result = await self._get_cached_result(cache_key)
        if cached_result:
            return cached_result
            
        # Initialize results
        results = {
            "dataset_id": dataset_id,
            "timestamp": datetime.utcnow().isoformat(),
            "row_count": len(data),
            "column_count": len(data.columns),
            "dimensions": {},
            "issues": [],
            "quality_score": 0.0,
            "passed": True
        }
        
        # Run quality assessments for each dimension
        dimension_scores = {}
        
        # Completeness check
        completeness_score, completeness_issues = await self._check_completeness(data)
        dimension_scores[QualityDimension.COMPLETENESS.value] = completeness_score
        results["issues"].extend(completeness_issues)
        
        # Accuracy check
        accuracy_score, accuracy_issues = await self._check_accuracy(data, rules)
        dimension_scores[QualityDimension.ACCURACY.value] = accuracy_score
        results["issues"].extend(accuracy_issues)
        
        # Consistency check
        consistency_score, consistency_issues = await self._check_consistency(data)
        dimension_scores[QualityDimension.CONSISTENCY.value] = consistency_score
        results["issues"].extend(consistency_issues)
        
        # Validity check
        validity_score, validity_issues = await self._check_validity(data, rules)
        dimension_scores[QualityDimension.VALIDITY.value] = validity_score
        results["issues"].extend(validity_issues)
        
        # Uniqueness check
        uniqueness_score, uniqueness_issues = await self._check_uniqueness(data)
        dimension_scores[QualityDimension.UNIQUENESS.value] = uniqueness_score
        results["issues"].extend(uniqueness_issues)
        
        # Timeliness check
        timeliness_score, timeliness_issues = await self._check_timeliness(data, rules)
        dimension_scores[QualityDimension.TIMELINESS.value] = timeliness_score
        results["issues"].extend(timeliness_issues)
        
        # Calculate overall quality score
        results["dimensions"] = dimension_scores
        results["quality_score"] = np.mean(list(dimension_scores.values()))
        results["passed"] = results["quality_score"] >= 0.8 and len([i for i in results["issues"] if i["severity"] == "critical"]) == 0
        
        # Apply custom rules if provided
        if rules:
            custom_score, custom_issues = await self._apply_custom_rules(data, rules)
            results["custom_rules_score"] = custom_score
            results["issues"].extend(custom_issues)
            
        # Cache results
        await self._cache_result(cache_key, results)
        
        # Track metrics
        await self._track_quality_metrics(dataset_id, results)
        
        return results
        
    async def execute_rule(self, rule_id: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Execute a specific quality rule"""
        rule = self.rules_cache.get(rule_id)
        if not rule:
            raise ValueError(f"Rule {rule_id} not found")
            
        logger.info(f"Executing rule {rule_id}: {rule['name']}")
        
        try:
            if rule["type"] == RuleType.SQL.value:
                result = await self._execute_sql_rule(rule, data)
            elif rule["type"] == RuleType.PYTHON.value:
                result = await self._execute_python_rule(rule, data)
            elif rule["type"] == RuleType.REGEX.value:
                result = await self._execute_regex_rule(rule, data)
            elif rule["type"] == RuleType.STATISTICAL.value:
                result = await self._execute_statistical_rule(rule, data)
            else:
                result = await self._execute_business_rule(rule, data)
                
            return {
                "rule_id": rule_id,
                "rule_name": rule["name"],
                "passed": result["passed"],
                "violations": result.get("violations", 0),
                "details": result.get("details", {})
            }
            
        except Exception as e:
            logger.error(f"Error executing rule {rule_id}: {e}")
            return {
                "rule_id": rule_id,
                "rule_name": rule["name"],
                "passed": False,
                "error": str(e)
            }
            
    async def calculate_quality_score(self, dataset_id: str, 
                                    dimension_scores: Dict[str, float]) -> float:
        """Calculate weighted quality score"""
        # Default weights (can be customized per dataset)
        weights = {
            QualityDimension.COMPLETENESS.value: 0.25,
            QualityDimension.ACCURACY.value: 0.20,
            QualityDimension.CONSISTENCY.value: 0.15,
            QualityDimension.VALIDITY.value: 0.15,
            QualityDimension.UNIQUENESS.value: 0.15,
            QualityDimension.TIMELINESS.value: 0.10
        }
        
        # Get custom weights if available
        custom_weights = await self._get_dataset_weights(dataset_id)
        if custom_weights:
            weights.update(custom_weights)
            
        # Calculate weighted score
        total_score = 0.0
        total_weight = 0.0
        
        for dimension, score in dimension_scores.items():
            weight = weights.get(dimension, 0.1)
            total_score += score * weight
            total_weight += weight
            
        return total_score / total_weight if total_weight > 0 else 0.0
        
    async def _check_completeness(self, data: pd.DataFrame) -> Tuple[float, List[Dict[str, Any]]]:
        """Check data completeness"""
        issues = []
        
        # Calculate null percentages for each column
        null_counts = data.isnull().sum()
        null_percentages = (null_counts / len(data) * 100).round(2)
        
        # Identify columns with significant missing data
        threshold = 5.0  # 5% threshold
        problematic_columns = null_percentages[null_percentages > threshold]
        
        for column, percentage in problematic_columns.items():
            severity = "critical" if percentage > 20 else "warning"
            issues.append({
                "type": "missing_data",
                "dimension": "completeness",
                "column": column,
                "percentage": float(percentage),
                "severity": severity,
                "message": f"Column '{column}' has {percentage}% missing values"
            })
            
        # Calculate overall completeness score
        total_cells = data.shape[0] * data.shape[1]
        missing_cells = data.isnull().sum().sum()
        completeness_score = 1.0 - (missing_cells / total_cells) if total_cells > 0 else 0.0
        
        return completeness_score, issues
        
    async def _check_accuracy(self, data: pd.DataFrame, 
                            rules: Optional[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
        """Check data accuracy based on business rules"""
        issues = []
        violations = 0
        total_checks = 0
        
        # Apply accuracy rules if provided
        if rules and "accuracy_rules" in rules:
            for rule in rules["accuracy_rules"]:
                column = rule.get("column")
                if column not in data.columns:
                    continue
                    
                total_checks += len(data)
                
                # Range checks
                if "min_value" in rule:
                    mask = data[column] < rule["min_value"]
                    violations += mask.sum()
                    if mask.any():
                        issues.append({
                            "type": "range_violation",
                            "dimension": "accuracy",
                            "column": column,
                            "rule": f"min_value: {rule['min_value']}",
                            "violations": int(mask.sum()),
                            "severity": "warning"
                        })
                        
                if "max_value" in rule:
                    mask = data[column] > rule["max_value"]
                    violations += mask.sum()
                    if mask.any():
                        issues.append({
                            "type": "range_violation",
                            "dimension": "accuracy",
                            "column": column,
                            "rule": f"max_value: {rule['max_value']}",
                            "violations": int(mask.sum()),
                            "severity": "warning"
                        })
                        
        # Calculate accuracy score
        accuracy_score = 1.0 - (violations / total_checks) if total_checks > 0 else 1.0
        
        return accuracy_score, issues
        
    async def _check_consistency(self, data: pd.DataFrame) -> Tuple[float, List[Dict[str, Any]]]:
        """Check data consistency"""
        issues = []
        consistency_checks = 0
        consistency_violations = 0
        
        # Check for inconsistent data types
        for column in data.columns:
            # Skip if column has uniform type
            if data[column].dtype == 'object':
                # Check for mixed types
                types = data[column].dropna().apply(type).value_counts()
                if len(types) > 1:
                    consistency_violations += 1
                    issues.append({
                        "type": "mixed_types",
                        "dimension": "consistency",
                        "column": column,
                        "types": [str(t) for t in types.index],
                        "severity": "warning"
                    })
            consistency_checks += 1
            
        # Check for inconsistent formats (e.g., dates, phone numbers)
        # This is a simplified check - in production, use more sophisticated patterns
        date_pattern = r'^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$'
        for column in data.select_dtypes(include=['object']).columns:
            sample = data[column].dropna().head(100)
            if sample.str.match(date_pattern).any():
                # Check if all values match same pattern
                formats = sample.apply(lambda x: "ISO" if re.match(r'^\d{4}-\d{2}-\d{2}$', str(x)) else "US")
                if len(formats.unique()) > 1:
                    consistency_violations += 1
                    issues.append({
                        "type": "inconsistent_format",
                        "dimension": "consistency",
                        "column": column,
                        "formats": formats.unique().tolist(),
                        "severity": "warning"
                    })
            consistency_checks += 1
            
        # Calculate consistency score
        consistency_score = 1.0 - (consistency_violations / consistency_checks) if consistency_checks > 0 else 1.0
        
        return consistency_score, issues
        
    async def _check_validity(self, data: pd.DataFrame, 
                            rules: Optional[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
        """Check data validity against schemas and patterns"""
        issues = []
        validity_checks = 0
        validity_violations = 0
        
        # Email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        for column in data.columns:
            if 'email' in column.lower():
                validity_checks += len(data[column].dropna())
                invalid_emails = data[column].dropna()[~data[column].dropna().str.match(email_pattern)]
                validity_violations += len(invalid_emails)
                if len(invalid_emails) > 0:
                    issues.append({
                        "type": "invalid_format",
                        "dimension": "validity",
                        "column": column,
                        "format": "email",
                        "violations": len(invalid_emails),
                        "severity": "error"
                    })
                    
        # Apply custom validation rules
        if rules and "validation_rules" in rules:
            for rule in rules["validation_rules"]:
                column = rule.get("column")
                pattern = rule.get("pattern")
                if column in data.columns and pattern:
                    validity_checks += len(data[column].dropna())
                    invalid = data[column].dropna()[~data[column].dropna().str.match(pattern)]
                    validity_violations += len(invalid)
                    if len(invalid) > 0:
                        issues.append({
                            "type": "pattern_violation",
                            "dimension": "validity",
                            "column": column,
                            "pattern": pattern,
                            "violations": len(invalid),
                            "severity": "error"
                        })
                        
        # Calculate validity score
        validity_score = 1.0 - (validity_violations / validity_checks) if validity_checks > 0 else 1.0
        
        return validity_score, issues
        
    async def _check_uniqueness(self, data: pd.DataFrame) -> Tuple[float, List[Dict[str, Any]]]:
        """Check for duplicate records"""
        issues = []
        
        # Check for complete duplicate rows
        duplicate_rows = data.duplicated().sum()
        if duplicate_rows > 0:
            issues.append({
                "type": "duplicate_rows",
                "dimension": "uniqueness",
                "count": int(duplicate_rows),
                "percentage": float(duplicate_rows / len(data) * 100),
                "severity": "warning"
            })
            
        # Check for duplicates in columns that should be unique (e.g., ID columns)
        for column in data.columns:
            if any(id_indicator in column.lower() for id_indicator in ['id', 'key', 'code']):
                duplicates = data[column].duplicated().sum()
                if duplicates > 0:
                    issues.append({
                        "type": "duplicate_values",
                        "dimension": "uniqueness",
                        "column": column,
                        "count": int(duplicates),
                        "severity": "critical"
                    })
                    
        # Calculate uniqueness score
        total_uniqueness_checks = len(data) + len([col for col in data.columns if 'id' in col.lower()])
        uniqueness_violations = duplicate_rows + sum([i["count"] for i in issues if i["type"] == "duplicate_values"])
        uniqueness_score = 1.0 - (uniqueness_violations / total_uniqueness_checks) if total_uniqueness_checks > 0 else 1.0
        
        return uniqueness_score, issues
        
    async def _check_timeliness(self, data: pd.DataFrame, 
                              rules: Optional[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
        """Check data timeliness"""
        issues = []
        timeliness_score = 1.0
        
        # Check for date columns
        date_columns = []
        for column in data.columns:
            if 'date' in column.lower() or 'time' in column.lower():
                try:
                    # Try to convert to datetime
                    pd.to_datetime(data[column].dropna().head(10))
                    date_columns.append(column)
                except:
                    pass
                    
        # Check if data is recent based on rules
        if rules and "timeliness_rules" in rules:
            for rule in rules["timeliness_rules"]:
                column = rule.get("column")
                max_age_days = rule.get("max_age_days", 30)
                
                if column in date_columns:
                    try:
                        dates = pd.to_datetime(data[column].dropna())
                        current_date = pd.Timestamp.now()
                        old_data = dates[dates < current_date - pd.Timedelta(days=max_age_days)]
                        
                        if len(old_data) > 0:
                            percentage = len(old_data) / len(dates) * 100
                            timeliness_score *= (1 - percentage / 100)
                            issues.append({
                                "type": "stale_data",
                                "dimension": "timeliness",
                                "column": column,
                                "threshold_days": max_age_days,
                                "stale_percentage": float(percentage),
                                "severity": "warning" if percentage < 20 else "error"
                            })
                    except Exception as e:
                        logger.error(f"Error checking timeliness for {column}: {e}")
                        
        return timeliness_score, issues
        
    async def _apply_custom_rules(self, data: pd.DataFrame, 
                                rules: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
        """Apply custom business rules"""
        issues = []
        total_rules = 0
        passed_rules = 0
        
        for rule_id in rules.get("custom_rule_ids", []):
            if rule_id in self.rules_cache:
                total_rules += 1
                result = await self.execute_rule(rule_id, data)
                if result["passed"]:
                    passed_rules += 1
                else:
                    issues.append({
                        "type": "custom_rule_violation",
                        "dimension": "custom",
                        "rule_id": rule_id,
                        "rule_name": result["rule_name"],
                        "severity": "error",
                        "details": result.get("details", {})
                    })
                    
        custom_score = passed_rules / total_rules if total_rules > 0 else 1.0
        return custom_score, issues
        
    async def _execute_sql_rule(self, rule: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Execute SQL-based rule using pandas"""
        # Convert DataFrame to SQL-queryable format (simplified)
        # In production, use actual SQL engine
        query = rule["query"]
        
        # Simple implementation - check if any rows match the condition
        try:
            # This is a placeholder - implement actual SQL execution
            violations = 0
            return {"passed": violations == 0, "violations": violations}
        except Exception as e:
            logger.error(f"SQL rule execution failed: {e}")
            return {"passed": False, "error": str(e)}
            
    async def _execute_python_rule(self, rule: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Execute Python expression rule"""
        expression = rule["expression"]
        
        try:
            # Create safe execution environment
            safe_dict = {
                'data': data,
                'pd': pd,
                'np': np,
                'len': len,
                'sum': sum,
                'min': min,
                'max': max
            }
            
            # Execute expression
            result = eval(expression, {"__builtins__": {}}, safe_dict)
            
            if isinstance(result, bool):
                return {"passed": result}
            elif isinstance(result, pd.Series):
                violations = (~result).sum()
                return {"passed": violations == 0, "violations": int(violations)}
            else:
                return {"passed": False, "error": "Invalid rule result type"}
                
        except Exception as e:
            logger.error(f"Python rule execution failed: {e}")
            return {"passed": False, "error": str(e)}
            
    async def _execute_regex_rule(self, rule: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Execute regex pattern rule"""
        column = rule["column"]
        pattern = rule["pattern"]
        
        if column not in data.columns:
            return {"passed": False, "error": f"Column {column} not found"}
            
        try:
            # Check if all non-null values match the pattern
            non_null_data = data[column].dropna()
            if len(non_null_data) == 0:
                return {"passed": True}
                
            matches = non_null_data.str.match(pattern)
            violations = (~matches).sum()
            
            return {
                "passed": violations == 0,
                "violations": int(violations),
                "details": {
                    "total_checked": len(non_null_data),
                    "matched": int(matches.sum())
                }
            }
        except Exception as e:
            logger.error(f"Regex rule execution failed: {e}")
            return {"passed": False, "error": str(e)}
            
    async def _execute_statistical_rule(self, rule: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Execute statistical rule"""
        column = rule["column"]
        
        if column not in data.columns:
            return {"passed": False, "error": f"Column {column} not found"}
            
        try:
            values = pd.to_numeric(data[column], errors='coerce').dropna()
            
            # Statistical checks
            violations = 0
            details = {}
            
            if "mean_range" in rule:
                mean_val = values.mean()
                min_mean, max_mean = rule["mean_range"]
                if mean_val < min_mean or mean_val > max_mean:
                    violations += 1
                details["mean"] = float(mean_val)
                
            if "std_range" in rule:
                std_val = values.std()
                min_std, max_std = rule["std_range"]
                if std_val < min_std or std_val > max_std:
                    violations += 1
                details["std"] = float(std_val)
                
            if "outlier_threshold" in rule:
                # Z-score based outlier detection
                threshold = rule["outlier_threshold"]
                z_scores = np.abs((values - values.mean()) / values.std())
                outliers = z_scores > threshold
                violations += outliers.sum()
                details["outliers"] = int(outliers.sum())
                
            return {
                "passed": violations == 0,
                "violations": violations,
                "details": details
            }
        except Exception as e:
            logger.error(f"Statistical rule execution failed: {e}")
            return {"passed": False, "error": str(e)}
            
    async def _execute_business_rule(self, rule: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """Execute complex business rule"""
        # This is a placeholder for complex business logic
        # In production, implement specific business rule types
        return {"passed": True, "details": {"message": "Business rule execution not implemented"}}
        
    async def _load_rules(self):
        """Load quality rules from storage"""
        # In production, load from database or configuration
        # For now, load some default rules
        self.rules_cache = {
            "rule_001": {
                "id": "rule_001",
                "name": "Email Format Validation",
                "type": RuleType.REGEX.value,
                "column": "email",
                "pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                "severity": "error"
            },
            "rule_002": {
                "id": "rule_002",
                "name": "Age Range Check",
                "type": RuleType.STATISTICAL.value,
                "column": "age",
                "mean_range": [25, 65],
                "outlier_threshold": 3,
                "severity": "warning"
            }
        }
        logger.info(f"Loaded {len(self.rules_cache)} quality rules")
        
    async def _get_cached_result(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached quality result"""
        if self.quality_cache:
            try:
                cached = self.quality_cache.get(key)
                if cached:
                    return json.loads(cached)
            except:
                pass
        return None
        
    async def _cache_result(self, key: str, result: Dict[str, Any]):
        """Cache quality result"""
        if self.quality_cache:
            try:
                self.quality_cache.put(key, json.dumps(result))
            except Exception as e:
                logger.error(f"Failed to cache result: {e}")
                
    async def _get_dataset_weights(self, dataset_id: str) -> Optional[Dict[str, float]]:
        """Get custom quality dimension weights for dataset"""
        # In production, retrieve from configuration
        return None
        
    async def _track_quality_metrics(self, dataset_id: str, results: Dict[str, Any]):
        """Track quality metrics for monitoring"""
        # In production, send to Prometheus or other monitoring system
        logger.info(f"Quality metrics for {dataset_id}: score={results['quality_score']}, issues={len(results['issues'])}")
        
    async def _monitor_quality_trends(self):
        """Monitor quality trends across datasets"""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                # In production, analyze trends and trigger alerts
                logger.debug("Monitoring quality trends")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring quality trends: {e}") 