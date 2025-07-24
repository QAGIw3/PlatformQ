"""
Quality checking stages for unified processing.

Provides reusable quality validation and monitoring stages.
"""

from typing import Any, Dict, List, Optional, Set, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re

from .unified_processor import ProcessingStage, ProcessingContext
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class QualityLevel(str, Enum):
    """Quality check severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class QualityCheckType(str, Enum):
    """Types of quality checks"""
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    SCHEMA = "schema"
    BUSINESS_RULE = "business_rule"


@dataclass
class QualityRule:
    """Quality rule definition"""
    name: str
    check_type: QualityCheckType
    level: QualityLevel
    condition: Callable[[Any], bool]
    message: str
    fields: Optional[List[str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityResult:
    """Result of quality check"""
    rule_name: str
    passed: bool
    level: QualityLevel
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class QualityCheckStage(ProcessingStage):
    """Stage for applying quality checks"""
    
    def __init__(
        self,
        rules: List[QualityRule],
        fail_on_error: bool = True,
        sample_rate: float = 1.0,
        collect_metrics: bool = True
    ):
        self.rules = rules
        self.fail_on_error = fail_on_error
        self.sample_rate = sample_rate
        self.collect_metrics = collect_metrics
        
        # Metrics
        self.check_counts = {rule.name: {"passed": 0, "failed": 0} for rule in rules}
        
    async def process(self, data: Any, context: ProcessingContext) -> Optional[Any]:
        """Apply quality checks to data"""
        # Sample if needed
        import random
        if self.sample_rate < 1.0 and random.random() > self.sample_rate:
            return data
            
        results = []
        has_error = False
        
        for rule in self.rules:
            try:
                # Apply rule
                passed = rule.condition(data)
                
                result = QualityResult(
                    rule_name=rule.name,
                    passed=passed,
                    level=rule.level,
                    message=rule.message if not passed else "Check passed"
                )
                
                # Add field values to details if specified
                if rule.fields and isinstance(data, dict):
                    result.details["field_values"] = {
                        field: data.get(field) for field in rule.fields
                    }
                    
                results.append(result)
                
                # Update metrics
                if self.collect_metrics:
                    if passed:
                        self.check_counts[rule.name]["passed"] += 1
                    else:
                        self.check_counts[rule.name]["failed"] += 1
                        
                # Check if we should fail
                if not passed and rule.level in (QualityLevel.ERROR, QualityLevel.CRITICAL):
                    has_error = True
                    
            except Exception as e:
                logger.error(f"Error applying rule {rule.name}: {e}")
                results.append(QualityResult(
                    rule_name=rule.name,
                    passed=False,
                    level=QualityLevel.ERROR,
                    message=f"Rule execution failed: {str(e)}"
                ))
                has_error = True
                
        # Store results in context
        if "quality_results" not in context.state:
            context.state["quality_results"] = []
        context.state["quality_results"].extend(results)
        
        # Log failures
        for result in results:
            if not result.passed and result.level in (QualityLevel.ERROR, QualityLevel.CRITICAL):
                logger.error(f"Quality check failed: {result.rule_name} - {result.message}")
                
        # Decide whether to continue
        if has_error and self.fail_on_error:
            return None  # Filter out the record
        else:
            return data


class SchemaValidationStage(ProcessingStage):
    """Stage for validating data schema"""
    
    def __init__(
        self,
        schema: Dict[str, Any],
        strict: bool = True,
        coerce_types: bool = False
    ):
        self.schema = schema
        self.strict = strict
        self.coerce_types = coerce_types
        
    async def process(self, data: Any, context: ProcessingContext) -> Optional[Any]:
        """Validate and optionally coerce data schema"""
        if not isinstance(data, dict):
            logger.error(f"Expected dict, got {type(data)}")
            return None if self.strict else data
            
        # Validate required fields
        required_fields = self.schema.get("required", [])
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field: {field}")
                if self.strict:
                    return None
                    
        # Validate field types
        properties = self.schema.get("properties", {})
        for field, field_schema in properties.items():
            if field in data:
                expected_type = field_schema.get("type")
                value = data[field]
                
                if not self._validate_type(value, expected_type):
                    if self.coerce_types:
                        # Try to coerce
                        coerced = self._coerce_type(value, expected_type)
                        if coerced is not None:
                            data[field] = coerced
                        elif self.strict:
                            logger.error(f"Cannot coerce {field} to {expected_type}")
                            return None
                    elif self.strict:
                        logger.error(f"Invalid type for {field}: expected {expected_type}")
                        return None
                        
        # Remove extra fields if strict
        if self.strict:
            allowed_fields = set(properties.keys())
            data = {k: v for k, v in data.items() if k in allowed_fields}
            
        return data
        
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate value type"""
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict
        }
        
        expected_python_type = type_map.get(expected_type)
        if expected_python_type:
            return isinstance(value, expected_python_type)
        return True
        
    def _coerce_type(self, value: Any, target_type: str) -> Optional[Any]:
        """Try to coerce value to target type"""
        try:
            if target_type == "string":
                return str(value)
            elif target_type == "integer":
                return int(value)
            elif target_type == "number":
                return float(value)
            elif target_type == "boolean":
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes", "on")
                return bool(value)
        except Exception:
            pass
        return None


class DataCleaningStage(ProcessingStage):
    """Stage for cleaning and normalizing data"""
    
    def __init__(
        self,
        trim_strings: bool = True,
        lowercase_fields: Optional[List[str]] = None,
        uppercase_fields: Optional[List[str]] = None,
        remove_nulls: bool = False,
        default_values: Optional[Dict[str, Any]] = None,
        date_formats: Optional[Dict[str, str]] = None
    ):
        self.trim_strings = trim_strings
        self.lowercase_fields = lowercase_fields or []
        self.uppercase_fields = uppercase_fields or []
        self.remove_nulls = remove_nulls
        self.default_values = default_values or {}
        self.date_formats = date_formats or {}
        
    async def process(self, data: Any, context: ProcessingContext) -> Optional[Any]:
        """Clean and normalize data"""
        if not isinstance(data, dict):
            return data
            
        cleaned = data.copy()
        
        # Process each field
        for field, value in list(cleaned.items()):
            # Remove nulls
            if self.remove_nulls and value is None:
                del cleaned[field]
                continue
                
            # Apply defaults
            if value is None and field in self.default_values:
                cleaned[field] = self.default_values[field]
                continue
                
            # String processing
            if isinstance(value, str):
                if self.trim_strings:
                    value = value.strip()
                    
                if field in self.lowercase_fields:
                    value = value.lower()
                elif field in self.uppercase_fields:
                    value = value.upper()
                    
                cleaned[field] = value
                
            # Date formatting
            if field in self.date_formats:
                cleaned[field] = self._format_date(value, self.date_formats[field])
                
        return cleaned
        
    def _format_date(self, value: Any, format: str) -> str:
        """Format date value"""
        if isinstance(value, str):
            # Parse and reformat
            from dateutil import parser
            try:
                dt = parser.parse(value)
                return dt.strftime(format)
            except Exception:
                return value
        elif isinstance(value, datetime):
            return value.strftime(format)
        return str(value)


class DeduplicationStage(ProcessingStage):
    """Stage for deduplicating data"""
    
    def __init__(
        self,
        key_fields: List[str],
        window_size: int = 10000,
        strategy: str = "keep_first"  # keep_first, keep_last, keep_all
    ):
        self.key_fields = key_fields
        self.window_size = window_size
        self.strategy = strategy
        self._seen_keys: Set[str] = set()
        self._key_queue: List[str] = []
        
    async def process(self, data: Any, context: ProcessingContext) -> Optional[Any]:
        """Check for duplicates"""
        if not isinstance(data, dict):
            return data
            
        # Generate key
        key_values = []
        for field in self.key_fields:
            value = data.get(field, "")
            key_values.append(str(value))
        key = "|".join(key_values)
        
        # Check if duplicate
        is_duplicate = key in self._seen_keys
        
        if is_duplicate:
            if self.strategy == "keep_first":
                return None  # Filter out
            elif self.strategy == "keep_last":
                # Keep this one, but we already have the key
                return data
            # keep_all - continue processing
            
        # Add to seen keys
        self._seen_keys.add(key)
        self._key_queue.append(key)
        
        # Maintain window
        if len(self._key_queue) > self.window_size:
            old_key = self._key_queue.pop(0)
            self._seen_keys.discard(old_key)
            
        return data


class AnomalyDetectionStage(ProcessingStage):
    """Stage for detecting anomalies"""
    
    def __init__(
        self,
        numeric_fields: List[str],
        method: str = "zscore",  # zscore, iqr, isolation_forest
        threshold: float = 3.0,
        window_size: int = 1000
    ):
        self.numeric_fields = numeric_fields
        self.method = method
        self.threshold = threshold
        self.window_size = window_size
        
        # Statistics tracking
        self._values: Dict[str, List[float]] = {field: [] for field in numeric_fields}
        
    async def process(self, data: Any, context: ProcessingContext) -> Optional[Any]:
        """Detect anomalies in data"""
        if not isinstance(data, dict):
            return data
            
        anomalies = []
        
        for field in self.numeric_fields:
            value = data.get(field)
            if value is None or not isinstance(value, (int, float)):
                continue
                
            # Add to history
            self._values[field].append(value)
            if len(self._values[field]) > self.window_size:
                self._values[field].pop(0)
                
            # Check for anomaly
            if len(self._values[field]) >= 10:  # Need minimum samples
                is_anomaly = self._detect_anomaly(value, self._values[field])
                if is_anomaly:
                    anomalies.append({
                        "field": field,
                        "value": value,
                        "method": self.method
                    })
                    
        # Add anomaly info to data
        if anomalies:
            if "_quality_metadata" not in data:
                data["_quality_metadata"] = {}
            data["_quality_metadata"]["anomalies"] = anomalies
            
        return data
        
    def _detect_anomaly(self, value: float, history: List[float]) -> bool:
        """Detect if value is anomalous"""
        if self.method == "zscore":
            import numpy as np
            mean = np.mean(history)
            std = np.std(history)
            if std == 0:
                return False
            zscore = abs((value - mean) / std)
            return zscore > self.threshold
            
        elif self.method == "iqr":
            import numpy as np
            q1 = np.percentile(history, 25)
            q3 = np.percentile(history, 75)
            iqr = q3 - q1
            lower = q1 - (self.threshold * iqr)
            upper = q3 + (self.threshold * iqr)
            return value < lower or value > upper
            
        return False


# Predefined quality rules
class CommonQualityRules:
    """Common quality rules for reuse"""
    
    @staticmethod
    def not_null(field: str) -> QualityRule:
        """Field must not be null"""
        return QualityRule(
            name=f"{field}_not_null",
            check_type=QualityCheckType.COMPLETENESS,
            level=QualityLevel.ERROR,
            condition=lambda data: isinstance(data, dict) and data.get(field) is not None,
            message=f"Field '{field}' is null",
            fields=[field]
        )
        
    @staticmethod
    def not_empty(field: str) -> QualityRule:
        """Field must not be empty"""
        return QualityRule(
            name=f"{field}_not_empty",
            check_type=QualityCheckType.COMPLETENESS,
            level=QualityLevel.ERROR,
            condition=lambda data: isinstance(data, dict) and bool(data.get(field)),
            message=f"Field '{field}' is empty",
            fields=[field]
        )
        
    @staticmethod
    def in_range(field: str, min_val: float, max_val: float) -> QualityRule:
        """Numeric field must be in range"""
        return QualityRule(
            name=f"{field}_in_range",
            check_type=QualityCheckType.VALIDITY,
            level=QualityLevel.WARNING,
            condition=lambda data: isinstance(data, dict) and (
                data.get(field) is None or 
                (isinstance(data.get(field), (int, float)) and min_val <= data.get(field) <= max_val)
            ),
            message=f"Field '{field}' not in range [{min_val}, {max_val}]",
            fields=[field]
        )
        
    @staticmethod
    def matches_pattern(field: str, pattern: str) -> QualityRule:
        """Field must match regex pattern"""
        regex = re.compile(pattern)
        return QualityRule(
            name=f"{field}_matches_pattern",
            check_type=QualityCheckType.VALIDITY,
            level=QualityLevel.WARNING,
            condition=lambda data: isinstance(data, dict) and (
                data.get(field) is None or 
                (isinstance(data.get(field), str) and regex.match(data.get(field)))
            ),
            message=f"Field '{field}' does not match pattern '{pattern}'",
            fields=[field]
        )
        
    @staticmethod
    def unique_combination(fields: List[str]) -> QualityRule:
        """Combination of fields should be unique"""
        seen = set()
        
        def check_unique(data):
            if not isinstance(data, dict):
                return True
            key = tuple(data.get(f) for f in fields)
            if key in seen:
                return False
            seen.add(key)
            return True
            
        return QualityRule(
            name=f"unique_{'_'.join(fields)}",
            check_type=QualityCheckType.UNIQUENESS,
            level=QualityLevel.ERROR,
            condition=check_unique,
            message=f"Duplicate combination of fields: {fields}",
            fields=fields
        )


# Export quality components
__all__ = [
    'QualityLevel',
    'QualityCheckType',
    'QualityRule',
    'QualityResult',
    'QualityCheckStage',
    'SchemaValidationStage',
    'DataCleaningStage',
    'DeduplicationStage',
    'AnomalyDetectionStage',
    'CommonQualityRules'
] 