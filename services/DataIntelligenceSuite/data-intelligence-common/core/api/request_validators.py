"""
Request validation components for standardized API patterns.

Provides common validators, constraints, and base request models.
"""

from typing import Any, Dict, List, Optional, Type, Union, Callable
from datetime import datetime
from enum import Enum
import re
from pydantic import BaseModel, Field, validator, root_validator
from pydantic.types import constr, conint, confloat


class SortOrder(str, Enum):
    """Standard sort order options."""
    ASC = "asc"
    DESC = "desc"


class BaseRequestModel(BaseModel):
    """Base model for all API requests with common fields."""
    
    request_id: Optional[str] = Field(
        None,
        description="Unique request identifier for tracing"
    )
    timestamp: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        description="Request timestamp"
    )
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PaginationRequest(BaseModel):
    """Standard pagination parameters."""
    
    page: conint(ge=1) = Field(1, description="Page number (1-indexed)")
    page_size: conint(ge=1, le=1000) = Field(
        50,
        description="Number of items per page"
    )
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: SortOrder = Field(SortOrder.ASC, description="Sort order")
    
    @validator("sort_by")
    def validate_sort_field(cls, v: Optional[str]) -> Optional[str]:
        """Validate sort field name."""
        if v and not re.match(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$", v):
            raise ValueError("Invalid sort field name")
        return v


class FilterRequest(BaseModel):
    """Standard filtering parameters."""
    
    filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Key-value pairs for filtering"
    )
    search: Optional[str] = Field(
        None,
        description="Full-text search query"
    )
    date_from: Optional[datetime] = Field(
        None,
        description="Start date for date range filters"
    )
    date_to: Optional[datetime] = Field(
        None,
        description="End date for date range filters"
    )
    
    @root_validator
    def validate_date_range(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate date range consistency."""
        date_from = values.get("date_from")
        date_to = values.get("date_to")
        
        if date_from and date_to and date_from > date_to:
            raise ValueError("date_from must be before date_to")
        
        return values


class BulkOperationRequest(BaseModel):
    """Standard bulk operation request."""
    
    operation: str = Field(..., description="Operation to perform")
    ids: List[str] = Field(
        ...,
        min_items=1,
        max_items=1000,
        description="List of resource IDs"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Operation-specific options"
    )
    
    @validator("ids")
    def validate_unique_ids(cls, v: List[str]) -> List[str]:
        """Ensure IDs are unique."""
        if len(set(v)) != len(v):
            raise ValueError("Duplicate IDs in bulk operation")
        return v


# Common field validators
def validate_email(email: str) -> str:
    """Validate email format."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, email):
        raise ValueError("Invalid email format")
    return email.lower()


def validate_phone(phone: str) -> str:
    """Validate phone number format."""
    # Remove common separators
    cleaned = re.sub(r"[\s\-\(\)]+", "", phone)
    
    # Check if it's a valid phone number
    if not re.match(r"^\+?\d{10,15}$", cleaned):
        raise ValueError("Invalid phone number format")
    
    return cleaned


def validate_url(url: str) -> str:
    """Validate URL format."""
    pattern = r"^https?://[^\s/$.?#].[^\s]*$"
    if not re.match(pattern, url, re.IGNORECASE):
        raise ValueError("Invalid URL format")
    return url


def validate_json_path(path: str) -> str:
    """Validate JSONPath expression."""
    if not path.startswith("$"):
        raise ValueError("JSONPath must start with $")
    
    # Basic validation for common JSONPath patterns
    valid_pattern = r"^\$(\.[a-zA-Z_][a-zA-Z0-9_]*|\[[0-9]+\]|\[\*\])*$"
    if not re.match(valid_pattern, path):
        raise ValueError("Invalid JSONPath expression")
    
    return path


# Constraint types
NonEmptyStr = constr(min_length=1, strip_whitespace=True)
Identifier = constr(regex=r"^[a-zA-Z_][a-zA-Z0-9_-]*$", min_length=1, max_length=255)
SafeString = constr(regex=r"^[a-zA-Z0-9\s\-_\.]+$", min_length=1, max_length=255)
Percentage = confloat(ge=0.0, le=100.0)
PositiveInt = conint(gt=0)
NonNegativeInt = conint(ge=0)


class ValidationRule(BaseModel):
    """Configurable validation rule."""
    
    field: str = Field(..., description="Field to validate")
    rule_type: str = Field(..., description="Type of validation")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Rule parameters"
    )
    message: Optional[str] = Field(
        None,
        description="Custom error message"
    )


class DynamicValidator:
    """Dynamic field validator based on configuration."""
    
    def __init__(self, rules: List[ValidationRule]):
        self.rules = rules
        self._validators = self._build_validators()
    
    def _build_validators(self) -> Dict[str, List[Callable]]:
        """Build validator functions from rules."""
        validators = {}
        
        for rule in self.rules:
            if rule.field not in validators:
                validators[rule.field] = []
            
            validator_func = self._get_validator(rule)
            if validator_func:
                validators[rule.field].append(validator_func)
        
        return validators
    
    def _get_validator(self, rule: ValidationRule) -> Optional[Callable]:
        """Get validator function for a rule."""
        validators_map = {
            "required": self._required_validator,
            "min_length": self._min_length_validator,
            "max_length": self._max_length_validator,
            "pattern": self._pattern_validator,
            "min_value": self._min_value_validator,
            "max_value": self._max_value_validator,
            "enum": self._enum_validator,
            "custom": self._custom_validator
        }
        
        validator_factory = validators_map.get(rule.rule_type)
        if validator_factory:
            return validator_factory(rule)
        
        return None
    
    def _required_validator(self, rule: ValidationRule) -> Callable:
        """Create required field validator."""
        def validate(v: Any) -> Any:
            if v is None or (isinstance(v, str) and not v.strip()):
                raise ValueError(rule.message or f"{rule.field} is required")
            return v
        return validate
    
    def _min_length_validator(self, rule: ValidationRule) -> Callable:
        """Create minimum length validator."""
        min_len = rule.params.get("min", 0)
        
        def validate(v: Any) -> Any:
            if v and len(str(v)) < min_len:
                raise ValueError(
                    rule.message or f"{rule.field} must be at least {min_len} characters"
                )
            return v
        return validate
    
    def _max_length_validator(self, rule: ValidationRule) -> Callable:
        """Create maximum length validator."""
        max_len = rule.params.get("max", float("inf"))
        
        def validate(v: Any) -> Any:
            if v and len(str(v)) > max_len:
                raise ValueError(
                    rule.message or f"{rule.field} must be at most {max_len} characters"
                )
            return v
        return validate
    
    def _pattern_validator(self, rule: ValidationRule) -> Callable:
        """Create pattern validator."""
        pattern = rule.params.get("pattern")
        if not pattern:
            return lambda v: v
        
        def validate(v: Any) -> Any:
            if v and not re.match(pattern, str(v)):
                raise ValueError(
                    rule.message or f"{rule.field} does not match required pattern"
                )
            return v
        return validate
    
    def _min_value_validator(self, rule: ValidationRule) -> Callable:
        """Create minimum value validator."""
        min_val = rule.params.get("min", float("-inf"))
        
        def validate(v: Any) -> Any:
            if v is not None and float(v) < min_val:
                raise ValueError(
                    rule.message or f"{rule.field} must be at least {min_val}"
                )
            return v
        return validate
    
    def _max_value_validator(self, rule: ValidationRule) -> Callable:
        """Create maximum value validator."""
        max_val = rule.params.get("max", float("inf"))
        
        def validate(v: Any) -> Any:
            if v is not None and float(v) > max_val:
                raise ValueError(
                    rule.message or f"{rule.field} must be at most {max_val}"
                )
            return v
        return validate
    
    def _enum_validator(self, rule: ValidationRule) -> Callable:
        """Create enum validator."""
        allowed = rule.params.get("values", [])
        
        def validate(v: Any) -> Any:
            if v and v not in allowed:
                raise ValueError(
                    rule.message or f"{rule.field} must be one of {allowed}"
                )
            return v
        return validate
    
    def _custom_validator(self, rule: ValidationRule) -> Callable:
        """Create custom validator from function name."""
        func_name = rule.params.get("function")
        if func_name == "email":
            return validate_email
        elif func_name == "phone":
            return validate_phone
        elif func_name == "url":
            return validate_url
        elif func_name == "json_path":
            return validate_json_path
        
        return lambda v: v
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate data against rules."""
        errors = {}
        
        for field, validators in self._validators.items():
            value = data.get(field)
            field_errors = []
            
            for validator in validators:
                try:
                    validator(value)
                except ValueError as e:
                    field_errors.append(str(e))
            
            if field_errors:
                errors[field] = field_errors
        
        return errors


# Request model factories
def create_request_model(
    name: str,
    fields: Dict[str, tuple],
    base: Type[BaseModel] = BaseRequestModel,
    validators: Optional[Dict[str, Callable]] = None
) -> Type[BaseModel]:
    """
    Dynamically create a request model.
    
    Args:
        name: Model name
        fields: Field definitions as {name: (type, Field(...))}
        base: Base model class
        validators: Field validators
    
    Returns:
        Generated model class
    """
    namespace = {
        "__module__": __name__,
        "__annotations__": {},
        **fields
    }
    
    # Add type annotations
    for field_name, (field_type, _) in fields.items():
        namespace["__annotations__"][field_name] = field_type
    
    # Add validators
    if validators:
        for field_name, validator_func in validators.items():
            namespace[f"validate_{field_name}"] = validator(
                field_name,
                allow_reuse=True
            )(validator_func)
    
    return type(name, (base,), namespace)


# Common request models
class ResourceCreateRequest(BaseRequestModel):
    """Standard resource creation request."""
    
    name: NonEmptyStr = Field(..., description="Resource name")
    description: Optional[str] = Field(None, description="Resource description")
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )
    tags: List[str] = Field(
        default_factory=list,
        description="Resource tags"
    )
    
    @validator("tags")
    def validate_tags(cls, v: List[str]) -> List[str]:
        """Validate tag format."""
        for tag in v:
            if not re.match(r"^[a-zA-Z0-9\-_]+$", tag):
                raise ValueError(f"Invalid tag format: {tag}")
        return v


class ResourceUpdateRequest(BaseRequestModel):
    """Standard resource update request."""
    
    name: Optional[NonEmptyStr] = Field(None, description="Resource name")
    description: Optional[str] = Field(None, description="Resource description")
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional metadata"
    )
    tags: Optional[List[str]] = Field(None, description="Resource tags")
    version: Optional[int] = Field(
        None,
        description="Version for optimistic locking"
    ) 