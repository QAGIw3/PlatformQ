"""
Validation Utilities

Common validation functions for data integrity.
"""

import re
import json
import uuid
from typing import Any, Dict, Optional, List
from datetime import datetime
from email_validator import validate_email as _validate_email, EmailNotValidError
from urllib.parse import urlparse
import jsonschema


def validate_email(email: str) -> bool:
    """
    Validate email address format.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        _validate_email(email)
        return True
    except EmailNotValidError:
        return False


def validate_url(url: str, schemes: Optional[List[str]] = None) -> bool:
    """
    Validate URL format.
    
    Args:
        url: URL to validate
        schemes: Allowed URL schemes (default: ['http', 'https'])
        
    Returns:
        True if valid, False otherwise
    """
    if schemes is None:
        schemes = ['http', 'https']
        
    try:
        result = urlparse(url)
        return all([
            result.scheme in schemes,
            result.netloc != '',
            result.scheme != '',
        ])
    except Exception:
        return False


def validate_json(json_string: str) -> bool:
    """
    Validate JSON string format.
    
    Args:
        json_string: JSON string to validate
        
    Returns:
        True if valid JSON, False otherwise
    """
    try:
        json.loads(json_string)
        return True
    except (json.JSONDecodeError, TypeError):
        return False


def validate_uuid(uuid_string: str, version: Optional[int] = None) -> bool:
    """
    Validate UUID string format.
    
    Args:
        uuid_string: UUID string to validate
        version: Specific UUID version to validate (1-5)
        
    Returns:
        True if valid UUID, False otherwise
    """
    try:
        uuid_obj = uuid.UUID(uuid_string)
        if version is not None:
            return uuid_obj.version == version
        return True
    except (ValueError, AttributeError):
        return False


def validate_datetime(
    datetime_string: str,
    format_string: str = "%Y-%m-%dT%H:%M:%S"
) -> bool:
    """
    Validate datetime string format.
    
    Args:
        datetime_string: Datetime string to validate
        format_string: Expected datetime format
        
    Returns:
        True if valid datetime, False otherwise
    """
    try:
        datetime.strptime(datetime_string, format_string)
        return True
    except (ValueError, TypeError):
        return False


def validate_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate data against JSON schema.
    
    Args:
        data: Data to validate
        schema: JSON schema to validate against
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        jsonschema.validate(instance=data, schema=schema)
        return True, None
    except jsonschema.exceptions.ValidationError as e:
        return False, str(e)
    except jsonschema.exceptions.SchemaError as e:
        return False, f"Schema error: {str(e)}"


# Common regex patterns
PATTERNS = {
    'phone': re.compile(r'^\+?1?\d{9,15}$'),
    'alphanumeric': re.compile(r'^[a-zA-Z0-9]+$'),
    'alpha': re.compile(r'^[a-zA-Z]+$'),
    'numeric': re.compile(r'^[0-9]+$'),
    'slug': re.compile(r'^[a-z0-9]+(?:-[a-z0-9]+)*$'),
    'ip_v4': re.compile(
        r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    ),
    'ip_v6': re.compile(
        r'^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|'
        r'([0-9a-fA-F]{1,4}:){1,7}:|'
        r'([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|'
        r'([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|'
        r'([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|'
        r'([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|'
        r'([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|'
        r'[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|'
        r':((:[0-9a-fA-F]{1,4}){1,7}|:)|'
        r'fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|'
        r'::(ffff(:0{1,4}){0,1}:){0,1}'
        r'((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}'
        r'(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|'
        r'([0-9a-fA-F]{1,4}:){1,4}:'
        r'((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}'
        r'(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$'
    ),
}


def validate_pattern(value: str, pattern_name: str) -> bool:
    """
    Validate value against a predefined pattern.
    
    Args:
        value: Value to validate
        pattern_name: Name of pattern to use
        
    Returns:
        True if matches pattern, False otherwise
    """
    pattern = PATTERNS.get(pattern_name)
    if pattern is None:
        raise ValueError(f"Unknown pattern: {pattern_name}")
        
    return bool(pattern.match(value))


def validate_phone(phone: str) -> bool:
    """Validate phone number format"""
    return validate_pattern(phone, 'phone')


def validate_ip(ip: str, version: Optional[int] = None) -> bool:
    """
    Validate IP address format.
    
    Args:
        ip: IP address to validate
        version: IP version (4 or 6, None for both)
        
    Returns:
        True if valid IP, False otherwise
    """
    if version == 4:
        return validate_pattern(ip, 'ip_v4')
    elif version == 6:
        return validate_pattern(ip, 'ip_v6')
    else:
        return validate_pattern(ip, 'ip_v4') or validate_pattern(ip, 'ip_v6')


def validate_range(
    value: float,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> bool:
    """
    Validate numeric value is within range.
    
    Args:
        value: Value to validate
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        
    Returns:
        True if within range, False otherwise
    """
    if min_value is not None and value < min_value:
        return False
    if max_value is not None and value > max_value:
        return False
    return True


def validate_length(
    value: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None
) -> bool:
    """
    Validate string length.
    
    Args:
        value: String to validate
        min_length: Minimum allowed length
        max_length: Maximum allowed length
        
    Returns:
        True if length is valid, False otherwise
    """
    length = len(value)
    return validate_range(length, min_length, max_length) 