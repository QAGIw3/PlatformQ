"""
Event Validator Pulsar Function

Validates and filters events based on schema and business rules.
Runs directly on Pulsar for minimal latency.
"""

from pulsar import Function
import json
import re
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging


class EventValidatorFunction(Function):
    """
    Pulsar Function for validating and filtering events
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._init_validation_rules()
        
    def _init_validation_rules(self):
        """Initialize validation rules"""
        self.required_fields = {
            'compute.order': ['user_id', 'resource_type', 'quantity', 'duration_hours'],
            'settlement.initiated': ['settlement_id', 'contract_id', 'amount'],
            'sla.violation': ['settlement_id', 'violation_type', 'severity'],
            'user.action': ['user_id', 'action', 'resource'],
        }
        
        self.field_validators = {
            'email': self._validate_email,
            'user_id': self._validate_uuid,
            'settlement_id': self._validate_uuid,
            'contract_id': self._validate_uuid,
            'resource_type': self._validate_resource_type,
            'quantity': self._validate_positive_number,
            'amount': self._validate_positive_number,
            'duration_hours': self._validate_positive_integer,
            'severity': self._validate_severity,
            'timestamp': self._validate_timestamp,
        }
        
        self.filters = {
            'test_events': lambda e: not e.get('is_test', False),
            'future_events': lambda e: e.get('timestamp', 0) <= int(datetime.now().timestamp() * 1000),
            'valid_users': lambda e: not e.get('user_id', '').startswith('test-'),
        }
        
    def process(self, input_data: bytes, context: Any) -> Optional[bytes]:
        """
        Process and validate incoming event
        
        Args:
            input_data: Raw event bytes
            context: Pulsar Function context
            
        Returns:
            Validated event bytes or None if filtered out
        """
        try:
            # Parse input event
            event = json.loads(input_data.decode('utf-8'))
            
            # Get event type
            event_type = event.get('event_type', 'unknown')
            
            # Apply filters
            if not self._apply_filters(event, context):
                return None
                
            # Validate required fields
            validation_errors = self._validate_required_fields(event, event_type)
            
            # Validate field formats
            validation_errors.extend(self._validate_field_formats(event))
            
            # Apply business rules
            validation_errors.extend(self._validate_business_rules(event, event_type))
            
            if validation_errors:
                # Log validation errors
                self.logger.warning(f"Event validation failed: {validation_errors}")
                context.record_metric('validation_failures', 1)
                
                # Decide whether to filter out or mark as invalid
                if self._should_filter_invalid(event_type, validation_errors):
                    context.record_metric('events_filtered', 1)
                    return None
                else:
                    # Mark as invalid but pass through
                    event['validation_errors'] = validation_errors
                    event['validation_status'] = 'failed'
            else:
                # Mark as valid
                event['validation_status'] = 'passed'
                event['validated_at'] = int(datetime.now().timestamp() * 1000)
                context.record_metric('events_validated', 1)
                
            # Add validation metadata
            event['validation_metadata'] = {
                'validator_version': '1.0.0',
                'rules_applied': len(validation_errors) == 0,
                'validated_by': 'event-validator-function',
            }
            
            return json.dumps(event).encode('utf-8')
            
        except Exception as e:
            self.logger.error(f"Error validating event: {e}")
            context.record_metric('validation_errors', 1)
            
            # On error, pass through with error flag
            try:
                error_event = json.loads(input_data.decode('utf-8'))
                error_event['validation_error'] = str(e)
                error_event['validation_status'] = 'error'
                return json.dumps(error_event).encode('utf-8')
            except:
                # If we can't even parse it, filter it out
                return None
                
    def _apply_filters(self, event: Dict[str, Any], context: Any) -> bool:
        """Apply filters to determine if event should be processed"""
        for filter_name, filter_func in self.filters.items():
            if not filter_func(event):
                context.record_metric(f'filtered_{filter_name}', 1)
                return False
        return True
        
    def _validate_required_fields(self, event: Dict[str, Any], event_type: str) -> List[str]:
        """Validate required fields are present"""
        errors = []
        
        # Get required fields for event type
        base_type = event_type.split('.')[0] + '.' + event_type.split('.')[1] if '.' in event_type else event_type
        required = self.required_fields.get(base_type, [])
        
        # Check each required field
        for field in required:
            if field not in event or event[field] is None:
                errors.append(f"Missing required field: {field}")
                
        return errors
        
    def _validate_field_formats(self, event: Dict[str, Any]) -> List[str]:
        """Validate field formats"""
        errors = []
        
        for field, value in event.items():
            if field in self.field_validators and value is not None:
                validator = self.field_validators[field]
                error = validator(field, value)
                if error:
                    errors.append(error)
                    
        return errors
        
    def _validate_business_rules(self, event: Dict[str, Any], event_type: str) -> List[str]:
        """Apply business rule validations"""
        errors = []
        
        # Compute order validations
        if event_type.startswith('compute.order'):
            errors.extend(self._validate_compute_order(event))
            
        # Settlement validations
        elif event_type.startswith('settlement'):
            errors.extend(self._validate_settlement(event))
            
        # SLA violation validations
        elif event_type.startswith('sla.violation'):
            errors.extend(self._validate_sla_violation(event))
            
        return errors
        
    def _validate_compute_order(self, event: Dict[str, Any]) -> List[str]:
        """Validate compute order specific rules"""
        errors = []
        
        quantity = event.get('quantity', 0)
        resource_type = event.get('resource_type', '').lower()
        duration = event.get('duration_hours', 0)
        
        # Resource-specific limits
        if resource_type == 'gpu':
            if quantity > 100:
                errors.append(f"GPU quantity {quantity} exceeds maximum limit of 100")
            if duration > 720:  # 30 days
                errors.append(f"GPU duration {duration}h exceeds maximum of 720h")
                
        elif resource_type == 'cpu':
            if quantity > 10000:
                errors.append(f"CPU quantity {quantity} exceeds maximum limit of 10000")
                
        # Minimum duration
        if 0 < duration < 1:
            errors.append("Minimum duration is 1 hour")
            
        return errors
        
    def _validate_settlement(self, event: Dict[str, Any]) -> List[str]:
        """Validate settlement specific rules"""
        errors = []
        
        amount = event.get('amount', 0)
        
        # Amount validations
        if amount < 0:
            errors.append("Settlement amount cannot be negative")
        elif amount > 1000000:
            errors.append("Settlement amount exceeds maximum limit")
            
        return errors
        
    def _validate_sla_violation(self, event: Dict[str, Any]) -> List[str]:
        """Validate SLA violation specific rules"""
        errors = []
        
        severity = event.get('severity', '').lower()
        violation_type = event.get('violation_type', '').lower()
        
        # Valid severity levels
        valid_severities = ['low', 'medium', 'high', 'critical']
        if severity and severity not in valid_severities:
            errors.append(f"Invalid severity: {severity}")
            
        # Valid violation types
        valid_types = ['uptime', 'latency', 'throughput', 'error_rate', 'response_time']
        if violation_type and violation_type not in valid_types:
            errors.append(f"Invalid violation type: {violation_type}")
            
        return errors
        
    def _should_filter_invalid(self, event_type: str, errors: List[str]) -> bool:
        """Determine if invalid event should be filtered out"""
        # Critical errors that should filter
        critical_errors = [
            'Missing required field',
            'Invalid UUID format',
            'exceeds maximum limit',
        ]
        
        for error in errors:
            for critical in critical_errors:
                if critical in error:
                    return True
                    
        # Test events with errors should be filtered
        if event_type.startswith('test.'):
            return True
            
        return False
        
    # Field validators
    def _validate_email(self, field: str, value: str) -> Optional[str]:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, value):
            return f"{field} has invalid email format"
        return None
        
    def _validate_uuid(self, field: str, value: str) -> Optional[str]:
        """Validate UUID format"""
        pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if not re.match(pattern, value.lower()):
            return f"{field} has invalid UUID format"
        return None
        
    def _validate_resource_type(self, field: str, value: str) -> Optional[str]:
        """Validate resource type"""
        valid_types = ['cpu', 'gpu', 'tpu', 'memory', 'storage', 'bandwidth']
        if value.lower() not in valid_types:
            return f"{field} has invalid resource type: {value}"
        return None
        
    def _validate_positive_number(self, field: str, value: Any) -> Optional[str]:
        """Validate positive number"""
        try:
            num = float(value)
            if num <= 0:
                return f"{field} must be positive"
        except:
            return f"{field} must be a valid number"
        return None
        
    def _validate_positive_integer(self, field: str, value: Any) -> Optional[str]:
        """Validate positive integer"""
        try:
            num = int(value)
            if num <= 0:
                return f"{field} must be a positive integer"
        except:
            return f"{field} must be a valid integer"
        return None
        
    def _validate_severity(self, field: str, value: str) -> Optional[str]:
        """Validate severity level"""
        valid_levels = ['low', 'medium', 'high', 'critical']
        if value.lower() not in valid_levels:
            return f"{field} has invalid severity: {value}"
        return None
        
    def _validate_timestamp(self, field: str, value: Any) -> Optional[str]:
        """Validate timestamp"""
        try:
            ts = int(value)
            # Check if timestamp is reasonable (within last year and not future)
            now = int(datetime.now().timestamp() * 1000)
            year_ago = now - (365 * 24 * 60 * 60 * 1000)
            
            if ts < year_ago:
                return f"{field} is too old"
            elif ts > now + (60 * 1000):  # Allow 1 minute future for clock skew
                return f"{field} is in the future"
        except:
            return f"{field} must be a valid timestamp"
        return None


# Configuration for Pulsar Function deployment
function_config = {
    "name": "event-validator",
    "py": "event_validator.py",
    "classname": "event_validator.EventValidatorFunction",
    "inputs": [
        "persistent://platformq/incoming/events"
    ],
    "output": "persistent://platformq/validated/events",
    "log-topic": "persistent://platformq/functions/logs",
    "processing-guarantees": "ATLEAST_ONCE",
    "parallelism": 2,
    "cpu": 0.25,
    "ram": 268435456,  # 256MB
    "disk": 536870912,  # 512MB
    "user-config": {
        "strict_mode": false,
        "filter_test_events": true
    }
} 