"""
Validation Utilities

Common validation functions for data platform service
"""

from typing import Dict, Any, List, Optional
import re
from ipaddress import ip_address, IPv4Address, IPv6Address


def validate_connection_config(source_type: str, config: Dict[str, Any]) -> None:
    """Validate database connection configuration"""
    
    # Common required fields
    required_fields = ["hostname"]
    
    # Source-specific validation
    if source_type in ["postgresql", "mysql", "oracle", "sqlserver", "db2"]:
        required_fields.extend(["database", "port"])
        
        # Validate port
        port = config.get("port")
        if port and not (1 <= int(port) <= 65535):
            raise ValueError(f"Invalid port number: {port}")
            
    elif source_type == "mongodb":
        required_fields.append("hosts")
        
    elif source_type == "cassandra":
        required_fields.extend(["keyspace", "port"])
        
    # Check required fields
    missing_fields = [field for field in required_fields if field not in config]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
        
    # Validate hostname/hosts
    if "hostname" in config:
        validate_hostname(config["hostname"])
    elif "hosts" in config:
        if isinstance(config["hosts"], str):
            hosts = [config["hosts"]]
        else:
            hosts = config["hosts"]
        for host in hosts:
            validate_hostname(host.split(":")[0])
            

def validate_hostname(hostname: str) -> None:
    """Validate hostname or IP address"""
    # Try to parse as IP address first
    try:
        ip_address(hostname)
        return
    except ValueError:
        pass
        
    # Validate as hostname
    if len(hostname) > 253:
        raise ValueError(f"Hostname too long: {hostname}")
        
    # Check each label
    labels = hostname.split(".")
    for label in labels:
        if not label or len(label) > 63:
            raise ValueError(f"Invalid hostname label: {label}")
            
        # Label must start with letter or digit
        if not re.match(r'^[a-zA-Z0-9]', label):
            raise ValueError(f"Invalid hostname label: {label}")
            
        # Label can only contain letters, digits, and hyphens
        if not re.match(r'^[a-zA-Z0-9-]*$', label):
            raise ValueError(f"Invalid hostname label: {label}")
            
        # Label cannot end with hyphen
        if label.endswith('-'):
            raise ValueError(f"Invalid hostname label: {label}")
            

def validate_table_name(table_name: str) -> None:
    """Validate table name"""
    if not table_name:
        raise ValueError("Table name cannot be empty")
        
    # Check for valid characters (alphanumeric, underscore, dot for schema)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")
        

def validate_s3_path(path: str) -> None:
    """Validate S3 path"""
    if not path.startswith("s3://") and not path.startswith("s3a://"):
        raise ValueError(f"Invalid S3 path: {path}")
        
    # Extract bucket and key
    parts = path.replace("s3://", "").replace("s3a://", "").split("/", 1)
    if not parts[0]:
        raise ValueError(f"Invalid S3 bucket name in path: {path}")
        
    # Validate bucket name
    bucket = parts[0]
    if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket):
        raise ValueError(f"Invalid S3 bucket name: {bucket}")
        
    if len(bucket) < 3 or len(bucket) > 63:
        raise ValueError(f"S3 bucket name must be between 3 and 63 characters: {bucket}")
        

def validate_kafka_topic(topic: str) -> None:
    """Validate Kafka/Pulsar topic name"""
    if not topic:
        raise ValueError("Topic name cannot be empty")
        
    # Check length
    if len(topic) > 249:
        raise ValueError(f"Topic name too long: {topic}")
        
    # Check for valid characters
    if not re.match(r'^[a-zA-Z0-9._-]+$', topic):
        raise ValueError(f"Invalid topic name: {topic}")
        

def validate_cron_expression(cron: str) -> None:
    """Validate cron expression"""
    parts = cron.split()
    
    if len(parts) != 5 and len(parts) != 6:
        raise ValueError(f"Invalid cron expression: {cron}")
        
    # Simple validation - could be enhanced
    for i, part in enumerate(parts):
        if part == "*":
            continue
            
        # Check for ranges
        if "-" in part:
            start, end = part.split("-")
            if not start.isdigit() or not end.isdigit():
                raise ValueError(f"Invalid cron range: {part}")
                
        # Check for lists
        elif "," in part:
            for val in part.split(","):
                if not val.isdigit():
                    raise ValueError(f"Invalid cron list: {part}")
                    
        # Check for step values
        elif "/" in part:
            range_part, step = part.split("/")
            if not step.isdigit():
                raise ValueError(f"Invalid cron step: {part}")
                
        # Single value
        elif not part.isdigit() and part not in ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]:
            raise ValueError(f"Invalid cron value: {part}")
            

def validate_json_schema(schema: Dict[str, Any]) -> None:
    """Validate JSON schema"""
    if not isinstance(schema, dict):
        raise ValueError("Schema must be a dictionary")
        
    # Check for required fields
    if "type" not in schema:
        raise ValueError("Schema must have a 'type' field")
        
    # Validate type
    valid_types = ["object", "array", "string", "number", "integer", "boolean", "null"]
    if schema["type"] not in valid_types:
        raise ValueError(f"Invalid schema type: {schema['type']}")
        
    # Validate properties for object type
    if schema["type"] == "object" and "properties" in schema:
        if not isinstance(schema["properties"], dict):
            raise ValueError("Schema properties must be a dictionary")
            
        # Recursively validate nested schemas
        for prop_name, prop_schema in schema["properties"].items():
            validate_json_schema(prop_schema)
            

def validate_avro_schema(schema: Dict[str, Any]) -> None:
    """Validate Avro schema"""
    if not isinstance(schema, dict):
        raise ValueError("Avro schema must be a dictionary")
        
    # Check required fields
    required_fields = ["type", "name"]
    if schema.get("type") == "record":
        required_fields.append("fields")
        
    missing_fields = [field for field in required_fields if field not in schema]
    if missing_fields:
        raise ValueError(f"Missing required Avro schema fields: {missing_fields}")
        
    # Validate fields for record type
    if schema.get("type") == "record":
        fields = schema.get("fields", [])
        if not isinstance(fields, list):
            raise ValueError("Avro schema fields must be a list")
            
        for field in fields:
            if not isinstance(field, dict):
                raise ValueError("Each Avro field must be a dictionary")
                
            if "name" not in field or "type" not in field:
                raise ValueError("Each Avro field must have 'name' and 'type'")
            

def validate_data_quality_rules(rules: List[Dict[str, Any]]) -> None:
    """Validate data quality rules"""
    if not isinstance(rules, list):
        raise ValueError("Quality rules must be a list")
        
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError("Each quality rule must be a dictionary")
            
        # Check required fields
        required_fields = ["name", "type", "column"]
        missing_fields = [field for field in required_fields if field not in rule]
        if missing_fields:
            raise ValueError(f"Missing required rule fields: {missing_fields}")
            
        # Validate rule type
        valid_types = [
            "not_null", "unique", "range", "pattern", "length",
            "custom", "referential", "statistical"
        ]
        if rule["type"] not in valid_types:
            raise ValueError(f"Invalid rule type: {rule['type']}")
            
        # Type-specific validation
        if rule["type"] == "range":
            if "min" not in rule and "max" not in rule:
                raise ValueError("Range rule must have 'min' or 'max'")
                
        elif rule["type"] == "pattern":
            if "pattern" not in rule:
                raise ValueError("Pattern rule must have 'pattern' field")
                
        elif rule["type"] == "length":
            if "min_length" not in rule and "max_length" not in rule:
                raise ValueError("Length rule must have 'min_length' or 'max_length'") 