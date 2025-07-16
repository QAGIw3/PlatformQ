"""
Configuration Management Module for SeaTunnel Pipelines

This module provides centralized configuration management for pipeline
templates, configurations, and best practices.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import yaml
from pathlib import Path
from enum import Enum

from sqlalchemy import Column, String, JSON, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

Base = declarative_base()


class ConfigType(str, Enum):
    """Types of configurations"""
    TEMPLATE = "template"
    CONNECTOR = "connector"
    TRANSFORM = "transform"
    PATTERN = "pattern"
    OPTIMIZATION = "optimization"


class PipelineTemplate(Base):
    """Database model for pipeline templates"""
    __tablename__ = "pipeline_templates"
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    type = Column(String, nullable=False)  # ConfigType
    category = Column(String)  # e.g., "data-lake", "cdc", "streaming"
    configuration = Column(JSON, nullable=False)
    metadata = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConnectorConfig(Base):
    """Database model for connector configurations"""
    __tablename__ = "connector_configs"
    
    id = Column(String, primary_key=True)
    connector_type = Column(String, nullable=False)
    name = Column(String, nullable=False)
    description = Column(Text)
    default_config = Column(JSON, nullable=False)
    required_params = Column(JSON)
    optional_params = Column(JSON)
    examples = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ConfigurationManager:
    """Manages pipeline configurations and templates"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self._init_default_templates()
    
    def _init_default_templates(self):
        """Initialize default pipeline templates"""
        default_templates = [
            {
                "id": "data_lake_ingestion",
                "name": "Data Lake Ingestion",
                "description": "Standard template for ingesting data into the data lake",
                "type": ConfigType.TEMPLATE,
                "category": "data-lake",
                "configuration": {
                    "pattern": "ingestion",
                    "source": {
                        "connector_type": "file",
                        "options": {
                            "format": "auto"
                        }
                    },
                    "transforms": [
                        {
                            "type": "schema_inference",
                            "config": {
                                "sample_size": 1000
                            }
                        },
                        {
                            "type": "data_quality",
                            "config": {
                                "null_check": True,
                                "duplicate_check": True
                            }
                        }
                    ],
                    "sinks": [
                        {
                            "connector_type": "minio",
                            "options": {
                                "format": "parquet",
                                "partition_by": ["year", "month", "day"],
                                "compression": "snappy"
                            }
                        }
                    ],
                    "monitoring": {
                        "metrics": ["rows_processed", "processing_time", "error_count"],
                        "alert_rules": [
                            {
                                "metric": "error_rate",
                                "threshold": 0.05,
                                "window": "5m"
                            }
                        ]
                    }
                }
            },
            {
                "id": "cdc_replication",
                "name": "CDC Database Replication",
                "description": "Template for Change Data Capture from databases",
                "type": ConfigType.TEMPLATE,
                "category": "cdc",
                "configuration": {
                    "pattern": "cdc",
                    "source": {
                        "connector_type": "postgres-cdc",
                        "connection_params": {
                            "slot.name": "platformq_cdc",
                            "publication.name": "platformq_pub"
                        }
                    },
                    "sinks": [
                        {
                            "connector_type": "pulsar",
                            "options": {
                                "compression": "LZ4"
                            }
                        },
                        {
                            "connector_type": "minio",
                            "options": {
                                "format": "json",
                                "partition_by": ["event_date"]
                            }
                        }
                    ],
                    "monitoring": {
                        "metrics": ["replication_lag", "change_events", "error_count"],
                        "alert_rules": [
                            {
                                "metric": "replication_lag",
                                "threshold": 300,
                                "window": "5m"
                            }
                        ]
                    }
                }
            },
            {
                "id": "stream_processing",
                "name": "Real-time Stream Processing",
                "description": "Template for processing streaming data",
                "type": ConfigType.TEMPLATE,
                "category": "streaming",
                "configuration": {
                    "pattern": "streaming",
                    "source": {
                        "connector_type": "pulsar",
                        "options": {
                            "cursor": "earliest"
                        }
                    },
                    "transforms": [
                        {
                            "type": "window",
                            "config": {
                                "window_type": "tumbling",
                                "window_size": "5 minutes"
                            }
                        },
                        {
                            "type": "aggregate",
                            "config": {
                                "aggregations": ["count", "avg", "max", "min"]
                            }
                        }
                    ],
                    "sinks": [
                        {
                            "connector_type": "ignite",
                            "options": {
                                "cache_mode": "PARTITIONED"
                            }
                        },
                        {
                            "connector_type": "elasticsearch",
                            "options": {
                                "index.number_of_shards": 3
                            }
                        }
                    ],
                    "monitoring": {
                        "metrics": ["lag", "throughput", "checkpoint_duration"],
                        "alert_rules": [
                            {
                                "metric": "lag",
                                "threshold": 60000,
                                "window": "5m"
                            }
                        ]
                    }
                }
            }
        ]
        
        # Insert default templates if they don't exist
        for template_data in default_templates:
            existing = self.db.query(PipelineTemplate).filter(
                PipelineTemplate.id == template_data["id"]
            ).first()
            
            if not existing:
                template = PipelineTemplate(**template_data)
                self.db.add(template)
        
        self.db.commit()
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get a pipeline template by ID"""
        template = self.db.query(PipelineTemplate).filter(
            PipelineTemplate.id == template_id,
            PipelineTemplate.is_active == True
        ).first()
        
        if template:
            return {
                "id": template.id,
                "name": template.name,
                "description": template.description,
                "category": template.category,
                "configuration": template.configuration
            }
        
        return None
    
    def list_templates(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all available templates"""
        query = self.db.query(PipelineTemplate).filter(
            PipelineTemplate.is_active == True
        )
        
        if category:
            query = query.filter(PipelineTemplate.category == category)
        
        templates = query.all()
        
        return [
            {
                "id": t.id,
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "type": t.type
            }
            for t in templates
        ]
    
    def create_template(self, template_data: Dict[str, Any]) -> str:
        """Create a new pipeline template"""
        import uuid
        
        template = PipelineTemplate(
            id=template_data.get("id", str(uuid.uuid4())),
            name=template_data["name"],
            description=template_data.get("description"),
            type=template_data.get("type", ConfigType.TEMPLATE),
            category=template_data.get("category"),
            configuration=template_data["configuration"],
            metadata=template_data.get("metadata", {})
        )
        
        self.db.add(template)
        self.db.commit()
        
        return template.id
    
    def update_template(self, template_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing template"""
        template = self.db.query(PipelineTemplate).filter(
            PipelineTemplate.id == template_id
        ).first()
        
        if not template:
            return False
        
        for key, value in updates.items():
            if hasattr(template, key):
                setattr(template, key, value)
        
        template.updated_at = datetime.utcnow()
        self.db.commit()
        
        return True
    
    def get_connector_config(self, connector_type: str) -> Optional[Dict[str, Any]]:
        """Get configuration details for a connector"""
        connector = self.db.query(ConnectorConfig).filter(
            ConnectorConfig.connector_type == connector_type,
            ConnectorConfig.is_active == True
        ).first()
        
        if connector:
            return {
                "connector_type": connector.connector_type,
                "name": connector.name,
                "description": connector.description,
                "default_config": connector.default_config,
                "required_params": connector.required_params,
                "optional_params": connector.optional_params,
                "examples": connector.examples
            }
        
        return None
    
    def validate_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a pipeline configuration"""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "suggestions": []
        }
        
        # Check required fields
        required_fields = ["source", "sync_mode"]
        for field in required_fields:
            if field not in config:
                validation_result["errors"].append(f"Missing required field: {field}")
                validation_result["is_valid"] = False
        
        # Validate source configuration
        if "source" in config:
            source_validation = self._validate_connector_config(
                config["source"],
                "source"
            )
            validation_result["errors"].extend(source_validation.get("errors", []))
            validation_result["warnings"].extend(source_validation.get("warnings", []))
        
        # Validate sink configuration
        if "sinks" in config:
            for i, sink in enumerate(config["sinks"]):
                sink_validation = self._validate_connector_config(
                    sink,
                    f"sink[{i}]"
                )
                validation_result["errors"].extend(sink_validation.get("errors", []))
                validation_result["warnings"].extend(sink_validation.get("warnings", []))
        else:
            validation_result["warnings"].append("No sinks configured")
        
        # Validate transformations
        if "transforms" in config:
            for i, transform in enumerate(config.get("transforms", [])):
                if "type" not in transform:
                    validation_result["errors"].append(f"Transform[{i}] missing type")
                    validation_result["is_valid"] = False
        
        # Performance suggestions
        if config.get("parallelism", 0) > 16:
            validation_result["suggestions"].append(
                "Consider reducing parallelism for better resource utilization"
            )
        
        return validation_result
    
    def _validate_connector_config(
        self, 
        connector_config: Dict[str, Any],
        config_type: str
    ) -> Dict[str, Any]:
        """Validate individual connector configuration"""
        result = {"errors": [], "warnings": []}
        
        if "connector_type" not in connector_config:
            result["errors"].append(f"{config_type} missing connector_type")
            return result
        
        # Get connector schema
        connector_type = connector_config["connector_type"]
        connector_schema = self.get_connector_config(connector_type)
        
        if not connector_schema:
            result["warnings"].append(f"Unknown connector type: {connector_type}")
            return result
        
        # Check required parameters
        required = connector_schema.get("required_params", [])
        connection_params = connector_config.get("connection_params", {})
        
        for param in required:
            if param not in connection_params:
                result["errors"].append(
                    f"{config_type} missing required parameter: {param}"
                )
        
        return result
    
    def export_configuration(
        self, 
        config: Dict[str, Any],
        format: str = "yaml"
    ) -> str:
        """Export configuration in specified format"""
        if format == "yaml":
            return yaml.dump(config, default_flow_style=False)
        elif format == "json":
            return json.dumps(config, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def import_configuration(
        self,
        config_str: str,
        format: str = "yaml"
    ) -> Dict[str, Any]:
        """Import configuration from string"""
        try:
            if format == "yaml":
                config = yaml.safe_load(config_str)
            elif format == "json":
                config = json.loads(config_str)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Validate imported configuration
            validation = self.validate_configuration(config)
            
            if not validation["is_valid"]:
                raise ValueError(f"Invalid configuration: {validation['errors']}")
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to import configuration: {e}")
            raise
    
    def get_optimization_suggestions(
        self,
        config: Dict[str, Any],
        metrics: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get optimization suggestions based on configuration and metrics"""
        suggestions = []
        
        # Check for missing optimizations
        if config.get("sync_mode") == "streaming":
            if "checkpoint_interval" not in config:
                suggestions.append({
                    "type": "performance",
                    "suggestion": "Add checkpoint_interval for streaming jobs",
                    "config_change": {"checkpoint_interval": 60000}
                })
        
        # Compression suggestions
        for sink in config.get("sinks", []):
            if sink["connector_type"] in ["minio", "s3"]:
                if "compression" not in sink.get("options", {}):
                    suggestions.append({
                        "type": "storage",
                        "suggestion": f"Enable compression for {sink['connector_type']} sink",
                        "config_change": {"options": {"compression": "snappy"}}
                    })
        
        # Metrics-based suggestions
        if metrics:
            if metrics.get("error_rate", 0) > 0.1:
                suggestions.append({
                    "type": "reliability",
                    "suggestion": "High error rate detected, consider adding retry logic",
                    "config_change": {"retry_attempts": 3, "retry_delay": 5000}
                })
            
            if metrics.get("processing_lag", 0) > 300:
                suggestions.append({
                    "type": "performance",
                    "suggestion": "High processing lag, consider increasing parallelism",
                    "config_change": {"parallelism": config.get("parallelism", 4) * 2}
                })
        
        return suggestions 