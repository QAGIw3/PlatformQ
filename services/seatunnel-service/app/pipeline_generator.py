"""
Pipeline Generator Module

This module provides intelligent pipeline generation based on asset metadata,
automatically configuring SeaTunnel connectors and transformations.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
import json
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class PipelinePattern(str, Enum):
    """Common pipeline patterns"""
    INGESTION = "ingestion"          # Source -> Lake
    ETL = "etl"                       # Lake -> Transform -> Warehouse
    STREAMING = "streaming"           # Stream -> Process -> Stream
    CDC = "cdc"                       # Database CDC -> Stream/Lake
    ENRICHMENT = "enrichment"         # Data + Reference -> Enriched
    AGGREGATION = "aggregation"       # Raw -> Aggregated
    REPLICATION = "replication"       # Source -> Multiple Targets
    FEDERATION = "federation"         # Multiple Sources -> Unified View


class AssetTypeMapper:
    """Maps asset types to data pipeline configurations"""
    
    # Asset type patterns to data source mappings
    ASSET_TYPE_MAPPINGS = {
        # CAD/3D Models
        r"^(CAD|3D_MODEL|BLENDER|FREECAD)": {
            "source_type": "file",
            "format": "binary",
            "processing": ["metadata_extraction", "thumbnail_generation", "format_conversion"],
            "sink_preferences": ["minio", "elasticsearch"]
        },
        
        # Documents
        r"^(DOCUMENT|PDF|DOCX|MARKDOWN)": {
            "source_type": "file",
            "format": "document",
            "processing": ["text_extraction", "nlp_enrichment", "indexing"],
            "sink_preferences": ["elasticsearch", "minio"]
        },
        
        # Structured Data
        r"^(CSV|EXCEL|PARQUET|AVRO)": {
            "source_type": "file",
            "format": "structured",
            "processing": ["schema_inference", "validation", "type_conversion"],
            "sink_preferences": ["cassandra", "minio", "ignite"]
        },
        
        # Database/CRM
        r"^(CRM_|DATABASE_|SQL_)": {
            "source_type": "database",
            "format": "relational",
            "processing": ["normalization", "deduplication"],
            "sink_preferences": ["cassandra", "elasticsearch", "minio"]
        },
        
        # Streaming/IoT
        r"^(IOT_|SENSOR_|TELEMETRY_)": {
            "source_type": "streaming",
            "format": "timeseries",
            "processing": ["windowing", "aggregation", "anomaly_detection"],
            "sink_preferences": ["ignite", "cassandra", "pulsar"]
        },
        
        # Simulation/Scientific
        r"^(SIMULATION_|OPENFOAM_|SCIENTIFIC_)": {
            "source_type": "file",
            "format": "scientific",
            "processing": ["data_extraction", "visualization_prep", "statistical_analysis"],
            "sink_preferences": ["minio", "cassandra"]
        },
        
        # Logs/Events
        r"^(LOG_|EVENT_|AUDIT_)": {
            "source_type": "streaming",
            "format": "semi_structured",
            "processing": ["parsing", "enrichment", "alerting"],
            "sink_preferences": ["elasticsearch", "pulsar"]
        }
    }
    
    @classmethod
    def get_pipeline_config(cls, asset_type: str) -> Dict[str, Any]:
        """Get pipeline configuration based on asset type"""
        for pattern, config in cls.ASSET_TYPE_MAPPINGS.items():
            if re.match(pattern, asset_type.upper()):
                return config
        
        # Default configuration
        return {
            "source_type": "file",
            "format": "generic",
            "processing": ["validation"],
            "sink_preferences": ["minio"]
        }


class TransformationBuilder:
    """Builds transformation configurations based on data characteristics"""
    
    @staticmethod
    def build_transformations(
        source_format: str,
        processing_steps: List[str],
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Build transformation pipeline"""
        transformations = []
        
        # Schema inference for structured data
        if "schema_inference" in processing_steps:
            transformations.append({
                "type": "schema_inference",
                "config": {
                    "sample_size": 1000,
                    "infer_types": True
                }
            })
        
        # Text extraction for documents
        if "text_extraction" in processing_steps:
            transformations.append({
                "type": "text_extractor",
                "config": {
                    "languages": metadata.get("languages", ["en"]),
                    "extract_metadata": True
                }
            })
        
        # NLP enrichment
        if "nlp_enrichment" in processing_steps:
            transformations.append({
                "type": "nlp",
                "config": {
                    "operations": ["entity_extraction", "sentiment_analysis", "keyword_extraction"],
                    "model": "spacy_lg"
                }
            })
        
        # Windowing for streaming data
        if "windowing" in processing_steps:
            transformations.append({
                "type": "window",
                "config": {
                    "window_type": "tumbling",
                    "window_size": metadata.get("window_size", "5 minutes"),
                    "watermark": "1 minute"
                }
            })
        
        # Aggregation
        if "aggregation" in processing_steps:
            transformations.append({
                "type": "aggregate",
                "config": {
                    "group_by": metadata.get("group_by_fields", []),
                    "aggregations": metadata.get("aggregations", ["count", "avg", "max", "min"])
                }
            })
        
        # Data quality checks
        if "validation" in processing_steps:
            transformations.append({
                "type": "data_quality",
                "config": {
                    "null_check": True,
                    "duplicate_check": True,
                    "range_check": metadata.get("range_validations", {}),
                    "pattern_check": metadata.get("pattern_validations", {})
                }
            })
        
        # Deduplication
        if "deduplication" in processing_steps:
            transformations.append({
                "type": "deduplicate",
                "config": {
                    "key_fields": metadata.get("unique_keys", []),
                    "keep": "last",
                    "time_window": metadata.get("dedup_window", "24 hours")
                }
            })
        
        # Format conversion
        if "format_conversion" in processing_steps:
            transformations.append({
                "type": "format_converter",
                "config": {
                    "target_format": metadata.get("target_format", "parquet"),
                    "compression": metadata.get("compression", "snappy")
                }
            })
        
        return transformations


class PipelineGenerator:
    """Generates SeaTunnel pipeline configurations from asset metadata"""
    
    def __init__(self):
        self.asset_mapper = AssetTypeMapper()
        self.transform_builder = TransformationBuilder()
    
    def generate_pipeline(
        self,
        asset_metadata: Dict[str, Any],
        tenant_id: str,
        pipeline_name: Optional[str] = None
    ) -> Tuple[Dict[str, Any], PipelinePattern]:
        """
        Generate a complete pipeline configuration from asset metadata
        
        Returns:
            Tuple of (pipeline_config, pipeline_pattern)
        """
        asset_type = asset_metadata.get("asset_type", "UNKNOWN")
        asset_id = asset_metadata.get("asset_id")
        source_uri = asset_metadata.get("raw_data_uri", "")
        
        # Get base configuration for asset type
        base_config = self.asset_mapper.get_pipeline_config(asset_type)
        
        # Determine pipeline pattern
        pattern = self._determine_pattern(base_config, asset_metadata)
        
        # Build source configuration
        source_config = self._build_source_config(
            base_config["source_type"],
            source_uri,
            asset_metadata
        )
        
        # Build transformations
        transformations = self.transform_builder.build_transformations(
            base_config["format"],
            base_config["processing"],
            asset_metadata
        )
        
        # Build sink configuration
        sink_configs = self._build_sink_configs(
            base_config["sink_preferences"],
            tenant_id,
            asset_metadata
        )
        
        # Generate pipeline name
        if not pipeline_name:
            pipeline_name = f"{asset_type.lower()}_{pattern.value}_{asset_id[:8]}"
        
        # Determine sync mode
        sync_mode = self._determine_sync_mode(base_config, pattern)
        
        # Build complete configuration
        pipeline_config = {
            "name": pipeline_name,
            "description": f"Auto-generated {pattern.value} pipeline for {asset_type}",
            "pattern": pattern.value,
            "source": source_config,
            "transforms": transformations,
            "sinks": sink_configs,  # Multiple sinks supported
            "sync_mode": sync_mode,
            "metadata": {
                "asset_id": asset_id,
                "asset_type": asset_type,
                "generated_at": datetime.utcnow().isoformat(),
                "generator_version": "1.0"
            }
        }
        
        # Add scheduling for batch jobs
        if sync_mode == "batch":
            pipeline_config["schedule"] = self._generate_schedule(asset_metadata)
        
        # Add monitoring configuration
        pipeline_config["monitoring"] = self._generate_monitoring_config(pattern)
        
        return pipeline_config, pattern
    
    def _determine_pattern(
        self,
        base_config: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> PipelinePattern:
        """Determine the pipeline pattern based on configuration and metadata"""
        source_type = base_config["source_type"]
        
        # Check for specific patterns in metadata
        if metadata.get("cdc_enabled"):
            return PipelinePattern.CDC
        
        if metadata.get("enrichment_sources"):
            return PipelinePattern.ENRICHMENT
        
        if "aggregation" in base_config.get("processing", []):
            return PipelinePattern.AGGREGATION
        
        if metadata.get("target_systems", []):
            if len(metadata["target_systems"]) > 1:
                return PipelinePattern.REPLICATION
        
        if metadata.get("federated_sources"):
            return PipelinePattern.FEDERATION
        
        # Default patterns by source type
        if source_type == "streaming":
            return PipelinePattern.STREAMING
        elif source_type == "database" and metadata.get("sync_type") == "cdc":
            return PipelinePattern.CDC
        else:
            return PipelinePattern.INGESTION
    
    def _build_source_config(
        self,
        source_type: str,
        source_uri: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build source connector configuration"""
        if source_type == "file":
            return self._build_file_source(source_uri, metadata)
        elif source_type == "database":
            return self._build_database_source(source_uri, metadata)
        elif source_type == "streaming":
            return self._build_streaming_source(source_uri, metadata)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
    
    def _build_file_source(self, uri: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Build file source configuration"""
        # Parse URI to determine storage type
        if uri.startswith("s3://") or uri.startswith("minio://"):
            return {
                "connector_type": "minio",
                "connection_params": {
                    "endpoint": metadata.get("minio_endpoint", "http://minio:9000"),
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    "bucket": uri.split("/")[2],
                    "prefix": "/".join(uri.split("/")[3:])
                },
                "format": metadata.get("file_format", "auto"),
                "options": {
                    "recursive": metadata.get("recursive", True),
                    "file_filter": metadata.get("file_pattern", "*")
                }
            }
        else:
            # Local file system
            return {
                "connector_type": "file",
                "connection_params": {
                    "path": uri,
                    "format": metadata.get("file_format", "auto")
                }
            }
    
    def _build_database_source(self, uri: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Build database source configuration"""
        db_type = metadata.get("database_type", "postgresql")
        
        if metadata.get("cdc_enabled"):
            # CDC configuration
            return {
                "connector_type": f"{db_type}-cdc",
                "connection_params": {
                    "hostname": metadata.get("db_host"),
                    "port": metadata.get("db_port", 5432),
                    "database": metadata.get("db_name"),
                    "username": metadata.get("db_user"),
                    "password": metadata.get("db_password"),
                    "table.whitelist": ",".join(metadata.get("tables", [])),
                    "slot.name": f"seatunnel_{metadata.get('asset_id', 'default')[:8]}"
                }
            }
        else:
            # Regular JDBC source
            return {
                "connector_type": "jdbc",
                "connection_params": {
                    "url": metadata.get("jdbc_url", uri),
                    "driver": metadata.get("jdbc_driver", "org.postgresql.Driver"),
                    "user": metadata.get("db_user"),
                    "password": metadata.get("db_password")
                },
                "query": metadata.get("query", f"SELECT * FROM {metadata.get('table_name', 'data')}")
            }
    
    def _build_streaming_source(self, uri: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Build streaming source configuration"""
        stream_type = metadata.get("stream_type", "pulsar")
        
        if stream_type == "pulsar":
            return {
                "connector_type": "pulsar",
                "connection_params": {
                    "service-url": metadata.get("pulsar_url", "pulsar://pulsar:6650"),
                    "admin-url": metadata.get("pulsar_admin_url", "http://pulsar:8080"),
                    "subscription-name": f"seatunnel-{metadata.get('asset_id', 'default')[:8]}"
                },
                "table_or_topic": metadata.get("topic", uri),
                "options": {
                    "cursor": metadata.get("start_cursor", "earliest")
                }
            }
        elif stream_type == "kafka":
            return {
                "connector_type": "kafka",
                "connection_params": {
                    "bootstrap.servers": metadata.get("kafka_brokers", "kafka:9092"),
                    "group.id": f"seatunnel-{metadata.get('asset_id', 'default')[:8]}"
                },
                "table_or_topic": metadata.get("topic", uri)
            }
        else:
            raise ValueError(f"Unknown stream type: {stream_type}")
    
    def _build_sink_configs(
        self,
        sink_preferences: List[str],
        tenant_id: str,
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Build sink configurations for multi-sink support"""
        sinks = []
        
        for sink_type in sink_preferences:
            if sink_type == "minio":
                sinks.append({
                    "connector_type": "minio",
                    "connection_params": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin"
                    },
                    "table_or_topic": f"{tenant_id}/processed/{metadata.get('asset_type', 'data').lower()}/",
                    "options": {
                        "format": "parquet",
                        "partition_by": ["year", "month", "day"],
                        "compression": "snappy"
                    }
                })
            
            elif sink_type == "elasticsearch":
                sinks.append({
                    "connector_type": "elasticsearch",
                    "connection_params": {
                        "hosts": ["http://elasticsearch:9200"],
                        "index": f"{tenant_id}_{metadata.get('asset_type', 'data').lower()}",
                        "index.type": "_doc"
                    },
                    "options": {
                        "index.number_of_shards": 3,
                        "index.number_of_replicas": 1
                    }
                })
            
            elif sink_type == "cassandra":
                sinks.append({
                    "connector_type": "cassandra",
                    "connection_params": {
                        "hosts": ["cassandra:9042"],
                        "keyspace": f"platformq_{tenant_id}",
                        "table": metadata.get('table_name', f"{metadata.get('asset_type', 'data').lower()}_data")
                    },
                    "options": {
                        "consistency": "LOCAL_QUORUM"
                    }
                })
            
            elif sink_type == "ignite":
                sinks.append({
                    "connector_type": "ignite",
                    "connection_params": {
                        "hosts": ["ignite:10800"],
                        "cache_name": f"{tenant_id}_{metadata.get('asset_type', 'data').lower()}"
                    },
                    "options": {
                        "write_mode": "overwrite",
                        "cache_mode": "PARTITIONED"
                    }
                })
            
            elif sink_type == "pulsar":
                sinks.append({
                    "connector_type": "pulsar",
                    "connection_params": {
                        "service-url": "pulsar://pulsar:6650",
                        "admin-url": "http://pulsar:8080"
                    },
                    "table_or_topic": f"persistent://platformq/{tenant_id}/processed-{metadata.get('asset_type', 'data').lower()}",
                    "options": {
                        "compression": "LZ4"
                    }
                })
        
        return sinks
    
    def _determine_sync_mode(self, base_config: Dict[str, Any], pattern: PipelinePattern) -> str:
        """Determine sync mode based on source type and pattern"""
        if pattern in [PipelinePattern.STREAMING, PipelinePattern.CDC]:
            return "streaming"
        elif base_config["source_type"] == "streaming":
            return "streaming"
        else:
            return "batch"
    
    def _generate_schedule(self, metadata: Dict[str, Any]) -> str:
        """Generate cron schedule for batch jobs"""
        # Check for explicit schedule in metadata
        if "schedule" in metadata:
            return metadata["schedule"]
        
        # Generate based on data characteristics
        data_volume = metadata.get("estimated_volume", "medium")
        update_frequency = metadata.get("update_frequency", "daily")
        
        schedules = {
            "hourly": "0 * * * *",
            "daily": "0 2 * * *",      # 2 AM daily
            "weekly": "0 2 * * 0",      # 2 AM Sunday
            "monthly": "0 2 1 * *"      # 2 AM first of month
        }
        
        return schedules.get(update_frequency, "0 2 * * *")
    
    def _generate_monitoring_config(self, pattern: PipelinePattern) -> Dict[str, Any]:
        """Generate monitoring configuration based on pipeline pattern"""
        base_metrics = [
            "rows_processed",
            "processing_time",
            "error_count",
            "throughput"
        ]
        
        pattern_specific_metrics = {
            PipelinePattern.STREAMING: ["lag", "backpressure", "checkpoint_duration"],
            PipelinePattern.CDC: ["replication_lag", "transaction_count"],
            PipelinePattern.AGGREGATION: ["aggregation_time", "group_count"],
            PipelinePattern.ENRICHMENT: ["enrichment_success_rate", "cache_hit_rate"]
        }
        
        metrics = base_metrics + pattern_specific_metrics.get(pattern, [])
        
        return {
            "metrics": metrics,
            "alert_rules": [
                {
                    "metric": "error_count",
                    "threshold": 10,
                    "window": "5m",
                    "severity": "warning"
                },
                {
                    "metric": "processing_time",
                    "threshold": 300000,  # 5 minutes
                    "window": "10m",
                    "severity": "critical"
                }
            ],
            "dashboard_enabled": True
        }


class PipelineOptimizer:
    """Optimizes pipeline configurations for performance"""
    
    @staticmethod
    def optimize_pipeline(pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply optimizations to pipeline configuration"""
        optimized = pipeline_config.copy()
        
        # Optimize parallelism based on data volume
        if "parallelism" not in optimized:
            data_volume = optimized.get("metadata", {}).get("estimated_volume", "medium")
            parallelism_map = {
                "small": 2,
                "medium": 4,
                "large": 8,
                "xlarge": 16
            }
            optimized["parallelism"] = parallelism_map.get(data_volume, 4)
        
        # Optimize checkpoint interval for streaming
        if optimized.get("sync_mode") == "streaming" and "checkpoint_interval" not in optimized:
            optimized["checkpoint_interval"] = 60000  # 1 minute
        
        # Enable compression for large data transfers
        for sink in optimized.get("sinks", []):
            if sink["connector_type"] in ["minio", "s3"] and "compression" not in sink.get("options", {}):
                sink["options"]["compression"] = "snappy"
        
        # Add resource limits
        if "resources" not in optimized:
            optimized["resources"] = {
                "memory": "2Gi",
                "cpu": "1000m"
            }
        
        return optimized 