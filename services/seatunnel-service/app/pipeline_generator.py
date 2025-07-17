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
    MULTI_PHYSICS = "multi_physics"   # Multi-physics simulation data flows
    SENSOR_STREAM = "sensor_stream"   # Real-time sensor/IoT ingestion
    CROSS_SERVICE = "cross_service"   # Service-to-service synchronization
    GRAPH_SYNC = "graph_sync"         # Database to graph synchronization


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
        },
        
        # Multi-physics simulation data
        r"^(MULTI_PHYSICS_|COUPLED_SIM_|THERMAL_STRUCTURAL_)": {
            "source_type": "streaming",
            "format": "simulation",
            "processing": ["state_extraction", "convergence_monitoring", "metric_aggregation"],
            "sink_preferences": ["ignite", "minio", "cassandra"],
            "pipeline_pattern": PipelinePattern.MULTI_PHYSICS
        },
        
        # Real-time sensor/IoT data
        r"^(SENSOR_|IOT_|TELEMETRY_|DEVICE_)": {
            "source_type": "streaming", 
            "format": "timeseries",
            "processing": ["timestamp_normalization", "windowing", "aggregation", "anomaly_detection"],
            "sink_preferences": ["ignite", "cassandra", "pulsar", "elasticsearch"],
            "pipeline_pattern": PipelinePattern.SENSOR_STREAM
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

    async def _generate_multi_physics_pipeline(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate pipeline for multi-physics simulation data"""
        
        domains = metadata.get("physics_domains", ["thermal", "structural"])
        coupling_type = metadata.get("coupling_type", "one_way")
        tenant_id = metadata.get("tenant_id", "public")
        
        # Source configuration for simulation data
        source = {
            "connector_type": "pulsar",
            "connection_params": {
                "service-url": "pulsar://pulsar:6650",
                "admin-url": "http://pulsar:8080",
                "subscription-name": f"seatunnel-multi-physics-{metadata.get('simulation_id', 'default')}"
            },
            "table_or_topic": f"multi-physics-updates-{tenant_id}"
        }
        
        # Transformations for simulation data
        transforms = [
            {
                "type": "simulation_state_extractor",
                "config": {
                    "domains": domains,
                    "extract_convergence": True,
                    "extract_coupling_metrics": coupling_type != "uncoupled"
                }
            },
            {
                "type": "window",
                "config": {
                    "window_type": "sliding",
                    "window_size": "30 seconds",
                    "slide_size": "5 seconds"
                }
            },
            {
                "type": "convergence_monitor",
                "config": {
                    "threshold": metadata.get("convergence_threshold", 1e-4),
                    "alert_on_stall": True,
                    "stall_iterations": 10
                }
            }
        ]
        
        # Multiple sinks for simulation data
        sinks = [
            {
                "connector_type": "ignite",
                "connection_params": {
                    "nodes": ["ignite:10800"],
                    "cache_name": f"simulation_state_{metadata.get('simulation_id')}"
                },
                "config": {
                    "write_mode": "overwrite",
                    "key_field": "domain_id"
                }
            },
            {
                "connector_type": "minio",
                "connection_params": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin"
                },
                "table_or_topic": f"simulations/{metadata.get('simulation_id')}/states",
                "options": {
                    "format": "parquet",
                    "partition_by": ["simulation_tick", "domain"]
                }
            }
        ]
        
        return {
            "name": f"Multi-Physics Simulation Pipeline - {metadata.get('simulation_id')}",
            "pattern": PipelinePattern.MULTI_PHYSICS.value,
            "source": source,
            "transforms": transforms,
            "sinks": sinks,
            "sync_mode": "streaming",
            "checkpoint_interval": 10000
        }
    
    async def _generate_sensor_stream_pipeline(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate pipeline for real-time sensor/IoT data"""
        
        sensor_type = metadata.get("sensor_type", "generic")
        sampling_rate = metadata.get("sampling_rate_hz", 100)
        
        source = {
            "connector_type": "pulsar",
            "connection_params": {
                "service-url": "pulsar://pulsar:6650",
                "admin-url": "http://pulsar:8080",
                "subscription-name": f"seatunnel-sensor-{sensor_type}"
            },
            "table_or_topic": f"sensor-data-{metadata.get('device_id', 'all')}"
        }
        
        transforms = [
            {
                "type": "timestamp_normalizer",
                "config": {
                    "timestamp_field": "event_time",
                    "timezone": metadata.get("timezone", "UTC")
                }
            },
            {
                "type": "data_quality",
                "config": {
                    "null_check": True,
                    "range_check": {
                        "temperature": [-50, 150],
                        "pressure": [0, 10000],
                        "humidity": [0, 100]
                    },
                    "outlier_detection": {
                        "method": "z_score",
                        "threshold": 3
                    }
                }
            },
            {
                "type": "window",
                "config": {
                    "window_type": "tumbling",
                    "window_size": f"{max(1, 1000/sampling_rate)} seconds",
                    "watermark": "1 second"
                }
            },
            {
                "type": "aggregate",
                "config": {
                    "group_by": ["sensor_id", "location"],
                    "aggregations": {
                        "value": ["avg", "min", "max", "stddev"],
                        "timestamp": ["min", "max"],
                        "count": ["count"]
                    }
                }
            }
        ]
        
        sinks = [
            {
                "connector_type": "ignite",
                "connection_params": {
                    "nodes": ["ignite:10800"],
                    "cache_name": f"sensor_realtime_{sensor_type}"
                },
                "config": {
                    "write_mode": "append",
                    "ttl": 3600  # 1 hour TTL for real-time data
                }
            },
            {
                "connector_type": "cassandra",
                "connection_params": {
                    "hosts": ["cassandra:9042"],
                    "keyspace": "sensor_data",
                    "table": f"{sensor_type}_timeseries"
                },
                "config": {
                    "consistency_level": "LOCAL_QUORUM",
                    "ttl": 2592000  # 30 days
                }
            },
            {
                "connector_type": "elasticsearch",
                "connection_params": {
                    "hosts": ["http://elasticsearch:9200"],
                    "index": f"sensors-{sensor_type}",
                    "index.type": "sensor_reading"
                },
                "config": {
                    "index.time_suffix": "yyyy.MM.dd",
                    "bulk.flush.max_actions": 1000
                }
            }
        ]
        
        return {
            "name": f"Sensor Stream Pipeline - {sensor_type}",
            "pattern": PipelinePattern.SENSOR_STREAM.value,
            "source": source,
            "transforms": transforms,
            "sinks": sinks,
            "sync_mode": "streaming",
            "checkpoint_interval": 5000
        }
    
    async def _generate_cross_service_pipeline(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate pipeline for cross-service data synchronization"""
        
        source_service = metadata.get("source_service", "unknown")
        target_services = metadata.get("target_services", [])
        
        # Determine source based on service
        if source_service == "digital-asset-service":
            source = {
                "connector_type": "postgres-cdc",
                "connection_params": {
                    "hostname": "postgres",
                    "port": 5432,
                    "database": "digital_assets",
                    "username": "postgres",
                    "password": "password",
                    "schema": "public",
                    "table.whitelist": "assets,asset_metadata",
                    "slot.name": "seatunnel_asset_sync"
                }
            }
        else:
            # Default to Pulsar event source
            source = {
                "connector_type": "pulsar",
                "connection_params": {
                    "service-url": "pulsar://pulsar:6650"
                },
                "table_or_topic": f"{source_service}-events"
            }
        
        transforms = [
            {
                "type": "schema_mapper",
                "config": {
                    "source_schema": f"{source_service}_schema",
                    "target_schemas": {service: f"{service}_schema" for service in target_services}
                }
            },
            {
                "type": "enricher",
                "config": {
                    "lookup_source": "ignite",
                    "lookup_cache": "reference_data",
                    "join_keys": ["id", "tenant_id"]
                }
            }
        ]
        
        # Create sinks for each target service
        sinks = []
        for target in target_services:
            if target == "search-service":
                sinks.append({
                    "connector_type": "elasticsearch",
                    "connection_params": {
                        "hosts": ["http://elasticsearch:9200"],
                        "index": f"{source_service}_data"
                    }
                })
            elif target == "graph-intelligence-service":
                sinks.append({
                    "connector_type": "janusgraph",
                    "connection_params": {
                        "gremlin.remote.remoteConnectionClass": "org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection",
                        "gremlin.remote.driver.clusterFile": "janusgraph-remote.yaml"
                    },
                    "config": {
                        "vertex_label": source_service.replace("-", "_"),
                        "id_field": "id"
                    }
                })
            else:
                # Default to Pulsar sink
                sinks.append({
                    "connector_type": "pulsar",
                    "connection_params": {
                        "service-url": "pulsar://pulsar:6650"
                    },
                    "table_or_topic": f"{target}-ingestion-events"
                })
        
        return {
            "name": f"Cross-Service Sync: {source_service} → {', '.join(target_services)}",
            "pattern": PipelinePattern.CROSS_SERVICE.value,
            "source": source,
            "transforms": transforms,
            "sinks": sinks,
            "sync_mode": "cdc" if "cdc" in source.get("connector_type", "") else "streaming"
        }
    
    async def _generate_graph_sync_pipeline(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate pipeline for Cassandra to JanusGraph synchronization"""
        
        entity_type = metadata.get("entity_type", "generic")
        relationship_config = metadata.get("relationships", {})
        
        source = {
            "connector_type": "cassandra-cdc",
            "connection_params": {
                "hosts": ["cassandra:9042"],
                "keyspace": metadata.get("keyspace", "platformq"),
                "table": metadata.get("table", entity_type),
                "consistency_level": "LOCAL_QUORUM"
            }
        }
        
        transforms = [
            {
                "type": "graph_entity_builder",
                "config": {
                    "vertex_label": entity_type,
                    "id_mapping": metadata.get("id_field", "id"),
                    "property_mappings": metadata.get("property_mappings", {}),
                    "relationships": relationship_config
                }
            },
            {
                "type": "relationship_resolver",
                "config": {
                    "lookup_strategy": "batch",
                    "cache_ttl": 3600,
                    "create_missing": False
                }
            }
        ]
        
        sinks = [
            {
                "connector_type": "janusgraph",
                "connection_params": {
                    "gremlin.hosts": ["janusgraph:8182"],
                    "gremlin.graph": "platformq_graph",
                    "serializer.className": "org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0"
                },
                "config": {
                    "batch_size": 100,
                    "vertex_id_key": metadata.get("graph_id_key", "cassandra_id"),
                    "edge_creation_strategy": metadata.get("edge_strategy", "merge")
                }
            }
        ]
        
        # Add lineage tracking
        if metadata.get("track_lineage", True):
            sinks.append({
                "connector_type": "pulsar",
                "connection_params": {
                    "service-url": "pulsar://pulsar:6650"
                },
                "table_or_topic": "graph-sync-lineage",
                "config": {
                    "include_source_metadata": True
                }
            })
        
        return {
            "name": f"Graph Sync Pipeline - {entity_type}",
            "pattern": PipelinePattern.GRAPH_SYNC.value,
            "source": source,
            "transforms": transforms,
            "sinks": sinks,
            "sync_mode": "cdc",
            "error_handling": {
                "max_retries": 3,
                "retry_delay": "exponential",
                "dead_letter_topic": f"graph-sync-dlq-{entity_type}"
            }
        }
    
    def _detect_pattern(self, metadata: Dict[str, Any]) -> PipelinePattern:
        """Detect pipeline pattern from metadata"""
        asset_type = metadata.get("asset_type", "").upper()
        
        if any(prefix in asset_type for prefix in ["MULTI_PHYSICS_", "COUPLED_SIM_", "THERMAL_STRUCTURAL_"]):
            return PipelinePattern.MULTI_PHYSICS
        elif any(prefix in asset_type for prefix in ["SENSOR_", "IOT_", "TELEMETRY_", "DEVICE_"]):
            return PipelinePattern.SENSOR_STREAM
        elif metadata.get("sync_type") == "cross_service":
            return PipelinePattern.CROSS_SERVICE
        elif metadata.get("target_type") == "graph" or metadata.get("sync_to_graph"):
            return PipelinePattern.GRAPH_SYNC
        elif metadata.get("cdc_enabled") or metadata.get("source_type") == "database_cdc":
            return PipelinePattern.CDC
        elif metadata.get("source_type") == "streaming":
            return PipelinePattern.STREAMING
        else:
            return PipelinePattern.INGESTION
    
    async def _generate_default_pipeline(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any],
        target_sinks: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Generate default pipeline using existing logic"""
        # Use the existing generate_pipeline method logic
        return self.generate_pipeline(metadata, metadata.get("tenant_id", "public")) 