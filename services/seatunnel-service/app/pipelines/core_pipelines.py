"""
Core Data Synchronization Pipelines for Unified Data Mesh

These pipelines implement the foundational data flows between
Cassandra (operational), Elasticsearch (search), and MinIO (analytical).
"""

from typing import Dict, Any, List
import json
from datetime import datetime
from enum import Enum

class CorePipelineType(Enum):
    """Core pipeline types for data mesh"""
    CASSANDRA_TO_LAKE = "cassandra_to_lake"
    CASSANDRA_TO_SEARCH = "cassandra_to_search"
    LAKE_TO_SEARCH = "lake_to_search"
    STREAM_TO_ALL = "stream_to_all"
    QUALITY_MONITORING = "quality_monitoring"
    SCHEMA_EVOLUTION = "schema_evolution"

# Core pipeline configurations
CORE_PIPELINES = {
    "digital_assets_sync": {
        "name": "Digital Assets Full Sync",
        "description": "Synchronize digital assets from Cassandra to data lake and search",
        "type": CorePipelineType.CASSANDRA_TO_LAKE,
        "config": {
            "env": {
                "execution.parallelism": 4,
                "execution.checkpoint.interval": 60000,
                "execution.checkpoint.data-uri": "s3://platformq-checkpoints/core/digital-assets/"
            },
            "source": [
                {
                    "connector_type": "cassandra",
                    "connection_params": {
                        "host": "cassandra",
                        "port": 9042,
                        "keyspace": "platformq",
                        "table": "digital_assets",
                        "consistency": "LOCAL_QUORUM"
                    },
                    "schema": {
                        "type": "record",
                        "name": "DigitalAsset",
                        "namespace": "com.platformq.core",
                        "fields": [
                            {"name": "cid", "type": "string"},
                            {"name": "asset_name", "type": "string"},
                            {"name": "asset_type", "type": "string"},
                            {"name": "owner_id", "type": "string"},
                            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
                            {"name": "metadata", "type": {"type": "map", "values": "string"}},
                            {"name": "tags", "type": {"type": "array", "items": "string"}}
                        ]
                    }
                }
            ],
            "transform": [
                {
                    "type": "sql",
                    "config": {
                        "query": """
                            SELECT 
                                cid as asset_id,
                                asset_name,
                                asset_type,
                                owner_id,
                                created_at,
                                metadata,
                                tags,
                                current_timestamp() as sync_timestamp,
                                'cassandra' as source_system
                            FROM digital_assets
                        """
                    }
                },
                {
                    "type": "schema_registry",
                    "config": {
                        "schema_registry_url": "http://schema-registry:8081",
                        "subject": "digital-asset",
                        "compatibility_check": True
                    }
                }
            ],
            "sink": [
                {
                    "connector_type": "minio",
                    "connection_params": {
                        "endpoint": "http://minio:9000",
                        "access_key": "${MINIO_ACCESS_KEY}",
                        "secret_key": "${MINIO_SECRET_KEY}",
                        "bucket": "platformq-data-lake"
                    },
                    "table_or_topic": "bronze/digital_assets",
                    "options": {
                        "format": "parquet",
                        "partition_by": ["asset_type", "year", "month", "day"],
                        "compression": "snappy",
                        "write_mode": "append"
                    }
                },
                {
                    "connector_type": "elasticsearch",
                    "connection_params": {
                        "hosts": ["http://elasticsearch:9200"],
                        "index": "digital_assets"
                    },
                    "options": {
                        "index_type": "_doc",
                        "id_field": "asset_id",
                        "refresh": "wait_for"
                    }
                }
            ]
        },
        "schedule": "0 */2 * * *",  # Every 2 hours
        "monitoring": {
            "alert_on_failure": True,
            "metrics": ["row_count", "error_rate", "processing_time"]
        }
    },

    "cad_sessions_realtime": {
        "name": "CAD Sessions Real-time Sync",
        "description": "Stream CAD session updates to Ignite cache and data lake",
        "type": CorePipelineType.STREAM_TO_ALL,
        "config": {
            "env": {
                "execution.parallelism": 8,
                "execution.checkpoint.interval": 30000,
                "execution.checkpoint.data-uri": "s3://platformq-checkpoints/core/cad-sessions/"
            },
            "source": [
                {
                    "connector_type": "pulsar",
                    "connection_params": {
                        "service-url": "pulsar://pulsar:6650",
                        "admin-url": "http://pulsar:8080",
                        "subscription-name": "cad-session-sync"
                    },
                    "table_or_topic": "persistent://platformq/*/cad-session-events",
                    "options": {
                        "consumer-type": "Shared",
                        "subscription-initial-position": "Latest"
                    }
                }
            ],
            "transform": [
                {
                    "type": "avro_deserialize",
                    "config": {
                        "schema_registry_url": "http://schema-registry:8081",
                        "subject": "cad-session"
                    }
                },
                {
                    "type": "enrich",
                    "config": {
                        "enrichments": [
                            {
                                "field": "user_count",
                                "expression": "SIZE(active_users)"
                            },
                            {
                                "field": "session_duration_ms",
                                "expression": "CURRENT_TIMESTAMP - created_at"
                            }
                        ]
                    }
                }
            ],
            "sink": [
                {
                    "connector_type": "ignite",
                    "connection_params": {
                        "hosts": ["ignite:10800"],
                        "cache_name": "cad_sessions"
                    },
                    "options": {
                        "key_field": "session_id",
                        "write_mode": "upsert",
                        "expiry_time": 86400  # 24 hours
                    }
                },
                {
                    "connector_type": "minio",
                    "connection_params": {
                        "endpoint": "http://minio:9000",
                        "access_key": "${MINIO_ACCESS_KEY}",
                        "secret_key": "${MINIO_SECRET_KEY}",
                        "bucket": "platformq-data-lake"
                    },
                    "table_or_topic": "bronze/cad_sessions",
                    "options": {
                        "format": "parquet",
                        "partition_by": ["tenant_id", "date"],
                        "compression": "snappy"
                    }
                }
            ]
        },
        "sync_mode": "streaming",
        "checkpoint_interval": 30000
    },

    "user_activity_aggregation": {
        "name": "User Activity Aggregation",
        "description": "Aggregate user activities from multiple sources",
        "type": CorePipelineType.LAKE_TO_SEARCH,
        "config": {
            "env": {
                "execution.parallelism": 4,
                "execution.checkpoint.interval": 300000
            },
            "source": [
                {
                    "connector_type": "jdbc",
                    "connection_params": {
                        "url": "jdbc:trino://trino:8081/hive/bronze",
                        "driver": "io.trino.jdbc.TrinoDriver",
                        "user": "platformq"
                    },
                    "query": """
                        WITH activity_union AS (
                            SELECT 
                                user_id,
                                event_type,
                                event_timestamp,
                                'cassandra' as source
                            FROM cassandra.auth_keyspace.activity_stream
                            WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
                            
                            UNION ALL
                            
                            SELECT 
                                user_id,
                                action as event_type,
                                timestamp as event_timestamp,
                                'pulsar' as source
                            FROM hive.bronze.user_events
                            WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
                        )
                        SELECT 
                            user_id,
                            DATE(event_timestamp) as activity_date,
                            COUNT(*) as total_events,
                            COUNT(DISTINCT event_type) as unique_event_types,
                            MIN(event_timestamp) as first_activity,
                            MAX(event_timestamp) as last_activity,
                            MAP_AGG(event_type, count) as event_type_counts
                        FROM (
                            SELECT 
                                user_id,
                                event_type,
                                event_timestamp,
                                COUNT(*) as count
                            FROM activity_union
                            GROUP BY user_id, event_type, event_timestamp
                        )
                        GROUP BY user_id, DATE(event_timestamp)
                    """
                }
            ],
            "transform": [
                {
                    "type": "calculate_metrics",
                    "config": {
                        "metrics": [
                            {
                                "name": "engagement_score",
                                "formula": "total_events * 0.3 + unique_event_types * 0.7"
                            },
                            {
                                "name": "session_duration_minutes",
                                "formula": "(last_activity - first_activity) / 60000"
                            }
                        ]
                    }
                }
            ],
            "sink": [
                {
                    "connector_type": "elasticsearch",
                    "connection_params": {
                        "hosts": ["http://elasticsearch:9200"],
                        "index": "user_activity_summary"
                    },
                    "options": {
                        "index_type": "_doc",
                        "id_field": "user_id,activity_date",
                        "index_pattern": "user_activity_summary-{activity_date}"
                    }
                },
                {
                    "connector_type": "minio",
                    "connection_params": {
                        "endpoint": "http://minio:9000",
                        "access_key": "${MINIO_ACCESS_KEY}",
                        "secret_key": "${MINIO_SECRET_KEY}",
                        "bucket": "platformq-data-lake"
                    },
                    "table_or_topic": "gold/user_activity_summary",
                    "options": {
                        "format": "parquet",
                        "partition_by": ["activity_date"],
                        "write_mode": "overwrite"
                    }
                }
            ]
        },
        "schedule": "0 1 * * *",  # Daily at 1 AM
        "sync_mode": "batch"
    },

    "data_quality_monitoring": {
        "name": "Data Quality Monitoring Pipeline",
        "description": "Monitor data quality across all layers",
        "type": CorePipelineType.QUALITY_MONITORING,
        "config": {
            "env": {
                "execution.parallelism": 2
            },
            "source": [
                {
                    "connector_type": "jdbc",
                    "connection_params": {
                        "url": "jdbc:trino://trino:8081",
                        "driver": "io.trino.jdbc.TrinoDriver",
                        "user": "platformq"
                    },
                    "query": """
                        SELECT 
                            catalog,
                            schema,
                            table_name,
                            COUNT(*) as row_count,
                            COUNT(DISTINCT tenant_id) as tenant_count,
                            APPROX_PERCENTILE(LENGTH(CAST(metadata AS VARCHAR)), 0.5) as median_metadata_size
                        FROM information_schema.tables t
                        JOIN (
                            SELECT * FROM cassandra.platformq.digital_assets
                            UNION ALL
                            SELECT * FROM hive.bronze.digital_assets
                            UNION ALL
                            SELECT * FROM elasticsearch.default.assets
                        ) data ON TRUE
                        GROUP BY catalog, schema, table_name
                    """
                }
            ],
            "transform": [
                {
                    "type": "data_quality_check",
                    "config": {
                        "checks": [
                            {
                                "name": "completeness",
                                "type": "null_check",
                                "columns": ["asset_id", "asset_name", "owner_id"]
                            },
                            {
                                "name": "uniqueness",
                                "type": "duplicate_check",
                                "columns": ["asset_id"]
                            },
                            {
                                "name": "consistency",
                                "type": "cross_source_check",
                                "validation": "COUNT(*) GROUP BY asset_id HAVING COUNT(DISTINCT source) > 1"
                            },
                            {
                                "name": "freshness",
                                "type": "timestamp_check",
                                "column": "sync_timestamp",
                                "threshold": "1 hour"
                            }
                        ]
                    }
                }
            ],
            "sink": [
                {
                    "connector_type": "pulsar",
                    "connection_params": {
                        "service-url": "pulsar://pulsar:6650"
                    },
                    "table_or_topic": "data-quality-metrics",
                    "options": {
                        "schema_subject": "data-quality-metric"
                    }
                },
                {
                    "connector_type": "minio",
                    "connection_params": {
                        "endpoint": "http://minio:9000",
                        "access_key": "${MINIO_ACCESS_KEY}",
                        "secret_key": "${MINIO_SECRET_KEY}",
                        "bucket": "platformq-data-lake"
                    },
                    "table_or_topic": "quality/metrics",
                    "options": {
                        "format": "parquet",
                        "partition_by": ["check_date", "layer"]
                    }
                }
            ]
        },
        "schedule": "0 */6 * * *",  # Every 6 hours
        "sync_mode": "batch"
    },

    "schema_evolution_sync": {
        "name": "Schema Evolution Synchronization",
        "description": "Detect and propagate schema changes across systems",
        "type": CorePipelineType.SCHEMA_EVOLUTION,
        "config": {
            "env": {
                "execution.parallelism": 1
            },
            "source": [
                {
                    "connector_type": "http",
                    "connection_params": {
                        "url": "http://schema-registry:8081/subjects",
                        "method": "GET"
                    }
                }
            ],
            "transform": [
                {
                    "type": "schema_evolution_detector",
                    "config": {
                        "schema_registry_url": "http://schema-registry:8081",
                        "compatibility_mode": "BACKWARD",
                        "evolution_strategies": {
                            "add_field": "ADD_DEFAULT",
                            "remove_field": "MARK_DEPRECATED",
                            "rename_field": "CREATE_ALIAS"
                        }
                    }
                },
                {
                    "type": "generate_migration",
                    "config": {
                        "target_systems": ["cassandra", "elasticsearch", "hive"],
                        "migration_templates": {
                            "cassandra": "ALTER TABLE {table} ADD {field} {type}",
                            "elasticsearch": {"mappings": {"properties": {"{field}": {"type": "{type}"}}}},
                            "hive": "ALTER TABLE {table} ADD COLUMNS ({field} {type})"
                        }
                    }
                }
            ],
            "sink": [
                {
                    "connector_type": "webhook",
                    "connection_params": {
                        "url": "http://workflow-service:8000/api/v1/trigger/schema-migration"
                    },
                    "options": {
                        "method": "POST",
                        "headers": {"Content-Type": "application/json"}
                    }
                }
            ]
        },
        "schedule": "0 */12 * * *",  # Twice daily
        "sync_mode": "batch"
    }
}

def get_core_pipeline(pipeline_name: str) -> Dict[str, Any]:
    """Get a core pipeline configuration by name"""
    if pipeline_name not in CORE_PIPELINES:
        raise ValueError(f"Core pipeline '{pipeline_name}' not found")
    return CORE_PIPELINES[pipeline_name]

def list_core_pipelines() -> List[Dict[str, str]]:
    """List all available core pipelines"""
    return [
        {
            "name": name,
            "description": config["description"],
            "type": config["type"].value,
            "schedule": config.get("schedule", "continuous")
        }
        for name, config in CORE_PIPELINES.items()
    ]

def generate_pipeline_config(pipeline_name: str, tenant_id: str) -> Dict[str, Any]:
    """Generate a tenant-specific pipeline configuration"""
    base_config = get_core_pipeline(pipeline_name)
    config = base_config["config"].copy()
    
    # Add tenant-specific modifications
    config["env"]["tenant_id"] = tenant_id
    
    # Update sink paths to include tenant
    for sink in config.get("sink", []):
        if "table_or_topic" in sink:
            sink["table_or_topic"] = f"{tenant_id}/{sink['table_or_topic']}"
    
    return {
        "name": f"{base_config['name']} - {tenant_id}",
        "description": base_config["description"],
        "config": config,
        "sync_mode": base_config.get("sync_mode", "batch"),
        "schedule": base_config.get("schedule")
    } 