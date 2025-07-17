"""
SeaTunnel Pipeline for Anomaly Data Synchronization

Syncs anomaly detection data between real-time analytics and data lake
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta
import json

from ..models import PipelineConfig, SourceConfig, SinkConfig, TransformConfig


class AnomalyDataSyncPipeline:
    """Pipeline for synchronizing anomaly detection data"""
    
    @staticmethod
    def create_realtime_to_lake_pipeline() -> PipelineConfig:
        """Create pipeline to sync real-time anomalies to data lake"""
        
        return PipelineConfig(
            name="anomaly_realtime_to_lake",
            description="Sync real-time anomaly data to data lake for ML training",
            source=SourceConfig(
                type="ignite",
                config={
                    "hosts": ["ignite:10800"],
                    "cache_name": "simulation_anomalies",
                    "query_type": "scan",
                    "batch_size": 1000
                }
            ),
            transforms=[
                TransformConfig(
                    type="field_mapper",
                    config={
                        "mappings": {
                            "anomaly_id": "id",
                            "simulation_id": "simulation_id",
                            "timestamp": "detection_time",
                            "anomaly_type": "type",
                            "anomaly_score": "score",
                            "feature_importance": "features",
                            "impact.severity": "severity",
                            "recommended_actions": "actions"
                        }
                    }
                ),
                TransformConfig(
                    type="timestamp",
                    config={
                        "source_field": "detection_time",
                        "target_field": "event_time",
                        "format": "ISO8601"
                    }
                ),
                TransformConfig(
                    type="enrichment",
                    config={
                        "enrichments": [
                            {
                                "field": "partition_date",
                                "type": "date_format",
                                "source": "event_time",
                                "format": "yyyy-MM-dd"
                            },
                            {
                                "field": "hour",
                                "type": "date_format",
                                "source": "event_time",
                                "format": "HH"
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="minio",
                config={
                    "endpoint": "http://minio:9000",
                    "bucket": "anomaly-data",
                    "path": "silver/anomalies/date=${partition_date}/hour=${hour}/",
                    "format": "parquet",
                    "compression": "snappy",
                    "partition_by": ["partition_date", "hour"]
                }
            ),
            execution_config={
                "parallelism": 2,
                "checkpoint_interval": 60000,  # 1 minute
                "execution_mode": "streaming"
            }
        )
    
    @staticmethod
    def create_historical_analysis_pipeline() -> PipelineConfig:
        """Create pipeline for historical anomaly analysis"""
        
        return PipelineConfig(
            name="anomaly_historical_analysis",
            description="Analyze historical anomaly patterns for ML training",
            source=SourceConfig(
                type="minio",
                config={
                    "endpoint": "http://minio:9000",
                    "bucket": "anomaly-data",
                    "path": "silver/anomalies/",
                    "format": "parquet",
                    "recursive": True
                }
            ),
            transforms=[
                TransformConfig(
                    type="filter",
                    config={
                        "condition": "severity > 0.5 AND type != 'false_positive'"
                    }
                ),
                TransformConfig(
                    type="aggregation",
                    config={
                        "group_by": ["simulation_id", "type"],
                        "aggregations": [
                            {
                                "field": "score",
                                "function": "avg",
                                "alias": "avg_score"
                            },
                            {
                                "field": "score",
                                "function": "max",
                                "alias": "max_score"
                            },
                            {
                                "field": "*",
                                "function": "count",
                                "alias": "anomaly_count"
                            }
                        ],
                        "window": {
                            "type": "tumbling",
                            "size": "1 hour"
                        }
                    }
                ),
                TransformConfig(
                    type="ml_feature_engineering",
                    config={
                        "features": [
                            {
                                "name": "anomaly_rate",
                                "expression": "anomaly_count / 3600.0"
                            },
                            {
                                "name": "severity_weighted_score",
                                "expression": "avg_score * severity"
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="elasticsearch",
                config={
                    "hosts": ["http://elasticsearch:9200"],
                    "index": "anomaly_patterns",
                    "document_type": "_doc",
                    "id_field": "simulation_id",
                    "refresh_policy": "wait_for"
                }
            ),
            execution_config={
                "parallelism": 4,
                "execution_mode": "batch"
            }
        )
    
    @staticmethod
    def create_ml_training_data_pipeline() -> PipelineConfig:
        """Create pipeline to prepare anomaly data for ML training"""
        
        return PipelineConfig(
            name="anomaly_ml_training_data",
            description="Prepare anomaly detection training data",
            source=SourceConfig(
                type="multi_source",
                config={
                    "sources": [
                        {
                            "name": "anomalies",
                            "type": "minio",
                            "config": {
                                "endpoint": "http://minio:9000",
                                "bucket": "anomaly-data",
                                "path": "silver/anomalies/",
                                "format": "parquet"
                            }
                        },
                        {
                            "name": "simulations",
                            "type": "minio",
                            "config": {
                                "endpoint": "http://minio:9000",
                                "bucket": "simulation-data",
                                "path": "silver/simulations/",
                                "format": "parquet"
                            }
                        }
                    ]
                }
            ),
            transforms=[
                TransformConfig(
                    type="join",
                    config={
                        "left_table": "anomalies",
                        "right_table": "simulations",
                        "join_type": "inner",
                        "on": ["simulation_id"],
                        "select": [
                            "anomalies.*",
                            "simulations.parameters",
                            "simulations.metrics"
                        ]
                    }
                ),
                TransformConfig(
                    type="feature_extraction",
                    config={
                        "features": [
                            {
                                "name": "convergence_rate",
                                "path": "metrics.convergence_rate",
                                "default": 0.0
                            },
                            {
                                "name": "error_rate",
                                "path": "metrics.error_rate",
                                "default": 0.0
                            },
                            {
                                "name": "resource_usage",
                                "path": "metrics.resource_usage",
                                "default": 0.0
                            },
                            {
                                "name": "time_step",
                                "path": "parameters.time_step",
                                "default": 0.1
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="label_encoding",
                    config={
                        "label_field": "type",
                        "encoding_type": "one_hot"
                    }
                ),
                TransformConfig(
                    type="data_quality",
                    config={
                        "checks": [
                            {
                                "type": "null_check",
                                "fields": ["score", "severity", "type"]
                            },
                            {
                                "type": "range_check",
                                "field": "score",
                                "min": -10.0,
                                "max": 10.0
                            }
                        ],
                        "action": "filter"  # Remove invalid records
                    }
                )
            ],
            sink=SinkConfig(
                type="minio",
                config={
                    "endpoint": "http://minio:9000",
                    "bucket": "ml-training-data",
                    "path": "anomaly_detection/version=${version}/",
                    "format": "parquet",
                    "schema_evolution": "merge",
                    "properties": {
                        "version": datetime.now().strftime("%Y%m%d_%H%M%S")
                    }
                }
            ),
            execution_config={
                "parallelism": 4,
                "execution_mode": "batch",
                "schedule": "0 0 * * *"  # Daily at midnight
            }
        )
    
    @staticmethod
    def create_alert_pipeline() -> PipelineConfig:
        """Create pipeline for real-time anomaly alerts"""
        
        return PipelineConfig(
            name="anomaly_alerts",
            description="Real-time anomaly alert pipeline",
            source=SourceConfig(
                type="pulsar",
                config={
                    "service_url": "pulsar://pulsar:6650",
                    "topics": ["anomaly-events"],
                    "subscription_name": "anomaly-alert-sub",
                    "subscription_type": "shared"
                }
            ),
            transforms=[
                TransformConfig(
                    type="filter",
                    config={
                        "condition": "impact.severity > 0.7"
                    }
                ),
                TransformConfig(
                    type="enrichment",
                    config={
                        "enrichments": [
                            {
                                "field": "notification_channels",
                                "type": "lookup",
                                "source": "simulation_id",
                                "lookup_cache": "notification_preferences",
                                "default": ["email", "slack"]
                            },
                            {
                                "field": "owner_email",
                                "type": "lookup",
                                "source": "simulation_id",
                                "lookup_cache": "simulation_owners"
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="template",
                    config={
                        "template_field": "alert_message",
                        "template": """
                        🚨 Anomaly Alert for Simulation ${simulation_id}
                        
                        Type: ${anomaly_type}
                        Severity: ${impact.severity}
                        Time: ${timestamp}
                        
                        Recommended Actions:
                        ${recommended_actions}
                        
                        View Dashboard: https://platform.example.com/simulations/${simulation_id}
                        """
                    }
                )
            ],
            sink=SinkConfig(
                type="notification",
                config={
                    "channels": "${notification_channels}",
                    "email": {
                        "to": "${owner_email}",
                        "subject": "Anomaly Alert: ${anomaly_type}",
                        "body": "${alert_message}"
                    },
                    "slack": {
                        "webhook_url": "${SLACK_WEBHOOK_URL}",
                        "message": "${alert_message}"
                    }
                }
            ),
            execution_config={
                "parallelism": 2,
                "execution_mode": "streaming",
                "checkpoint_interval": 30000  # 30 seconds
            }
        )


def register_anomaly_pipelines(pipeline_registry):
    """Register all anomaly-related pipelines"""
    
    pipelines = [
        AnomalyDataSyncPipeline.create_realtime_to_lake_pipeline(),
        AnomalyDataSyncPipeline.create_historical_analysis_pipeline(),
        AnomalyDataSyncPipeline.create_ml_training_data_pipeline(),
        AnomalyDataSyncPipeline.create_alert_pipeline()
    ]
    
    for pipeline in pipelines:
        pipeline_registry.register(pipeline.name, pipeline) 