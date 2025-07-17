"""
Automated Data Lake to Graph Pipelines

Synchronizes data from MinIO data lake to JanusGraph for relationship analysis
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

from ..models import PipelineConfig, SourceConfig, SinkConfig, TransformConfig


class LakeToGraphPipeline:
    """Pipeline for syncing data lake to graph database"""
    
    @staticmethod
    def create_asset_lineage_pipeline() -> PipelineConfig:
        """Create pipeline to sync asset lineage to graph"""
        
        return PipelineConfig(
            name="asset_lineage_to_graph",
            description="Sync asset lineage from data lake to JanusGraph",
            source=SourceConfig(
                type="minio",
                config={
                    "endpoint": "http://minio:9000",
                    "bucket": "asset-data",
                    "path": "silver/lineage/",
                    "format": "parquet",
                    "recursive": True,
                    "schema": {
                        "asset_id": "string",
                        "parent_id": "string",
                        "transformation_type": "string",
                        "created_at": "timestamp",
                        "metadata": "map<string,string>"
                    }
                }
            ),
            transforms=[
                TransformConfig(
                    type="deduplication",
                    config={
                        "keys": ["asset_id", "parent_id"],
                        "keep": "last"
                    }
                ),
                TransformConfig(
                    type="graph_entity_builder",
                    config={
                        "vertex_mappings": [
                            {
                                "label": "Asset",
                                "id_field": "asset_id",
                                "properties": ["created_at", "metadata.type", "metadata.size"]
                            },
                            {
                                "label": "Asset",
                                "id_field": "parent_id",
                                "properties": []
                            }
                        ],
                        "edge_mappings": [
                            {
                                "label": "derived_from",
                                "from_field": "asset_id",
                                "to_field": "parent_id",
                                "properties": ["transformation_type", "created_at"]
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="validation",
                    config={
                        "rules": [
                            {
                                "field": "asset_id",
                                "type": "not_null"
                            },
                            {
                                "field": "parent_id",
                                "type": "not_null"
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "batch_size": 1000,
                    "write_mode": "upsert",
                    "consistency_level": "LOCAL_QUORUM"
                }
            ),
            execution_config={
                "parallelism": 4,
                "checkpoint_interval": 300000,  # 5 minutes
                "execution_mode": "streaming",
                "watermark_delay": 60000  # 1 minute
            }
        )
    
    @staticmethod
    def create_collaboration_network_pipeline() -> PipelineConfig:
        """Create pipeline to build collaboration network in graph"""
        
        return PipelineConfig(
            name="collaboration_network_graph",
            description="Build collaboration network from user interactions",
            source=SourceConfig(
                type="multi_source",
                config={
                    "sources": [
                        {
                            "name": "project_members",
                            "type": "minio",
                            "config": {
                                "endpoint": "http://minio:9000",
                                "bucket": "project-data",
                                "path": "silver/members/",
                                "format": "parquet"
                            }
                        },
                        {
                            "name": "asset_collaborators",
                            "type": "minio",
                            "config": {
                                "endpoint": "http://minio:9000",
                                "bucket": "asset-data",
                                "path": "silver/collaborators/",
                                "format": "parquet"
                            }
                        },
                        {
                            "name": "simulation_participants",
                            "type": "minio",
                            "config": {
                                "endpoint": "http://minio:9000",
                                "bucket": "simulation-data",
                                "path": "silver/participants/",
                                "format": "parquet"
                            }
                        }
                    ]
                }
            ),
            transforms=[
                TransformConfig(
                    type="union",
                    config={
                        "tables": ["project_members", "asset_collaborators", "simulation_participants"],
                        "dedup_keys": ["user_id", "entity_id"]
                    }
                ),
                TransformConfig(
                    type="aggregation",
                    config={
                        "group_by": ["user_id_1", "user_id_2"],
                        "aggregations": [
                            {
                                "field": "*",
                                "function": "count",
                                "alias": "collaboration_count"
                            },
                            {
                                "field": "entity_type",
                                "function": "collect_set",
                                "alias": "collaboration_types"
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="graph_entity_builder",
                    config={
                        "vertex_mappings": [
                            {
                                "label": "User",
                                "id_field": "user_id_1",
                                "properties": ["user_name", "user_role"]
                            },
                            {
                                "label": "User",
                                "id_field": "user_id_2",
                                "properties": ["user_name", "user_role"]
                            }
                        ],
                        "edge_mappings": [
                            {
                                "label": "collaborates_with",
                                "from_field": "user_id_1",
                                "to_field": "user_id_2",
                                "properties": ["collaboration_count", "collaboration_types", "last_collaboration"]
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "batch_size": 500,
                    "write_mode": "upsert"
                }
            ),
            execution_config={
                "parallelism": 2,
                "execution_mode": "batch",
                "schedule": "0 */6 * * *"  # Every 6 hours
            }
        )
    
    @staticmethod
    def create_semantic_relationship_pipeline() -> PipelineConfig:
        """Create pipeline to build semantic relationships in graph"""
        
        return PipelineConfig(
            name="semantic_relationships_graph",
            description="Extract and sync semantic relationships to graph",
            source=SourceConfig(
                type="elasticsearch",
                config={
                    "hosts": ["http://elasticsearch:9200"],
                    "indices": ["assets", "simulations", "projects"],
                    "query": {
                        "range": {
                            "updated_at": {
                                "gte": "now-1d"
                            }
                        }
                    },
                    "scroll_size": 1000
                }
            ),
            transforms=[
                TransformConfig(
                    type="nlp_extraction",
                    config={
                        "text_fields": ["description", "content", "tags"],
                        "extractors": [
                            {
                                "type": "entity_recognition",
                                "model": "en_core_web_sm",
                                "entity_types": ["PERSON", "ORG", "PRODUCT"]
                            },
                            {
                                "type": "keyword_extraction",
                                "algorithm": "textrank",
                                "num_keywords": 10
                            },
                            {
                                "type": "topic_modeling",
                                "algorithm": "lda",
                                "num_topics": 5
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="similarity_computation",
                    config={
                        "method": "cosine",
                        "threshold": 0.7,
                        "features": ["keywords", "topics", "entities"]
                    }
                ),
                TransformConfig(
                    type="graph_entity_builder",
                    config={
                        "vertex_mappings": [
                            {
                                "label": "Entity",
                                "id_field": "entity_id",
                                "properties": ["entity_type", "title", "keywords", "topics"]
                            }
                        ],
                        "edge_mappings": [
                            {
                                "label": "semantically_related",
                                "from_field": "entity_id_1",
                                "to_field": "entity_id_2",
                                "properties": ["similarity_score", "common_keywords", "common_topics"]
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "batch_size": 200,
                    "write_mode": "upsert"
                }
            ),
            execution_config={
                "parallelism": 4,
                "execution_mode": "batch",
                "schedule": "0 2 * * *"  # Daily at 2 AM
            }
        )
    
    @staticmethod
    def create_workflow_dependency_pipeline() -> PipelineConfig:
        """Create pipeline to sync workflow dependencies to graph"""
        
        return PipelineConfig(
            name="workflow_dependencies_graph",
            description="Build workflow dependency graph from execution data",
            source=SourceConfig(
                type="ignite",
                config={
                    "hosts": ["ignite:10800"],
                    "cache_name": "workflow_executions",
                    "query_type": "sql",
                    "query": """
                    SELECT 
                        w.workflow_id,
                        w.parent_workflow_id,
                        w.name,
                        w.type,
                        w.status,
                        w.created_at,
                        w.dependencies
                    FROM workflow_executions w
                    WHERE w.created_at > CURRENT_TIMESTAMP - INTERVAL '7' DAY
                    """
                }
            ),
            transforms=[
                TransformConfig(
                    type="json_parser",
                    config={
                        "json_fields": ["dependencies"],
                        "flatten": True
                    }
                ),
                TransformConfig(
                    type="graph_entity_builder",
                    config={
                        "vertex_mappings": [
                            {
                                "label": "Workflow",
                                "id_field": "workflow_id",
                                "properties": ["name", "type", "status", "created_at"]
                            }
                        ],
                        "edge_mappings": [
                            {
                                "label": "depends_on",
                                "from_field": "workflow_id",
                                "to_field": "dependency_id",
                                "properties": ["dependency_type", "required"]
                            },
                            {
                                "label": "child_of",
                                "from_field": "workflow_id",
                                "to_field": "parent_workflow_id",
                                "properties": ["created_at"]
                            }
                        ]
                    }
                ),
                TransformConfig(
                    type="cycle_detection",
                    config={
                        "edge_label": "depends_on",
                        "action": "log_warning"
                    }
                )
            ],
            sink=SinkConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "batch_size": 500,
                    "write_mode": "upsert"
                }
            ),
            execution_config={
                "parallelism": 2,
                "execution_mode": "streaming",
                "checkpoint_interval": 180000  # 3 minutes
            }
        )
    
    @staticmethod
    def create_ml_model_lineage_pipeline() -> PipelineConfig:
        """Create pipeline to track ML model lineage in graph"""
        
        return PipelineConfig(
            name="ml_model_lineage_graph",
            description="Track ML model lineage and relationships",
            source=SourceConfig(
                type="mlflow",
                config={
                    "tracking_uri": "http://mlflow:5000",
                    "experiment_names": ["*"],
                    "include_metrics": True,
                    "include_params": True,
                    "include_artifacts": False
                }
            ),
            transforms=[
                TransformConfig(
                    type="model_relationship_extractor",
                    config={
                        "extract_parent_models": True,
                        "extract_training_data": True,
                        "extract_feature_importance": True
                    }
                ),
                TransformConfig(
                    type="graph_entity_builder",
                    config={
                        "vertex_mappings": [
                            {
                                "label": "MLModel",
                                "id_field": "model_id",
                                "properties": ["model_name", "version", "metrics", "params", "created_at"]
                            },
                            {
                                "label": "Dataset",
                                "id_field": "dataset_id",
                                "properties": ["dataset_name", "size", "features"]
                            },
                            {
                                "label": "Experiment",
                                "id_field": "experiment_id",
                                "properties": ["experiment_name", "tags"]
                            }
                        ],
                        "edge_mappings": [
                            {
                                "label": "trained_on",
                                "from_field": "model_id",
                                "to_field": "dataset_id",
                                "properties": ["training_date", "data_version"]
                            },
                            {
                                "label": "derived_from",
                                "from_field": "model_id",
                                "to_field": "parent_model_id",
                                "properties": ["improvement_metrics"]
                            },
                            {
                                "label": "part_of",
                                "from_field": "model_id",
                                "to_field": "experiment_id",
                                "properties": ["run_id"]
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "batch_size": 100,
                    "write_mode": "upsert"
                }
            ),
            execution_config={
                "parallelism": 2,
                "execution_mode": "batch",
                "schedule": "0 */4 * * *"  # Every 4 hours
            }
        )


class GraphAnalyticsPipeline:
    """Pipeline for graph analytics and insights"""
    
    @staticmethod
    def create_community_detection_pipeline() -> PipelineConfig:
        """Create pipeline for community detection in graph"""
        
        return PipelineConfig(
            name="graph_community_detection",
            description="Detect communities in collaboration network",
            source=SourceConfig(
                type="janusgraph",
                config={
                    "hosts": ["janusgraph:8182"],
                    "graph_name": "platformq",
                    "traversal": """
                    g.V().hasLabel('User')
                     .project('user', 'collaborators')
                     .by('user_id')
                     .by(out('collaborates_with').values('user_id').fold())
                    """
                }
            ),
            transforms=[
                TransformConfig(
                    type="graph_algorithm",
                    config={
                        "algorithm": "louvain",
                        "parameters": {
                            "resolution": 1.0,
                            "randomize": True
                        }
                    }
                ),
                TransformConfig(
                    type="community_metrics",
                    config={
                        "metrics": ["modularity", "conductance", "size_distribution"]
                    }
                ),
                TransformConfig(
                    type="enrichment",
                    config={
                        "enrichments": [
                            {
                                "field": "community_id",
                                "type": "lookup",
                                "source": "user_id",
                                "lookup_cache": "user_profiles"
                            }
                        ]
                    }
                )
            ],
            sink=SinkConfig(
                type="multi_sink",
                config={
                    "sinks": [
                        {
                            "type": "ignite",
                            "config": {
                                "hosts": ["ignite:10800"],
                                "cache_name": "user_communities",
                                "key_field": "user_id"
                            }
                        },
                        {
                            "type": "elasticsearch",
                            "config": {
                                "hosts": ["http://elasticsearch:9200"],
                                "index": "graph_communities",
                                "document_type": "_doc"
                            }
                        }
                    ]
                }
            ),
            execution_config={
                "parallelism": 4,
                "execution_mode": "batch",
                "schedule": "0 0 * * 0"  # Weekly
            }
        )


def register_lake_to_graph_pipelines(pipeline_registry):
    """Register all lake to graph pipelines"""
    
    pipelines = [
        LakeToGraphPipeline.create_asset_lineage_pipeline(),
        LakeToGraphPipeline.create_collaboration_network_pipeline(),
        LakeToGraphPipeline.create_semantic_relationship_pipeline(),
        LakeToGraphPipeline.create_workflow_dependency_pipeline(),
        LakeToGraphPipeline.create_ml_model_lineage_pipeline(),
        GraphAnalyticsPipeline.create_community_detection_pipeline()
    ]
    
    for pipeline in pipelines:
        pipeline_registry.register(pipeline.name, pipeline) 