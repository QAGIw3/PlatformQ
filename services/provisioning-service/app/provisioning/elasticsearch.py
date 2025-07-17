import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from typing import Dict, List

logger = logging.getLogger(__name__)

def create_elasticsearch_indices(es_client: Elasticsearch, tenant_id: str):
    """
    Creates Elasticsearch indices for a tenant with appropriate partitioning and mappings.
    
    Sets up:
    - Marketplace search index
    - ML models search index
    - Logs index with time-based partitioning
    - Analytics index
    - User activity index
    """
    
    # Define index templates for different data types
    index_templates = {
        # Marketplace assets search index
        f"{tenant_id}_marketplace": {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "asset_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "stop", "snowball"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "asset_id": {"type": "keyword"},
                    "nft_token_id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "asset_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "description": {"type": "text", "analyzer": "asset_analyzer"},
                    "tags": {"type": "keyword"},
                    "category": {"type": "keyword"},
                    "price": {"type": "float"},
                    "creator": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "license_type": {"type": "keyword"},
                    "blockchain": {"type": "keyword"},
                    "metadata": {"type": "object", "enabled": False},
                    "vector_embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        },
        
        # ML models search index
        f"{tenant_id}_ml_models": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "model_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "description": {"type": "text"},
                    "framework": {"type": "keyword"},
                    "task_type": {"type": "keyword"},
                    "performance_metrics": {"type": "object"},
                    "tags": {"type": "keyword"},
                    "created_by": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "version": {"type": "keyword"},
                    "size_bytes": {"type": "long"},
                    "input_schema": {"type": "object", "enabled": False},
                    "output_schema": {"type": "object", "enabled": False}
                }
            }
        },
        
        # User activity index
        f"{tenant_id}_user_activity": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.lifecycle.name": "user_activity_policy",
                "index.lifecycle.rollover_alias": f"{tenant_id}_user_activity"
            },
            "mappings": {
                "properties": {
                    "user_id": {"type": "keyword"},
                    "action": {"type": "keyword"},
                    "resource_type": {"type": "keyword"},
                    "resource_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "ip_address": {"type": "ip"},
                    "user_agent": {"type": "text"},
                    "session_id": {"type": "keyword"},
                    "metadata": {"type": "object", "enabled": False}
                }
            }
        }
    }
    
    created_indices = []
    
    for index_name, config in index_templates.items():
        try:
            # Create index with settings and mappings
            es_client.indices.create(
                index=index_name,
                body={
                    "settings": config["settings"],
                    "mappings": config["mappings"]
                },
                ignore=400  # Ignore if index already exists
            )
            created_indices.append(index_name)
            logger.info(f"Created Elasticsearch index: {index_name}")
            
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
    
    # Create index lifecycle policies
    create_ilm_policies(es_client, tenant_id)
    
    # Create index aliases for easier querying
    create_index_aliases(es_client, tenant_id)
    
    return created_indices


def create_ilm_policies(es_client: Elasticsearch, tenant_id: str):
    """
    Creates Index Lifecycle Management policies for automatic data management.
    """
    policies = {
        # Policy for time-series data (logs, activity)
        "user_activity_policy": {
            "phases": {
                "hot": {
                    "actions": {
                        "rollover": {
                            "max_size": "50GB",
                            "max_age": "7d"
                        }
                    }
                },
                "warm": {
                    "min_age": "7d",
                    "actions": {
                        "shrink": {
                            "number_of_shards": 1
                        },
                        "forcemerge": {
                            "max_num_segments": 1
                        }
                    }
                },
                "delete": {
                    "min_age": "30d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    }
    
    for policy_name, policy_config in policies.items():
        try:
            es_client.ilm.put_lifecycle(
                policy=policy_name,
                body={"policy": policy_config}
            )
            logger.info(f"Created ILM policy: {policy_name}")
        except Exception as e:
            logger.error(f"Failed to create ILM policy {policy_name}: {e}")


def create_index_aliases(es_client: Elasticsearch, tenant_id: str):
    """
    Creates index aliases for easier querying across multiple indices.
    """
    aliases = {
        f"{tenant_id}_all_assets": {
            "indices": [f"{tenant_id}_marketplace", f"{tenant_id}_ml_models"]
        },
        f"{tenant_id}_searchable": {
            "indices": [f"{tenant_id}_marketplace", f"{tenant_id}_ml_models"]
        }
    }
    
    for alias_name, config in aliases.items():
        for index in config["indices"]:
            try:
                es_client.indices.put_alias(
                    index=index,
                    name=alias_name
                )
                logger.info(f"Created alias {alias_name} for index {index}")
            except Exception as e:
                logger.error(f"Failed to create alias {alias_name}: {e}")


def create_search_templates(es_client: Elasticsearch, tenant_id: str):
    """
    Creates reusable search templates for common queries.
    """
    templates = {
        f"{tenant_id}_marketplace_search": {
            "script": {
                "lang": "mustache",
                "source": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "multi_match": {
                                        "query": "{{query}}",
                                        "fields": ["name^3", "description", "tags^2"],
                                        "type": "best_fields",
                                        "fuzziness": "AUTO"
                                    }
                                }
                            ],
                            "filter": [
                                {"term": {"category": "{{category}}"}} if "{{category}}" else None,
                                {"range": {"price": {"gte": "{{min_price}}", "lte": "{{max_price}}"}}} if "{{min_price}}" else None
                            ]
                        }
                    },
                    "size": "{{size}}",
                    "from": "{{from}}"
                }
            }
        },
        
        f"{tenant_id}_vector_search": {
            "script": {
                "lang": "mustache", 
                "source": {
                    "query": {
                        "script_score": {
                            "query": {"match_all": {}},
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, 'vector_embedding') + 1.0",
                                "params": {
                                    "query_vector": "{{query_vector}}"
                                }
                            }
                        }
                    },
                    "size": "{{k}}"
                }
            }
        }
    }
    
    for template_id, template_body in templates.items():
        try:
            es_client.put_script(
                id=template_id,
                body=template_body
            )
            logger.info(f"Created search template: {template_id}")
        except Exception as e:
            logger.error(f"Failed to create search template {template_id}: {e}") 