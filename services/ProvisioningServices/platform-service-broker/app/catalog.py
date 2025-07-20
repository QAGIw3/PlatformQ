"""
Platform Q Service Catalog Definitions
Defines all services available through the Open Service Broker API
"""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

class ServiceMetadata(BaseModel):
    """Service metadata model."""
    displayName: str
    imageUrl: str = ""
    longDescription: str = ""
    providerDisplayName: str = "Platform Q"
    documentationUrl: str = ""
    supportUrl: str = ""

class PlanMetadata(BaseModel):
    """Plan metadata model."""
    displayName: str
    bullets: List[str] = []
    costs: List[Dict[str, Any]] = []

class ServicePlan(BaseModel):
    """Service plan model."""
    id: str
    name: str
    description: str
    metadata: PlanMetadata
    free: bool = False
    bindable: bool = True
    schemas: Dict[str, Any] = Field(default_factory=dict)

class Service(BaseModel):
    """Service model."""
    id: str
    name: str
    description: str
    tags: List[str] = []
    requires: List[str] = []
    bindable: bool = True
    metadata: ServiceMetadata
    dashboard_client: Dict[str, Any] = Field(default_factory=dict)
    plan_updateable: bool = True
    plans: List[ServicePlan]

# Platform Q Service Catalog
PLATFORM_Q_CATALOG = {
    "services": [
        {
            "id": "cassandra-service",
            "name": "cassandra",
            "description": "Apache Cassandra distributed database service",
            "tags": ["nosql", "database", "cassandra"],
            "bindable": True,
            "metadata": {
                "displayName": "Apache Cassandra",
                "imageUrl": "https://cassandra.apache.org/_/img/cassandra_logo.png",
                "longDescription": "Apache Cassandra is a highly scalable, distributed NoSQL database designed to handle large amounts of data across many commodity servers.",
                "documentationUrl": "https://cassandra.apache.org/doc/latest/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "cassandra-starter",
                    "name": "starter",
                    "description": "Starter plan with 3 nodes and 1TB storage",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "3 nodes",
                            "1TB total storage",
                            "Replication factor 2",
                            "99.9% SLA"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 500.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "keyspace_name": {
                                            "type": "string",
                                            "pattern": "^[a-z][a-z0-9_]*$"
                                        },
                                        "replication_factor": {
                                            "type": "integer",
                                            "minimum": 1,
                                            "maximum": 3,
                                            "default": 2
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "cassandra-standard",
                    "name": "standard",
                    "description": "Standard plan with 5 nodes and 5TB storage",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "5 nodes",
                            "5TB total storage",
                            "Replication factor 3",
                            "99.95% SLA",
                            "24/7 support"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1500.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "cassandra-premium",
                    "name": "premium",
                    "description": "Premium plan with 10+ nodes and unlimited storage",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "10+ nodes (auto-scaling)",
                            "Unlimited storage",
                            "Replication factor 3-5",
                            "99.99% SLA",
                            "24/7 premium support",
                            "Cross-region replication"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 5000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "ignite-service",
            "name": "ignite",
            "description": "Apache Ignite in-memory computing platform",
            "tags": ["cache", "in-memory", "ignite", "compute-grid"],
            "bindable": True,
            "metadata": {
                "displayName": "Apache Ignite",
                "imageUrl": "https://ignite.apache.org/images/logo.png",
                "longDescription": "Apache Ignite is a distributed in-memory computing platform for transactional, analytical, and streaming workloads.",
                "documentationUrl": "https://ignite.apache.org/docs/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "ignite-starter",
                    "name": "starter",
                    "description": "Starter plan with 8GB memory and 2 nodes",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "2 nodes",
                            "8GB memory",
                            "1 backup copy",
                            "SQL support"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 300.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "cache_name": {
                                            "type": "string",
                                            "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$"
                                        },
                                        "max_memory_mb": {
                                            "type": "integer",
                                            "minimum": 1024,
                                            "maximum": 8192,
                                            "default": 4096
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "ignite-standard",
                    "name": "standard",
                    "description": "Standard plan with 32GB memory and 4 nodes",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "4 nodes",
                            "32GB memory",
                            "2 backup copies",
                            "SQL and compute grid",
                            "Persistence enabled"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "ignite-premium",
                    "name": "premium",
                    "description": "Premium plan with 128GB+ memory and auto-scaling",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "Auto-scaling nodes",
                            "128GB+ memory",
                            "3 backup copies",
                            "Full feature set",
                            "Machine learning support",
                            "Cross-region deployment"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 3000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "pulsar-service",
            "name": "pulsar",
            "description": "Apache Pulsar distributed messaging and streaming",
            "tags": ["messaging", "streaming", "pulsar", "pubsub"],
            "bindable": True,
            "metadata": {
                "displayName": "Apache Pulsar",
                "imageUrl": "https://pulsar.apache.org/img/logo.svg",
                "longDescription": "Apache Pulsar is a cloud-native, distributed messaging and streaming platform.",
                "documentationUrl": "https://pulsar.apache.org/docs/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "pulsar-starter",
                    "name": "starter",
                    "description": "Starter plan with 100GB storage and 1000 msg/s",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "100GB storage",
                            "1000 messages/second",
                            "7 days retention",
                            "Single region"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 200.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "namespace": {
                                            "type": "string",
                                            "default": "default"
                                        },
                                        "retention_hours": {
                                            "type": "integer",
                                            "minimum": 24,
                                            "maximum": 168,
                                            "default": 72
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "pulsar-standard",
                    "name": "standard",
                    "description": "Standard plan with 1TB storage and 10K msg/s",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "1TB storage",
                            "10,000 messages/second",
                            "30 days retention",
                            "Geo-replication ready",
                            "Functions support"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 800.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "pulsar-premium",
                    "name": "premium",
                    "description": "Premium plan with unlimited storage and throughput",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "Unlimited storage",
                            "Unlimited throughput",
                            "Custom retention",
                            "Multi-region geo-replication",
                            "Pulsar Functions",
                            "Pulsar SQL",
                            "Tiered storage"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 2500.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "minio-service",
            "name": "minio",
            "description": "MinIO S3-compatible object storage",
            "tags": ["storage", "object-storage", "s3", "minio"],
            "bindable": True,
            "metadata": {
                "displayName": "MinIO Object Storage",
                "imageUrl": "https://min.io/resources/img/logo/MINIO_wordmark.png",
                "longDescription": "MinIO is a high performance, S3 compatible object storage system.",
                "documentationUrl": "https://docs.min.io/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "minio-starter",
                    "name": "starter",
                    "description": "Starter plan with 100GB storage",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "100GB storage",
                            "S3 compatible API",
                            "Versioning support",
                            "99.9% durability"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 50.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "bucket_name": {
                                            "type": "string",
                                            "pattern": "^[a-z0-9][a-z0-9-]*[a-z0-9]$"
                                        },
                                        "storage_quota_gb": {
                                            "type": "integer",
                                            "minimum": 10,
                                            "maximum": 100,
                                            "default": 100
                                        },
                                        "enable_versioning": {
                                            "type": "boolean",
                                            "default": True
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "minio-standard",
                    "name": "standard",
                    "description": "Standard plan with 1TB storage",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "1TB storage",
                            "S3 compatible API",
                            "Versioning and lifecycle",
                            "99.99% durability",
                            "Encryption at rest"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 200.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "minio-premium",
                    "name": "premium",
                    "description": "Premium plan with unlimited storage",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "Unlimited storage",
                            "S3 compatible API",
                            "Advanced features",
                            "99.999% durability",
                            "Geo-replication",
                            "Tiered storage",
                            "Advanced encryption"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "elasticsearch-service",
            "name": "elasticsearch",
            "description": "Elasticsearch search and analytics engine",
            "tags": ["search", "analytics", "elasticsearch", "logging"],
            "bindable": True,
            "metadata": {
                "displayName": "Elasticsearch",
                "imageUrl": "https://www.elastic.co/static/images/elastic-logo-200.png",
                "longDescription": "Elasticsearch is a distributed, RESTful search and analytics engine capable of addressing a growing number of use cases.",
                "documentationUrl": "https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "elasticsearch-starter",
                    "name": "starter",
                    "description": "Starter plan with 3 nodes and 100GB storage",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "3 nodes",
                            "100GB storage",
                            "Basic features",
                            "7 days retention"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 300.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "index_prefix": {
                                            "type": "string",
                                            "pattern": "^[a-z][a-z0-9-]*$"
                                        },
                                        "app_shards": {
                                            "type": "integer",
                                            "minimum": 1,
                                            "maximum": 5,
                                            "default": 3
                                        },
                                        "app_replicas": {
                                            "type": "integer",
                                            "minimum": 0,
                                            "maximum": 2,
                                            "default": 1
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "elasticsearch-standard",
                    "name": "standard",
                    "description": "Standard plan with 5 nodes and 1TB storage",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "5 nodes",
                            "1TB storage",
                            "Advanced features",
                            "30 days retention",
                            "Machine learning"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "elasticsearch-premium",
                    "name": "premium",
                    "description": "Premium plan with auto-scaling and unlimited storage",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "Auto-scaling nodes",
                            "Unlimited storage",
                            "All features",
                            "Custom retention",
                            "Advanced ML",
                            "Cross-cluster search",
                            "Dedicated master nodes"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 3000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "janusgraph-service",
            "name": "janusgraph",
            "description": "JanusGraph distributed graph database",
            "tags": ["graph", "database", "janusgraph", "gremlin"],
            "bindable": True,
            "metadata": {
                "displayName": "JanusGraph",
                "imageUrl": "https://janusgraph.org/img/janusgraph.png",
                "longDescription": "JanusGraph is a scalable graph database optimized for storing and querying graphs containing hundreds of billions of vertices and edges.",
                "documentationUrl": "https://docs.janusgraph.org/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "janusgraph-starter",
                    "name": "starter",
                    "description": "Starter plan with basic graph capabilities",
                    "metadata": {
                        "displayName": "Starter",
                        "bullets": [
                            "Single graph instance",
                            "Cassandra backend",
                            "Elasticsearch indexing",
                            "10M vertices/edges"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 400.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True,
                    "schemas": {
                        "service_instance": {
                            "create": {
                                "parameters": {
                                    "$schema": "http://json-schema.org/draft-07/schema",
                                    "type": "object",
                                    "properties": {
                                        "graph_name": {
                                            "type": "string",
                                            "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$"
                                        },
                                        "storage_backend": {
                                            "type": "string",
                                            "enum": ["cassandra"],
                                            "default": "cassandra"
                                        },
                                        "index_backend": {
                                            "type": "string",
                                            "enum": ["elasticsearch"],
                                            "default": "elasticsearch"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "id": "janusgraph-standard",
                    "name": "standard",
                    "description": "Standard plan with enhanced graph capabilities",
                    "metadata": {
                        "displayName": "Standard",
                        "bullets": [
                            "Multiple graphs",
                            "Cassandra backend",
                            "Elasticsearch indexing",
                            "100M vertices/edges",
                            "Advanced queries"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1200.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "janusgraph-premium",
                    "name": "premium",
                    "description": "Premium plan with unlimited graph capabilities",
                    "metadata": {
                        "displayName": "Premium",
                        "bullets": [
                            "Unlimited graphs",
                            "Choice of backends",
                            "Advanced indexing",
                            "Unlimited vertices/edges",
                            "OLAP support",
                            "Graph analytics",
                            "Custom schema"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 3500.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        },
        {
            "id": "platformq-bundle",
            "name": "platformq-complete",
            "description": "Complete Platform Q service bundle",
            "tags": ["bundle", "complete", "all-services"],
            "bindable": True,
            "metadata": {
                "displayName": "Platform Q Complete Bundle",
                "imageUrl": "https://platformq.io/logo.png",
                "longDescription": "Complete Platform Q bundle including all services: Cassandra, Ignite, Pulsar, MinIO, Elasticsearch, and JanusGraph.",
                "documentationUrl": "https://docs.platformq.io/",
                "supportUrl": "https://platformq.io/support"
            },
            "plan_updateable": True,
            "plans": [
                {
                    "id": "bundle-starter",
                    "name": "starter",
                    "description": "Starter bundle with all services",
                    "metadata": {
                        "displayName": "Starter Bundle",
                        "bullets": [
                            "All Platform Q services",
                            "Starter tier for each",
                            "Integrated provisioning",
                            "Single billing",
                            "Basic support"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 1500.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "bundle-standard",
                    "name": "standard",
                    "description": "Standard bundle with all services",
                    "metadata": {
                        "displayName": "Standard Bundle",
                        "bullets": [
                            "All Platform Q services",
                            "Standard tier for each",
                            "Integrated provisioning",
                            "Single billing",
                            "24/7 support",
                            "20% discount"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 5000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                },
                {
                    "id": "bundle-premium",
                    "name": "premium",
                    "description": "Premium bundle with all services",
                    "metadata": {
                        "displayName": "Premium Bundle",
                        "bullets": [
                            "All Platform Q services",
                            "Premium tier for each",
                            "Integrated provisioning",
                            "Single billing",
                            "24/7 premium support",
                            "30% discount",
                            "Custom configuration"
                        ],
                        "costs": [
                            {
                                "amount": {"usd": 12000.0},
                                "unit": "MONTHLY"
                            }
                        ]
                    },
                    "free": False,
                    "bindable": True
                }
            ]
        }
    ]
}

def get_catalog() -> Dict[str, Any]:
    """Get the complete service catalog."""
    return PLATFORM_Q_CATALOG

def get_service(service_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific service by ID."""
    for service in PLATFORM_Q_CATALOG["services"]:
        if service["id"] == service_id:
            return service
    return None

def get_plan(service_id: str, plan_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific plan by service and plan ID."""
    service = get_service(service_id)
    if service:
        for plan in service["plans"]:
            if plan["id"] == plan_id:
                return plan
    return None 