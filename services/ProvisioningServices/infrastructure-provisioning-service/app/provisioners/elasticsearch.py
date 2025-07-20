"""
Elasticsearch Provisioner

Provisions Elasticsearch indices and configurations for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

from elasticsearch import Elasticsearch

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class ElasticsearchProvisioner(IResourceProvisioner):
    """Provisions Elasticsearch resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.es_client = None
    
    async def initialize(self):
        """Initialize Elasticsearch connection"""
        try:
            config = self.settings.get_elasticsearch_connection_dict()
            self.es_client = Elasticsearch(**config)
            
            # Test connection
            if not self.es_client.ping():
                raise Exception("Failed to connect to Elasticsearch")
            
            logger.info("Elasticsearch provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Elasticsearch connection"""
        if self.es_client:
            self.es_client.close()
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Elasticsearch indices for tenant"""
        index_prefix = f"tenant-{tenant_id}"
        
        try:
            # Create indices
            await self._create_indices(index_prefix, metadata)
            
            # Create ILM policies
            await self._create_ilm_policies(index_prefix)
            
            # Create index templates
            await self._create_index_templates(index_prefix)
            
            # Create aliases
            await self._create_index_aliases(index_prefix)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.ELASTICSEARCH,
                resource_name=index_prefix,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=",".join(self.settings.elasticsearch_hosts),
                configuration={
                    "index_prefix": index_prefix,
                    "shards": self.settings.default_elasticsearch_shards,
                    "replicas": self.settings.default_elasticsearch_replicas,
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned Elasticsearch for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Elasticsearch for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Elasticsearch indices"""
        try:
            # Delete all indices with the prefix
            indices = self.es_client.indices.get(f"{resource_name}-*")
            for index in indices:
                self.es_client.indices.delete(index=index)
                logger.info(f"Deleted index: {index}")
            
            # Delete ILM policies
            policies = [
                f"{resource_name}-hot-warm-delete",
                f"{resource_name}-logs-policy"
            ]
            for policy in policies:
                try:
                    self.es_client.ilm.delete_lifecycle(policy=policy)
                    logger.info(f"Deleted ILM policy: {policy}")
                except:
                    pass
            
            # Delete index templates
            templates = [
                f"{resource_name}-logs",
                f"{resource_name}-metrics",
                f"{resource_name}-events"
            ]
            for template in templates:
                try:
                    self.es_client.indices.delete_template(name=template)
                    logger.info(f"Deleted template: {template}")
                except:
                    pass
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Elasticsearch indices {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Elasticsearch provisioning"""
        index_prefix = f"tenant-{tenant_id}"
        
        try:
            # Check if at least one index exists
            indices = self.es_client.indices.get(f"{index_prefix}-*")
            return len(indices) > 0
            
        except Exception as e:
            logger.error(f"Failed to validate Elasticsearch for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.ELASTICSEARCH
    
    async def _create_indices(self, index_prefix: str, metadata: Dict[str, Any]):
        """Create default indices for the tenant"""
        shards = metadata.get('shards', self.settings.default_elasticsearch_shards)
        replicas = metadata.get('replicas', self.settings.default_elasticsearch_replicas)
        
        indices = {
            f"{index_prefix}-logs": {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas,
                    "index.lifecycle.name": f"{index_prefix}-logs-policy",
                    "index.lifecycle.rollover_alias": f"{index_prefix}-logs-write"
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "level": {"type": "keyword"},
                        "service": {"type": "keyword"},
                        "message": {"type": "text"},
                        "trace_id": {"type": "keyword"},
                        "span_id": {"type": "keyword"}
                    }
                }
            },
            f"{index_prefix}-metrics": {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "metric_name": {"type": "keyword"},
                        "value": {"type": "double"},
                        "tags": {"type": "object"}
                    }
                }
            },
            f"{index_prefix}-events": {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "event_type": {"type": "keyword"},
                        "entity_id": {"type": "keyword"},
                        "data": {"type": "object"}
                    }
                }
            }
        }
        
        for index_name, index_config in indices.items():
            self.es_client.indices.create(
                index=f"{index_name}-000001",
                body=index_config,
                ignore=400  # Ignore if already exists
            )
            logger.info(f"Created index: {index_name}-000001")
    
    async def _create_ilm_policies(self, index_prefix: str):
        """Create ILM policies for the tenant"""
        policies = {
            f"{index_prefix}-hot-warm-delete": {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
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
                            "min_age": "90d",
                            "actions": {
                                "delete": {}
                            }
                        }
                    }
                }
            },
            f"{index_prefix}-logs-policy": {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "rollover": {
                                    "max_size": "10GB",
                                    "max_age": "1d"
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
        }
        
        for policy_name, policy_config in policies.items():
            self.es_client.ilm.put_lifecycle(
                policy=policy_name,
                body=policy_config
            )
            logger.info(f"Created ILM policy: {policy_name}")
    
    async def _create_index_templates(self, index_prefix: str):
        """Create index templates for the tenant"""
        templates = {
            f"{index_prefix}-logs": {
                "index_patterns": [f"{index_prefix}-logs-*"],
                "settings": {
                    "number_of_shards": self.settings.default_elasticsearch_shards,
                    "number_of_replicas": self.settings.default_elasticsearch_replicas,
                    "index.lifecycle.name": f"{index_prefix}-logs-policy",
                    "index.lifecycle.rollover_alias": f"{index_prefix}-logs-write"
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "level": {"type": "keyword"},
                        "service": {"type": "keyword"},
                        "message": {"type": "text"},
                        "trace_id": {"type": "keyword"},
                        "span_id": {"type": "keyword"}
                    }
                }
            },
            f"{index_prefix}-metrics": {
                "index_patterns": [f"{index_prefix}-metrics-*"],
                "settings": {
                    "number_of_shards": self.settings.default_elasticsearch_shards,
                    "number_of_replicas": self.settings.default_elasticsearch_replicas
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "metric_name": {"type": "keyword"},
                        "value": {"type": "double"},
                        "tags": {"type": "object"}
                    }
                }
            }
        }
        
        for template_name, template_config in templates.items():
            self.es_client.indices.put_template(
                name=template_name,
                body=template_config
            )
            logger.info(f"Created index template: {template_name}")
    
    async def _create_index_aliases(self, index_prefix: str):
        """Create index aliases for the tenant"""
        aliases = {
            f"{index_prefix}-logs-write": f"{index_prefix}-logs-000001",
            f"{index_prefix}-logs-read": f"{index_prefix}-logs-*",
            f"{index_prefix}-metrics-write": f"{index_prefix}-metrics-000001",
            f"{index_prefix}-metrics-read": f"{index_prefix}-metrics-*",
            f"{index_prefix}-events-write": f"{index_prefix}-events-000001",
            f"{index_prefix}-events-read": f"{index_prefix}-events-*"
        }
        
        for alias_name, index_pattern in aliases.items():
            if alias_name.endswith("-write"):
                self.es_client.indices.put_alias(
                    index=index_pattern,
                    name=alias_name,
                    body={
                        "is_write_index": True
                    }
                )
            else:
                # Read aliases are created via templates
                pass
            
            logger.info(f"Created alias: {alias_name} -> {index_pattern}") 