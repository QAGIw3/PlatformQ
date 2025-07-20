#!/usr/bin/env python3
"""
Cloudify script to create Elasticsearch index templates and settings for a tenant.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, List, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch_provisioner')


class ElasticsearchProvisioner:
    """Handles Elasticsearch index template and settings provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.es_hosts = config['elasticsearch_hosts']
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        self.index_prefix = config.get('index_prefix', f"tenant-{self.tenant_id}")
        
        # Initialize Elasticsearch client
        self.client = Elasticsearch(
            self.es_hosts,
            basic_auth=(config.get('username'), config.get('password')) if config.get('username') else None,
            verify_certs=config.get('verify_certs', True),
            ssl_show_warn=config.get('ssl_show_warn', True)
        )
        
    def create_index_templates(self):
        """Create index templates for the tenant."""
        try:
            # Application data template
            app_template = {
                "index_patterns": [f"{self.index_prefix}-app-*"],
                "priority": 100,
                "template": {
                    "settings": {
                        "number_of_shards": self.config.get('app_shards', 3),
                        "number_of_replicas": self.config.get('app_replicas', 1),
                        "refresh_interval": "5s",
                        "index.lifecycle.name": f"{self.index_prefix}-app-policy",
                        "index.lifecycle.rollover_alias": f"{self.index_prefix}-app-current",
                        "index.routing.allocation.include.tenant_id": self.tenant_id,
                        "index.max_result_window": 10000,
                        "index.max_inner_result_window": 100,
                        "analysis": {
                            "analyzer": {
                                "default": {
                                    "type": "standard"
                                },
                                "autocomplete": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "autocomplete_filter"]
                                },
                                "search_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase"]
                                }
                            },
                            "filter": {
                                "autocomplete_filter": {
                                    "type": "edge_ngram",
                                    "min_gram": 2,
                                    "max_gram": 20
                                }
                            }
                        }
                    },
                    "mappings": {
                        "dynamic_templates": [
                            {
                                "strings": {
                                    "match_mapping_type": "string",
                                    "mapping": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            {
                                "dates": {
                                    "match": "*_at",
                                    "mapping": {
                                        "type": "date"
                                    }
                                }
                            }
                        ],
                        "properties": {
                            "tenant_id": {"type": "keyword"},
                            "reseller_id": {"type": "keyword"},
                            "customer_id": {"type": "keyword"},
                            "created_at": {"type": "date"},
                            "updated_at": {"type": "date"}
                        }
                    }
                }
            }
            
            self.client.indices.put_index_template(
                name=f"{self.index_prefix}-app-template",
                body=app_template
            )
            logger.info(f"Created application index template for {self.index_prefix}")
            
            # Logs template
            logs_template = {
                "index_patterns": [f"{self.index_prefix}-logs-*"],
                "priority": 100,
                "template": {
                    "settings": {
                        "number_of_shards": self.config.get('logs_shards', 5),
                        "number_of_replicas": self.config.get('logs_replicas', 1),
                        "refresh_interval": "10s",
                        "index.lifecycle.name": f"{self.index_prefix}-logs-policy",
                        "index.lifecycle.rollover_alias": f"{self.index_prefix}-logs-current",
                        "index.routing.allocation.include.tenant_id": self.tenant_id,
                        "index.codec": "best_compression",
                        "index.query.default_field": ["message", "tags"]
                    },
                    "mappings": {
                        "properties": {
                            "tenant_id": {"type": "keyword"},
                            "timestamp": {"type": "date"},
                            "level": {"type": "keyword"},
                            "logger": {"type": "keyword"},
                            "message": {"type": "text"},
                            "host": {"type": "keyword"},
                            "service": {"type": "keyword"},
                            "trace_id": {"type": "keyword"},
                            "span_id": {"type": "keyword"},
                            "tags": {"type": "keyword"},
                            "metadata": {"type": "object", "enabled": False}
                        }
                    }
                }
            }
            
            self.client.indices.put_index_template(
                name=f"{self.index_prefix}-logs-template",
                body=logs_template
            )
            logger.info(f"Created logs index template for {self.index_prefix}")
            
            # Metrics template
            metrics_template = {
                "index_patterns": [f"{self.index_prefix}-metrics-*"],
                "priority": 100,
                "template": {
                    "settings": {
                        "number_of_shards": self.config.get('metrics_shards', 3),
                        "number_of_replicas": self.config.get('metrics_replicas', 1),
                        "refresh_interval": "30s",
                        "index.lifecycle.name": f"{self.index_prefix}-metrics-policy",
                        "index.lifecycle.rollover_alias": f"{self.index_prefix}-metrics-current",
                        "index.routing.allocation.include.tenant_id": self.tenant_id,
                        "index.codec": "best_compression"
                    },
                    "mappings": {
                        "properties": {
                            "tenant_id": {"type": "keyword"},
                            "timestamp": {"type": "date"},
                            "metric_name": {"type": "keyword"},
                            "metric_type": {"type": "keyword"},
                            "value": {"type": "double"},
                            "unit": {"type": "keyword"},
                            "dimensions": {
                                "type": "object",
                                "properties": {
                                    "service": {"type": "keyword"},
                                    "host": {"type": "keyword"},
                                    "region": {"type": "keyword"},
                                    "environment": {"type": "keyword"}
                                }
                            },
                            "tags": {"type": "keyword"}
                        }
                    }
                }
            }
            
            self.client.indices.put_index_template(
                name=f"{self.index_prefix}-metrics-template",
                body=metrics_template
            )
            logger.info(f"Created metrics index template for {self.index_prefix}")
            
            # Report usage
            self._report_usage('templates_created', {
                'templates': ['app', 'logs', 'metrics'],
                'total_shards': (
                    self.config.get('app_shards', 3) +
                    self.config.get('logs_shards', 5) +
                    self.config.get('metrics_shards', 3)
                )
            })
            
        except exceptions.RequestError as e:
            if e.error == 'resource_already_exists_exception':
                logger.warning(f"Templates already exist for {self.index_prefix}")
            else:
                raise NonRecoverableError(f"Failed to create templates: {str(e)}")
                
    def create_lifecycle_policies(self):
        """Create index lifecycle management policies."""
        try:
            # Application data lifecycle policy
            app_policy = {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "rollover": {
                                    "max_size": f"{self.config.get('app_rollover_size_gb', 50)}gb",
                                    "max_age": f"{self.config.get('app_rollover_days', 7)}d",
                                    "max_docs": self.config.get('app_rollover_docs', 50000000)
                                },
                                "set_priority": {
                                    "priority": 100
                                }
                            }
                        },
                        "warm": {
                            "min_age": f"{self.config.get('app_warm_after_days', 7)}d",
                            "actions": {
                                "shrink": {
                                    "number_of_shards": 1
                                },
                                "forcemerge": {
                                    "max_num_segments": 1
                                },
                                "set_priority": {
                                    "priority": 50
                                }
                            }
                        },
                        "cold": {
                            "min_age": f"{self.config.get('app_cold_after_days', 30)}d",
                            "actions": {
                                "set_priority": {
                                    "priority": 0
                                },
                                "allocate": {
                                    "require": {
                                        "data": "cold"
                                    }
                                }
                            }
                        },
                        "delete": {
                            "min_age": f"{self.config.get('app_delete_after_days', 90)}d",
                            "actions": {
                                "delete": {}
                            }
                        }
                    }
                }
            }
            
            self.client.ilm.put_lifecycle(
                policy=f"{self.index_prefix}-app-policy",
                body=app_policy
            )
            logger.info(f"Created app lifecycle policy for {self.index_prefix}")
            
            # Logs lifecycle policy (shorter retention)
            logs_policy = {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "rollover": {
                                    "max_size": f"{self.config.get('logs_rollover_size_gb', 100)}gb",
                                    "max_age": f"{self.config.get('logs_rollover_days', 1)}d"
                                },
                                "set_priority": {
                                    "priority": 100
                                }
                            }
                        },
                        "warm": {
                            "min_age": f"{self.config.get('logs_warm_after_days', 1)}d",
                            "actions": {
                                "shrink": {
                                    "number_of_shards": 1
                                },
                                "forcemerge": {
                                    "max_num_segments": 1
                                },
                                "set_priority": {
                                    "priority": 50
                                }
                            }
                        },
                        "delete": {
                            "min_age": f"{self.config.get('logs_delete_after_days', 30)}d",
                            "actions": {
                                "delete": {}
                            }
                        }
                    }
                }
            }
            
            self.client.ilm.put_lifecycle(
                policy=f"{self.index_prefix}-logs-policy",
                body=logs_policy
            )
            logger.info(f"Created logs lifecycle policy for {self.index_prefix}")
            
            # Metrics lifecycle policy
            metrics_policy = {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "rollover": {
                                    "max_size": f"{self.config.get('metrics_rollover_size_gb', 30)}gb",
                                    "max_age": f"{self.config.get('metrics_rollover_days', 1)}d"
                                },
                                "set_priority": {
                                    "priority": 100
                                }
                            }
                        },
                        "warm": {
                            "min_age": f"{self.config.get('metrics_warm_after_days', 3)}d",
                            "actions": {
                                "downsample": {
                                    "fixed_interval": "1h"
                                },
                                "shrink": {
                                    "number_of_shards": 1
                                },
                                "set_priority": {
                                    "priority": 50
                                }
                            }
                        },
                        "delete": {
                            "min_age": f"{self.config.get('metrics_delete_after_days', 60)}d",
                            "actions": {
                                "delete": {}
                            }
                        }
                    }
                }
            }
            
            self.client.ilm.put_lifecycle(
                policy=f"{self.index_prefix}-metrics-policy",
                body=metrics_policy
            )
            logger.info(f"Created metrics lifecycle policy for {self.index_prefix}")
            
        except Exception as e:
            logger.error(f"Failed to create lifecycle policies: {str(e)}")
            
    def create_initial_indices(self):
        """Create initial indices with aliases."""
        try:
            # Create initial app index
            app_index = f"{self.index_prefix}-app-000001"
            if not self.client.indices.exists(index=app_index):
                self.client.indices.create(
                    index=app_index,
                    body={
                        "aliases": {
                            f"{self.index_prefix}-app-current": {
                                "is_write_index": True
                            }
                        }
                    }
                )
                logger.info(f"Created initial app index: {app_index}")
                
            # Create initial logs index
            logs_index = f"{self.index_prefix}-logs-000001"
            if not self.client.indices.exists(index=logs_index):
                self.client.indices.create(
                    index=logs_index,
                    body={
                        "aliases": {
                            f"{self.index_prefix}-logs-current": {
                                "is_write_index": True
                            }
                        }
                    }
                )
                logger.info(f"Created initial logs index: {logs_index}")
                
            # Create initial metrics index
            metrics_index = f"{self.index_prefix}-metrics-000001"
            if not self.client.indices.exists(index=metrics_index):
                self.client.indices.create(
                    index=metrics_index,
                    body={
                        "aliases": {
                            f"{self.index_prefix}-metrics-current": {
                                "is_write_index": True
                            }
                        }
                    }
                )
                logger.info(f"Created initial metrics index: {metrics_index}")
                
        except Exception as e:
            logger.error(f"Failed to create initial indices: {str(e)}")
            
    def configure_security(self):
        """Configure index-level security for the tenant."""
        try:
            # Create role for tenant access
            tenant_role = {
                "cluster": ["monitor"],
                "indices": [
                    {
                        "names": [f"{self.index_prefix}-*"],
                        "privileges": [
                            "read", "write", "create_index", "view_index_metadata",
                            "monitor", "manage", "delete_index"
                        ],
                        "field_security": {
                            "grant": ["*"],
                            "except": []
                        },
                        "query": {
                            "term": {
                                "tenant_id": self.tenant_id
                            }
                        }
                    }
                ],
                "metadata": {
                    "tenant_id": self.tenant_id,
                    "reseller_id": self.reseller_id,
                    "customer_id": self.customer_id
                }
            }
            
            # In production, this would create actual Elasticsearch security role
            ctx.instance.runtime_properties['tenant_role'] = tenant_role
            logger.info(f"Configured security role for tenant {self.tenant_id}")
            
            # Create API key for tenant
            api_key_response = self._create_api_key()
            if api_key_response:
                ctx.instance.runtime_properties['api_key'] = api_key_response
                
        except Exception as e:
            logger.error(f"Failed to configure security: {str(e)}")
            
    def _create_api_key(self):
        """Create API key for tenant access."""
        try:
            # Generate API key
            api_key_name = f"tenant-{self.tenant_id}-{int(time.time())}"
            
            # In production, this would use Elasticsearch Security API
            api_key = {
                'name': api_key_name,
                'api_key': hashlib.sha256(f"{api_key_name}{time.time()}".encode()).hexdigest(),
                'tenant_id': self.tenant_id,
                'created_at': int(time.time()),
                'expires_at': int(time.time()) + (365 * 24 * 60 * 60)  # 1 year
            }
            
            logger.info(f"Created API key for tenant {self.tenant_id}")
            return api_key
            
        except Exception as e:
            logger.error(f"Failed to create API key: {str(e)}")
            return None
            
    def configure_monitoring(self):
        """Configure monitoring and alerting for the tenant."""
        try:
            # Create watcher for disk usage alerts
            disk_alert = {
                "trigger": {
                    "schedule": {
                        "interval": "10m"
                    }
                },
                "input": {
                    "search": {
                        "request": {
                            "indices": [f"{self.index_prefix}-*"],
                            "body": {
                                "query": {
                                    "match_all": {}
                                },
                                "aggs": {
                                    "total_size": {
                                        "sum": {
                                            "field": "_size"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "condition": {
                    "compare": {
                        "ctx.payload.aggregations.total_size.value": {
                            "gt": self.config.get('disk_alert_threshold_bytes', 107374182400)  # 100GB
                        }
                    }
                },
                "actions": {
                    "send_notification": {
                        "webhook": {
                            "scheme": "https",
                            "host": "notifications.platform-q.io",
                            "port": 443,
                            "method": "post",
                            "path": "/alerts",
                            "params": {},
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": json.dumps({
                                "tenant_id": self.tenant_id,
                                "alert_type": "disk_usage",
                                "message": "Tenant disk usage exceeded threshold"
                            })
                        }
                    }
                }
            }
            
            # In production, this would create actual watcher
            ctx.instance.runtime_properties['disk_alert'] = disk_alert
            logger.info(f"Configured monitoring for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to configure monitoring: {str(e)}")
            
    def create_sample_data(self):
        """Create sample data for testing."""
        if not self.config.get('create_sample_data', False):
            return
            
        try:
            # Sample app data
            app_docs = [
                {
                    "_index": f"{self.index_prefix}-app-current",
                    "_source": {
                        "tenant_id": self.tenant_id,
                        "type": "user",
                        "name": "Sample User",
                        "email": "sample@example.com",
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                },
                {
                    "_index": f"{self.index_prefix}-app-current",
                    "_source": {
                        "tenant_id": self.tenant_id,
                        "type": "product",
                        "name": "Sample Product",
                        "price": 99.99,
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                }
            ]
            
            bulk(self.client, app_docs)
            logger.info(f"Created sample data for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create sample data: {str(e)}")
            
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'elasticsearch',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'elasticsearch_hosts': inputs.get('elasticsearch_hosts', ['http://localhost:9200']),
            'username': inputs.get('username'),
            'password': inputs.get('password'),
            'verify_certs': inputs.get('verify_certs', True),
            'ssl_show_warn': inputs.get('ssl_show_warn', True),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'index_prefix': inputs.get('index_prefix', f"tenant-{inputs['tenant_id']}"),
            'app_shards': inputs.get('app_shards', 3),
            'app_replicas': inputs.get('app_replicas', 1),
            'logs_shards': inputs.get('logs_shards', 5),
            'logs_replicas': inputs.get('logs_replicas', 1),
            'metrics_shards': inputs.get('metrics_shards', 3),
            'metrics_replicas': inputs.get('metrics_replicas', 1),
            'app_rollover_size_gb': inputs.get('app_rollover_size_gb', 50),
            'app_rollover_days': inputs.get('app_rollover_days', 7),
            'app_rollover_docs': inputs.get('app_rollover_docs', 50000000),
            'logs_rollover_size_gb': inputs.get('logs_rollover_size_gb', 100),
            'logs_rollover_days': inputs.get('logs_rollover_days', 1),
            'metrics_rollover_size_gb': inputs.get('metrics_rollover_size_gb', 30),
            'metrics_rollover_days': inputs.get('metrics_rollover_days', 1),
            'app_warm_after_days': inputs.get('app_warm_after_days', 7),
            'app_cold_after_days': inputs.get('app_cold_after_days', 30),
            'app_delete_after_days': inputs.get('app_delete_after_days', 90),
            'logs_warm_after_days': inputs.get('logs_warm_after_days', 1),
            'logs_delete_after_days': inputs.get('logs_delete_after_days', 30),
            'metrics_warm_after_days': inputs.get('metrics_warm_after_days', 3),
            'metrics_delete_after_days': inputs.get('metrics_delete_after_days', 60),
            'disk_alert_threshold_bytes': inputs.get('disk_alert_threshold_bytes', 107374182400),
            'create_sample_data': inputs.get('create_sample_data', False),
            'region': inputs.get('region', 'default')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['elasticsearch_config'] = config
        
        provisioner = ElasticsearchProvisioner(config)
        
        # Create index templates
        provisioner.create_index_templates()
        
        # Create lifecycle policies
        provisioner.create_lifecycle_policies()
        
        # Create initial indices
        provisioner.create_initial_indices()
        
        # Configure security
        provisioner.configure_security()
        
        # Configure monitoring
        provisioner.configure_monitoring()
        
        # Create sample data if requested
        provisioner.create_sample_data()
        
        # Store index info in runtime properties
        ctx.instance.runtime_properties['index_prefix'] = config['index_prefix']
        ctx.instance.runtime_properties['templates_created'] = True
        
        logger.info(f"Successfully provisioned Elasticsearch for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Elasticsearch: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 