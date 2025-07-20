#!/usr/bin/env python3
"""
Cloudify script to create JanusGraph graph and schema for a tenant.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
from gremlin_python.driver import client, serializer
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('janusgraph_provisioner')


class JanusGraphProvisioner:
    """Handles JanusGraph graph and schema provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.gremlin_server = config['gremlin_server']
        self.gremlin_port = config.get('gremlin_port', 8182)
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        self.graph_name = config.get('graph_name', f"tenant_{self.tenant_id}")
        
        # Initialize Gremlin client
        self.client = client.Client(
            f'ws://{self.gremlin_server}:{self.gremlin_port}/gremlin',
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
    def create_graph(self):
        """Create graph instance for the tenant."""
        try:
            # Check if graph exists
            result = self.client.submit(f"ConfiguredGraphFactory.getGraphNames()").all().result()
            
            if self.graph_name in result:
                logger.info(f"Graph {self.graph_name} already exists")
                # Bind to existing graph
                self.client.submit(f"graph = ConfiguredGraphFactory.open('{self.graph_name}')")
                self.client.submit("g = graph.traversal()")
            else:
                # Create graph configuration
                self._create_graph_configuration()
                
                # Create graph
                self.client.submit(f"ConfiguredGraphFactory.create('{self.graph_name}')")
                self.client.submit(f"graph = ConfiguredGraphFactory.open('{self.graph_name}')")
                self.client.submit("g = graph.traversal()")
                
                logger.info(f"Created graph: {self.graph_name}")
                
                # Create schema
                self._create_schema()
                
                # Report usage
                self._report_usage('graph_created', {
                    'graph_name': self.graph_name,
                    'backend': self.config.get('storage_backend', 'cassandra'),
                    'index_backend': self.config.get('index_backend', 'elasticsearch')
                })
                
        except Exception as e:
            raise NonRecoverableError(f"Failed to create graph: {str(e)}")
            
    def _create_graph_configuration(self):
        """Create graph configuration for the tenant."""
        try:
            # Base configuration template
            config_template = f"tenant_{self.tenant_id}_template"
            
            # Storage backend configuration
            storage_backend = self.config.get('storage_backend', 'cassandra')
            if storage_backend == 'cassandra':
                storage_config = {
                    'storage.backend': 'cassandra',
                    'storage.cassandra.keyspace': f"janusgraph_{self.tenant_id}",
                    'storage.cassandra.replication-factor': self.config.get('replication_factor', 3),
                    'storage.cassandra.replication-strategy-class': 'org.apache.cassandra.locator.SimpleStrategy',
                    'storage.cassandra.consistency-level': 'QUORUM'
                }
                
                # Add Cassandra hosts
                cassandra_hosts = self.config.get('cassandra_hosts', ['localhost'])
                storage_config['storage.hostname'] = ','.join(cassandra_hosts)
            else:
                raise ValueError(f"Unsupported storage backend: {storage_backend}")
                
            # Index backend configuration
            index_backend = self.config.get('index_backend', 'elasticsearch')
            if index_backend == 'elasticsearch':
                index_config = {
                    'index.search.backend': 'elasticsearch',
                    'index.search.elasticsearch.hostname': ','.join(
                        self.config.get('elasticsearch_hosts', ['localhost'])
                    ),
                    'index.search.elasticsearch.index-name': f"janusgraph_{self.tenant_id}",
                    'index.search.elasticsearch.create.ext.number_of_shards': 
                        self.config.get('es_shards', 3),
                    'index.search.elasticsearch.create.ext.number_of_replicas': 
                        self.config.get('es_replicas', 1)
                }
            else:
                raise ValueError(f"Unsupported index backend: {index_backend}")
                
            # Cache configuration
            cache_config = {
                'cache.db-cache': self.config.get('enable_db_cache', True),
                'cache.db-cache-size': self.config.get('db_cache_size', 0.5),
                'cache.db-cache-time': self.config.get('db_cache_time_ms', 10000),
                'cache.tx-cache-size': self.config.get('tx_cache_size', 20000)
            }
            
            # Combine all configurations
            full_config = {**storage_config, **index_config, **cache_config}
            
            # Create configuration template
            self.client.submit(f"mgmt = ConfiguredGraphFactory.getInstance()")
            
            for key, value in full_config.items():
                if isinstance(value, bool):
                    value_str = 'true' if value else 'false'
                elif isinstance(value, (int, float)):
                    value_str = str(value)
                else:
                    value_str = f'"{value}"'
                    
                self.client.submit(
                    f"mgmt.setTemplateConfiguration(new MapConfiguration("
                    f"ImmutableMap.of('{key}', {value_str})))"
                )
                
            # Register graph name with configuration
            self.client.submit(
                f"ConfiguredGraphFactory.createConfiguration("
                f"new MapConfiguration(ImmutableMap.of("
                f"'graph.graphname', '{self.graph_name}')))"
            )
            
            logger.info(f"Created graph configuration for {self.graph_name}")
            
        except Exception as e:
            logger.error(f"Failed to create graph configuration: {str(e)}")
            raise
            
    def _create_schema(self):
        """Create graph schema for the tenant."""
        try:
            # Open management interface
            self.client.submit("mgmt = graph.openManagement()")
            
            # Create vertex labels
            vertex_labels = [
                # Core entities
                ('tenant', {'partitioned': False}),
                ('user', {'partitioned': True}),
                ('resource', {'partitioned': True}),
                ('service', {'partitioned': True}),
                ('event', {'partitioned': True}),
                ('metric', {'partitioned': True}),
                
                # Knowledge graph entities
                ('entity', {'partitioned': True}),
                ('concept', {'partitioned': False}),
                ('document', {'partitioned': True}),
                ('tag', {'partitioned': False})
            ]
            
            for label, options in vertex_labels:
                if options.get('partitioned', False):
                    self.client.submit(f"mgmt.makeVertexLabel('{label}').partition().make()")
                else:
                    self.client.submit(f"mgmt.makeVertexLabel('{label}').make()")
                logger.info(f"Created vertex label: {label}")
                
            # Create edge labels
            edge_labels = [
                ('owns', 'MULTI'),
                ('uses', 'MULTI'),
                ('belongs_to', 'MANY2ONE'),
                ('created_by', 'MANY2ONE'),
                ('related_to', 'MULTI'),
                ('contains', 'MULTI'),
                ('references', 'MULTI'),
                ('tagged_with', 'MULTI'),
                ('depends_on', 'MULTI'),
                ('triggers', 'MULTI')
            ]
            
            for label, multiplicity in edge_labels:
                self.client.submit(
                    f"mgmt.makeEdgeLabel('{label}').multiplicity({multiplicity}).make()"
                )
                logger.info(f"Created edge label: {label}")
                
            # Create properties
            properties = [
                # Identifiers
                ('tenant_id', 'String', 'SINGLE'),
                ('reseller_id', 'String', 'SINGLE'),
                ('customer_id', 'String', 'SINGLE'),
                ('resource_id', 'String', 'SINGLE'),
                ('user_id', 'String', 'SINGLE'),
                
                # Common properties
                ('name', 'String', 'SINGLE'),
                ('display_name', 'String', 'SINGLE'),
                ('description', 'String', 'SINGLE'),
                ('type', 'String', 'SINGLE'),
                ('status', 'String', 'SINGLE'),
                ('created_at', 'Long', 'SINGLE'),
                ('updated_at', 'Long', 'SINGLE'),
                ('deleted_at', 'Long', 'SINGLE'),
                
                # Metadata
                ('metadata', 'String', 'SINGLE'),  # JSON string
                ('tags', 'String', 'SET'),
                ('attributes', 'String', 'LIST'),
                
                # Metrics
                ('value', 'Double', 'SINGLE'),
                ('unit', 'String', 'SINGLE'),
                ('timestamp', 'Long', 'SINGLE'),
                
                # Search
                ('content', 'String', 'SINGLE'),
                ('embedding', 'String', 'SINGLE')  # Vector as JSON
            ]
            
            for prop_name, prop_type, cardinality in properties:
                self.client.submit(
                    f"mgmt.makePropertyKey('{prop_name}').dataType({prop_type}.class)"
                    f".cardinality(Cardinality.{cardinality}).make()"
                )
                logger.info(f"Created property: {prop_name} ({prop_type})")
                
            # Create indices
            self._create_indices()
            
            # Commit schema
            self.client.submit("mgmt.commit()")
            logger.info("Committed schema changes")
            
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            # Try to rollback
            try:
                self.client.submit("mgmt.rollback()")
            except:
                pass
            raise
            
    def _create_indices(self):
        """Create graph indices for efficient querying."""
        try:
            # Composite indices for exact match queries
            composite_indices = [
                ('by_tenant_id', ['tenant_id'], ['tenant', 'user', 'resource', 'service']),
                ('by_resource_id', ['resource_id'], ['resource']),
                ('by_user_id', ['user_id'], ['user']),
                ('by_type', ['type'], ['resource', 'service', 'entity']),
                ('by_status', ['status'], ['resource', 'service', 'user']),
                ('by_tenant_type', ['tenant_id', 'type'], ['resource', 'service']),
                ('by_tenant_status', ['tenant_id', 'status'], ['resource', 'service'])
            ]
            
            for index_name, properties, vertex_labels in composite_indices:
                # Build property keys string
                prop_keys = '.'.join([f"addKey(mgmt.getPropertyKey('{p}'))" for p in properties])
                
                # Build index only on specific vertex labels
                for label in vertex_labels:
                    specific_index_name = f"{index_name}_{label}"
                    self.client.submit(
                        f"mgmt.buildIndex('{specific_index_name}', Vertex.class)"
                        f".{prop_keys}"
                        f".indexOnly(mgmt.getVertexLabel('{label}'))"
                        f".buildCompositeIndex()"
                    )
                    logger.info(f"Created composite index: {specific_index_name}")
                    
            # Mixed indices for full-text search
            mixed_indices = [
                ('search_content', ['name', 'display_name', 'description', 'content']),
                ('search_metadata', ['metadata', 'tags'])
            ]
            
            for index_name, properties in mixed_indices:
                index_builder = f"mgmt.buildIndex('{index_name}', Vertex.class)"
                
                for prop in properties:
                    if prop in ['name', 'display_name', 'description', 'content']:
                        # Text search mapping
                        index_builder += (
                            f".addKey(mgmt.getPropertyKey('{prop}'), "
                            f"Mapping.TEXT.asParameter())"
                        )
                    else:
                        # String search mapping
                        index_builder += (
                            f".addKey(mgmt.getPropertyKey('{prop}'), "
                            f"Mapping.STRING.asParameter())"
                        )
                        
                index_builder += ".buildMixedIndex('search')"
                self.client.submit(index_builder)
                logger.info(f"Created mixed index: {index_name}")
                
            # Edge indices
            edge_indices = [
                ('edge_by_timestamp', 'timestamp', ['created_by', 'uses', 'triggers']),
                ('edge_by_type', 'type', ['related_to', 'references'])
            ]
            
            for index_name, property, edge_labels in edge_indices:
                for label in edge_labels:
                    specific_index_name = f"{index_name}_{label}"
                    self.client.submit(
                        f"mgmt.buildIndex('{specific_index_name}', Edge.class)"
                        f".addKey(mgmt.getPropertyKey('{property}'))"
                        f".indexOnly(mgmt.getEdgeLabel('{label}'))"
                        f".buildCompositeIndex()"
                    )
                    logger.info(f"Created edge index: {specific_index_name}")
                    
        except Exception as e:
            logger.error(f"Failed to create indices: {str(e)}")
            raise
            
    def create_tenant_vertex(self):
        """Create the root tenant vertex."""
        try:
            # Check if tenant vertex exists
            result = self.client.submit(
                f"g.V().has('tenant_id', '{self.tenant_id}').count()"
            ).all().result()
            
            if result[0] > 0:
                logger.info(f"Tenant vertex already exists for {self.tenant_id}")
                return
                
            # Create tenant vertex
            self.client.submit(
                f"g.addV('tenant')"
                f".property('tenant_id', '{self.tenant_id}')"
                f".property('reseller_id', '{self.reseller_id or ''}')"
                f".property('customer_id', '{self.customer_id or ''}')"
                f".property('name', 'Tenant {self.tenant_id}')"
                f".property('created_at', {int(time.time())}L)"
                f".property('status', 'active')"
            )
            
            logger.info(f"Created tenant vertex for {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create tenant vertex: {str(e)}")
            
    def configure_security(self):
        """Configure graph-level security for the tenant."""
        try:
            # Security configuration would typically involve:
            # 1. Creating user credentials
            # 2. Setting up access control lists
            # 3. Configuring query filters
            
            security_config = {
                'tenant_id': self.tenant_id,
                'graph_name': self.graph_name,
                'allowed_operations': [
                    'read', 'write', 'delete', 'admin'
                ],
                'query_filter': f"has('tenant_id', '{self.tenant_id}')",
                'max_traversal_depth': self.config.get('max_traversal_depth', 10),
                'timeout_ms': self.config.get('query_timeout_ms', 30000)
            }
            
            # Store security configuration
            ctx.instance.runtime_properties['security_config'] = security_config
            logger.info(f"Configured security for graph {self.graph_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure security: {str(e)}")
            
    def create_sample_data(self):
        """Create sample graph data for testing."""
        if not self.config.get('create_sample_data', False):
            return
            
        try:
            # Create sample vertices
            self.client.submit(
                f"user1 = g.addV('user')"
                f".property('tenant_id', '{self.tenant_id}')"
                f".property('user_id', 'user-001')"
                f".property('name', 'John Doe')"
                f".property('created_at', {int(time.time())}L)"
                f".next()"
            )
            
            self.client.submit(
                f"resource1 = g.addV('resource')"
                f".property('tenant_id', '{self.tenant_id}')"
                f".property('resource_id', 'res-001')"
                f".property('name', 'Sample Resource')"
                f".property('type', 'compute')"
                f".property('status', 'active')"
                f".property('created_at', {int(time.time())}L)"
                f".next()"
            )
            
            # Create sample edges
            self.client.submit(
                f"g.V().has('user_id', 'user-001')"
                f".addE('owns')"
                f".to(g.V().has('resource_id', 'res-001'))"
                f".property('created_at', {int(time.time())}L)"
            )
            
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
                'service': 'janusgraph',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")
            
    def cleanup(self):
        """Cleanup resources."""
        if self.client:
            self.client.close()
            logger.info("Closed Gremlin connection")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'gremlin_server': inputs.get('gremlin_server', 'localhost'),
            'gremlin_port': inputs.get('gremlin_port', 8182),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'graph_name': inputs.get('graph_name', f"tenant_{inputs['tenant_id']}"),
            'storage_backend': inputs.get('storage_backend', 'cassandra'),
            'cassandra_hosts': inputs.get('cassandra_hosts', ['localhost']),
            'index_backend': inputs.get('index_backend', 'elasticsearch'),
            'elasticsearch_hosts': inputs.get('elasticsearch_hosts', ['localhost:9200']),
            'replication_factor': inputs.get('replication_factor', 3),
            'es_shards': inputs.get('es_shards', 3),
            'es_replicas': inputs.get('es_replicas', 1),
            'enable_db_cache': inputs.get('enable_db_cache', True),
            'db_cache_size': inputs.get('db_cache_size', 0.5),
            'db_cache_time_ms': inputs.get('db_cache_time_ms', 10000),
            'tx_cache_size': inputs.get('tx_cache_size', 20000),
            'max_traversal_depth': inputs.get('max_traversal_depth', 10),
            'query_timeout_ms': inputs.get('query_timeout_ms', 30000),
            'create_sample_data': inputs.get('create_sample_data', False),
            'region': inputs.get('region', 'default')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['janusgraph_config'] = config
        
        provisioner = JanusGraphProvisioner(config)
        
        # Create graph
        provisioner.create_graph()
        
        # Create tenant vertex
        provisioner.create_tenant_vertex()
        
        # Configure security
        provisioner.configure_security()
        
        # Create sample data if requested
        provisioner.create_sample_data()
        
        # Store graph info in runtime properties
        ctx.instance.runtime_properties['graph_name'] = config['graph_name']
        ctx.instance.runtime_properties['graph_created'] = True
        
        logger.info(f"Successfully provisioned JanusGraph for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision JanusGraph: {str(e)}")
        raise NonRecoverableError(str(e))
        
    finally:
        if 'provisioner' in locals():
            provisioner.cleanup()


if __name__ == '__main__':
    main() 