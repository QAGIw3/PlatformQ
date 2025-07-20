#!/usr/bin/env python
"""
Create Cassandra Keyspace for Tenant

This script is executed by Cloudify to create a Cassandra keyspace
for a new tenant with the appropriate settings.
"""

import os
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from cloudify import ctx
from cloudify.exceptions import NonRecoverableError


def create_keyspace():
    """Create Cassandra keyspace for tenant"""
    
    # Get inputs from Cloudify
    tenant_id = ctx.node.properties['tenant_id']
    keyspace_name = ctx.node.properties['keyspace_name']
    replication_factor = ctx.node.properties['replication_factor']
    
    ctx.logger.info(f"Creating Cassandra keyspace {keyspace_name} for tenant {tenant_id}")
    
    # Get Cassandra connection details from Cloudify secrets
    cassandra_hosts = ctx.get_secret('cassandra_hosts').split(',')
    cassandra_username = ctx.get_secret('cassandra_username')
    cassandra_password = ctx.get_secret('cassandra_password')
    cassandra_datacenter = ctx.get_secret('cassandra_datacenter')
    
    # Connect to Cassandra
    try:
        auth_provider = PlainTextAuthProvider(
            username=cassandra_username,
            password=cassandra_password
        )
        
        cluster = Cluster(
            cassandra_hosts,
            auth_provider=auth_provider
        )
        session = cluster.connect()
        
        # Determine replication strategy based on environment
        if len(cassandra_hosts) > 1:
            # Multi-node cluster, use NetworkTopologyStrategy
            replication_strategy = f"""
                {{'class': 'NetworkTopologyStrategy', 
                 '{cassandra_datacenter}': {replication_factor}}}
            """
        else:
            # Single node, use SimpleStrategy
            replication_strategy = f"""
                {{'class': 'SimpleStrategy', 
                 'replication_factor': {replication_factor}}}
            """
        
        # Create keyspace
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH replication = {replication_strategy}
            AND durable_writes = true
        """
        
        session.execute(create_keyspace_query)
        ctx.logger.info(f"Created keyspace {keyspace_name}")
        
        # Create default tables
        session.execute(f"USE {keyspace_name}")
        
        # Metadata table
        metadata_table = f"""
            CREATE TABLE IF NOT EXISTS metadata (
                key text PRIMARY KEY,
                value text,
                created_at timestamp,
                updated_at timestamp
            )
        """
        session.execute(metadata_table)
        
        # Events table with time-based partitioning
        events_table = f"""
            CREATE TABLE IF NOT EXISTS events (
                event_date date,
                event_id timeuuid,
                event_type text,
                user_id text,
                data text,
                created_at timestamp,
                PRIMARY KEY ((event_date), event_id)
            ) WITH CLUSTERING ORDER BY (event_id DESC)
        """
        session.execute(events_table)
        
        # User data table
        user_data_table = f"""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id text PRIMARY KEY,
                profile_data text,
                preferences text,
                created_at timestamp,
                updated_at timestamp
            )
        """
        session.execute(user_data_table)
        
        ctx.logger.info(f"Created default tables in keyspace {keyspace_name}")
        
        # Store keyspace info in runtime properties
        ctx.instance.runtime_properties['keyspace_name'] = keyspace_name
        ctx.instance.runtime_properties['replication_factor'] = replication_factor
        ctx.instance.runtime_properties['tables'] = ['metadata', 'events', 'user_data']
        
        # Report usage to metering system
        report_usage(tenant_id, keyspace_name, 'provision')
        
        cluster.shutdown()
        
    except Exception as e:
        ctx.logger.error(f"Failed to create Cassandra keyspace: {str(e)}")
        raise NonRecoverableError(f"Cassandra provisioning failed: {str(e)}")


def report_usage(tenant_id, keyspace_name, action):
    """Report usage to CloudKitty and OpenMeter"""
    
    # This would integrate with the metering services
    # For now, just log
    ctx.logger.info(f"Reporting {action} of keyspace {keyspace_name} for tenant {tenant_id}")
    
    # Example of sending metrics
    # cloudkitty_client.report_usage(
    #     tenant_id=tenant_id,
    #     service='cassandra',
    #     resource_id=keyspace_name,
    #     action=action,
    #     metadata={
    #         'replication_factor': ctx.instance.runtime_properties.get('replication_factor'),
    #         'tables_count': len(ctx.instance.runtime_properties.get('tables', []))
    #     }
    # )


if __name__ == '__main__':
    create_keyspace() 