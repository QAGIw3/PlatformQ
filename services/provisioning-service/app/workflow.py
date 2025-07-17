from cassandra.cluster import Session
from minio import Minio
from pulsar.admin import PulsarAdmin
from openproject import OpenProject
from pyignite import Client as IgniteClient
from elasticsearch import Elasticsearch

from .provisioning.cassandra import create_cassandra_keyspace
from .provisioning.minio import create_minio_bucket
from .provisioning.pulsar import create_pulsar_namespace
from .provisioning.openproject import create_openproject_project
from .provisioning.data_lake import create_data_lake_partitions, create_data_catalog_entry
from .provisioning.ignite import create_ignite_caches, create_ignite_sql_schemas
from .provisioning.elasticsearch import create_elasticsearch_indices, create_search_templates

def provision_tenant(
    tenant_id: str,
    tenant_name: str,
    cassandra_session: Session,
    minio_client: Minio,
    pulsar_admin: PulsarAdmin,
    openproject_client: OpenProject,
    ignite_client: IgniteClient = None,
    es_client: Elasticsearch = None,
):
    """
    Orchestrates the provisioning of all necessary resources for a new tenant.
    """
    create_cassandra_keyspace(cassandra_session, tenant_id)
    create_minio_bucket(minio_client, tenant_id)
    
    # Set up data lake partitions after bucket creation
    create_data_lake_partitions(minio_client, tenant_id)
    create_data_catalog_entry(minio_client, tenant_id)
    
    # Create Ignite caches with partitioning
    if ignite_client:
        create_ignite_caches(ignite_client, tenant_id)
        create_ignite_sql_schemas(ignite_client, tenant_id)
    
    # Create Elasticsearch indices with partitioning
    if es_client:
        create_elasticsearch_indices(es_client, tenant_id)
        create_search_templates(es_client, tenant_id)
    
    create_pulsar_namespace(pulsar_admin, tenant_id)
    create_openproject_project(openproject_client, tenant_id, tenant_name) 