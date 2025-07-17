from cassandra.cluster import Session
from minio import Minio
from pulsar.admin import PulsarAdmin
from openproject import OpenProject

from .provisioning.cassandra import create_cassandra_keyspace
from .provisioning.minio import create_minio_bucket
from .provisioning.pulsar import create_pulsar_namespace
from .provisioning.openproject import create_openproject_project

def provision_tenant(
    tenant_id: str,
    tenant_name: str,
    cassandra_session: Session,
    minio_client: Minio,
    pulsar_admin: PulsarAdmin,
    openproject_client: OpenProject,
):
    """
    Orchestrates the provisioning of all necessary resources for a new tenant.
    """
    create_cassandra_keyspace(cassandra_session, tenant_id)
    create_minio_bucket(minio_client, tenant_id)
    create_pulsar_namespace(pulsar_admin, tenant_id)
    create_openproject_project(openproject_client, tenant_id, tenant_name) 