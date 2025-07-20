"""Configuration for Platform Service Broker"""

import os
from typing import Dict, Any, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service broker configuration"""
    
    # Service info
    service_name: str = "platform-service-broker"
    service_port: int = Field(default=8080, env="SERVICE_PORT")
    
    # Broker features
    enable_openstack_broker: bool = Field(default=True, env="ENABLE_OPENSTACK_BROKER")
    enable_platform_broker: bool = Field(default=True, env="ENABLE_PLATFORM_BROKER")
    
    # OpenStack configuration
    openstack_auth_url: str = Field(default="http://keystone:5000/v3", env="OS_AUTH_URL")
    openstack_region: str = Field(default="RegionOne", env="OS_REGION_NAME")
    openstack_interface: str = Field(default="public", env="OS_INTERFACE")
    openstack_service_project: str = Field(default="service", env="OS_SERVICE_PROJECT")
    openstack_service_user: str = Field(default="platform-broker", env="OS_SERVICE_USER")
    openstack_service_password: str = Field(default="", env="OS_SERVICE_PASSWORD")
    openstack_service_domain: str = Field(default="default", env="OS_SERVICE_DOMAIN")
    
    # Cloudify configuration
    cloudify_url: Optional[str] = Field(default=None, env="CLOUDIFY_URL")
    cloudify_username: str = Field(default="admin", env="CLOUDIFY_USERNAME")
    cloudify_password: str = Field(default="admin", env="CLOUDIFY_PASSWORD")
    cloudify_tenant: str = Field(default="default_tenant", env="CLOUDIFY_TENANT")
    
    # CloudKitty configuration
    cloudkitty_enabled: bool = Field(default=True, env="CLOUDKITTY_ENABLED")
    cloudkitty_url: Optional[str] = Field(default=None, env="CLOUDKITTY_URL")
    
    # OpenMeter configuration
    openmeter_enabled: bool = Field(default=True, env="OPENMETER_ENABLED")
    openmeter_url: Optional[str] = Field(default=None, env="OPENMETER_URL")
    openmeter_api_key: Optional[str] = Field(default=None, env="OPENMETER_API_KEY")
    
    # Crossplane configuration
    crossplane_enabled: bool = Field(default=True, env="CROSSPLANE_ENABLED")
    crossplane_namespace: str = Field(default="crossplane-system", env="CROSSPLANE_NAMESPACE")
    
    # Platform Q services configuration
    cassandra_hosts: str = Field(default="cassandra:9042", env="CASSANDRA_HOSTS")
    cassandra_keyspace: str = Field(default="platformq", env="CASSANDRA_KEYSPACE")
    
    ignite_host: str = Field(default="ignite", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    
    pulsar_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    
    minio_endpoint: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    
    elasticsearch_url: str = Field(default="http://elasticsearch:9200", env="ELASTICSEARCH_URL")
    
    janusgraph_url: str = Field(default="http://janusgraph:8182", env="JANUSGRAPH_URL")
    
    # Consul configuration
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    
    # Vault configuration
    vault_enabled: bool = Field(default=True, env="VAULT_ENABLED")
    vault_address: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    vault_token: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    def get_broker_config(self) -> Dict[str, Any]:
        """Get broker configuration as dict"""
        return {
            "openstack": {
                "auth_url": self.openstack_auth_url,
                "region_name": self.openstack_region,
                "interface": self.openstack_interface,
                "service_project": self.openstack_service_project,
                "service_user": self.openstack_service_user,
                "service_password": self.openstack_service_password,
                "service_domain": self.openstack_service_domain
            },
            "cloudify": {
                "url": self.cloudify_url,
                "username": self.cloudify_username,
                "password": self.cloudify_password,
                "tenant": self.cloudify_tenant
            } if self.cloudify_url else None,
            "cloudkitty": {
                "enabled": self.cloudkitty_enabled,
                "url": self.cloudkitty_url
            },
            "openmeter": {
                "enabled": self.openmeter_enabled,
                "url": self.openmeter_url,
                "api_key": self.openmeter_api_key
            },
            "crossplane": {
                "enabled": self.crossplane_enabled,
                "namespace": self.crossplane_namespace
            },
            "platform_services": {
                "cassandra": {
                    "hosts": self.cassandra_hosts,
                    "keyspace": self.cassandra_keyspace
                },
                "ignite": {
                    "host": self.ignite_host,
                    "port": self.ignite_port
                },
                "pulsar": {
                    "url": self.pulsar_url
                },
                "minio": {
                    "endpoint": self.minio_endpoint,
                    "access_key": self.minio_access_key,
                    "secret_key": self.minio_secret_key
                },
                "elasticsearch": {
                    "url": self.elasticsearch_url
                },
                "janusgraph": {
                    "url": self.janusgraph_url
                }
            },
            "consul": {
                "host": self.consul_host,
                "port": self.consul_port
            },
            "vault": {
                "enabled": self.vault_enabled,
                "address": self.vault_address,
                "token": self.vault_token
            }
        } 