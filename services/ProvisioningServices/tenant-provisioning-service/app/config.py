"""Configuration settings for Tenant Provisioning Service"""

from pydantic import BaseSettings, Field
from typing import List, Dict
import os


class Settings(BaseSettings):
    """Configuration settings"""
    
    # Service info
    service_name: str = "tenant-provisioning-service"
    service_host: str = Field(default="0.0.0.0", env="SERVICE_HOST")
    service_port: int = Field(default=8001, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # API Configuration
    api_version: str = "v1"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Database Configuration
    cassandra_hosts: List[str] = Field(
        default_factory=lambda: os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
    )
    cassandra_keyspace: str = Field(default="platformq", env="CASSANDRA_KEYSPACE")
    
    # Storage Configuration
    minio_endpoint: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, env="MINIO_SECURE")
    
    # Messaging Configuration
    pulsar_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_admin_url: str = Field(default="http://pulsar:8080", env="PULSAR_ADMIN_URL")
    
    # Caching Configuration
    ignite_host: str = Field(default="ignite", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    
    # Elasticsearch Configuration
    elasticsearch_hosts: List[str] = Field(
        default_factory=lambda: os.getenv("ELASTICSEARCH_HOSTS", "elasticsearch:9200").split(",")
    )
    
    # Consul Configuration
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    consul_token: str = Field(default="", env="CONSUL_TOKEN")
    
    # Vault Configuration
    vault_enabled: bool = Field(default=True, env="VAULT_ENABLED")
    vault_addr: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    vault_token: str = Field(default="", env="VAULT_TOKEN")
    
    # OpenProject Configuration
    openproject_url: str = Field(default="http://openproject:8080", env="OPENPROJECT_URL")
    openproject_api_key: str = Field(default="", env="OPENPROJECT_API_KEY")
    
    # Service URLs
    nextcloud_url: str = Field(default="http://nextcloud", env="NEXTCLOUD_URL")
    janusgraph_url: str = Field(default="http://janusgraph:8182", env="JANUSGRAPH_URL")
    
    # Provisioning Configuration
    provisioning_timeout: int = Field(default=300, env="PROVISIONING_TIMEOUT")  # seconds
    max_retry_attempts: int = Field(default=3, env="MAX_RETRY_ATTEMPTS")
    parallel_provisioning: bool = Field(default=True, env="PARALLEL_PROVISIONING")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance
settings = Settings() 