"""
Configuration for Infrastructure Provisioning Service
"""
import os
from typing import List, Dict, Any
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Configuration settings for Infrastructure Provisioning Service"""
    
    # Service info
    service_name: str = "infrastructure-provisioning-service"
    service_host: str = Field(default="0.0.0.0", env="SERVICE_HOST")
    service_port: int = Field(default=8004, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # API Configuration
    api_version: str = "v1"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Database Configuration
    cassandra_hosts: List[str] = Field(
        default_factory=lambda: os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
    )
    cassandra_keyspace: str = Field(default="platformq", env="CASSANDRA_KEYSPACE")
    cassandra_username: str = Field(default="", env="CASSANDRA_USERNAME")
    cassandra_password: str = Field(default="", env="CASSANDRA_PASSWORD")
    
    # Storage Configuration
    minio_endpoint: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, env="MINIO_SECURE")
    
    # Messaging Configuration
    pulsar_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_admin_url: str = Field(default="http://pulsar:8080", env="PULSAR_ADMIN_URL")
    pulsar_topic_prefix: str = Field(default="persistent://public/default/", env="PULSAR_TOPIC_PREFIX")
    
    # Caching Configuration
    ignite_host: str = Field(default="ignite", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    
    # Elasticsearch Configuration
    elasticsearch_hosts: List[str] = Field(
        default_factory=lambda: os.getenv("ELASTICSEARCH_HOSTS", "elasticsearch:9200").split(",")
    )
    elasticsearch_username: str = Field(default="", env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str = Field(default="", env="ELASTICSEARCH_PASSWORD")
    
    # Consul Configuration
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    consul_token: str = Field(default="", env="CONSUL_TOKEN")
    
    # Vault Configuration
    vault_enabled: bool = Field(default=True, env="VAULT_ENABLED")
    vault_addr: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    vault_token: str = Field(default="", env="VAULT_TOKEN")
    vault_mount_path: str = Field(default="secret", env="VAULT_MOUNT_PATH")
    
    # JanusGraph Configuration
    janusgraph_host: str = Field(default="janusgraph", env="JANUSGRAPH_HOST")
    janusgraph_port: int = Field(default=8182, env="JANUSGRAPH_PORT")
    
    # Provisioning Configuration
    provisioning_timeout: int = Field(default=300, env="PROVISIONING_TIMEOUT")  # seconds
    max_retry_attempts: int = Field(default=3, env="MAX_RETRY_ATTEMPTS")
    parallel_provisioning: bool = Field(default=True, env="PARALLEL_PROVISIONING")
    
    # Resource Cleanup Configuration
    cleanup_enabled: bool = Field(default=True, env="CLEANUP_ENABLED")
    cleanup_interval_hours: int = Field(default=24, env="CLEANUP_INTERVAL_HOURS")
    orphan_resource_age_hours: int = Field(default=48, env="ORPHAN_RESOURCE_AGE_HOURS")
    
    # Resource Validation Configuration
    validation_enabled: bool = Field(default=True, env="VALIDATION_ENABLED")
    validation_interval_minutes: int = Field(default=30, env="VALIDATION_INTERVAL_MINUTES")
    
    # Infrastructure Defaults
    default_cassandra_replication_factor: int = Field(default=3, env="DEFAULT_CASSANDRA_REPLICATION_FACTOR")
    default_elasticsearch_shards: int = Field(default=3, env="DEFAULT_ELASTICSEARCH_SHARDS")
    default_elasticsearch_replicas: int = Field(default=1, env="DEFAULT_ELASTICSEARCH_REPLICAS")
    default_pulsar_partitions: int = Field(default=4, env="DEFAULT_PULSAR_PARTITIONS")
    default_minio_bucket_versioning: bool = Field(default=True, env="DEFAULT_MINIO_BUCKET_VERSIONING")
    
    # Service Discovery
    consul_service_name: str = Field(
        default="infrastructure-provisioning-service",
        env="CONSUL_SERVICE_NAME"
    )
    consul_health_check_interval: str = Field(default="10s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    consul_deregister_critical_after: str = Field(default="30s", env="CONSUL_DEREGISTER_CRITICAL_AFTER")
    
    # Monitoring
    prometheus_enabled: bool = Field(default=True, env="PROMETHEUS_ENABLED")
    jaeger_enabled: bool = Field(default=True, env="JAEGER_ENABLED")
    jaeger_agent_host: str = Field(default="jaeger", env="JAEGER_AGENT_HOST")
    jaeger_agent_port: int = Field(default=6831, env="JAEGER_AGENT_PORT")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    def get_cassandra_connection_dict(self) -> Dict[str, Any]:
        """Get Cassandra connection parameters"""
        config = {
            "contact_points": self.cassandra_hosts,
            "keyspace": self.cassandra_keyspace,
        }
        if self.cassandra_username:
            config["username"] = self.cassandra_username
            config["password"] = self.cassandra_password
        return config
    
    def get_minio_connection_dict(self) -> Dict[str, Any]:
        """Get MinIO connection parameters"""
        return {
            "endpoint": self.minio_endpoint,
            "access_key": self.minio_access_key,
            "secret_key": self.minio_secret_key,
            "secure": self.minio_secure
        }
    
    def get_elasticsearch_connection_dict(self) -> Dict[str, Any]:
        """Get Elasticsearch connection parameters"""
        config = {
            "hosts": self.elasticsearch_hosts,
        }
        if self.elasticsearch_username:
            config["http_auth"] = (self.elasticsearch_username, self.elasticsearch_password)
        return config
    
    def get_consul_connection_dict(self) -> Dict[str, Any]:
        """Get Consul connection parameters"""
        config = {
            "host": self.consul_host,
            "port": self.consul_port,
        }
        if self.consul_token:
            config["token"] = self.consul_token
        return config
    
    def get_vault_connection_dict(self) -> Dict[str, Any]:
        """Get Vault connection parameters"""
        return {
            "url": self.vault_addr,
            "token": self.vault_token,
        } 