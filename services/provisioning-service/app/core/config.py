import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Configuration settings for the Provisioning Service."""
    
    # Service info
    service_name: str = "provisioning-service"
    
    # Cassandra settings
    cassandra_host: str = os.getenv("CASSANDRA_HOST", "cassandra")
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    
    # MinIO settings
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    # Pulsar settings
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    pulsar_admin_url: str = os.getenv("PULSAR_ADMIN_URL", "http://pulsar:8080")
    
    # OpenProject settings
    openproject_url: str = os.getenv("OPENPROJECT_URL", "http://openproject:8080")
    openproject_api_key: str = os.getenv("OPENPROJECT_API_KEY", "")
    
    # Ignite settings
    ignite_host: str = os.getenv("IGNITE_HOST", "ignite")
    ignite_port: int = int(os.getenv("IGNITE_PORT", "10800"))
    
    # Elasticsearch settings
    elasticsearch_host: str = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
    elasticsearch_port: int = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
    elasticsearch_scheme: str = os.getenv("ELASTICSEARCH_SCHEME", "http")
    elasticsearch_username: str = os.getenv("ELASTICSEARCH_USERNAME", "")
    elasticsearch_password: str = os.getenv("ELASTICSEARCH_PASSWORD", "")
    
    # Nextcloud settings
    nextcloud_url: str = os.getenv("NEXTCLOUD_URL", "http://nextcloud-web")
    nextcloud_admin_user: str = os.getenv("NEXTCLOUD_ADMIN_USER", "nc-admin")
    nextcloud_admin_pass: str = os.getenv("NEXTCLOUD_ADMIN_PASS", "strongpassword")
    
    # Data lake settings
    data_retention_days: int = int(os.getenv("DATA_RETENTION_DAYS", "90"))
    enable_lifecycle_policies: bool = os.getenv("ENABLE_LIFECYCLE_POLICIES", "true").lower() == "true"
    
    # Partitioning settings
    partition_by_time: bool = True
    partition_by_domain: bool = True
    partition_time_format: str = "year={YYYY}/month={MM}/day={DD}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings() 