"""
Configuration for User Provisioning Service
"""
import os
from typing import List, Dict, Any
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Configuration settings for User Provisioning Service"""
    
    # Service info
    service_name: str = "user-provisioning-service"
    service_host: str = Field(default="0.0.0.0", env="SERVICE_HOST")
    service_port: int = Field(default=8005, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # API Configuration
    api_version: str = "v1"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Messaging Configuration
    pulsar_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_topic_prefix: str = Field(default="persistent://public/default/", env="PULSAR_TOPIC_PREFIX")
    
    # Service URLs
    auth_service_url: str = Field(default="http://auth-service:8000", env="AUTH_SERVICE_URL")
    nextcloud_url: str = Field(default="http://nextcloud", env="NEXTCLOUD_URL")
    openproject_url: str = Field(default="http://openproject:8080", env="OPENPROJECT_URL")
    vault_addr: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    keycloak_url: str = Field(default="http://keycloak:8080", env="KEYCLOAK_URL")
    gitlab_url: str = Field(default="http://gitlab", env="GITLAB_URL")
    
    # Nextcloud Configuration
    nextcloud_admin_user: str = Field(default="admin", env="NEXTCLOUD_ADMIN_USER")
    nextcloud_admin_pass: str = Field(default="admin", env="NEXTCLOUD_ADMIN_PASS")
    nextcloud_default_quota: str = Field(default="10GB", env="NEXTCLOUD_DEFAULT_QUOTA")
    
    # OpenProject Configuration
    openproject_api_key: str = Field(default="", env="OPENPROJECT_API_KEY")
    
    # Vault Configuration
    vault_token: str = Field(default="", env="VAULT_TOKEN")
    
    # Keycloak Configuration
    keycloak_realm: str = Field(default="platformq", env="KEYCLOAK_REALM")
    keycloak_client_id: str = Field(default="user-provisioning", env="KEYCLOAK_CLIENT_ID")
    keycloak_client_secret: str = Field(default="", env="KEYCLOAK_CLIENT_SECRET")
    
    # GitLab Configuration
    gitlab_token: str = Field(default="", env="GITLAB_TOKEN")
    
    # Provisioning Configuration
    provisioning_timeout: int = Field(default=60, env="PROVISIONING_TIMEOUT")  # seconds
    max_retry_attempts: int = Field(default=3, env="MAX_RETRY_ATTEMPTS")
    batch_provisioning_delay: float = Field(default=0.5, env="BATCH_PROVISIONING_DELAY")
    
    # Password Configuration
    password_length: int = Field(default=16, env="PASSWORD_LENGTH")
    password_include_symbols: bool = Field(default=True, env="PASSWORD_INCLUDE_SYMBOLS")
    
    # Default Groups and Roles
    default_user_groups: List[str] = Field(
        default_factory=lambda: os.getenv("DEFAULT_USER_GROUPS", "users").split(",")
    )
    default_user_roles: List[str] = Field(
        default_factory=lambda: os.getenv("DEFAULT_USER_ROLES", "user").split(",")
    )
    
    # Service Discovery
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    consul_service_name: str = Field(
        default="user-provisioning-service",
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
    
    def get_service_config(self, service_name: str) -> Dict[str, Any]:
        """Get configuration for a specific service"""
        configs = {
            "nextcloud": {
                "url": self.nextcloud_url,
                "admin_user": self.nextcloud_admin_user,
                "admin_pass": self.nextcloud_admin_pass,
                "default_quota": self.nextcloud_default_quota
            },
            "openproject": {
                "url": self.openproject_url,
                "api_key": self.openproject_api_key
            },
            "vault": {
                "url": self.vault_addr,
                "token": self.vault_token
            },
            "keycloak": {
                "url": self.keycloak_url,
                "realm": self.keycloak_realm,
                "client_id": self.keycloak_client_id,
                "client_secret": self.keycloak_client_secret
            },
            "gitlab": {
                "url": self.gitlab_url,
                "token": self.gitlab_token
            }
        }
        return configs.get(service_name, {}) 