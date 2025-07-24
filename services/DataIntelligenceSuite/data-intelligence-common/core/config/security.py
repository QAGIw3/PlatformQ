"""Security configurations."""

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from enum import Enum
from .base import SecurityConfig, BaseConfig


class AuthType(str, Enum):
    """Authentication types"""
    NONE = "none"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    JWT = "jwt"
    MTLS = "mtls"
    BASIC = "basic"


class EncryptionAlgorithm(str, Enum):
    """Encryption algorithms"""
    AES256 = "AES256"
    AES128 = "AES128"
    RSA2048 = "RSA2048"
    RSA4096 = "RSA4096"
    CHACHA20 = "CHACHA20"


@dataclass
class VaultConfig(BaseConfig):
    """HashiCorp Vault configuration"""
    url: str = "http://localhost:8200"
    token: str = ""
    namespace: Optional[str] = None
    mount_point: str = "secret"
    kv_version: int = 2
    timeout: int = 30
    max_retries: int = 3
    ca_cert_path: Optional[str] = None
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None
    verify_ssl: bool = True
    
    # Token renewal
    auto_renew_token: bool = True
    token_renewal_threshold: int = 86400  # 24 hours
    
    # Secret paths
    secret_paths: Dict[str, str] = field(default_factory=lambda: {
        "database": "database/creds",
        "api_keys": "api-keys",
        "certificates": "pki/issue",
        "encryption_keys": "transit/keys"
    })


@dataclass
class ConsulConfig(BaseConfig):
    """HashiCorp Consul configuration"""
    url: str = "http://localhost:8500"
    token: str = ""
    datacenter: str = "dc1"
    namespace: Optional[str] = None
    scheme: str = "http"
    verify_ssl: bool = True
    ca_cert_path: Optional[str] = None
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None
    
    # Service discovery
    service_prefix: str = "platformq"
    health_check_interval: int = 10
    deregister_critical_after: int = 60
    
    # Distributed locking
    lock_prefix: str = "locks"
    lock_timeout: int = 15
    lock_delay: int = 15
    
    # KV store
    kv_prefix: str = "config"
    watch_enabled: bool = True


@dataclass
class AuthConfig(SecurityConfig):
    """Authentication configuration"""
    auth_type: AuthType = AuthType.JWT
    
    # JWT settings
    jwt_secret_key: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 30
    jwt_refresh_enabled: bool = True
    jwt_refresh_expiration_days: int = 7
    
    # OAuth2 settings
    oauth2_provider_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    oauth2_redirect_uri: Optional[str] = None
    oauth2_scope: List[str] = field(default_factory=lambda: ["openid", "profile", "email"])
    
    # API Key settings
    api_key_header: str = "X-API-Key"
    api_key_prefix: str = "Bearer"
    
    # mTLS settings
    mtls_ca_cert_path: Optional[str] = None
    mtls_verify_client: bool = True
    
    # Session settings
    session_lifetime_minutes: int = 60
    session_sliding_expiration: bool = True
    
    # Security headers
    enable_cors: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    enable_csrf_protection: bool = True
    
    # Rate limiting
    enable_rate_limiting: bool = True
    rate_limit_per_minute: int = 60
    rate_limit_per_hour: int = 1000


@dataclass
class EncryptionConfig(BaseConfig):
    """Encryption configuration"""
    algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES256
    
    # Key management
    key_rotation_enabled: bool = True
    key_rotation_days: int = 90
    key_derivation_iterations: int = 100000
    
    # Vault Transit Engine
    use_vault_transit: bool = True
    transit_key_name: str = "platformq-master"
    
    # Local encryption (fallback)
    local_key_path: Optional[str] = None
    salt_length: int = 32
    
    # Data encryption settings
    encrypt_at_rest: bool = True
    encrypt_in_transit: bool = True
    encrypt_pii: bool = True
    
    # Field-level encryption
    field_encryption_enabled: bool = True
    encrypted_fields: List[str] = field(default_factory=lambda: [
        "ssn", "credit_card", "bank_account", "password",
        "email", "phone", "address", "date_of_birth"
    ])


@dataclass
class SecurityPolicyConfig(BaseConfig):
    """Security policy configuration"""
    # Password policy
    min_password_length: int = 12
    require_uppercase: bool = True
    require_lowercase: bool = True
    require_numbers: bool = True
    require_special_chars: bool = True
    password_history_count: int = 5
    password_expiration_days: int = 90
    
    # Account lockout
    max_login_attempts: int = 5
    lockout_duration_minutes: int = 30
    
    # Audit logging
    audit_enabled: bool = True
    audit_log_retention_days: int = 365
    audit_sensitive_operations: List[str] = field(default_factory=lambda: [
        "login", "logout", "password_change", "permission_change",
        "data_export", "data_delete", "config_change"
    ])
    
    # Network security
    allowed_ip_ranges: List[str] = field(default_factory=list)
    blocked_ip_ranges: List[str] = field(default_factory=list)
    
    # Data protection
    data_retention_days: int = 730  # 2 years
    data_anonymization_enabled: bool = True
    
    # Compliance
    compliance_standards: List[str] = field(default_factory=lambda: [
        "GDPR", "CCPA", "HIPAA", "SOC2"
    ])


@dataclass
class SecretsConfig(BaseConfig):
    """Secrets management configuration"""
    # Secret storage
    secret_backend: str = "vault"  # vault, kubernetes, aws-secrets-manager
    
    # Vault specific
    vault_config: Optional[VaultConfig] = None
    
    # Kubernetes secrets
    k8s_namespace: str = "default"
    k8s_secret_prefix: str = "platformq"
    
    # AWS Secrets Manager
    aws_region: str = "us-east-1"
    aws_secret_prefix: str = "/platformq"
    
    # Secret rotation
    auto_rotate_secrets: bool = True
    rotation_schedule: Dict[str, int] = field(default_factory=lambda: {
        "database": 30,  # days
        "api_keys": 90,
        "certificates": 365
    })
    
    # Secret caching
    cache_secrets: bool = True
    cache_ttl_seconds: int = 300
    
    # Secret validation
    validate_on_fetch: bool = True
    required_secrets: List[str] = field(default_factory=list) 