"""
Compliance Service configuration.
"""

from pydantic_settings import BaseSettings
from typing import Dict, List, Optional


class Settings(BaseSettings):
    """Compliance service settings"""
    
    # Service info
    service_name: str = "compliance-service"
    service_version: str = "1.0.0"
    
    # API settings
    api_prefix: str = "/api/v1"
    
    # Identity verification providers
    identity_providers: List[str] = ["jumio", "onfido"]
    identity_api_keys: Dict[str, str] = {}
    
    # Sanctions databases
    sanctions_databases: List[str] = ["ofac", "eu", "un"]
    sanctions_update_hours: int = 24
    
    # Transaction monitoring
    transaction_rules_engine: str = "drools"
    alert_thresholds: Dict[str, float] = {
        "velocity": 0.8,
        "amount": 0.9,
        "pattern": 0.7
    }
    
    # KYC settings
    document_storage_url: str = "s3://compliance-docs"
    kyc_encryption_key: Optional[str] = None
    kyc_expiry_days: int = 365
    
    # AML settings
    aml_model_path: Optional[str] = None
    risk_scoring_interval_minutes: int = 60
    
    # Regulatory reporting
    regulatory_apis: Dict[str, str] = {}
    reporting_jurisdictions: List[str] = ["US", "EU", "UK"]
    
    # Risk factors configuration
    risk_factors: Dict[str, float] = {
        "jurisdiction": 0.3,
        "transaction_volume": 0.25,
        "account_age": 0.15,
        "kyc_level": 0.2,
        "transaction_patterns": 0.1
    }
    
    # Infrastructure
    ignite_host: str = "localhost:10800"
    pulsar_url: str = "pulsar://localhost:6650"
    elasticsearch_url: str = "http://localhost:9200"
    redis_url: str = "redis://localhost:6379"
    
    class Config:
        env_prefix = "COMPLIANCE_"
        case_sensitive = False


# Global settings instance
settings = Settings() 