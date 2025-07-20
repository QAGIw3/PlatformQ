"""Configuration settings for Structured Products Service."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Service info
    service_name: str = "structured-products-service"
    service_version: str = "1.0.0"
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_structured_topic: str = "persistent://public/default/structured-products-events"
    
    # External services
    options_service_url: str = "http://localhost:8006"
    futures_service_url: str = "http://localhost:8005"
    risk_service_url: str = "http://localhost:8004"
    
    # Product parameters
    max_product_complexity: int = 10  # Max number of legs
    pricing_iterations: int = 10000  # Monte Carlo iterations
    
    class Config:
        env_prefix = "STRUCTURED_"
        case_sensitive = False 