from platformq.shared.config import Settings as BaseSettings
from typing import List

class Settings(BaseSettings):
    jwt_secret_key: str
    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str
    provisioning_service_url: str = "http://provisioning-service:8000"


    class Config:
        env_prefix = "AUTH_"

settings = Settings() 