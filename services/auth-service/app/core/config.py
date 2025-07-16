from platformq.shared.config import Settings
from typing import List

class AppSettings(Settings):
    jwt_secret_key: str
    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str


    class Config:
        env_prefix = "AUTH_"

settings = AppSettings() 