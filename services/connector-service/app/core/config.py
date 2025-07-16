from platformq.shared.config import Settings
from typing import List, Dict

class AppSettings(Settings):
    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str
    connector_plugins: Dict = {}

    class Config:
        env_prefix = "CONNECTOR_"

settings = AppSettings() 