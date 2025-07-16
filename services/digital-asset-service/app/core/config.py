from platformq.shared.config import Settings
from typing import List

class AppSettings(Settings):
    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str
    database_url: str
    minio_access_key: str
    minio_secret_key: str

    class Config:
        env_prefix = "DAS_"

settings = AppSettings() 