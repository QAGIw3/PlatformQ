from platformq.shared.config import Settings
from typing import List

class AppSettings(Settings):
    cassandra_hosts: List[str]
    cassandra_port: int
    cassandra_user: str
    cassandra_password: str
    pulsar_url: str
    otel_exporter_otlp_endpoint: str
    nextcloud_url: str
    nextcloud_user: str
    nextcloud_password: str

    class Config:
        env_prefix = "PROPOSALS_"

settings = AppSettings() 