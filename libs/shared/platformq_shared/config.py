import consul
import hvac
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    def __init__(self, consul_host="consul", vault_url="http://vault:8200", vault_token="root"):
        self.consul_client = consul.Consul(host=consul_host, port=8500)
        
        try:
            self.vault_client = hvac.Client(url=vault_url, token=vault_token)
            assert self.vault_client.is_authenticated()
            logger.info("Successfully connected to Vault.")
        except Exception as e:
            logger.error(f"Failed to connect to Vault: {e}")
            self.vault_client = None

    def get_config(self, key, default=None):
        index, data = self.consul_client.kv.get(key)
        if data is None:
            if default is not None:
                logger.warning(f"Config key '{key}' not found in Consul, using default value.")
                return default
            raise RuntimeError(f"Config key '{key}' not found in Consul.")
        return data['Value'].decode('utf-8')

    def get_secret(self, path, key):
        if not self.vault_client:
            raise RuntimeError("Vault client is not available.")
        
        response = self.vault_client.secrets.kv.v2.read_secret_version(path=path)
        if response is None or 'data' not in response or 'data' not in response['data']:
            raise RuntimeError(f"Secret not found at path '{path}'")
        return response['data']['data'][key]

    def load_settings(self):
        """Loads a standard set of platform settings."""
        return {
            "CASSANDRA_HOSTS": self.get_config("platformq/cassandra/hosts").split(','),
            "CASSANDRA_PORT": int(self.get_config("platformq/cassandra/port")),
            "CASSANDRA_USER": self.get_config("platformq/cassandra/user"),
            "CASSANDRA_PASSWORD": self.get_secret("platformq/cassandra", "password"),
            "PULSAR_URL": self.get_config("platformq/pulsar/url"),
            "OTEL_EXPORTER_OTLP_ENDPOINT": self.get_config("platformq/opentelemetry/endpoint", default="otel-collector:4317"),
        } 