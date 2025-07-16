import os
import consul
import hvac
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    def __init__(self, consul_host="consul"):
        self.consul_client = consul.Consul(host=consul_host, port=8500)
        self.vault_client = self._init_vault_client()

    def _init_vault_client(self):
        vault_addr = os.getenv("VAULT_ADDR")
        role_id = os.getenv("VAULT_ROLE_ID")
        secret_id = os.getenv("VAULT_SECRET_ID")

        if not all([vault_addr, role_id, secret_id]):
            logger.warning("Vault environment variables not set. Vault client will not be available.")
            return None
        
        try:
            client = hvac.Client(url=vault_addr)
            client.auth.approle.login(
                role_id=role_id,
                secret_id=secret_id,
            )
            assert client.is_authenticated()
            logger.info("Successfully connected to Vault using AppRole.")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to Vault with AppRole: {e}")
            return None

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
            raise RuntimeError("Vault client is not available. Check Vault environment variables.")
        
        try:
            response = self.vault_client.secrets.kv.v2.read_secret_version(path=path)
            if response is None or 'data' not in response or 'data' not in response['data']:
                raise RuntimeError(f"Secret not found at path '{path}'")
            return response['data']['data'][key]
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{key}' from Vault at path '{path}': {e}")
            raise

    def load_settings(self):
        """Loads a standard set of platform settings."""
        settings = {
            "CASSANDRA_HOSTS": self.get_config("platformq/cassandra/hosts").split(','),
            "CASSANDRA_PORT": int(self.get_config("platformq/cassandra/port")),
            "PULSAR_URL": self.get_config("platformq/pulsar/url"),
            "OTEL_EXPORTER_OTLP_ENDPOINT": self.get_config("platformq/opentelemetry/endpoint", default="otel-collector:4317"),
        }
        
        if self.vault_client:
            settings["CASSANDRA_USER"] = self.get_secret("platformq/cassandra", "user")
            settings["CASSANDRA_PASSWORD"] = self.get_secret("platformq/cassandra", "password")
            settings["ARWEAVE_WALLET"] = self.get_secret("platformq/arweave", "wallet")
        
        return settings 