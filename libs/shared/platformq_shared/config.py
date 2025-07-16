import os
import consul
import hvac
import logging

logger = logging.getLogger(__name__)

KUBE_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

class ConfigLoader:
    def __init__(self, consul_host="consul"):
        self.consul_client = consul.Consul(host=consul_host, port=8500)
        self.vault_client = self._init_vault_client()

    def _init_vault_client(self):
        vault_addr = os.getenv("VAULT_ADDR")
        if not vault_addr:
            logger.warning("VAULT_ADDR not set. Vault client will not be available.")
            return None

        client = hvac.Client(url=vault_addr)

        # 1. Try Kubernetes Service Account auth first
        if os.path.exists(KUBE_SA_TOKEN_PATH):
            try:
                with open(KUBE_SA_TOKEN_PATH, 'r') as f:
                    jwt = f.read()
                
                # The 'role' here should match the 'role_name' in the Terraform config
                # We can get this from an env var, e.g., VAULT_K8S_ROLE
                k8s_role = os.getenv("VAULT_K8S_ROLE")
                if k8s_role:
                    client.auth.kubernetes.login(role=k8s_role, jwt=jwt)
                    if client.is_authenticated():
                        logger.info("Successfully connected to Vault using Kubernetes auth.")
                        return client
            except Exception as e:
                logger.warning(f"Kubernetes auth failed, falling back: {e}")

        # 2. Try GCP auth
        try:
            # The role here should match the 'role' in the Terraform config
            gcp_role = os.getenv("VAULT_GCP_ROLE")
            if gcp_role:
                client.auth.gcp.login(role=gcp_role)
                if client.is_authenticated():
                    logger.info("Successfully connected to Vault using GCP auth.")
                    return client
        except Exception as e:
            logger.warning(f"GCP auth failed, falling back: {e}")

        # 3. Fallback to AppRole auth
        role_id = os.getenv("VAULT_ROLE_ID")
        secret_id = os.getenv("VAULT_SECRET_ID")
        if role_id and secret_id:
            try:
                client.auth.approle.login(
                    role_id=role_id,
                    secret_id=secret_id,
                )
                if client.is_authenticated():
                    logger.info("Successfully connected to Vault using AppRole auth.")
                    return client
            except Exception as e:
                logger.warning(f"AppRole auth failed: {e}")
        
        logger.error("Could not authenticate to Vault with any available method.")
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
            "PULSAR_URL": self.get_config("platformq/pulsar/url"),
            "OTEL_EXPORTER_OTLP_ENDPOINT": self.get_config("platformq/opentelemetry/endpoint", default="otel-collector:4317"),
        }
        
        if self.vault_client:
            cassandra_config = self.get_secret("platformq/cassandra", "config")
            settings["CASSANDRA_HOSTS"] = cassandra_config.get("hosts").split(',')
            settings["CASSANDRA_PORT"] = int(cassandra_config.get("port"))
            settings["CASSANDRA_USER"] = self.get_secret("platformq/cassandra", "user")
            settings["CASSANDRA_PASSWORD"] = self.get_secret("platformq/cassandra", "password")
            settings["ARWEAVE_WALLET"] = self.get_secret("platformq/arweave", "wallet")
            settings["MINIO_ACCESS_KEY"] = self.get_secret("platformq/minio", "access_key")
            settings["MINIO_SECRET_KEY"] = self.get_secret("platformq/minio", "secret_key")
            settings["POSTGRES_PASSWORD"] = self.get_secret("platformq/postgres", "password")
            settings["S2S_JWT_SECRET"] = self.get_secret("platformq/jwt", "s2s_secret_key")
            settings["ETHEREUM_PRIVATE_KEY"] = self.get_secret("platformq/ethereum", "private_key")
            settings["POLYGON_PRIVATE_KEY"] = self.get_secret("platformq/polygon", "private_key")
            settings["CREDENTIAL_ENCRYPTION_KEY"] = self.get_secret("platformq/credentials", "encryption_key")
            settings["BRIDGE_PRIVATE_KEY"] = self.get_secret("platformq/bridge", "private_key")
            settings["REPUTATION_OPERATOR_PRIVATE_KEY"] = self.get_secret("platformq/reputation", "operator_private_key")
            settings["ZULIP_API_KEY"] = self.get_secret("platformq/zulip", "api_key")
            settings["OPENPROJECT_API_KEY"] = self.get_secret("platformq/openproject", "api_key")
        
        return settings 