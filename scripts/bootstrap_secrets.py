import hvac
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bootstrap_vault_secrets():
    """
    Populates Vault with the initial secrets for the platform.
    """
    try:
        client = hvac.Client(
            url="http://localhost:8200",
            token="root"
        )
        assert client.is_authenticated()
        
        secrets = {
            "platformq/cassandra": {"password": "cassandra"},
            "platformq/jwt": {"secret_key": "a_very_secret_key_that_should_be_changed"},
            "platformq/zulip": {"api_key": "YOUR_ZULIP_BOT_API_KEY", "email": "notification-bot@platformq.local"},
            "platformq/onlyoffice": {"jwt_secret": "a_very_long_and_secret_string_for_onlyoffice"},
            "platformq/nextcloud": {"admin_user": "nc-admin", "admin_pass": "strongpassword"},
        }
        
        logger.info("Populating Vault KV store...")
        for path, data in secrets.items():
            client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=data,
            )
            logger.info(f"  - Set secret at secret/data/{path}")
            
        logger.info("Secrets bootstrap complete.")
        
    except Exception as e:
        logger.error(f"Failed to bootstrap Vault secrets: {e}")
        raise

if __name__ == "__main__":
    bootstrap_vault_secrets() 