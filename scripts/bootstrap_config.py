import consul
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bootstrap_consul_config():
    """
    Populates the Consul KV store with initial configuration for the platform.
    """
    try:
        c = consul.Consul(host="localhost", port=8500)
        
        config = {
            "platformq/cassandra/hosts": "cassandra",
            "platformq/cassandra/port": "9042",
            "platformq/cassandra/user": "cassandra",
            "platformq/pulsar/url": "pulsar://pulsar:6650",
            "platformq/zulip/site": "http://platformq-zulip:8080",
            "platformq/nextcloud/url": "http://platformq-nextcloud",
            # We can add service-specific config too
            "platformq/auth-service/algorithm": "HS256",
        }
        
        logger.info("Populating Consul KV store...")
        for key, value in config.items():
            c.kv.put(key, value)
            logger.info(f"  - Set {key}")
            
        logger.info("Configuration bootstrap complete.")
        
    except Exception as e:
        logger.error(f"Failed to bootstrap Consul configuration: {e}")
        raise

if __name__ == "__main__":
    bootstrap_consul_config() 