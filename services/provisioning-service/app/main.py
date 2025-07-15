import pulsar
import avro.schema
import avro.io
import io
import logging
import requests
import os
import sys
import re

# Add project root to path to allow imports from shared_lib
# This is a hack for local development; in a container, this is handled by PYTHONPATH
if "/app" not in sys.path:
    sys.path.append("/app")
    
from shared_lib.event_publisher import EventPublisher # We use it for creating a client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar:6650")
SUBSCRIPTION_NAME = "provisioning-service-sub"
# Subscribe to all 'user-events' topics across all tenants
TOPIC_PATTERN = "persistent://platformq/.*/user-events"
SCHEMA_PATH = "schemas/user_created.avsc"

NEXTCLOUD_URL = os.environ.get("NEXTCLOUD_URL", "http://nextcloud-web") # K8s service name
NEXTCLOUD_ADMIN_USER = os.environ.get("NEXTCLOUD_ADMIN_USER", "nc-admin")
NEXTCLOUD_ADMIN_PASS = os.environ.get("NEXTCLOUD_ADMIN_PASS", "strongpassword")

def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name."""
    match = re.search(r'platformq/(.*?)/user-events', topic)
    if match:
        return match.group(1)
    return None

def provision_nextcloud_user(tenant_id: str, user_data: dict):
    """Calls the Nextcloud User Provisioning API to create a user."""
    logger.info(f"Provisioning user in Nextcloud for tenant {tenant_id}: {user_data['email']}")
    
    url = f"{NEXTCLOUD_URL}/ocs/v1.php/cloud/users"
    headers = {"OCS-APIRequest": "true"}
    auth = (NEXTCLOUD_ADMIN_USER, NEXTCLOUD_ADMIN_PASS)
    
    payload = {
        "userid": user_data["email"],
        "password": "a-long-random-password-that-will-not-be-used", # Password is not used due to SSO
        "email": user_data["email"],
        "displayName": user_data["full_name"],
    }
    
    try:
        response = requests.post(url, headers=headers, auth=auth, data=payload)
        response.raise_for_status()
        logger.info(f"Successfully provisioned user {user_data['email']} in Nextcloud for tenant {tenant_id}.")
    except requests.exceptions.HTTPError as e:
        # It's common for the user to already exist, which is not a critical error.
        if e.response.status_code == 400 and "already exists" in e.response.text:
             logger.warning(f"User {user_data['email']} already exists in Nextcloud for tenant {tenant_id}.")
        else:
             logger.error(f"Failed to provision user in Nextcloud for tenant {tenant_id}: {e.response.text}")

def main():
    logger.info("Starting Tenant-Aware Provisioning Service...")
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern=TOPIC_PATTERN,
        subscription_name=SUBSCRIPTION_NAME,
        consumer_type=pulsar.ConsumerType.Shared,
    )
    
    try:
        schema = avro.schema.parse(open(SCHEMA_PATH).read())
        
        while True:
            msg = consumer.receive()
            try:
                tenant_id = get_tenant_from_topic(msg.topic_name())
                if not tenant_id:
                    logger.warning(f"Could not extract tenant from topic: {msg.topic_name()}")
                    consumer.acknowledge(msg)
                    continue

                bytes_reader = io.BytesIO(msg.data())
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = avro.io.DatumReader(schema)
                user_data = reader.read(decoder)
                
                logger.info(f"Received user_created event for tenant {tenant_id}: {user_data}")
                provision_nextcloud_user(tenant_id, user_data)
                
                consumer.acknowledge(msg)
            except Exception as e:
                consumer.negative_acknowledge(msg)
                logger.error(f"Failed to process message: {e}")

    finally:
        consumer.close()
        client.close()
        logger.info("Provisioning Service shut down.")

if __name__ == "__main__":
    main() 