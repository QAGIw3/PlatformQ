import pulsar
import avro.schema
import avro.io
import io
import logging
import os
import sys
import re
import signal

# Add project root to path to allow imports from shared_lib
# This is a hack for local development; in a container, this is handled by PYTHONPATH
if "/app" not in sys.path:
    sys.path.append("/app")
    
from platformq_shared.event_publisher import EventPublisher # We use it for creating a client
from platformq_shared.events import UserCreatedEvent
from pulsar.schema import AvroSchema
from .nextcloud_provisioner import NextcloudProvisioner

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

def main():
    logger.info("Starting Tenant-Aware Provisioning Service...")

    # Initialize and start the background provisioner for Nextcloud
    provisioner = NextcloudProvisioner(
        admin_user=NEXTCLOUD_ADMIN_USER,
        admin_pass=NEXTCLOUD_ADMIN_PASS,
        nextcloud_url=NEXTCLOUD_URL,
    )
    provisioner_thread = provisioner.start()

    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern=TOPIC_PATTERN,
        subscription_name=SUBSCRIPTION_NAME,
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(UserCreatedEvent)
    )

    def shutdown(signum, frame):
        logger.info("Shutdown signal received. Stopping consumer and provisioner...")
        provisioner.stop()
        provisioner_thread.join() # Wait for the worker to finish
        consumer.close()
        client.close()
        logger.info("Provisioning Service shut down gracefully.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    try:
        while True:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None:
                continue
            try:
                tenant_id = get_tenant_from_topic(msg.topic_name())
                if not tenant_id:
                    logger.warning(f"Could not extract tenant from topic: {msg.topic_name()}")
                    consumer.acknowledge(msg)
                    continue

                user_data = msg.value() # Deserialization is automatic
                logger.info(f"Received user_created event for tenant {tenant_id}: {user_data.email}")
                
                # Add provisioning task to the non-blocking queue
                provisioner.add_user_to_queue(tenant_id, user_data)
                
                consumer.acknowledge(msg)
            except Exception as e:
                consumer.negative_acknowledge(msg)
                logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        shutdown(None, None)

if __name__ == "__main__":
    main() 