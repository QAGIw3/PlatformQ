from shared_lib.base_service import create_base_app
from shared_lib.config import ConfigLoader
import zulip
import pulsar
import avro.schema
import avro.io
import io
import logging
import threading
from fastapi import Request, Response, status
from platformq_shared.events import UserCreatedEvent, DocumentUpdatedEvent
from pulsar.schema import AvroSchema
import schedule
import time

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
ZULIP_API_KEY = config_loader.get_secret("platformq/zulip", "api_key")
ZULIP_EMAIL = config_loader.get_secret("platformq/zulip", "email")
ZULIP_SITE = config_loader.get_config("platformq/zulip/site")

# --- Zulip Client ---
zulip_client = zulip.Client(api_key=ZULIP_API_KEY, email=ZULIP_EMAIL, site=ZULIP_SITE)

# --- Pulsar Consumer Thread ---
def notification_consumer_loop():
    logger.info("Starting notification consumer thread...")
    client = pulsar.Client(settings["PULSAR_URL"])
    
    # Subscribe to multiple topics with different schemas
    user_consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/user-events",
        subscription_name="notification-service-user-sub",
        schema=AvroSchema(UserCreatedEvent)
    )
    doc_consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/document-events",
        subscription_name="notification-service-doc-sub",
        schema=AvroSchema(DocumentUpdatedEvent)
    )
    
    # This is a simplified example of handling multiple consumers.
    # A real implementation might use a more robust multi-threaded approach.
    while True:
        try:
            msg = client.receive() # Receive from any subscribed topic
            data = msg.value()
            topic = msg.topic_name()
            
            message_content = ""
            if isinstance(data, UserCreatedEvent):
                message_content = f"New user signed up! Welcome, **{data.full_name}** ({data.email})."
            elif isinstance(data, DocumentUpdatedEvent):
                message_content = f"User {data.saved_by_user_id} just updated the document: `{data.document_path}`"
            
            if message_content:
                zulip_message = {
                    "type": "stream", "to": "general", "topic": "Platform Activity", "content": message_content
                }
                zulip_client.send_message(zulip_message)
                logger.info(f"Sent notification to Zulip for event from topic {topic}.")

            client.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            client.negative_acknowledge(msg)

# --- Background Task for Graph Intelligence ---
def check_for_graph_insights():
    logger.info("Checking for new graph insights...")
    try:
        # In a real app, we would need a way to get a service-to-service auth token
        # to call the graph-intelligence-service securely.
        # response = requests.get("http://kong:8000/graph/api/v1/insights/community-detection", headers=...)
        # new_communities = response.json()['data']
        
        # Conceptual: If new_communities have been found since the last check...
        # For this example, we'll just simulate a finding.
        new_communities = [{"community_id": 1, "users": ["user1", "user2", "user3"]}]

        for community in new_communities:
            user_list = ", ".join(community['users'])
            message = {
                "type": "stream",
                "to": "general",
                "topic": "Platform Insights",
                "content": f"**New Collaboration Hub Detected!**\nA new community of users has formed around a recent project, including: {user_list}. Consider creating a dedicated Zulip channel for them to collaborate!"
            }
            zulip_client.send_message(message)
            logger.info("Sent graph insight notification to Zulip.")

    except Exception as e:
        logger.error(f"Failed to check for graph insights: {e}")

def intelligence_scheduler_loop():
    schedule.every(1).hour.do(check_for_graph_insights)
    while True:
        schedule.run_pending()
        time.sleep(1)

# --- FastAPI App ---
app = create_base_app(
    service_name="notification-service",
    # This service doesn't use the db or api key auth directly, so we pass placeholders
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
)

@app.on_event("startup")
def startup_event():
    # Start the Pulsar consumer in a background thread
    pulsar_thread = threading.Thread(target=notification_consumer_loop, daemon=True)
    pulsar_thread.start()
    
    # Start the intelligence scheduler in another thread
    intel_thread = threading.Thread(target=intelligence_scheduler_loop, daemon=True)
    intel_thread.start()

@app.post("/webhooks/nextcloud")
async def handle_nextcloud_webhook(request: Request):
    # Here you would add logic to parse the Nextcloud webhook
    # create a structured Avro event, and publish it to the
    # 'content-events' Pulsar topic.
    logger.info("Received webhook from Nextcloud.")
    return Response(status_code=status.HTTP_204_NO_CONTENT) 