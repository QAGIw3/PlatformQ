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
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/(user-events|document-events)",
        subscription_name="notification-service-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    # Pre-load schemas
    user_schema = avro.schema.parse(open("schemas/user_created.avsc").read())
    doc_schema = avro.schema.parse(open("schemas/document_updated.avsc").read())

    while True:
        try:
            msg = consumer.receive()
            topic = msg.topic_name()
            bytes_reader = io.BytesIO(msg.data())
            decoder = avro.io.BinaryDecoder(bytes_reader)
            
            message_content = ""
            
            if "user-events" in topic:
                reader = avro.io.DatumReader(user_schema)
                data = reader.read(decoder)
                message_content = f"New user signed up! Welcome, **{data['full_name']}** ({data['email']})."
            
            elif "document-events" in topic:
                reader = avro.io.DatumReader(doc_schema)
                data = reader.read(decoder)
                message_content = f"User {data['saved_by_user_id']} just updated the document: `{data['document_path']}`"

            if message_content:
                zulip_message = {
                    "type": "stream", "to": "general", "topic": "Platform Activity", "content": message_content
                }
                zulip_client.send_message(zulip_message)
                logger.info(f"Sent notification to Zulip for event from topic {topic}.")

            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            consumer.negative_acknowledge(msg)

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
    thread = threading.Thread(target=notification_consumer_loop, daemon=True)
    thread.start()

@app.post("/webhooks/nextcloud")
async def handle_nextcloud_webhook(request: Request):
    # Here you would add logic to parse the Nextcloud webhook
    # create a structured Avro event, and publish it to the
    # 'content-events' Pulsar topic.
    logger.info("Received webhook from Nextcloud.")
    return Response(status_code=status.HTTP_204_NO_CONTENT) 