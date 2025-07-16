from platformq_shared.base_service import create_base_app
from fastapi import Depends
from cassandra.cluster import Session
from uuid import UUID
import logging
import threading
import time
import pulsar
from pulsar.schema import AvroSchema
from platformq_shared.config import ConfigLoader
from platformq_shared.events import DAOEvent # Import DAOEvent
import json
from datetime import datetime

from app.api.deps import get_current_tenant_and_user, get_db_session

logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
PULSAR_URL = settings.get("PULSAR_URL", "pulsar://pulsar:6650")

# This service uses the shared library, but needs to provide its own dependencies
# as it doesn't have the full auth-service crud modules.
# For simplicity, we will define placeholder functions.
def get_api_key_crud_placeholder(): return None
def get_user_crud_placeholder(): return None
def get_password_verifier_placeholder(): return None


def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name using a regex."""
    import re
    match = re.search(r'platformq/([a-f0-9-]+)/', topic)
    if match:
        return match.group(1)
    logger.warning(f"Could not extract tenant ID from topic: {topic}")
    return None

# --- DAO Event Consumer Loop ---
def dao_analytics_consumer_loop(app):
    logger.info("Starting DAO analytics consumer thread...")
    client = pulsar.Client(PULSAR_URL)
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/dao-events",
        subscription_name="analytics-dao-event-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(DAOEvent)
    )

    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None: continue
            
            event = msg.value()
            tenant_id = get_tenant_from_topic(msg.topic_name())
            
            if not tenant_id:
                logger.warning(f"Skipping DAO event from topic {msg.topic_name()} due to missing tenant ID.")
                consumer.acknowledge(msg)
                continue

            logger.info(f"[Analytics] Processing DAO event for tenant {tenant_id}: {event.event_type} (DAO: {event.dao_id})")
            
            # Store event in Cassandra for analytics
            db_session = app.state.db_manager.get_session()
            db_session.set_keyspace('analytics_keyspace') # Assuming an analytics keyspace

            # Example: Insert into a table for DAO events
            query = """
            INSERT INTO dao_events (
                tenant_id, dao_id, event_type, blockchain_tx_hash, proposal_id, voter_id,
                event_data, event_timestamp, event_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            """
            prepared = db_session.prepare(query)

            try:
                event_data_str = json.dumps(event.event_data) if isinstance(event.event_data, dict) else event.event_data
            except TypeError:
                event_data_str = str(event.event_data) # Fallback for non-serializable data

            db_session.execute(prepared, [
                UUID(tenant_id), UUID(event.dao_id), event.event_type, event.blockchain_tx_hash,
                event.proposal_id, event.voter_id, event_data_str, datetime.fromtimestamp(event.event_timestamp / 1000)
            ])
            logger.info(f"[Analytics] Stored DAO event: {event.event_type} for DAO {event.dao_id}")

            consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in DAO analytics consumer loop: {e}", exc_info=True)
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)
                
    consumer.close()
    client.close()


app = create_base_app(
    service_name="analytics-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

@app.on_event("startup")
def startup_event():
    # Existing startup logic from base_service
    # ... (no changes needed for existing base_service setup)
    # Start DAO event consumer thread
    app.state.stop_event = threading.Event() # Ensure stop_event is initialized
    dao_consumer_thread = threading.Thread(target=dao_analytics_consumer_loop, args=(app,), daemon=True)
    dao_consumer_thread.start()
    app.state.dao_consumer_thread = dao_consumer_thread

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutdown signal received, stopping analytics consumer threads.")
    app.state.stop_event.set()
    if hasattr(app.state, 'dao_consumer_thread'):
        app.state.dao_consumer_thread.join()
    # ... (existing shutdown logic from base_service)
    logger.info("Analytics consumer threads stopped.")


@app.get("/api/v1/activity")
def get_activity_stream(
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """
    Fetches the unified activity stream for the current user's tenant.
    """
    tenant_id = context["tenant_id"]
    query = "SELECT * FROM activity_stream WHERE tenant_id = %s LIMIT 50"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [tenant_id])
    return [dict(row._asdict()) for row in rows]

# New API endpoint for DAO insights (placeholder)
@app.get("/api/v1/dao-insights/{dao_id}")
def get_dao_insights(
    dao_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """
    Provides insights into a specific DAO.
    """
    tenant_id = context["tenant_id"]
    db.set_keyspace('analytics_keyspace')

    # Example: Count total proposals and votes for a DAO
    total_proposals_query = "SELECT COUNT(*) FROM dao_events WHERE tenant_id = %s AND dao_id = %s AND event_type = 'ProposalCreated' ALLOW FILTERING"
    total_votes_query = "SELECT COUNT(*) FROM dao_events WHERE tenant_id = %s AND dao_id = %s AND event_type = 'VoteCast' ALLOW FILTERING"
    
    proposals_count = db.execute(db.prepare(total_proposals_query), [tenant_id, dao_id]).one().count
    votes_count = db.execute(db.prepare(total_votes_query), [tenant_id, dao_id]).one().count

    return {
        "dao_id": str(dao_id),
        "total_proposals": proposals_count,
        "total_votes_cast": votes_count,
        "last_updated": datetime.utcnow().isoformat()
    } 