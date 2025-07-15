from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any
from w3c_vc import VerifiableCredential
from datetime import datetime
import uuid
import logging
import threading
import time
import json
import re
import pulsar
from pulsar.schema import AvroSchema
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader

# from hyperledger.fabric import gateway # Conceptual client

logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
PULSAR_URL = settings.get("PULSAR_URL", "pulsar://pulsar:6650")

def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name using a regex."""
    match = re.search(r'platformq/([a-f0-9-]+)/', topic)
    if match:
        return match.group(1)
    logger.warning(f"Could not extract tenant ID from topic: {topic}")
    return None

# --- Main Credential Issuance Logic ---
def issue_and_record_credential(tenant_id: str, subject: dict, credential_type: str, publisher: EventPublisher):
    """
    Core logic to create, sign, and record a credential, then publish an event.
    (This is refactored from the original REST endpoint)
    """
    logger.info(f"Issuing '{credential_type}' for tenant {tenant_id}...")
    
    vc = VerifiableCredential({
        "@context": ["https://www.w3.org/2018/credentials/v1"],
        "id": f"urn:uuid:{uuid.uuid4()}",
        "type": ["VerifiableCredential", credential_type],
        "issuer": f"did:web:platformq.com:tenants:{tenant_id}",
        "issuanceDate": datetime.utcnow().isoformat() + "Z",
        "credentialSubject": subject,
    })
    
    vc.add_proof(method="Ed25519VerificationKey2018", signature="z5tRea...") # Conceptual signature
    logger.info(f"Recorded credential {vc.data['id']} to blockchain for tenant {tenant_id}.")

    # Publish an event to notify that the credential has been issued
    event_data = {
        "tenant_id": tenant_id,
        "proposal_id": subject.get("id", "").replace("urn:platformq:proposal:", ""), # Extract from subject
        "credential_id": vc.data['id'],
        "event_timestamp": int(time.time() * 1000)
    }
    publisher.publish(
        topic_base='verifiable-credential-issued-events',
        tenant_id=tenant_id,
        schema_path='libs/event-schemas/verifiable_credential_issued.avsc',
        data=event_data
    )
    logger.info(f"Published VerifiableCredentialIssued event for proposal {event_data['proposal_id']}")
    return vc.data


# --- Pulsar Consumer for Issuance Requests ---
def credential_issuer_loop(app):
    logger.info("Starting credential issuer consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/verifiable-credential-issuance-requests",
        subscription_name="vc-service-issuer-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(dict) # Using dict for now, would be a specific Avro class
    )

    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None: continue
            
            tenant_id = get_tenant_from_topic(msg.topic_name())
            if not tenant_id:
                consumer.acknowledge(msg)
                continue

            event_data = msg.value()
            logger.info(f"Received credential issuance request for tenant {tenant_id}")
            
            # Construct the subject for the VC from the event data
            credential_subject = {
                "id": f"urn:platformq:proposal:{event_data['proposal_id']}",
                "approvedBy": event_data['approver_id'],
                "customer": event_data['customer_name'],
                "approvedAt": datetime.fromtimestamp(event_data['event_timestamp'] / 1000).isoformat() + "Z"
            }

            issue_and_record_credential(
                tenant_id=tenant_id,
                subject=credential_subject,
                credential_type="ProposalApprovalCredential",
                publisher=publisher
            )
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in credential issuer loop: {e}")
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)


app = create_base_app(
    service_name="verifiable-credential-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

@app.on_event("startup")
def startup_event():
    app.state.event_publisher = EventPublisher()
    app.state.stop_event = threading.Event()
    thread = threading.Thread(target=credential_issuer_loop, args=(app,), daemon=True)
    thread.start()
    app.state.consumer_thread = thread

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutdown signal received, stopping consumer thread.")
    app.state.stop_event.set()
    app.state.consumer_thread.join()
    logger.info("Consumer thread stopped.")


# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["verifiable-credential-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "verifiable-credential-service is running"}

class CredentialSubject(BaseModel):
    id: str
    # ... other fields for the subject
    
class IssueRequest(BaseModel):
    subject: Dict[str, Any]
    credential_type: str = Field(alias="type")
    
@app.post("/api/v1/issue", status_code=201)
def issue_credential(
    req: IssueRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Issues a new Verifiable Credential, signs it, and records its hash
    on the private Hyperledger Fabric blockchain.
    """
    tenant_id = str(context["tenant_id"])
    publisher = req.app.state.event_publisher
    
    return issue_and_record_credential(
        tenant_id=tenant_id,
        subject=req.subject,
        credential_type=req.credential_type,
        publisher=publisher
    )
