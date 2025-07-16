from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from w3c_vc import VerifiableCredential
from datetime import datetime
import uuid
import logging
import threading
import time
import json
import re
import pulsar
import base58
import asyncio
from pulsar.schema import AvroSchema
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from platformq_shared.events import IssueVerifiableCredential, VerifiableCredentialIssued
from .schemas.dao_schemas import DAOMembershipCredentialSubject, VotingPowerCredentialSubject, ReputationScoreCredentialSubject, ProposalApprovalCredentialSubject

# Import blockchain components
from .blockchain import BlockchainClient, ChainType, EthereumClient, PolygonClient, FabricClient
from .blockchain.bridge import CrossChainBridge, AtomicCrossChainBridge
from .did import DIDManager, DIDResolver
from .zkp import ZKPManager, SelectiveDisclosure
from .storage.ipfs_storage import IPFSCredentialStorage, DistributedCredentialNetwork
from .trust.reputation import TrustNetwork, CredentialVerificationNetwork
from .standards.advanced_standards import (
    VerifiablePresentationBuilder,
    SoulBoundTokenManager,
    CredentialManifest,
    PresentationRequest,
    PresentationPurpose
)
from .api import endpoints
from fastapi import FastAPI
from .messaging.pulsar_consumer import start_consumer, stop_consumer, PulsarConsumer
from .blockchain.reputation_oracle import reputation_oracle_service

# from hyperledger.fabric import gateway # Conceptual client
import requests
from functools import lru_cache, wraps
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
PULSAR_URL = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
SEARCH_SERVICE_URL = settings.get("SEARCH_SERVICE_URL", "http://search-service:80")

# Initialize blockchain clients
blockchain_clients = {}
did_manager = DIDManager()
did_resolver = DIDResolver()
zkp_manager = ZKPManager()
selective_disclosure = SelectiveDisclosure()

# Initialize new components
cross_chain_bridge = None
atomic_bridge = None
ipfs_storage = None
distributed_storage = None
trust_network = TrustNetwork()
verification_network = CredentialVerificationNetwork(trust_network)
credential_manifest = CredentialManifest()
sbt_manager = None

def initialize_blockchain_clients():
    """Initialize blockchain clients based on configuration"""
    # Ethereum client
    if settings.get("ETHEREUM_ENABLED", False):
        eth_config = {
            "provider_url": settings.get("ETHEREUM_PROVIDER_URL"),
            "private_key": settings.get("ETHEREUM_PRIVATE_KEY"),
            "contract_address": settings.get("ETHEREUM_CONTRACT_ADDRESS")
        }
        blockchain_clients["ethereum"] = EthereumClient(eth_config)
    
    # Polygon client
    if settings.get("POLYGON_ENABLED", False):
        polygon_config = {
            "provider_url": settings.get("POLYGON_PROVIDER_URL"),
            "private_key": settings.get("POLYGON_PRIVATE_KEY"),
            "contract_address": settings.get("POLYGON_CONTRACT_ADDRESS")
        }
        blockchain_clients["polygon"] = PolygonClient(polygon_config)
    
    # Fabric client
    if settings.get("FABRIC_ENABLED", False):
        fabric_config = {
            "network_profile": settings.get("FABRIC_NETWORK_PROFILE"),
            "channel_name": settings.get("FABRIC_CHANNEL_NAME"),
            "chaincode_name": settings.get("FABRIC_CHAINCODE_NAME")
        }
        blockchain_clients["fabric"] = FabricClient(fabric_config)
        
    # Initialize cross-chain bridge
    global cross_chain_bridge, atomic_bridge
    if blockchain_clients:
        cross_chain_bridge = CrossChainBridge(blockchain_clients)
        atomic_bridge = AtomicCrossChainBridge(blockchain_clients)
        
    # Initialize SBT manager with primary blockchain
    global sbt_manager
    primary_blockchain = blockchain_clients.get("ethereum") or next(iter(blockchain_clients.values()), None)
    if primary_blockchain:
        sbt_manager = SoulBoundTokenManager(primary_blockchain)

def initialize_storage():
    """Initialize IPFS storage components"""
    global ipfs_storage, distributed_storage
    
    storage_proxy_url = settings.get("STORAGE_PROXY_URL", "http://storage-proxy-service:8000")
    encryption_key = settings.get("CREDENTIAL_ENCRYPTION_KEY")
    
    ipfs_storage = IPFSCredentialStorage(storage_proxy_url, encryption_key)
    
    # Initialize distributed storage with multiple nodes if configured
    storage_nodes = settings.get("IPFS_STORAGE_NODES", [storage_proxy_url])
    if len(storage_nodes) > 1:
        distributed_storage = DistributedCredentialNetwork(storage_nodes)

def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name using a regex."""
    match = re.search(r'platformq/([a-f0-9-]+)/', topic)
    if match:
        return match.group(1)
    logger.warning(f"Could not extract tenant ID from topic: {topic}")
    return None

# --- Main Credential Issuance Logic ---
async def issue_and_record_credential(tenant_id: str, subject: Dict[str, Any], credential_type: str, 
                                    publisher: EventPublisher, options: Optional[Dict[str, Any]] = None):
    """
    Core logic to create, sign, and record a credential, then publish an event.
    Enhanced with blockchain anchoring, IPFS storage, and trust network integration.
    Extended to handle DAO-specific credential subjects.
    """
    options = options or {}
    logger.info(f"Issuing '{credential_type}' for tenant {tenant_id}...")
    
    # Create DID for issuer if not exists
    issuer_did = options.get("issuer_did")
    if not issuer_did:
        issuer_did_doc = did_manager.create_did_for_tenant(tenant_id, method="web")
        issuer_did = issuer_did_doc.did
    
    # Prepare credential subject based on type
    vc_subject_data = subject
    if credential_type == "DAOMembershipCredential":
        DAOMembershipCredentialSubject.model_validate(subject) # Validate subject against schema
        vc_subject_data = DAOMembershipCredentialSubject(**subject).model_dump(mode='json', exclude_unset=True)
    elif credential_type == "VotingPowerCredential":
        VotingPowerCredentialSubject.model_validate(subject) # Validate subject against schema
        vc_subject_data = VotingPowerCredentialSubject(**subject).model_dump(mode='json', exclude_unset=True)
    elif credential_type == "ReputationScoreCredential":
        ReputationScoreCredentialSubject.model_validate(subject) # Validate subject against schema
        vc_subject_data = ReputationScoreCredentialSubject(**subject).model_dump(mode='json', exclude_unset=True)
    elif credential_type == "ProposalApprovalCredential":
        ProposalApprovalCredentialSubject.model_validate(subject) # Validate subject against schema
        vc_subject_data = ProposalApprovalCredentialSubject(**subject).model_dump(mode='json', exclude_unset=True)

    # Create the verifiable credential
    vc = VerifiableCredential({
        "@context": [
            "https://www.w3.org/2018/credentials/v1",
            "https://w3id.org/security/v2",
            "https://platformq.com/contexts/dao-v1.jsonld" # Add DAO context
        ],
        "id": f"urn:uuid:{uuid.uuid4()}",
        "type": ["VerifiableCredential", credential_type],
        "issuer": issuer_did,
        "issuanceDate": datetime.utcnow().isoformat() + "Z",
        "credentialSubject": vc_subject_data,
    })
    
    # Add proof (using DID-based signing)
    key_pair = did_manager.generate_key_pair()
    vc.add_proof(
        method="Ed25519VerificationKey2020",
        verification_method=f"{issuer_did}#key-1",
        signature=base58.b58encode(key_pair["private_key"]).decode()  # Simplified
    )
    
    # Store on IPFS if enabled
    ipfs_cid = None
    if ipfs_storage and options.get("store_on_ipfs", True):
        storage_receipt = await ipfs_storage.store_credential(
            vc.data,
            tenant_id,
            encrypt=options.get("encrypt_storage", True)
        )
        ipfs_cid = storage_receipt["cid"]
        vc.data["proof"]["ipfsCID"] = ipfs_cid
    
    # Anchor on blockchain(s)
    blockchain_type = options.get("blockchain", "ethereum")
    if blockchain_type in blockchain_clients:
        client = blockchain_clients[blockchain_type]
        if await client.connect():
            anchor = await client.anchor_credential(vc.data, tenant_id)
            vc.data["proof"]["blockchainAnchor"] = anchor.to_dict()
            await client.disconnect()
            logger.info(f"Credential anchored on {blockchain_type}: {anchor.transaction_hash}")
    
    # Store credential hash on multiple chains if requested
    if options.get("multi_chain", False):
        for chain_name, client in blockchain_clients.items():
            if chain_name != blockchain_type:  # Skip already anchored chain
                if await client.connect():
                    anchor = await client.anchor_credential(vc.data, tenant_id)
                    if "additionalAnchors" not in vc.data["proof"]:
                        vc.data["proof"]["additionalAnchors"] = []
                    vc.data["proof"]["additionalAnchors"].append(anchor.to_dict())
                    await client.disconnect()
    
    # Create SoulBound Token if requested
    if sbt_manager and options.get("create_sbt", False):
        owner_address = options.get("owner_address")
        issuer_address = options.get("issuer_address", issuer_did)
        if owner_address:
            sbt = await sbt_manager.mint_sbt(
                vc.data,
                owner_address,
                issuer_address,
                metadata_uri=f"ipfs://{ipfs_cid}" if ipfs_cid else None
            )
            vc.data["soulBoundToken"] = {
                "tokenId": sbt.token_id,
                "soulSignature": sbt.soul_signature
            }
    
    # Add issuer to trust network
    trust_network.add_entity(issuer_did)
    
    logger.info(f"Recorded credential {vc.data['id']} with blockchain anchor(s) and IPFS storage")

    # Publish an event to notify that the credential has been issued
    if credential_type == "ProposalApprovalCredential":
        event = VerifiableCredentialIssued(
            tenant_id=tenant_id,
            proposal_id=subject.get("proposalId", ""),
            credential_id=vc.data['id'],
        )
    else:
        event = VerifiableCredentialIssued(
            tenant_id=tenant_id,
            proposal_id=subject.get("id", "").replace("urn:platformq:proposal:", ""), # Default behavior
            credential_id=vc.data['id'],
        )

    publisher.publish(
        topic_base='verifiable-credential-issued-events',
        tenant_id=tenant_id,
        schema_class=VerifiableCredentialIssued,
        data=event
    )
    logger.info(f"Published VerifiableCredentialIssued event for proposal {event.proposal_id}")
    return vc.data

async def handle_trust_event(event_type: str, event_data: dict, publisher: EventPublisher):
    """
    Handles events from the trust-engine-events topic, issues a corresponding
    Verifiable Credential, and updates the user's on-chain reputation.
    """
    # Mapping from our event types to credential details
    EVENT_TO_VC_MAP = {
        "DAOProposalApproved": {
            "credential_type": "DAOProposalApprovalCredential",
            "user_id_field": "proposer_id",
            "activity_id_field": "proposal_id",
            "reputation_score": 20
        },
        "ProjectMilestoneCompleted": {
            "credential_type": "ProjectMilestoneCredential",
            "user_id_field": "user_id",
            "activity_id_field": "milestone_id",
            "reputation_score": 10
        },
        "AssetPeerReviewed": {
            "credential_type": "PeerReviewCredential",
            "user_id_field": "reviewer_id",
            "activity_id_field": "asset_id",
            "reputation_score": 5
        }
    }

    config = EVENT_TO_VC_MAP.get(event_type)
    if not config:
        logger.warning(f"Unknown trust event type: {event_type}")
        return

    user_id = event_data.get(config["user_id_field"])
    activity_id = event_data.get(config["activity_id_field"])
    tenant_id = event_data.get("tenant_id", "default") # Assume a default tenant for now

    if not user_id or not activity_id:
        logger.error(f"Missing user_id or activity_id in event: {event_data}")
        return

    # Construct the credential subject based on the event
    subject = {
        "id": f"urn:platformq:user:{user_id}",
        "activity": f"urn:platformq:{config['activity_id_field']}:{activity_id}",
        "awardedFor": event_type,
        "eventTimestamp": datetime.fromtimestamp(event_data["timestamp"] / 1000).isoformat() + "Z"
    }

    # Issue the verifiable credential
    await issue_and_record_credential(
        tenant_id=tenant_id,
        subject=subject,
        credential_type=config["credential_type"],
        publisher=publisher
    )

    # Update on-chain reputation
    user_address = get_user_address_from_id(user_id)
    if not user_address:
        logger.warning(f"Skipping reputation update for user {user_id} because they have no linked blockchain address.")
        return

    score_to_add = config["reputation_score"]

    try:
        current_score = reputation_oracle_service.contract.functions.getReputation(user_address).call()
        new_score = current_score + score_to_add
        reputation_oracle_service.set_reputation(user_address, new_score)
        logger.info(f"Updated reputation for {user_address} by {score_to_add}. New score: {new_score}")
    except Exception as e:
        logger.error(f"Failed to update on-chain reputation for {user_address}: {e}")


# --- Pulsar Consumer for Issuance Requests ---
def credential_issuer_loop(app):
    logger.info("Starting credential issuer consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/verifiable-credential-issuance-requests",
        subscription_name="vc-service-issuer-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(IssueVerifiableCredential)
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
                "id": f"urn:platformq:proposal:{event_data.proposal_id}",
                "approvedBy": event_data.approver_id,
                "customer": event_data.customer_name,
                "approvedAt": datetime.fromtimestamp(event_data.event_timestamp / 1000).isoformat() + "Z"
            }

            # Run async function in sync context
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(
                issue_and_record_credential(
                    tenant_id=tenant_id,
                    subject=credential_subject,
                    credential_type="ProposalApprovalCredential",
                    publisher=publisher
                )
            )
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in credential issuer loop: {e}")
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)


# --- SBT Issuance Consumer for High-Value Assets ---
def sbt_issuer_loop(app):
    logger.info("Starting SBT issuer consumer for high-value assets...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    
    # Define the event schema inline
    from pulsar.schema import Record, String, Double, Long
    class SBTIssuanceRequested(Record):
        tenant_id = String()
        asset_id = String()
        asset_type = String()
        asset_value = Double()
        owner_address = String()
        metadata = String()  # JSON string
        event_timestamp = Long()
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/sbt-issuance-requests",
        subscription_name="vc-service-sbt-issuer-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(SBTIssuanceRequested)
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
            logger.info(f"Received SBT issuance request for asset {event_data.asset_id}")
            
            # Parse metadata
            metadata = json.loads(event_data.metadata) if event_data.metadata else {}
            
            # Create credential subject for the high-value asset
            credential_subject = {
                "id": f"urn:platformq:asset:{event_data.asset_id}",
                "assetType": event_data.asset_type,
                "assetValue": event_data.asset_value,
                "ownerAddress": event_data.owner_address,
                "assetName": metadata.get("asset_name", "Unknown"),
                "issuedAt": datetime.fromtimestamp(event_data.event_timestamp / 1000).isoformat() + "Z",
                "valueCategory": "high-value",
                "estimatedValue": metadata.get("estimated_value", f"${event_data.asset_value:.2f}")
            }

            # Run async function in sync context
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Issue the credential with SBT
            vc_data = loop.run_until_complete(
                issue_and_record_credential(
                    tenant_id=tenant_id,
                    subject=credential_subject,
                    credential_type="HighValueAssetCredential",
                    publisher=publisher,
                    options={
                        "store_on_ipfs": True,
                        "encrypt_storage": True,
                        "create_sbt": True,
                        "owner_address": event_data.owner_address,
                        "blockchain": "ethereum",
                        "multi_chain": False
                    }
                )
            )
            
            logger.info(f"SBT issued for high-value asset {event_data.asset_id}: Token ID {vc_data.get('soulBoundToken', {}).get('tokenId')}")
            consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in SBT issuer loop: {e}", exc_info=True)
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)


# Placeholder dependencies - replace with actual implementations
def get_db_session():
    return None

def get_api_key_crud_placeholder():
    return None

def get_user_crud_placeholder():
    return None

def get_password_verifier_placeholder():
    return None

def get_current_tenant_and_user():
    # Placeholder - would get from auth context
    return {"tenant_id": "test-tenant", "user_id": "test-user"}

app = create_base_app(
    service_name="verifiable-credential-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

@app.on_event("startup")
async def on_startup():
    app.state.pulsar_consumer = PulsarConsumer(app)
    app.state.pulsar_consumer.start()

@app.on_event("shutdown")
async def on_shutdown():
    if hasattr(app.state, "pulsar_consumer"):
        app.state.pulsar_consumer.stop()


@app.on_event("startup")
async def startup_event():
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
    app.state.event_publisher = EventPublisher(pulsar_url=pulsar_url)
    app.state.event_publisher.connect()
    app.state.stop_event = threading.Event()
    
    # Start the new trust event consumer
    app.state.trust_consumer = PulsarConsumer(app)
    app.state.trust_consumer.start()
    
    # Initialize blockchain clients
    initialize_blockchain_clients()
    
    # Initialize storage
    initialize_storage()
    
    # Initialize bridge contracts if enabled
    if cross_chain_bridge:
        await cross_chain_bridge.initialize_bridge_contracts()
    
    # Start consumer threads
    # Original credential issuer
    thread = threading.Thread(target=credential_issuer_loop, args=(app,), daemon=True)
    thread.start()
    app.state.consumer_thread = thread
    
    # SBT issuer for high-value assets
    sbt_thread = threading.Thread(target=sbt_issuer_loop, args=(app,), daemon=True)
    sbt_thread.start()
    app.state.sbt_consumer_thread = sbt_thread

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutdown signal received, stopping consumer threads.")
    app.state.stop_event.set()
    app.state.consumer_thread.join()
    if hasattr(app.state, 'sbt_consumer_thread'):
        app.state.sbt_consumer_thread.join()
    if app.state.trust_consumer:
        app.state.trust_consumer.stop()
    if app.state.event_publisher:
        app.state.event_publisher.close()
    if ipfs_storage:
        await ipfs_storage.close()
    logger.info("Consumer threads stopped.")

def get_user_address_from_id(user_id: str) -> Optional[str]:
    """
    Placeholder function to simulate looking up a user's blockchain address.
    In a real system, this would involve an API call to the auth-service or
    querying a shared user database.
    """
    # This mock mapping simulates a lookup table.
    MOCK_USER_ADDRESS_MAP = {
        "user_id_1": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", # Default hardhat account 0
        "user_id_2": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8", # Default hardhat account 1
        "user_id_3": "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC", # Default hardhat account 2
        "test-user": "0x90F79bf6EB2c4f870365E785982E1f101E93b906" # Default hardhat account 3
    }
    address = MOCK_USER_ADDRESS_MAP.get(user_id)
    if not address:
        logger.warning(f"Could not find blockchain address for user_id: {user_id}")
    return address

def timed_lru_cache(seconds: int, maxsize: int = 128):
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime
            return func(*args, **kwargs)

        return wrapped_func
    return wrapper_cache

@timed_lru_cache(seconds=60)
def get_reputation_from_chain(user_address: str) -> int:
    """Cached function to get reputation from the blockchain."""
    logger.info(f"Cache miss. Fetching reputation for {user_address} from blockchain.")
    try:
        score = reputation_oracle_service.contract.functions.getReputation(user_address).call()
        return score
    except Exception as e:
        logger.error(f"Failed to fetch reputation from chain for {user_address}: {e}")
        # Return a default/stale value or raise? For now, return 0
        return 0

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["verifiable-credential-service"])

@app.get("/api/v1/dids/{did}/credentials", response_model=List[Dict[str, Any]])
def get_user_credentials(
    did: str,
    request: Request
):
    """
    Retrieve all verifiable credentials issued to a specific DID by querying the search service.
    """
    # This is a placeholder for getting tenant from a real auth token
    auth_header = request.headers.get("Authorization", "")
    tenant_id = "default" # fallback
    if "mock-service-token-for-tenant-" in auth_header:
        tenant_id = auth_header.replace("Bearer mock-service-token-for-tenant-", "")

    search_query = {
        "query": "",
        "search_type": "verifiable_credential",
        "filters": {
            "credential_subject.id": did # Assuming the subject ID is the DID
        },
        "size": 100
    }
    
    try:
        headers = {"Authorization": auth_header}
        response = requests.post(
            f"{SEARCH_SERVICE_URL}/api/v1/search",
            json=search_query,
            headers=headers,
            timeout=5 # Add a timeout
        )
        response.raise_for_status()
        
        search_results = response.json()
        credentials = [hit["source"] for hit in search_results.get("hits", [])]
        return credentials
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to query search service for credentials: {e}")
        raise HTTPException(status_code=503, detail="The search service is currently unavailable.")

@app.get("/api/v1/reputation/{user_id}")
def get_user_reputation(user_id: str):
    """
    Retrieves a user's reputation score. This endpoint uses a cache
    to avoid excessive calls to the blockchain.
    """
    user_address = get_user_address_from_id(user_id)
    if not user_address:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found or has no linked blockchain address.")
    
    score = get_reputation_from_chain(user_address)
    
    return {"user_id": user_id, "user_address": user_address, "reputation_score": score}

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "verifiable-credential-service is running"}

# Enhanced API Models
class CredentialSubject(BaseModel):
    id: str
    # ... other fields for the subject
    
class IssueRequest(BaseModel):
    subject: Dict[str, Any]
    credential_type: str = Field(alias="type", description="Type of the credential, e.g., 'DAOMembershipCredential'")
    blockchain: Optional[str] = "ethereum"
    multi_chain: Optional[bool] = False
    issuer_did: Optional[str] = None
    store_on_ipfs: Optional[bool] = True
    encrypt_storage: Optional[bool] = True
    create_sbt: Optional[bool] = False
    owner_address: Optional[str] = None
    
@app.post("/api/v1/issue", status_code=201)
async def issue_credential(
    req: IssueRequest,
    request: Request,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Issues a new Verifiable Credential with blockchain anchoring and IPFS storage
    """
    tenant_id = str(context["tenant_id"])
    publisher = request.app.state.event_publisher
    
    options = {
        "blockchain": req.blockchain,
        "multi_chain": req.multi_chain,
        "issuer_did": req.issuer_did,
        "store_on_ipfs": req.store_on_ipfs,
        "encrypt_storage": req.encrypt_storage,
        "create_sbt": req.create_sbt,
        "owner_address": req.owner_address
    }
    
    return await issue_and_record_credential(
        tenant_id=tenant_id,
        subject=req.subject,
        credential_type=req.credential_type,
        publisher=publisher,
        options=options
    )

# Cross-Chain Bridge Endpoints
class BridgeTransferRequest(BaseModel):
    credential_id: str
    credential_data: Dict[str, Any]
    source_chain: str
    target_chain: str

@app.post("/api/v1/bridge/transfer")
async def bridge_credential(
    req: BridgeTransferRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Bridge a credential from one blockchain to another"""
    if not cross_chain_bridge:
        raise HTTPException(status_code=503, detail="Cross-chain bridge not initialized")
    
    tenant_id = str(context["tenant_id"])
    
    result = await cross_chain_bridge.transfer_credential(
        credential_data=req.credential_data,
        source_chain=req.source_chain,
        target_chain=req.target_chain,
        tenant_id=tenant_id
    )
    
    return {
        "request_id": result.request_id,
        "status": result.status.value,
        "source_anchor": result.source_anchor.to_dict() if result.source_anchor else None,
        "target_anchor": result.target_anchor.to_dict() if result.target_anchor else None
    }

@app.get("/api/v1/bridge/status/{request_id}")
async def get_bridge_status(request_id: str):
    """Get status of a bridge transfer request"""
    if not cross_chain_bridge:
        raise HTTPException(status_code=503, detail="Cross-chain bridge not initialized")
    
    result = await cross_chain_bridge.get_bridge_status(request_id)
    if not result:
        raise HTTPException(status_code=404, detail="Bridge request not found")
    
    return {
        "request_id": result.request_id,
        "status": result.status.value,
        "created_at": result.created_at.isoformat(),
        "completed_at": result.completed_at.isoformat() if result.completed_at else None
    }

# Trust Network Endpoints
class TrustRelationshipRequest(BaseModel):
    to_entity: str
    trust_value: float
    credential_type: Optional[str] = None
    evidence: Optional[List[str]] = None

@app.post("/api/v1/trust/relationships")
async def add_trust_relationship(
    req: TrustRelationshipRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Add a trust relationship between entities"""
    from_entity = context["user_id"]
    
    trust_network.add_trust_relationship(
        from_entity=from_entity,
        to_entity=req.to_entity,
        trust_value=req.trust_value,
        credential_type=req.credential_type,
        evidence=req.evidence
    )
    
    return {"status": "success", "from": from_entity, "to": req.to_entity}

@app.get("/api/v1/trust/reputation/{entity_id}")
async def get_entity_reputation(entity_id: str):
    """Get reputation score for an entity"""
    score = trust_network.get_entity_reputation(entity_id)
    if not score:
        raise HTTPException(status_code=404, detail="Entity not found")
    
    return {
        "entity_id": score.entity_id,
        "score": score.score,
        "level": score.level.name,
        "factors": score.factors,
        "last_updated": score.last_updated.isoformat()
    }

@app.get("/api/v1/trust/network/stats")
async def get_trust_network_stats():
    """Get statistics about the trust network"""
    return trust_network.get_network_statistics()

# Verification Network Endpoints
class NetworkVerificationRequest(BaseModel):
    credential_id: str
    credential_data: Dict[str, Any]
    required_verifiers: Optional[int] = 3
    consensus_threshold: Optional[float] = 0.66

@app.post("/api/v1/verify/network")
async def verify_via_network(
    req: NetworkVerificationRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Request verification of a credential from the trust network"""
    requester_id = context["user_id"]
    
    result = await verification_network.request_verification(
        credential_id=req.credential_id,
        credential_data=req.credential_data,
        requester_id=requester_id,
        required_verifiers=req.required_verifiers,
        consensus_threshold=req.consensus_threshold
    )
    
    return result

# Verifiable Presentation Endpoints
class CreatePresentationRequest(BaseModel):
    credentials: List[Dict[str, Any]]
    challenge: str
    domain: str
    purpose: str = "authentication"
    disclosed_claims: Optional[Dict[str, List[str]]] = None

@app.post("/api/v1/presentations/create")
async def create_presentation(
    req: CreatePresentationRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a Verifiable Presentation"""
    holder_did = did_manager.get_did_for_user(context["tenant_id"], context["user_id"])
    
    builder = VerifiablePresentationBuilder(holder_did)
    
    # Create presentation request
    presentation_request = PresentationRequest(
        id=f"pr_{uuid.uuid4().hex}",
        input_descriptors=[],  # Would be populated based on requirements
        purpose=PresentationPurpose(req.purpose),
        challenge=req.challenge,
        domain=req.domain
    )
    
    presentation = builder.create_presentation(
        credentials=req.credentials,
        presentation_request=presentation_request,
        disclosed_claims=req.disclosed_claims
    )
    
    return presentation

# SoulBound Token Endpoints
class MintSBTRequest(BaseModel):
    credential_id: str
    credential_data: Dict[str, Any]
    owner_address: str

@app.post("/api/v1/sbt/mint")
async def mint_soulbound_token(
    req: MintSBTRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Mint a SoulBound Token for a credential"""
    if not sbt_manager:
        raise HTTPException(status_code=503, detail="SBT manager not initialized")
    
    issuer_did = did_manager.get_did_for_user(context["tenant_id"], context["user_id"])
    
    # Store credential on IPFS first
    ipfs_cid = None
    if ipfs_storage:
        receipt = await ipfs_storage.store_credential(
            req.credential_data,
            context["tenant_id"]
        )
        ipfs_cid = receipt["cid"]
    
    sbt = await sbt_manager.mint_sbt(
        credential=req.credential_data,
        owner_address=req.owner_address,
        issuer_address=issuer_did,
        metadata_uri=f"ipfs://{ipfs_cid}" if ipfs_cid else None
    )
    
    return {
        "token_id": sbt.token_id,
        "soul_signature": sbt.soul_signature,
        "metadata_uri": sbt.metadata_uri,
        "created_at": sbt.created_at.isoformat()
    }

@app.get("/api/v1/sbt/{token_id}/verify")
async def verify_soulbound_token(
    token_id: str,
    owner_address: str
):
    """Verify a SoulBound Token"""
    if not sbt_manager:
        raise HTTPException(status_code=503, detail="SBT manager not initialized")
    
    result = await sbt_manager.verify_sbt(token_id, owner_address)
    return result

# Credential Manifest Endpoints
class CreateManifestRequest(BaseModel):
    credential_type: str
    name: str
    description: str
    input_descriptors: List[Dict[str, Any]]
    output_descriptors: List[Dict[str, Any]]

@app.post("/api/v1/manifests")
async def create_credential_manifest(
    req: CreateManifestRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a credential manifest for discovery"""
    issuer_did = did_manager.get_did_for_user(context["tenant_id"], context["user_id"])
    
    manifest = credential_manifest.create_manifest(
        issuer_did=issuer_did,
        credential_type=req.credential_type,
        name=req.name,
        description=req.description,
        input_descriptors=req.input_descriptors,
        output_descriptors=req.output_descriptors
    )
    
    return manifest

# IPFS Storage Endpoints
@app.get("/api/v1/credentials/ipfs/{cid}")
async def retrieve_from_ipfs(
    cid: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Retrieve a credential from IPFS"""
    if not ipfs_storage:
        raise HTTPException(status_code=503, detail="IPFS storage not initialized")
    
    credential = await ipfs_storage.retrieve_credential(
        cid=cid,
        tenant_id=context["tenant_id"]
    )
    
    if not credential:
        raise HTTPException(status_code=404, detail="Credential not found")
    
    return credential

# DID Management Endpoints (existing, kept for compatibility)
class CreateDIDRequest(BaseModel):
    method: str = "web"
    options: Optional[Dict[str, Any]] = {}

@app.post("/api/v1/dids", status_code=201)
async def create_did(
    req: CreateDIDRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a new DID for the tenant"""
    tenant_id = str(context["tenant_id"])
    user_id = str(context["user_id"])
    
    # Create DID for user
    did_doc, key_pair = did_manager.create_did_for_user(
        tenant_id=tenant_id,
        user_id=user_id,
        method=req.method
    )
    
    return {
        "did": did_doc.did,
        "did_document": did_doc.to_dict(),
        "keys": {
            "public_key": key_pair.get("public_key_base58"),
            # Private key would be stored securely, not returned
        }
    }

@app.get("/api/v1/dids/{did}")
async def resolve_did(did: str):
    """Resolve a DID to its document"""
    did_doc = await did_resolver.resolve(did)
    if not did_doc:
        raise HTTPException(status_code=404, detail="DID not found")
    return did_doc.to_dict()

# Selective Disclosure Endpoints (existing, kept for compatibility)
class SelectiveDisclosureRequest(BaseModel):
    credential_id: str
    disclosed_attributes: List[str]
    proof_type: str = "BbsBlsSignature2020"
    holder_did: Optional[str] = None

@app.post("/api/v1/credentials/present")
async def create_presentation(
    req: SelectiveDisclosureRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a verifiable presentation with selective disclosure"""
    # In a real implementation, fetch credential from storage
    # For now, create a mock credential
    credential = {
        "@context": ["https://www.w3.org/2018/credentials/v1"],
        "id": req.credential_id,
        "type": ["VerifiableCredential"],
        "credentialSubject": {
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com",
            "address": "123 Main St"
        }
    }
    
    # Create derived credential with selective disclosure
    derived = selective_disclosure.create_derived_credential(
        original_credential=credential,
        disclosed_attributes=req.disclosed_attributes,
        holder_binding={"did": req.holder_did} if req.holder_did else None
    )
    
    # Create ZKP proof
    proof = zkp_manager.create_proof(
        credential=credential,
        revealed_attributes=req.disclosed_attributes,
        proof_type=req.proof_type
    )
    
    derived["proof"]["zkp"] = {
        "type": proof.proof_type,
        "created": proof.created.isoformat() + "Z",
        "proofValue": proof.proof_value
    }
    
    return derived

# Blockchain Verification Endpoints (existing, kept for compatibility)
@app.get("/api/v1/credentials/{credential_id}/verify")
async def verify_credential(
    credential_id: str,
    blockchain: str = "ethereum"
):
    """Verify a credential's blockchain anchor"""
    if blockchain not in blockchain_clients:
        raise HTTPException(status_code=400, detail=f"Blockchain {blockchain} not supported")
    
    client = blockchain_clients[blockchain]
    if not await client.connect():
        raise HTTPException(status_code=503, detail="Blockchain connection failed")
    
    anchor = await client.verify_credential_anchor(credential_id)
    await client.disconnect()
    
    if not anchor:
        raise HTTPException(status_code=404, detail="Credential anchor not found")
    
    return anchor.to_dict()

@app.get("/api/v1/credentials/{credential_id}/history")
async def get_credential_history(
    credential_id: str,
    blockchain: str = "ethereum"
):
    """Get the history of a credential including revocations"""
    if blockchain not in blockchain_clients:
        raise HTTPException(status_code=400, detail=f"Blockchain {blockchain} not supported")
    
    client = blockchain_clients[blockchain]
    if not await client.connect():
        raise HTTPException(status_code=503, detail="Blockchain connection failed")
    
    history = await client.get_credential_history(credential_id)
    await client.disconnect()
    
    return [anchor.to_dict() for anchor in history]
