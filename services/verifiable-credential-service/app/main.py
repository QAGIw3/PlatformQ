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
# Import blockchain gateway client instead of direct blockchain components
from .blockchain_gateway_client import BlockchainGatewayClient, ChainType, AnchorResult
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
from .api import endpoints, direct_issuance, cross_chain, presentations, kyc_zkp, aml_zkp
from fastapi import FastAPI
from .messaging.pulsar_consumer import start_consumer, stop_consumer, PulsarConsumer
from .blockchain.reputation_oracle import reputation_oracle_service

# from hyperledger.fabric import gateway # Conceptual client
import requests
from functools import lru_cache, wraps
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
platform_settings = config_loader.load_settings()
settings = config_loader.load_settings() # Keep for now for non-secret config
PULSAR_URL = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
SEARCH_SERVICE_URL = settings.get("SEARCH_SERVICE_URL", "http://search-service:80")

# Initialize components
blockchain_gateway_client = None
did_manager = DIDManager()
did_resolver = DIDResolver()
zkp_manager = ZKPManager()
selective_disclosure = SelectiveDisclosure()

# Initialize new components
ipfs_storage = None
distributed_storage = None
trust_network = TrustNetwork()
verification_network = CredentialVerificationNetwork(trust_network)
credential_manifest = CredentialManifest()
sbt_manager = None

def initialize_blockchain_gateway():
    """Initialize blockchain gateway client"""
    global blockchain_gateway_client
    gateway_url = settings.get("BLOCKCHAIN_GATEWAY_URL", "http://blockchain-gateway-service:8000")
    blockchain_gateway_client = BlockchainGatewayClient(gateway_url)
    logger.info(f"Initialized blockchain gateway client with URL: {gateway_url}")
        
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
    encryption_key = platform_settings.get("CREDENTIAL_ENCRYPTION_KEY")
    
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
    
    # Anchor on blockchain(s) via gateway
    blockchain_type = options.get("blockchain", "ethereum")
    if blockchain_gateway_client:
        try:
            anchor = await blockchain_gateway_client.anchor_credential(vc.data, blockchain_type, tenant_id)
            vc.data["proof"]["blockchainAnchor"] = anchor.to_dict()
            logger.info(f"Credential anchored on {blockchain_type}: {anchor.transaction_hash}")
        except Exception as e:
            logger.error(f"Failed to anchor credential on {blockchain_type}: {e}")
    
    # Store credential hash on multiple chains if requested
    if options.get("multi_chain", False) and blockchain_gateway_client:
        chains = options.get("chains", ["ethereum", "polygon", "arbitrum"])
        # Remove already anchored chain
        chains = [c for c in chains if c != blockchain_type]
        
        if chains:
            try:
                results = await blockchain_gateway_client.anchor_multiple_chains(vc.data, chains, tenant_id)
                vc.data["proof"]["additionalAnchors"] = []
                for chain, anchor in results.items():
                    vc.data["proof"]["additionalAnchors"].append(anchor.to_dict())
                    logger.info(f"Additional anchor on {chain}: {anchor.transaction_hash}")
            except Exception as e:
                logger.error(f"Failed to anchor on multiple chains: {e}")
    
    # Create SoulBound Token if requested
    if blockchain_gateway_client and options.get("create_sbt", False):
        owner_address = options.get("owner_address")
        if owner_address:
            try:
                sbt_result = await blockchain_gateway_client.create_soulbound_token(
                    vc.data,
                    owner_address,
                    blockchain_type,
                    metadata_uri=f"ipfs://{ipfs_cid}" if ipfs_cid else None
                )
                vc.data["soulBoundToken"] = {
                    "tokenId": sbt_result.get("token_id"),
                    "transactionHash": sbt_result.get("transaction_hash"),
                    "chain": blockchain_type
                }
                logger.info(f"Created SoulBound Token: {sbt_result.get('token_id')}")
            except Exception as e:
                logger.error(f"Failed to create SoulBound Token: {e}")
    
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

@lru_cache(maxsize=128)
def get_user_did_from_id(user_id: str) -> Optional[str]:
    """
    Retrieves a user's DID from the auth-service, with caching.
    """
    try:
        response = httpx.get(f"{AUTH_SERVICE_URL}/internal/users/{user_id}")
        response.raise_for_status()
        user_data = response.json()
        return user_data.get("did")
    except httpx.RequestError as e:
        logger.error(f"Failed to get user DID from auth-service for user {user_id}: {e}")
        return None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.warning(f"User {user_id} not found in auth-service.")
        else:
            logger.error(f"HTTP error getting user DID for {user_id}: {e}")
        return None


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

    # Resolve user_id to DID
    user_did = get_user_did_from_id(user_id)
    subject_id = user_did if user_did else f"urn:platformq:user:{user_id}"

    # Construct the credential subject based on the event
    subject = {
        "id": subject_id,
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


# --- Pulsar Consumer for Asset Creation VC Requests ---
def asset_creation_vc_consumer_loop(app):
    logger.info("Starting asset creation VC consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    
    # Define the schema inline if not in shared lib
    from pulsar.schema import Record, String, Long
    class AssetCreationCredentialRequested(Record):
        tenant_id = String()
        asset_id = String()
        asset_hash = String()
        creator_did = String()
        asset_name = String()
        asset_type = String()
        asset_metadata = String()
        raw_data_uri = String(required=False)
        creation_timestamp = Long()
        source_tool = String(required=False)
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/asset-creation-credential-requests",
        subscription_name="vc-service-asset-creation-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(AssetCreationCredentialRequested)
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
            logger.info(f"Received asset creation VC request for asset {event_data.asset_id}")
            
            # Construct the subject for the Asset Creation VC
            credential_subject = {
                "id": f"urn:platformq:asset:{event_data.asset_id}",
                "assetHash": event_data.asset_hash,
                "creator": event_data.creator_did,
                "assetName": event_data.asset_name,
                "assetType": event_data.asset_type,
                "metadata": json.loads(event_data.asset_metadata) if event_data.asset_metadata else {},
                "createdAt": datetime.fromtimestamp(event_data.creation_timestamp / 1000).isoformat() + "Z",
                "sourceTool": event_data.source_tool,
                "rawDataUri": event_data.raw_data_uri
            }

            # Run async function in sync context
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            vc_result = loop.run_until_complete(
                issue_and_record_credential(
                    tenant_id=tenant_id,
                    subject=credential_subject,
                    credential_type="AssetCreationCredential",
                    publisher=publisher,
                    options={
                        "store_on_ipfs": True,
                        "create_sbt": False  # Don't create SBT for every asset
                    }
                )
            )
            
            # Update the asset with the VC ID (publish event for digital-asset-service to consume)
            if vc_result and "id" in vc_result:
                from pulsar.schema import Record, String
                class AssetVCCreated(Record):
                    tenant_id = String()
                    asset_id = String()
                    vc_id = String()
                    vc_type = String()
                
                update_event = AssetVCCreated(
                    tenant_id=tenant_id,
                    asset_id=event_data.asset_id,
                    vc_id=vc_result["id"],
                    vc_type="creation"
                )
                
                publisher.publish(
                    topic_base='asset-vc-created-events',
                    tenant_id=tenant_id,
                    schema_class=AssetVCCreated,
                    data=update_event
                )
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in asset creation VC consumer: {e}")
            consumer.negative_acknowledge(msg)

    consumer.close()
    client.close()


# --- Pulsar Consumer for Trust Score VC Requests ---
def trust_score_vc_consumer_loop(app):
    logger.info("Starting trust score VC consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    
    # Define the schema
    from pulsar.schema import Record, String, Double, Long, Array
    class TrustScoreCredentialRequested(Record):
        tenant_id = String()
        user_did = String()
        trust_score = Double()
        evidence_vc_ids = Array(String())
        calculation_timestamp = Long()
        valid_until = Long()
        blockchain_address = String(required=False)
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/trust-score-credential-requests",
        subscription_name="vc-service-trust-score-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(TrustScoreCredentialRequested)
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
            logger.info(f"Received trust score VC request for user {event_data.user_did}")
            
            # Construct the subject for the Trust Score VC
            credential_subject = {
                "id": event_data.user_did,
                "trustScore": event_data.trust_score,
                "calculationEvidence": event_data.evidence_vc_ids,
                "calculatedAt": datetime.fromtimestamp(event_data.calculation_timestamp / 1000).isoformat() + "Z",
                "validUntil": datetime.fromtimestamp(event_data.valid_until / 1000).isoformat() + "Z",
                "scoreCategory": get_trust_level(event_data.trust_score)
            }

            # Run async function in sync context
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            vc_result = loop.run_until_complete(
                issue_and_record_credential(
                    tenant_id=tenant_id,
                    subject=credential_subject,
                    credential_type="TrustScoreCredential",
                    publisher=publisher,
                    options={
                        "store_on_ipfs": True,
                        "create_sbt": event_data.trust_score >= 80,  # Create SBT for high trust scores
                        "blockchain_address": event_data.blockchain_address
                    }
                )
            )
            
            # Update ReputationOracle if blockchain address provided
            if event_data.blockchain_address and vc_result:
                try:
                    loop.run_until_complete(
                        reputation_oracle_service.update_reputation(
                            entity_address=event_data.blockchain_address,
                            new_score=int(event_data.trust_score),
                            vc_hash=Web3.keccak(text=json.dumps(vc_result)),
                            expiry=int(event_data.valid_until / 1000)
                        )
                    )
                    logger.info(f"Updated on-chain reputation for {event_data.blockchain_address}")
                except Exception as e:
                    logger.error(f"Failed to update on-chain reputation: {e}")
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in trust score VC consumer: {e}")
            consumer.negative_acknowledge(msg)

    consumer.close()
    client.close()


def get_trust_level(score: float) -> str:
    """Categorize trust score into levels"""
    if score >= 90:
        return "exceptional"
    elif score >= 75:
        return "high"
    elif score >= 50:
        return "moderate"
    elif score >= 25:
        return "developing"
    else:
        return "initial"


# --- Pulsar Consumer for Processing VC Requests ---
def processing_vc_consumer_loop(app):
    logger.info("Starting processing VC consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(PULSAR_URL)
    
    # Define the schema
    from pulsar.schema import Record, String, Long
    class AssetProcessingCredentialRequested(Record):
        tenant_id = String()
        parent_asset_id = String()
        parent_asset_vc_id = String(required=False)
        output_asset_id = String()
        output_asset_hash = String()
        processing_job_id = String()
        processor_did = String()
        algorithm = String()
        parameters = String()
        processing_timestamp = Long()
        processing_duration_ms = Long(required=False)
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/asset-processing-credential-requests",
        subscription_name="vc-service-processing-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(AssetProcessingCredentialRequested)
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
            logger.info(f"Received processing VC request for job {event_data.processing_job_id}")
            
            # Construct the subject for the Processing VC
            credential_subject = {
                "id": f"urn:platformq:processing:{event_data.processing_job_id}",
                "parentAsset": f"urn:platformq:asset:{event_data.parent_asset_id}",
                "parentAssetVC": event_data.parent_asset_vc_id,
                "outputAsset": f"urn:platformq:asset:{event_data.output_asset_id}",
                "outputAssetHash": event_data.output_asset_hash,
                "processor": event_data.processor_did,
                "algorithm": event_data.algorithm,
                "parameters": json.loads(event_data.parameters) if event_data.parameters else {},
                "processedAt": datetime.fromtimestamp(event_data.processing_timestamp / 1000).isoformat() + "Z",
                "processingDuration": event_data.processing_duration_ms
            }

            # Run async function in sync context
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            vc_result = loop.run_until_complete(
                issue_and_record_credential(
                    tenant_id=tenant_id,
                    subject=credential_subject,
                    credential_type="AssetProcessingCredential",
                    publisher=publisher,
                    options={
                        "store_on_ipfs": True,
                        "create_sbt": False
                    }
                )
            )
            
            # Update the output asset with the processing VC
            if vc_result and "id" in vc_result:
                from pulsar.schema import Record, String
                class AssetVCCreated(Record):
                    tenant_id = String()
                    asset_id = String()
                    vc_id = String()
                    vc_type = String()
                
                update_event = AssetVCCreated(
                    tenant_id=tenant_id,
                    asset_id=event_data.output_asset_id,
                    vc_id=vc_result["id"],
                    vc_type="processing"
                )
                
                publisher.publish(
                    topic_base='asset-vc-created-events',
                    tenant_id=tenant_id,
                    schema_class=AssetVCCreated,
                    data=update_event
                )
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in processing VC consumer: {e}")
            consumer.negative_acknowledge(msg)

    consumer.close()
    client.close()


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
    initialize_blockchain_gateway()
    
    # Initialize storage
    initialize_storage()
    
    # Initialize cross-chain bridge
    try:
        from .blockchain.cross_chain_bridge import CrossChainBridge
        bridge_private_key = platform_settings.get("BRIDGE_PRIVATE_KEY")
        if bridge_private_key:
            app.state.cross_chain_bridge = CrossChainBridge(
                private_key=bridge_private_key,
                event_publisher=app.state.event_publisher
            )
            logger.info("Cross-chain bridge initialized")
        else:
            logger.warning("BRIDGE_PRIVATE_KEY not set, cross-chain bridge disabled")
            app.state.cross_chain_bridge = None
    except Exception as e:
        logger.error(f"Failed to initialize cross-chain bridge: {e}")
        app.state.cross_chain_bridge = None
    
    # Initialize VP builder
    app.state.vp_builder = VerifiablePresentationBuilder()
    app.state.did_manager = did_manager
    
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
    
    # Asset creation VC issuer
    asset_vc_thread = threading.Thread(target=asset_creation_vc_consumer_loop, args=(app,), daemon=True)
    asset_vc_thread.start()
    app.state.asset_vc_consumer_thread = asset_vc_thread
    
    # Trust score VC issuer
    trust_vc_thread = threading.Thread(target=trust_score_vc_consumer_loop, args=(app,), daemon=True)
    trust_vc_thread.start()
    app.state.trust_vc_consumer_thread = trust_vc_thread
    
    # Processing VC issuer
    processing_vc_thread = threading.Thread(target=processing_vc_consumer_loop, args=(app,), daemon=True)
    processing_vc_thread.start()
    app.state.processing_vc_consumer_thread = processing_vc_thread

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutdown signal received, stopping consumer threads.")
    app.state.stop_event.set()
    app.state.consumer_thread.join()
    if hasattr(app.state, 'sbt_consumer_thread'):
        app.state.sbt_consumer_thread.join()
    if hasattr(app.state, 'asset_vc_consumer_thread'):
        app.state.asset_vc_consumer_thread.join()
    if hasattr(app.state, 'trust_vc_consumer_thread'):
        app.state.trust_vc_consumer_thread.join()
    if hasattr(app.state, 'processing_vc_consumer_thread'):
        app.state.processing_vc_consumer_thread.join()
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

# Cross-chain operations
app.include_router(cross_chain.router, prefix="/api/v1/cross-chain", tags=["cross-chain"])
app.include_router(presentations.router, prefix="/api/v1", tags=["presentations"])
app.include_router(direct_issuance.router, prefix="/api/v1", tags=["direct-issuance"])

# KYC Zero-Knowledge Proof operations
app.include_router(kyc_zkp.router, prefix="/api/v1", tags=["kyc-zkp"])

# AML Zero-Knowledge Proof operations
app.include_router(aml_zkp.router, prefix="/api/v1", tags=["aml-zkp"])

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

# Blockchain Verification Endpoints (updated to use gateway)
@app.get("/api/v1/credentials/{credential_id}/verify")
async def verify_credential(
    credential_id: str,
    blockchain: str = "ethereum"
):
    """Verify a credential's blockchain anchor via gateway"""
    if not blockchain_gateway_client:
        raise HTTPException(status_code=503, detail="Blockchain gateway not initialized")
    
    try:
        anchor = await blockchain_gateway_client.verify_anchor(credential_id, blockchain)
        
        if not anchor:
            raise HTTPException(status_code=404, detail="Credential anchor not found")
        
        return anchor.to_dict()
    except Exception as e:
        logger.error(f"Error verifying credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/credentials/{credential_id}/history")
async def get_credential_history(
    credential_id: str,
    blockchain: str = "ethereum"
):
    """Get the history of a credential including revocations"""
    # Note: This endpoint would need to be implemented in blockchain-gateway-service
    # For now, return a placeholder response
    logger.warning("Credential history endpoint not yet implemented in gateway")
    return {
        "message": "Credential history tracking coming soon",
        "credential_id": credential_id,
        "chain": blockchain
    }
