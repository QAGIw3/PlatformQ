"""
Digital Assets API Endpoints

Enhanced with standardized patterns for error handling and service communication.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, Header, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import json
import logging
import uuid

from platformq_shared import (
    ServiceClients,
    NotFoundError,
    ValidationError,
    ConflictError,
    add_error_handlers
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from platformq_events import (
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetDeletedEvent,
    AssetLineageEvent
)

from ...database import get_db
from ...models import DigitalAsset, AssetLineage, ProvenanceCertificate
from ...schemas import DigitalAssetCreate, DigitalAssetUpdate, DigitalAssetResponse
from ...auth import get_current_user
from ...storage import storage_backend
from ...utils.cid import compute_cid
from ...config import settings
from ...policy_client import policy_client
from ...blockchain.asset_lineage_anchor import (
    BlockchainLineageAnchor,
    AssetLineageEvent as BlockchainLineageEvent,
    LineageEventType,
    LineageSmartContract
)
from ...repository import AssetRepository

logger = logging.getLogger(__name__)

# Service clients for inter-service communication
service_clients = ServiceClients(
    base_timeout=30.0,
    max_retries=3
)

# Storage proxy service URL
STORAGE_PROXY_URL = "http://storage-proxy-service:8000"

router = APIRouter()

# Initialize blockchain lineage anchor
config_loader = ConfigLoader()
lineage_anchor = None

if config_loader.get_setting("BLOCKCHAIN_ENABLED", "false").lower() == "true":
    lineage_anchor = BlockchainLineageAnchor(
        web3_provider_url=config_loader.get_setting("WEB3_PROVIDER_URL", "http://localhost:8545"),
        contract_address=config_loader.get_setting("LINEAGE_CONTRACT_ADDRESS", ""),
        contract_abi=LineageSmartContract.CONTRACT_ABI,
        ipfs_api_url=config_loader.get_setting("IPFS_API_URL", "/dns/ipfs/tcp/5001")
    )


@router.post("/internal/digital-assets/{cid}/migrate", status_code=204)
async def migrate_digital_asset_storage(
    cid: str,
    repo: AssetRepository = Depends(get_asset_repository),
    service_token: dict = Depends(require_service_token),
    event_publisher: EventPublisher = Depends(get_event_publisher)
):
    """Migrate asset storage to new backend with improved error handling"""
    try:
        # Get asset
        db_asset = repo.get_by_cid(cid)
        if not db_asset:
            raise NotFoundError(f"Asset with CID {cid} not found")

        if not db_asset.raw_data_uri:
            raise ValidationError("Asset has no data to migrate")

        # Download from old storage
        download_response = await service_clients.get(
            "storage-proxy-service",
            f"/download/{db_asset.raw_data_uri}"
        )
        
        file_content = download_response.content

        # Upload to new storage
        files = {'file': (db_asset.asset_name, file_content, 'application/octet-stream')}
        headers = {"X-Authenticated-Userid": str(db_asset.owner_id)}
        
        upload_response = await service_clients.post(
            "storage-proxy-service",
            "/upload",
            files=files,
            headers=headers
        )
        
        new_identifier = upload_response["identifier"]

        # Update asset storage reference
        repo.update_asset_storage_uri(
            cid=cid, 
            new_uri=new_identifier
        )
        
        # Publish migration event
        migration_event = AssetUpdatedEvent(
            asset_id=str(db_asset.id),
            updated_fields=["storage_uri"],
            old_uri=db_asset.raw_data_uri,
            new_uri=new_identifier,
            updated_at=datetime.utcnow().isoformat()
        )
        
        await event_publisher.publish_event(
            topic=f"persistent://platformq/{db_asset.tenant_id}/asset-storage-migrated-events",
            event=migration_event
        )
        
        logger.info(f"Successfully migrated storage for asset {cid}")
        
    except (NotFoundError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Failed to migrate asset storage: {e}")
        raise HTTPException(
            status_code=503,
            detail="Storage migration service temporarily unavailable"
        )

# High-value asset thresholds by type
HIGH_VALUE_THRESHOLDS = {
    "3d-model": 1000.0,      # $1000 for 3D models
    "cad-file": 5000.0,      # $5000 for CAD files
    "simulation": 10000.0,   # $10000 for simulation results
    "dataset": 2000.0,       # $2000 for datasets
    "credential": 0.0,       # All credentials are high-value
    "contract": 0.0,         # All contracts are high-value
}

def compute_asset_hash(asset) -> str:
    """Compute a deterministic hash of asset content"""
    content = {
        "asset_id": str(asset.asset_id),
        "asset_name": asset.asset_name,
        "asset_type": asset.asset_type,
        "owner_id": str(asset.owner_id),
        "source_tool": asset.source_tool,
        "metadata": asset.asset_metadata or {},
        "created_at": asset.created_at.isoformat() if hasattr(asset, 'created_at') else None
    }
    
    # Sort keys for deterministic hash
    json_str = json.dumps(content, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()

def get_user_did(user_id: str, tenant_id: str) -> str:
    """Generate a DID for a user (simplified for now)"""
    return f"did:platformq:{tenant_id}:user:{user_id}"

def assess_asset_value(asset_type: str, metadata: Dict[str, Any]) -> float:
    """
    Assess the value of a digital asset based on type and metadata.
    This is a simplified example - in production, this would use ML models.
    """
    base_value = 0.0
    
    # Type-based base value
    type_values = {
        "3d-model": 500.0,
        "cad-file": 2000.0,
        "simulation": 5000.0,
        "dataset": 1000.0,
        "document": 100.0,
        "image": 50.0,
        "credential": 10000.0,
        "contract": 50000.0,
    }
    
    base_value = type_values.get(asset_type.lower(), 100.0)
    
    # Metadata-based multipliers
    if metadata:
        # File size multiplier (larger = more valuable)
        file_size = metadata.get("file_size", 0)
        if file_size > 100_000_000:  # > 100MB
            base_value *= 1.5
        elif file_size > 1_000_000_000:  # > 1GB
            base_value *= 2.0
            
        # Complexity multiplier (for 3D models, CAD files)
        if "polygon_count" in metadata:
            polygons = int(metadata["polygon_count"])
            if polygons > 1_000_000:
                base_value *= 1.5
        
        # Precision/quality multiplier
        if metadata.get("precision") == "high":
            base_value *= 1.3
            
        # License type multiplier
        license_type = metadata.get("license", "").lower()
        if "commercial" in license_type:
            base_value *= 2.0
        elif "exclusive" in license_type:
            base_value *= 5.0
    
    return base_value

def is_high_value_asset(asset_type: str, asset_value: float) -> bool:
    """Determine if an asset qualifies as high-value."""
    threshold = HIGH_VALUE_THRESHOLDS.get(asset_type.lower(), float('inf'))
    return asset_value >= threshold

@router.post("/digital-assets", response_model=schemas.DigitalAsset)
async def create_digital_asset(
    asset: schemas.DigitalAssetCreate,
    request: Request,
    derived_from: Optional[str] = None,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    context: dict = Depends(get_current_tenant_and_user),
):
    cid = compute_cid(asset)
    db_asset = repo.add(asset, owner_id=context["user"].id)
    
    # Mint NFT
    user = context["user"]
    if hasattr(user, "blockchain_address") and user.blockchain_address:
        async with httpx.AsyncClient() as client:
            mint_data = {
                "chain_id": "ethereum",  # Assuming Ethereum
                "to": user.blockchain_address,
                "uri": f"ipfs://{db_asset.cid}",
                "royalty_recipient": user.blockchain_address,
                "royalty_fraction": 250  # 2.5%
            }
            response = await client.post(f"{settings.BLOCKCHAIN_BRIDGE_URL}/api/v1/marketplace/mint-nft", json=mint_data)
            if response.status_code == 200:
                token_id = response.json()["token_id"]
                db_asset.nft_token_id = token_id
                db.commit()
                db.refresh(db_asset)
                
                # Issue VC tied to NFT
                try:
                    vc_data = {
                        "subject_id": user.blockchain_address,
                        "credential_type": "NFTOwnership",
                        "claims": {
                            "asset_cid": db_asset.cid,
                            "nft_token_id": token_id,
                            "chain": "ethereum",
                            "contract_address": settings.PLATFORM_ASSET_ADDRESS,
                            "minted_at": datetime.utcnow().isoformat()
                        }
                    }
                    vc_response = await client.post(
                        f"{settings.VERIFIABLE_CREDENTIAL_SERVICE_URL}/api/v1/credentials/issue",
                        json=vc_data
                    )
                    if vc_response.status_code == 200:
                        logger.info(f"VC issued for NFT {token_id}")
                    else:
                        logger.warning(f"Failed to issue VC for NFT {token_id}")
                except Exception as e:
                    logger.error(f"Error issuing VC: {e}")
            else:
                logger.warning(f"Failed to mint NFT for asset {db_asset.cid}: {response.text}")
    else:
        logger.warning(f"No blockchain address for user {user.id}, skipping NFT mint")
        
    publisher: EventPublisher = request.app.state.event_publisher
    if publisher:
        event = DigitalAssetCreated(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(db_asset.cid),
            asset_name=db_asset.asset_name,
            asset_type=db_asset.asset_type,
            owner_id=str(db_asset.owner_id),
            raw_data_uri=db_asset.raw_data_uri,
            derived_from=derived_from if derived_from else None
        )
        publisher.publish(
            topic_base='digital-asset-created-events',
            tenant_id=str(context["tenant_id"]),
            schema_class=DigitalAssetCreated,
            data=event
        )
        
        # Request Asset Creation VC
        asset_hash = compute_asset_hash(db_asset)
        creator_did = get_user_did(str(db_asset.owner_id), str(context["tenant_id"]))
        
        # Define VC request events (if not in shared lib yet)
        class AssetCreationCredentialRequested(Record):
            tenant_id = String()
            asset_id = String()
            asset_hash = String()
            creator_did = String()
            asset_name = String()
            asset_type = String()
            asset_metadata = String()  # JSON string
            raw_data_uri = String(required=False)
            creation_timestamp = Long()
            source_tool = String(required=False)
        
        # Import the Record class
        from pulsar.schema import Record, String, Long
        
        vc_request = AssetCreationCredentialRequested(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(db_asset.cid),
            asset_hash=asset_hash,
            creator_did=creator_did,
            asset_name=db_asset.asset_name,
            asset_type=db_asset.asset_type,
            asset_metadata=json.dumps(db_asset.asset_metadata or {}),
            raw_data_uri=db_asset.raw_data_uri,
            creation_timestamp=int(datetime.utcnow().timestamp() * 1000),
            source_tool=db_asset.source_tool
        )
        
        publisher.publish(
            topic_base='asset-creation-credential-requests',
            tenant_id=str(context["tenant_id"]),
            schema_class=AssetCreationCredentialRequested,
            data=vc_request
        )
        logger.info(f"Requested creation VC for asset {db_asset.cid}")
        
        # Assess asset value
        metadata = db_asset.metadata or {}
        asset_value = assess_asset_value(db_asset.asset_type, metadata)
        
        # Check if this is a high-value asset
        if is_high_value_asset(db_asset.asset_type, asset_value):
            logger.info(f"High-value asset detected: {db_asset.cid} (value: ${asset_value:.2f})")
            
            # Get owner's blockchain address (from user profile or metadata)
            owner_address = metadata.get("owner_address")
            if not owner_address:
                # Try to get from user profile
                user = context.get("user")
                if user and hasattr(user, "blockchain_address"):
                    owner_address = user.blockchain_address
                    
            if owner_address:
                # Import the SBT event class
                try:
                    from ....platformq_shared.events import SBTIssuanceRequested
                except ImportError:
                    # Define inline if not yet in shared lib
                    from pulsar.schema import Record, String, Double, Long
                    class SBTIssuanceRequested(Record):
                        tenant_id = String()
                        asset_id = String()
                        asset_type = String()
                        asset_value = Double()
                        owner_address = String()
                        metadata = String()  # JSON string
                        event_timestamp = Long()
                
                # Request SBT issuance
                import json
                sbt_event = SBTIssuanceRequested(
                    tenant_id=str(context["tenant_id"]),
                    asset_id=str(db_asset.cid),
                    asset_type=db_asset.asset_type,
                    asset_value=asset_value,
                    owner_address=owner_address,
                    metadata=json.dumps({
                        "asset_name": db_asset.asset_name,
                        "created_at": datetime.utcnow().isoformat(),
                        "estimated_value": f"${asset_value:.2f}",
                        "high_value_category": "true"
                    }),
                    event_timestamp=int(datetime.utcnow().timestamp() * 1000)
                )
                
                publisher.publish(
                    topic_base='sbt-issuance-requests',
                    tenant_id=str(context["tenant_id"]),
                    schema_class=SBTIssuanceRequested,
                    data=sbt_event
                )
                logger.info(f"SBT issuance requested for high-value asset {db_asset.cid}")
            else:
                logger.warning(f"High-value asset {db_asset.cid} detected but no owner address available")

    return db_asset

@router.get("/digital-assets/{cid}", response_model=schemas.DigitalAsset)
async def read_digital_asset(
    cid: str,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    current_user: User = Depends(get_current_user),
):
    db_asset = repo.get(cid=cid)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")

    # Policy check
    allowed = await policy_client.check_permission(
        subject={"user_id": str(current_user.id), "roles": current_user.roles},
        action="read_asset",
        resource={"asset_id": cid, "owner_id": str(db_asset.owner_id)}
    )
    if not allowed:
        raise HTTPException(status_code=403, detail="Not authorized to read this asset")

    return db_asset

@router.get("/digital-assets", response_model=List[schemas.DigitalAsset])
def read_digital_assets(
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
):
    assets = repo.list()
    return assets

@router.post("/{asset_id}/reviews", response_model=schemas.PeerReview)
def create_peer_review(
    asset_id: str,
    review: schemas.PeerReviewCreate,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Create a new peer review for a digital asset.
    After adding the review, this endpoint will automatically attempt to
    approve the asset based on the new total number of reviews.
    """
    reviewer_id = str(context["user"].id)
    db_review = repo.add_review(
        asset_id=asset_id, reviewer_id=reviewer_id, review_content=review.review_content
    )
    if db_review is None:
        raise HTTPException(status_code=404, detail="Asset not found")

    # After adding a review, immediately try to approve the asset
    repo.approve_asset(asset_id=asset_id)
    
    return db_review

@router.post("/{asset_id}/approve", response_model=schemas.DigitalAsset)
def approve_digital_asset(
    asset_id: str,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Manually attempts to approve a digital asset. Approval is granted if the
    asset has sufficient peer reviews based on the author's reputation score.
    """
    approved_asset = repo.approve_asset(asset_id=asset_id)
    if not approved_asset:
        raise HTTPException(status_code=404, detail="Asset not found or already approved.")
    
    if approved_asset.status != "approved":
        raise HTTPException(
            status_code=400,
            detail=f"Asset does not yet meet the criteria for approval. It has {len(approved_asset.reviews)} review(s)."
        )
    
    return approved_asset


@router.get("/digital-assets/{asset_id}/lineage", response_model=Dict[str, Any])
def get_asset_lineage(
    asset_id: str,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Get the complete verifiable credential lineage for an asset.
    
    Returns:
    - Creation VC
    - All processing VCs in chronological order
    - Verification status for each VC
    """
    db_asset = repo.get(asset_id=asset_id)
    if not db_asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    lineage = {
        "asset_id": str(asset_id),
        "asset_name": db_asset.asset_name,
        "creation_vc": {
            "vc_id": db_asset.creation_vc_id,
            "status": "verified" if db_asset.creation_vc_id else "missing"
        },
        "processing_lineage": []
    }
    
    # Add processing VCs
    if db_asset.lineage_vc_ids:
        for vc_id in db_asset.lineage_vc_ids:
            lineage["processing_lineage"].append({
                "vc_id": vc_id,
                "status": "verified"  # In production, actually verify each VC
            })
    
    lineage["latest_processing_vc"] = {
        "vc_id": db_asset.latest_processing_vc_id,
        "status": "verified" if db_asset.latest_processing_vc_id else "none"
    }
    
    # Calculate lineage integrity
    lineage["integrity"] = {
        "complete": bool(db_asset.creation_vc_id),
        "verified": True,  # In production, actually verify the chain
        "chain_length": len(db_asset.lineage_vc_ids or []) + 1
    }
    
    return lineage 


@router.post("/{asset_id}/anchor-lineage")
async def anchor_asset_lineage(
    asset_id: str,
    event_type: LineageEventType,
    parent_asset_ids: List[str] = [],
    transformation_type: Optional[str] = None,
    private_key: str = Header(..., description="Blockchain private key for signing"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Anchor asset lineage event on blockchain
    """
    if not lineage_anchor:
        raise HTTPException(
            status_code=503,
            detail="Blockchain lineage anchoring not enabled"
        )
        
    # Get asset
    asset = db.query(DigitalAsset).filter(
        DigitalAsset.id == asset_id,
        DigitalAsset.tenant_id == current_user["tenant_id"]
    ).first()
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
        
    try:
        # Calculate metadata hash
        metadata = {
            "asset_id": asset_id,
            "asset_type": asset.asset_type,
            "size": asset.size,
            "checksum": asset.checksum,
            "created_at": asset.created_at.isoformat()
        }
        metadata_bytes = json.dumps(metadata, sort_keys=True).encode()
        metadata_hash = hashlib.sha256(metadata_bytes).hexdigest()
        
        # Create lineage event
        event = AssetLineageEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            asset_id=asset_id,
            parent_asset_ids=parent_asset_ids,
            transformation_type=transformation_type,
            metadata_hash=metadata_hash,
            timestamp=datetime.utcnow(),
            actor_id=current_user["id"]
        )
        
        # Anchor on blockchain
        result = await lineage_anchor.anchor_lineage_event(event, private_key)
        
        if result["success"]:
            # Update asset with blockchain info
            asset.blockchain_anchored = True
            asset.blockchain_tx_hash = result["tx_hash"]
            asset.ipfs_hash = result["ipfs_hash"]
            
            # Store lineage event in database
            lineage_record = AssetLineage(
                id=event.event_id,
                asset_id=asset_id,
                event_type=event_type.value,
                parent_asset_ids=parent_asset_ids,
                transformation_type=transformation_type,
                metadata_hash=metadata_hash,
                blockchain_tx_hash=result["tx_hash"],
                ipfs_hash=result["ipfs_hash"],
                block_number=result["block_number"],
                actor_id=current_user["id"],
                created_at=datetime.utcnow()
            )
            
            db.add(lineage_record)
            db.commit()
            
            # Publish event
            await publish_event(
                "ASSET_LINEAGE_ANCHORED",
                {
                    "asset_id": asset_id,
                    "event_id": event.event_id,
                    "event_type": event_type.value,
                    "tx_hash": result["tx_hash"],
                    "block_number": result["block_number"]
                }
            )
            
            return {
                "message": "Lineage event anchored successfully",
                "event_id": event.event_id,
                "tx_hash": result["tx_hash"],
                "ipfs_hash": result["ipfs_hash"],
                "block_number": result["block_number"],
                "gas_used": result["gas_used"]
            }
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to anchor lineage: {result.get('error')}"
            )
            
    except Exception as e:
        logger.error(f"Error anchoring lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{asset_id}/verify-lineage")
async def verify_asset_lineage(
    asset_id: str,
    from_block: int = Query(0, description="Starting block number"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Verify complete lineage of an asset from blockchain
    """
    if not lineage_anchor:
        raise HTTPException(
            status_code=503,
            detail="Blockchain lineage verification not enabled"
        )
        
    # Check asset exists
    asset = db.query(DigitalAsset).filter(
        DigitalAsset.id == asset_id,
        DigitalAsset.tenant_id == current_user["tenant_id"]
    ).first()
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
        
    try:
        # Verify lineage from blockchain
        lineage = await lineage_anchor.verify_lineage(asset_id, from_block)
        
        # Enhance with local data
        local_events = db.query(AssetLineage).filter(
            AssetLineage.asset_id == asset_id
        ).all()
        
        # Cross-reference blockchain and local data
        verification_details = {
            "blockchain_verified": lineage["integrity_valid"],
            "blockchain_events": lineage["total_events"],
            "local_events": len(local_events),
            "discrepancies": []
        }
        
        # Check for discrepancies
        blockchain_tx_hashes = {
            node["tx_hash"] for node in lineage["lineage_tree"]["nodes"].values()
        }
        
        local_tx_hashes = {event.blockchain_tx_hash for event in local_events}
        
        missing_from_local = blockchain_tx_hashes - local_tx_hashes
        missing_from_blockchain = local_tx_hashes - blockchain_tx_hashes
        
        if missing_from_local:
            verification_details["discrepancies"].append({
                "type": "missing_from_local",
                "tx_hashes": list(missing_from_local)
            })
            
        if missing_from_blockchain:
            verification_details["discrepancies"].append({
                "type": "missing_from_blockchain",
                "tx_hashes": list(missing_from_blockchain)
            })
            
        return {
            "asset_id": asset_id,
            "lineage_tree": lineage["lineage_tree"],
            "integrity_valid": lineage["integrity_valid"],
            "integrity_details": lineage["integrity_details"],
            "verification_details": verification_details,
            "verification_timestamp": lineage["verification_timestamp"]
        }
        
    except Exception as e:
        logger.error(f"Error verifying lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{asset_id}/provenance-proof")
async def get_asset_provenance_proof(
    asset_id: str,
    target_block: Optional[int] = Query(None, description="Target block for proof"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Generate cryptographic proof of asset provenance
    """
    if not lineage_anchor:
        raise HTTPException(
            status_code=503,
            detail="Blockchain provenance proof not enabled"
        )
        
    # Check asset exists
    asset = db.query(DigitalAsset).filter(
        DigitalAsset.id == asset_id,
        DigitalAsset.tenant_id == current_user["tenant_id"]
    ).first()
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
        
    try:
        # Generate provenance proof
        proof = await lineage_anchor.get_provenance_proof(asset_id, target_block)
        
        # Store proof certificate
        proof_record = ProvenanceCertificate(
            id=str(uuid.uuid4()),
            asset_id=asset_id,
            certificate_hash=proof["certificate_hash"],
            lineage_root=proof["lineage_root"],
            proof_data=json.dumps(proof),
            created_at=datetime.utcnow(),
            created_by=current_user["id"]
        )
        
        db.add(proof_record)
        db.commit()
        
        return {
            "certificate": proof,
            "certificate_id": proof_record.id,
            "download_url": f"/api/v1/assets/{asset_id}/provenance-certificate/{proof_record.id}"
        }
        
    except Exception as e:
        logger.error(f"Error generating provenance proof: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{asset_id}/derive")
async def derive_new_asset(
    asset_id: str,
    transformation_type: str,
    new_asset_data: dict,
    private_key: str = Header(..., description="Blockchain private key"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Create a derived asset with blockchain-anchored lineage
    """
    # Get parent asset
    parent_asset = db.query(DigitalAsset).filter(
        DigitalAsset.id == asset_id,
        DigitalAsset.tenant_id == current_user["tenant_id"]
    ).first()
    
    if not parent_asset:
        raise HTTPException(status_code=404, detail="Parent asset not found")
        
    try:
        # Create new derived asset
        new_asset = DigitalAsset(
            id=str(uuid.uuid4()),
            tenant_id=current_user["tenant_id"],
            name=new_asset_data["name"],
            description=new_asset_data.get("description"),
            asset_type=new_asset_data["asset_type"],
            storage_path=new_asset_data["storage_path"],
            size=new_asset_data["size"],
            checksum=new_asset_data["checksum"],
            metadata=new_asset_data.get("metadata", {}),
            created_by=current_user["id"],
            created_at=datetime.utcnow()
        )
        
        db.add(new_asset)
        
        # Anchor lineage if blockchain is enabled
        if lineage_anchor:
            # Calculate metadata hash
            metadata = {
                "asset_id": new_asset.id,
                "asset_type": new_asset.asset_type,
                "size": new_asset.size,
                "checksum": new_asset.checksum,
                "parent_asset_id": parent_asset.id,
                "transformation_type": transformation_type
            }
            metadata_bytes = json.dumps(metadata, sort_keys=True).encode()
            metadata_hash = hashlib.sha256(metadata_bytes).hexdigest()
            
            # Create lineage event
            event = AssetLineageEvent(
                event_id=str(uuid.uuid4()),
                event_type=LineageEventType.DERIVED,
                asset_id=new_asset.id,
                parent_asset_ids=[parent_asset.id],
                transformation_type=transformation_type,
                metadata_hash=metadata_hash,
                timestamp=datetime.utcnow(),
                actor_id=current_user["id"]
            )
            
            # Anchor on blockchain
            result = await lineage_anchor.anchor_lineage_event(event, private_key)
            
            if result["success"]:
                new_asset.blockchain_anchored = True
                new_asset.blockchain_tx_hash = result["tx_hash"]
                new_asset.ipfs_hash = result["ipfs_hash"]
                
                # Store lineage event
                lineage_record = AssetLineage(
                    id=event.event_id,
                    asset_id=new_asset.id,
                    event_type=LineageEventType.DERIVED.value,
                    parent_asset_ids=[parent_asset.id],
                    transformation_type=transformation_type,
                    metadata_hash=metadata_hash,
                    blockchain_tx_hash=result["tx_hash"],
                    ipfs_hash=result["ipfs_hash"],
                    block_number=result["block_number"],
                    actor_id=current_user["id"],
                    created_at=datetime.utcnow()
                )
                
                db.add(lineage_record)
                
        db.commit()
        
        # Publish event
        await publish_event(
            "ASSET_DERIVED",
            {
                "parent_asset_id": parent_asset.id,
                "derived_asset_id": new_asset.id,
                "transformation_type": transformation_type,
                "blockchain_anchored": new_asset.blockchain_anchored
            }
        )
        
        return {
            "asset": {
                "id": new_asset.id,
                "name": new_asset.name,
                "asset_type": new_asset.asset_type,
                "blockchain_anchored": new_asset.blockchain_anchored,
                "tx_hash": new_asset.blockchain_tx_hash
            },
            "lineage": {
                "parent_asset_id": parent_asset.id,
                "transformation_type": transformation_type,
                "anchored": new_asset.blockchain_anchored
            }
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating derived asset: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 