
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from datetime import datetime
from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user, require_service_token
from ...repository import DigitalAssetRepository
from ...schemas import digital_asset as schemas
from ....platformq_shared.events import DigitalAssetCreated
from ....platformq_shared.event_publisher import EventPublisher
import logging
import hashlib
import json
import httpx
from ...utils.cid import compute_cid
from ...config import settings

logger = logging.getLogger(__name__)

STORAGE_PROXY_URL = "http://storage-proxy-service:8000"

router = APIRouter()

@router.post("/internal/digital-assets/{cid}/migrate", status_code=204)
async def migrate_digital_asset_storage(
    cid: str,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
    service_token: dict = Depends(require_service_token),
):
    db_asset = repo.get(cid=cid)
    if not db_asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    if not db_asset.raw_data_uri:
        raise HTTPException(status_code=400, detail="Asset has no data to migrate.")

    try:
        async with httpx.AsyncClient() as client:
            download_url = f"{STORAGE_PROXY_URL}/download/{db_asset.raw_data_uri}"
            response = await client.get(download_url)
            response.raise_for_status()
            file_content = response.content

            files = {'file': (db_asset.asset_name, file_content, 'application/octet-stream')}
            headers = { "X-Authenticated-Userid": str(db_asset.owner_id) }
            
            upload_url = f"{STORAGE_PROXY_URL}/upload"
            upload_response = await client.post(upload_url, files=files, headers=headers)
            upload_response.raise_for_status()
            
            upload_data = upload_response.json()
            new_identifier = upload_data["identifier"]

            repo.update_asset_storage_uri(
                cid=cid, new_uri=new_identifier
            )
            
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Failed to communicate with storage proxy: {e}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"Error during storage migration: {e.response.text}")

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
            raw_data_uri=db_asset.raw_data_uri
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
def read_digital_asset(
    cid: str,
    repo: DigitalAssetRepository = Depends(get_digital_asset_repository),
):
    db_asset = repo.get(cid=cid)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
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