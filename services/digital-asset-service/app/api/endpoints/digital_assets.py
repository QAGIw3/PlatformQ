
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from uuid import UUID
from datetime import datetime
from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user
from ...crud import crud_digital_asset, crud_processing_rule
from ...schemas import digital_asset as schemas
from ...schemas.asset import PeerReviewCreate, DigitalAsset
from ....platformq_shared.events import DigitalAssetCreated
from ....platformq_shared.event_publisher import EventPublisher
import logging
from ...crud.asset import asset as crud_asset
from .deps import get_current_context

logger = logging.getLogger(__name__)

router = APIRouter()

# High-value asset thresholds by type
HIGH_VALUE_THRESHOLDS = {
    "3d-model": 1000.0,      # $1000 for 3D models
    "cad-file": 5000.0,      # $5000 for CAD files
    "simulation": 10000.0,   # $10000 for simulation results
    "dataset": 2000.0,       # $2000 for datasets
    "credential": 0.0,       # All credentials are high-value
    "contract": 0.0,         # All contracts are high-value
}

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
def create_digital_asset(
    asset: schemas.DigitalAssetCreate,
    request: Request,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Create a new digital asset.
    
    After creation, this endpoint:
    1. Publishes a `DigitalAssetCreated` event
    2. For high-value assets, requests SBT issuance
    """
    db_asset = crud_digital_asset.create_asset(db=db, asset=asset)
    
    # Publish the asset created event
    publisher: EventPublisher = request.app.state.event_publisher
    if publisher:
        event = DigitalAssetCreated(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(db_asset.asset_id),
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
        
        # Assess asset value
        metadata = db_asset.metadata or {}
        asset_value = assess_asset_value(db_asset.asset_type, metadata)
        
        # Check if this is a high-value asset
        if is_high_value_asset(db_asset.asset_type, asset_value):
            logger.info(f"High-value asset detected: {db_asset.asset_id} (value: ${asset_value:.2f})")
            
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
                    asset_id=str(db_asset.asset_id),
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
                logger.info(f"SBT issuance requested for high-value asset {db_asset.asset_id}")
            else:
                logger.warning(f"High-value asset {db_asset.asset_id} detected but no owner address available")

    return db_asset

@router.get("/digital-assets/{asset_id}", response_model=schemas.DigitalAsset)
def read_digital_asset(
    asset_id: UUID,
    db: Session = Depends(get_db_session),
):
    """
    Retrieve a single digital asset by its ID.
    """
    db_asset = crud_digital_asset.get_asset(db, asset_id=asset_id)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    return db_asset

@router.get("/digital-assets", response_model=List[schemas.DigitalAsset])
def read_digital_assets(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db_session),
):
    """
    Retrieve a list of all digital assets.
    """
    assets = crud_digital_asset.get_assets(db, skip=skip, limit=limit)
    return assets

@router.post("/{asset_id}/reviews", response_model=schemas.PeerReview)
def create_peer_review(
    asset_id: UUID,
    review: schemas.PeerReviewCreate,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_context),
):
    """
    Create a new peer review for a digital asset.
    After adding the review, this endpoint will automatically attempt to
    approve the asset based on the new total number of reviews.
    """
    reviewer_id = str(context["user"].id)
    db_review = crud_digital_asset.add_review(
        db=db, asset_id=asset_id, reviewer_id=reviewer_id, review_content=review.review_content
    )
    if db_review is None:
        raise HTTPException(status_code=404, detail="Asset not found")

    # After adding a review, immediately try to approve the asset
    crud_digital_asset.approve_asset(db=db, asset_id=asset_id)
    
    return db_review

@router.post("/{asset_id}/approve", response_model=schemas.DigitalAsset)
def approve_digital_asset(
    asset_id: UUID,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_context)
):
    """
    Manually attempts to approve a digital asset. Approval is granted if the
    asset has sufficient peer reviews based on the author's reputation score.
    """
    approved_asset = crud_digital_asset.approve_asset(db=db, asset_id=asset_id)
    if not approved_asset:
        raise HTTPException(status_code=404, detail="Asset not found or already approved.")
    
    if approved_asset.status != "approved":
        raise HTTPException(
            status_code=400,
            detail=f"Asset does not yet meet the criteria for approval. It has {len(approved_asset.reviews)} review(s)."
        )

    return approved_asset 