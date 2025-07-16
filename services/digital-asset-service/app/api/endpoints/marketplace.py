from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
import logging

from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user
from ...crud import crud_digital_asset
from ...schemas import digital_asset as schemas
from ...db import models
from ....platformq_shared.event_publisher import EventPublisher
from pulsar.schema import Record, String, Long, Double, Boolean

logger = logging.getLogger(__name__)

router = APIRouter()

# Event schemas for marketplace
class AssetListedForSale(Record):
    tenant_id = String()
    asset_id = String()
    seller_address = String()
    price = Double()
    currency = String()
    timestamp = Long()

class AssetLicenseOffered(Record):
    tenant_id = String()
    asset_id = String()
    licensor_address = String()
    license_type = String()
    price = Double()
    duration = Long()
    timestamp = Long()

class RoyaltyConfigured(Record):
    tenant_id = String()
    asset_id = String()
    beneficiary_address = String()
    percentage = Long()
    creation_vc_id = String()
    timestamp = Long()


@router.get("/marketplace/assets")
def list_marketplace_assets(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    for_sale: Optional[bool] = None,
    licensable: Optional[bool] = None,
    asset_type: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    sort_by: str = Query("created_at", regex="^(created_at|sale_price|asset_name)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    List assets available in the marketplace.
    """
    query = db.query(models.DigitalAsset)
    
    # Apply filters
    if for_sale is not None:
        query = query.filter(models.DigitalAsset.is_for_sale == for_sale)
    
    if licensable is not None:
        query = query.filter(models.DigitalAsset.is_licensable == licensable)
    
    if asset_type:
        query = query.filter(models.DigitalAsset.asset_type == asset_type)
    
    if min_price is not None:
        query = query.filter(models.DigitalAsset.sale_price >= min_price)
    
    if max_price is not None:
        query = query.filter(models.DigitalAsset.sale_price <= max_price)
    
    # Apply sorting
    order_column = getattr(models.DigitalAsset, sort_by)
    if sort_order == "desc":
        query = query.order_by(order_column.desc())
    else:
        query = query.order_by(order_column.asc())
    
    # Pagination
    total = query.count()
    assets = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "assets": assets
    }


@router.post("/digital-assets/{asset_id}/list-for-sale")
def list_asset_for_sale(
    asset_id: UUID,
    price: float = Query(..., gt=0),
    currency: str = Query("ETH", regex="^(ETH|MATIC|USDC)$"),
    royalty_percentage: Optional[int] = Query(None, ge=0, le=5000),  # Max 50%
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(lambda request: request.app.state.event_publisher)
):
    """
    List an asset for sale in the marketplace.
    
    This endpoint enables asset owners to list their digital assets for sale
    on the decentralized marketplace. It performs several critical checks
    and updates before making the asset available for purchase.
    
    Args:
        asset_id: UUID of the asset to list
        price: Sale price in the specified currency (must be positive)
        currency: Currency for the sale price (ETH, MATIC, or USDC)
        royalty_percentage: Optional royalty percentage for resales (0-5000 basis points)
        db: Database session (injected)
        context: User context with tenant and user info (injected)
        publisher: Event publisher for blockchain events (injected)
    
    Security Checks:
        1. Asset existence verification
        2. Ownership validation (only owner can list)
        3. Creation credential requirement (ensures provenance)
        4. Blockchain address requirement (for on-chain transactions)
    
    Process Flow:
        1. Retrieve and validate the asset
        2. Verify user ownership
        3. Ensure asset has creation credentials
        4. Update asset with marketplace metadata
        5. Configure royalty settings if provided
        6. Publish marketplace event for indexing
        7. Trigger blockchain transaction (in production)
    
    Returns:
        Updated asset object with marketplace fields populated
    
    Raises:
        HTTPException 404: Asset not found
        HTTPException 403: User doesn't own the asset
        HTTPException 400: Missing creation credential or blockchain address
    
    Example:
        POST /digital-assets/123e4567-e89b-12d3-a456-426614174000/list-for-sale?price=0.5&currency=ETH&royalty_percentage=250
    """
    # Get the asset
    asset = crud_digital_asset.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Check ownership
    if str(asset.owner_id) != str(context["user"].id):
        raise HTTPException(status_code=403, detail="Not authorized to list this asset")
    
    # Check if asset has creation VC
    if not asset.creation_vc_id:
        raise HTTPException(status_code=400, detail="Asset must have creation credential")
    
    # Update asset
    update_data = {
        "is_for_sale": True,
        "sale_price": price,
        "metadata": {
            **asset.asset_metadata,
            "sale_currency": currency,
            "listed_at": datetime.utcnow().isoformat()
        }
    }
    
    if royalty_percentage is not None:
        update_data["royalty_percentage"] = royalty_percentage
    
    # Get user's blockchain address
    user = context["user"]
    if hasattr(user, "blockchain_address") and user.blockchain_address:
        update_data["blockchain_address"] = user.blockchain_address
    else:
        raise HTTPException(status_code=400, detail="User must have blockchain address")
    
    asset = crud_digital_asset.update_asset(db, asset_id, schemas.DigitalAssetUpdate(**update_data))
    
    # Publish event
    if publisher:
        event = AssetListedForSale(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(asset_id),
            seller_address=user.blockchain_address,
            price=price,
            currency=currency,
            timestamp=int(datetime.utcnow().timestamp() * 1000)
        )
        publisher.publish(
            topic_base='asset-listed-events',
            tenant_id=str(context["tenant_id"]),
            schema_class=AssetListedForSale,
            data=event
        )
    
    # Configure royalty on smart contract if specified
    if royalty_percentage is not None and asset.creation_vc_id:
        royalty_event = RoyaltyConfigured(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(asset_id),
            beneficiary_address=user.blockchain_address,
            percentage=royalty_percentage,
            creation_vc_id=asset.creation_vc_id,
            timestamp=int(datetime.utcnow().timestamp() * 1000)
        )
        publisher.publish(
            topic_base='royalty-configured-events',
            tenant_id=str(context["tenant_id"]),
            schema_class=RoyaltyConfigured,
            data=royalty_event
        )
    
    return asset


@router.post("/digital-assets/{asset_id}/create-license-offer")
def create_license_offer(
    asset_id: UUID,
    license_type: str = Query(..., regex="^(view|edit|commercial|exclusive)$"),
    price: float = Query(..., gt=0),
    duration: int = Query(..., gt=0),  # Duration in seconds
    max_usage: int = Query(0, ge=0),  # 0 = unlimited
    terms: Optional[Dict[str, Any]] = None,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(lambda request: request.app.state.event_publisher)
):
    """
    Create a license offer for an asset.
    """
    # Get the asset
    asset = crud_digital_asset.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Check ownership
    if str(asset.owner_id) != str(context["user"].id):
        raise HTTPException(status_code=403, detail="Not authorized to license this asset")
    
    # Check if asset has creation VC
    if not asset.creation_vc_id:
        raise HTTPException(status_code=400, detail="Asset must have creation credential")
    
    # Get user's blockchain address
    user = context["user"]
    if not hasattr(user, "blockchain_address") or not user.blockchain_address:
        raise HTTPException(status_code=400, detail="User must have blockchain address")
    
    # Prepare license terms
    license_terms = {
        "type": license_type,
        "price": price,
        "duration": duration,
        "max_usage": max_usage,
        "currency": "ETH",
        "created_at": datetime.utcnow().isoformat(),
        **(terms or {})
    }
    
    # Update asset
    update_data = {
        "is_licensable": True,
        "license_terms": license_terms,
        "blockchain_address": user.blockchain_address
    }
    
    asset = crud_digital_asset.update_asset(db, asset_id, schemas.DigitalAssetUpdate(**update_data))
    
    # Publish event
    if publisher:
        event = AssetLicenseOffered(
            tenant_id=str(context["tenant_id"]),
            asset_id=str(asset_id),
            licensor_address=user.blockchain_address,
            license_type=license_type,
            price=price,
            duration=duration,
            timestamp=int(datetime.utcnow().timestamp() * 1000)
        )
        publisher.publish(
            topic_base='asset-license-offered-events',
            tenant_id=str(context["tenant_id"]),
            schema_class=AssetLicenseOffered,
            data=event
        )
    
    return asset


@router.delete("/digital-assets/{asset_id}/unlist")
def unlist_asset(
    asset_id: UUID,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Remove an asset from sale/licensing.
    """
    # Get the asset
    asset = crud_digital_asset.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Check ownership
    if str(asset.owner_id) != str(context["user"].id):
        raise HTTPException(status_code=403, detail="Not authorized to unlist this asset")
    
    # Update asset
    update_data = {
        "is_for_sale": False,
        "sale_price": None,
        "is_licensable": False,
        "license_terms": None
    }
    
    asset = crud_digital_asset.update_asset(db, asset_id, schemas.DigitalAssetUpdate(**update_data))
    
    return {"message": "Asset unlisted successfully", "asset": asset}


@router.get("/digital-assets/{asset_id}/marketplace-info")
def get_asset_marketplace_info(
    asset_id: UUID,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Get detailed marketplace information for an asset.
    """
    asset = crud_digital_asset.get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Calculate potential royalties based on lineage
    lineage_count = len(asset.lineage_vc_ids) if asset.lineage_vc_ids else 0
    
    marketplace_info = {
        "asset_id": asset.asset_id,
        "is_for_sale": asset.is_for_sale,
        "sale_price": asset.sale_price,
        "is_licensable": asset.is_licensable,
        "license_terms": asset.license_terms,
        "royalty_percentage": asset.royalty_percentage,
        "owner_blockchain_address": asset.blockchain_address,
        "creation_vc_id": asset.creation_vc_id,
        "lineage_depth": lineage_count,
        "smart_contracts": asset.smart_contract_addresses or {},
        "metadata": {
            "listed_at": asset.asset_metadata.get("listed_at") if asset.asset_metadata else None,
            "sale_currency": asset.asset_metadata.get("sale_currency", "ETH") if asset.asset_metadata else "ETH"
        }
    }
    
    return marketplace_info 