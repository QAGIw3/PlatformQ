
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user
from ...crud import crud_digital_asset
from ...schemas import digital_asset as schemas
from ....platformq_shared.events import DigitalAssetCreated
from ....platformq_shared.event_publisher import EventPublisher

router = APIRouter()

@router.post("/digital-assets", response_model=schemas.DigitalAsset)
def create_digital_asset(
    asset: schemas.DigitalAssetCreate,
    request: Request,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Create a new digital asset.
    
    After creation, this endpoint publishes a `DigitalAssetCreated` event.
    """
    db_asset = crud_digital_asset.create_asset(db=db, asset=asset)
    
    # Publish the event
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