from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import uuid

from ..crud import crud_digital_asset
from ..schemas import digital_asset as schemas
from . import deps

router = APIRouter()

@router.post("/", response_model=schemas.DigitalAsset, status_code=201)
def create_digital_asset(
    asset_in: schemas.DigitalAssetCreate,
    db: Session = Depends(deps.get_db_session),
):
    """
    Create a new digital asset.
    """
    return crud_digital_asset.create_asset(db=db, asset=asset_in)

@router.get("/", response_model=List[schemas.DigitalAsset])
def read_digital_assets(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(deps.get_db_session),
):
    """
    Retrieve all digital assets with pagination.
    """
    assets = crud_digital_asset.get_assets(db, skip=skip, limit=limit)
    return assets

@router.get("/{asset_id}", response_model=schemas.DigitalAsset)
def read_digital_asset(
    asset_id: uuid.UUID,
    db: Session = Depends(deps.get_db_session),
):
    """
    Retrieve a specific digital asset by its ID.
    """
    db_asset = crud_digital_asset.get_asset(db, asset_id=asset_id)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Digital Asset not found")
    return db_asset

@router.patch("/{asset_id}", response_model=schemas.DigitalAsset)
def update_digital_asset(
    asset_id: uuid.UUID,
    asset_in: schemas.DigitalAssetUpdate,
    db: Session = Depends(deps.get_db_session),
):
    """
    Update a digital asset's information.
    """
    db_asset = crud_digital_asset.update_asset(db, asset_id=asset_id, asset_update=asset_in)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Digital Asset not found")
    return db_asset

@router.delete("/{asset_id}", response_model=schemas.DigitalAsset)
def delete_digital_asset(
    asset_id: uuid.UUID,
    db: Session = Depends(deps.get_db_session),
):
    """
    Delete a digital asset.
    """
    db_asset = crud_digital_asset.delete_asset(db, asset_id=asset_id)
    if db_asset is None:
        raise HTTPException(status_code=404, detail="Digital Asset not found")
    return db_asset 