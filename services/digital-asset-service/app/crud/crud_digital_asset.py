from sqlalchemy.orm import Session
from typing import List, Optional
import uuid

from ..db import models
from ..schemas import digital_asset as schemas

def get_asset(db: Session, asset_id: uuid.UUID) -> Optional[models.DigitalAsset]:
    """
    Retrieves a single digital asset by its UUID.
    """
    return db.query(models.DigitalAsset).filter(models.DigitalAsset.asset_id == asset_id).first()

def get_assets(db: Session, skip: int = 0, limit: int = 100) -> List[models.DigitalAsset]:
    """
    Retrieves a list of digital assets with pagination.
    """
    return db.query(models.DigitalAsset).offset(skip).limit(limit).all()

def create_asset(db: Session, asset: schemas.DigitalAssetCreate) -> models.DigitalAsset:
    """
    Creates a new digital asset in the database.
    """
    db_asset = models.DigitalAsset(
        asset_name=asset.asset_name,
        asset_type=asset.asset_type,
        owner_id=asset.owner_id,
        source_tool=asset.source_tool,
        source_asset_id=asset.source_asset_id,
        raw_data_uri=asset.raw_data_uri,
        tags=asset.tags,
        asset_metadata=asset.metadata,
        payload_schema_version=asset.payload_schema_version,
        payload=asset.payload
    )
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    return db_asset

def update_asset(db: Session, asset_id: uuid.UUID, asset_update: schemas.DigitalAssetUpdate) -> Optional[models.DigitalAsset]:
    """
    Updates an existing digital asset's fields.
    """
    db_asset = get_asset(db, asset_id)
    if not db_asset:
        return None

    update_data = asset_update.dict(exclude_unset=True)
    # Special handling for payload and payload_schema_version
    if 'payload_schema_version' in update_data:
        db_asset.payload_schema_version = update_data['payload_schema_version']
    if 'payload' in update_data:
        db_asset.payload = update_data['payload']

    # Update other fields normally, excluding payload-related ones handled above
    for key, value in update_data.items():
        if key not in ['payload_schema_version', 'payload']:
            setattr(db_asset, key, value)
    
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    return db_asset

def update_asset_metadata(db: Session, asset_id: uuid.UUID, new_metadata: dict, payload_schema_version: Optional[str] = None, payload: Optional[bytes] = None) -> Optional[models.DigitalAsset]:
    """
    Updates an existing digital asset's metadata and/or payload.
    If payload and payload_schema_version are provided, they will update the respective fields.
    Otherwise, new_metadata will be merged into the existing asset_metadata.
    """
    db_asset = get_asset(db, asset_id)
    if not db_asset:
        return None

    if payload is not None and payload_schema_version is not None:
        db_asset.payload = payload
        db_asset.payload_schema_version = payload_schema_version
    elif new_metadata is not None:
        # Merge the new metadata with the existing metadata
        if db_asset.asset_metadata is None:
            db_asset.asset_metadata = {}
        
        updated_metadata = db_asset.asset_metadata.copy()
        updated_metadata.update(new_metadata)
        
        db_asset.asset_metadata = updated_metadata
    
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    return db_asset

def delete_asset(db: Session, asset_id: uuid.UUID) -> Optional[models.DigitalAsset]:
    """
    Deletes a digital asset from the database.
    """
    db_asset = get_asset(db, asset_id)
    if not db_asset:
        return None
        
    db.delete(db_asset)
    db.commit()
    return db_asset 