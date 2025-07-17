from sqlalchemy.orm import Session
from sqlalchemy.exc import StaleDataError
from fastapi import HTTPException
from typing import Optional
from .. import models, schemas


def get_asset(db: Session, cid: str) -> Optional[models.DigitalAsset]:
    """Get a digital asset by CID"""
    return db.query(models.DigitalAsset).filter(models.DigitalAsset.cid == cid).first()


def create_asset(db: Session, asset: schemas.DigitalAssetCreate) -> models.DigitalAsset:
    """Create a new digital asset"""
    db_asset = models.DigitalAsset(**asset.dict())
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    return db_asset


def update_asset(db: Session, cid: str, asset_update: schemas.DigitalAssetUpdate) -> models.DigitalAsset:
    """Update a digital asset with optimistic locking"""
    asset = db.query(models.DigitalAsset).filter(models.DigitalAsset.cid == cid).first()
    if not asset:
        return None
    
    # Store current version
    current_version = asset.version
    
    # Update fields
    update_data = asset_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(asset, field, value)
    
    # Increment version
    asset.version = current_version + 1
    
    try:
        # This will fail if version was changed by another transaction
        result = db.query(models.DigitalAsset).filter(
            models.DigitalAsset.cid == cid,
            models.DigitalAsset.version == current_version
        ).update({"version": asset.version, **update_data})
        
        if result == 0:
            db.rollback()
            raise HTTPException(status_code=409, detail="Concurrent update conflict")
        
        db.commit()
        db.refresh(asset)
        return asset
    except Exception as e:
        db.rollback()
        if "Concurrent update conflict" in str(e):
            raise
        raise HTTPException(status_code=500, detail=str(e))


def delete_asset(db: Session, cid: str) -> bool:
    """Delete a digital asset"""
    asset = db.query(models.DigitalAsset).filter(models.DigitalAsset.cid == cid).first()
    if not asset:
        return False
    db.delete(asset)
    db.commit()
    return True 