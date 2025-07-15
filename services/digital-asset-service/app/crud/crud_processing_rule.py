
from sqlalchemy.orm import Session
from ..db import models
from ..schemas import digital_asset as schemas
import uuid

def get_rule(db: Session, asset_type: str) -> models.AssetProcessingRule:
    """
    Retrieves a single processing rule by its asset_type key.
    """
    return db.query(models.AssetProcessingRule).filter(models.AssetProcessingRule.asset_type == asset_type).first()

def get_rules(db: Session, skip: int = 0, limit: int = 100) -> list[models.AssetProcessingRule]:
    """
    Retrieves a list of all processing rules.
    """
    return db.query(models.AssetProcessingRule).offset(skip).limit(limit).all()

def create_rule(db: Session, rule: schemas.AssetProcessingRuleCreate, tenant_id: uuid.UUID, user_id: uuid.UUID) -> models.AssetProcessingRule:
    """
    Creates a new asset processing rule.
    """
    db_rule = models.AssetProcessingRule(
        **rule.dict(),
        tenant_id=tenant_id,
        created_by_id=user_id,
    )
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule

def delete_rule(db: Session, asset_type: str) -> models.AssetProcessingRule:
    """
    Deletes a processing rule by its asset_type key.
    """
    db_rule = get_rule(db, asset_type)
    if db_rule:
        db.delete(db_rule)
        db.commit()
    return db_rule 