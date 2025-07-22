
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user
from ...crud import crud_processing_rule
from ...schemas import digital_asset as schemas

router = APIRouter()

@router.post("/processing-rules", response_model=schemas.AssetProcessingRule)
def create_processing_rule(
    rule: schemas.AssetProcessingRuleCreate,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Create a new asset processing rule.
    """
    tenant_id = context["tenant_id"]
    user: User = context["user"]
    
    # Check if a rule for this asset_type already exists
    db_rule = crud_processing_rule.get_rule(db, asset_type=rule.asset_type)
    if db_rule:
        raise HTTPException(status_code=400, detail=f"A rule for asset_type '{rule.asset_type}' already exists.")
        
    return crud_processing_rule.create_rule(db=db, rule=rule, tenant_id=tenant_id, user_id=user.id)

@router.get("/processing-rules", response_model=List[schemas.AssetProcessingRule])
def read_processing_rules(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db_session),
):
    """
    Retrieve all processing rules.
    """
    rules = crud_processing_rule.get_rules(db, skip=skip, limit=limit)
    return rules

@router.get("/processing-rules/{asset_type}", response_model=schemas.AssetProcessingRule)
def read_processing_rule(
    asset_type: str,
    db: Session = Depends(get_db_session),
):
    """
    Retrieve a single processing rule by its asset_type.
    """
    db_rule = crud_processing_rule.get_rule(db, asset_type=asset_type)
    if db_rule is None:
        raise HTTPException(status_code=404, detail="Processing rule not found")
    return db_rule

@router.delete("/processing-rules/{asset_type}", response_model=schemas.AssetProcessingRule)
def delete_processing_rule(
    asset_type: str,
    db: Session = Depends(get_db_session),
):
    """
    Delete a processing rule by its asset_type.
    """
    db_rule = crud_processing_rule.delete_rule(db, asset_type=asset_type)
    if db_rule is None:
        raise HTTPException(status_code=404, detail="Processing rule not found")
    return db_rule 