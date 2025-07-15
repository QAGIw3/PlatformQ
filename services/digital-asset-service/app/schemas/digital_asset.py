from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
import uuid

class AssetLinkBase(BaseModel):
    target_asset_id: uuid.UUID
    link_type: str = Field(..., description="e.g., 'derived_from', 'related_to', 'part_of'")

class AssetLinkCreate(AssetLinkBase):
    pass

class AssetLink(AssetLinkBase):
    class Config:
        orm_mode = True

class DigitalAssetBase(BaseModel):
    asset_name: str
    asset_type: str = Field(..., description="e.g., CRM_CONTACT, 3D_MODEL, SIMULATION_LOG")
    owner_id: uuid.UUID
    source_tool: Optional[str] = None
    source_asset_id: Optional[str] = None
    tags: List[str] = []
    metadata: Dict[str, str] = Field(default_factory=dict, alias="asset_metadata")
    raw_data_uri: Optional[str] = None

class DigitalAssetCreate(DigitalAssetBase):
    links: List[AssetLinkCreate] = []
    # The payload is handled separately during the creation process,
    # so it's not part of this base schema.

class DigitalAssetUpdate(BaseModel):
    asset_name: Optional[str] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, str]] = Field(default=None, alias="asset_metadata")

class DigitalAsset(DigitalAssetBase):
    asset_id: uuid.UUID
    created_at: datetime
    links: List[AssetLink] = []

    class Config:
        orm_mode = True 