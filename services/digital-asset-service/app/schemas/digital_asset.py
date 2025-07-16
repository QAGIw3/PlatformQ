from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
import uuid

# --- Asset Processing Rule Schemas ---

class AssetProcessingRuleBase(BaseModel):
    asset_type: str = Field(..., description="The type of asset this rule applies to (e.g., '3D_MODEL'). This is the key.")
    wasm_module_id: str = Field(..., description="The ID/name of the WASM module in the functions-service to execute.")

class AssetProcessingRuleCreate(AssetProcessingRuleBase):
    pass

class AssetProcessingRule(AssetProcessingRuleBase):
    tenant_id: uuid.UUID
    created_by_id: uuid.UUID
    created_at: datetime

    class Config:
        orm_mode = True

# --- Asset Link Schemas ---

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
    payload_schema_version: Optional[str] = None # New field for schema version of the payload
    payload: Optional[bytes] = None # New field for structured, serialized payload data
    
    # Marketplace fields
    is_for_sale: bool = False
    sale_price: Optional[float] = None
    is_licensable: bool = False
    license_terms: Optional[Dict[str, Any]] = None
    royalty_percentage: int = 250  # Default 2.5%
    blockchain_address: Optional[str] = None

class DigitalAssetCreate(DigitalAssetBase):
    links: List[AssetLinkCreate] = []
    # The payload is handled separately during the creation process,
    # so it's not part of this base schema.

class DigitalAssetUpdate(BaseModel):
    asset_name: Optional[str] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, str]] = Field(default=None, alias="asset_metadata")
    payload_schema_version: Optional[str] = None # Allow updating payload schema version
    payload: Optional[bytes] = None # Allow updating payload data
    
    # Marketplace updates
    is_for_sale: Optional[bool] = None
    sale_price: Optional[float] = None
    is_licensable: Optional[bool] = None
    license_terms: Optional[Dict[str, Any]] = None
    royalty_percentage: Optional[int] = None
    blockchain_address: Optional[str] = None

class DigitalAsset(DigitalAssetBase):
    asset_id: uuid.UUID
    created_at: datetime
    links: List[AssetLink] = []
    processing_rule: Optional[AssetProcessingRule] = None # Include the rule in the response

    class Config:
        orm_mode = True 