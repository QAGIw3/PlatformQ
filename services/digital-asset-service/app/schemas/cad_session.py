from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid

class CADSessionBase(BaseModel):
    asset_id: uuid.UUID
    tenant_id: uuid.UUID
    active_users: List[str] = []
    vector_clock: Dict[str, int] = {}
    session_metadata: Dict[str, Any] = {}

class CADSessionCreate(CADSessionBase):
    pass

class CADSession(CADSessionBase):
    session_id: uuid.UUID
    created_by: uuid.UUID
    created_at: datetime
    closed_at: Optional[datetime] = None
    operation_count: int = 0
    last_checkpoint_id: Optional[uuid.UUID] = None
    
    class Config:
        orm_mode = True

class CADCheckpoint(BaseModel):
    checkpoint_id: uuid.UUID
    session_id: uuid.UUID
    created_at: datetime
    snapshot_uri: str
    operation_range_start: str
    operation_range_end: str
    operation_count: int
    crdt_state_size: int
    
    class Config:
        orm_mode = True

class CreateGeometryVersion(BaseModel):
    snapshot_uri: str = Field(..., description="URI to the geometry snapshot in MinIO")
    vertex_count: Optional[int] = None
    edge_count: Optional[int] = None
    face_count: Optional[int] = None
    file_size_bytes: Optional[int] = None
    commit_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class GeometryVersion(BaseModel):
    version_id: uuid.UUID
    asset_id: uuid.UUID
    version_number: int
    created_by: uuid.UUID
    created_at: datetime
    geometry_snapshot_uri: Optional[str] = None
    parent_version_id: Optional[uuid.UUID] = None
    vertex_count: Optional[int] = None
    edge_count: Optional[int] = None
    face_count: Optional[int] = None
    file_size_bytes: Optional[int] = None
    version_metadata: Dict[str, Any] = {}
    commit_message: Optional[str] = None
    
    class Config:
        orm_mode = True 