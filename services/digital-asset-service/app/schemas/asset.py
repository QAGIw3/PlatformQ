from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

class PeerReviewBase(BaseModel):
    review_content: str

class PeerReviewCreate(PeerReviewBase):
    pass

class PeerReview(PeerReviewBase):
    id: UUID = Field(default_factory=uuid4)
    reviewer_id: str
    asset_id: UUID
    created_at: datetime = Field(default_factory=datetime.utcnow)

class DigitalAssetBase(BaseModel):
    name: str
    description: Optional[str] = None
    s3_url: str

class DigitalAssetCreate(DigitalAssetBase):
    pass

class DigitalAsset(DigitalAssetBase):
    id: UUID
    owner_id: str
    reviews: List[PeerReview] = []
    status: str = "pending"
    version: int = 1

    class Config:
        orm_mode = True 