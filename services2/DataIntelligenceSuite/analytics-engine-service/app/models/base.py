"""
Base Pydantic models
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID


class BaseRequest(BaseModel):
    """Base request model"""
    request_id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    

class BaseResponse(BaseModel):
    """Base response model"""
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    

class PaginatedResponse(BaseResponse):
    """Paginated response model"""
    total: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool
