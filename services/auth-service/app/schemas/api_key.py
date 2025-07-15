import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ApiKeyBase(BaseModel):
    key_prefix: str
    user_id: UUID
    is_active: bool
    created_at: datetime.datetime
    expires_at: Optional[datetime.datetime] = None


class ApiKeyInfo(ApiKeyBase):
    """
    Schema for listing API keys (without the secret).
    """

    last_used_at: Optional[datetime.datetime] = None

    class Config:
        orm_mode = True


class ApiKeyCreateResponse(ApiKeyInfo):
    """
    Schema for the response when a new key is created.
    This is the ONLY time the full key is sent to the client.
    """

    full_key: str


class ApiKeyCreate(BaseModel):
    expires_at: Optional[datetime.datetime] = None
