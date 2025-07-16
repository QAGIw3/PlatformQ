import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr


# Base model for User properties
class UserBase(BaseModel):
    email: EmailStr
    username: str
    full_name: Optional[str] = None
    bio: Optional[str] = None
    profile_picture_url: Optional[str] = None
    did: Optional[str] = None
    wallet_address: Optional[str] = None
    storage_backend: Optional[str] = None
    storage_config: Optional[str] = None


class StorageConfigUpdate(BaseModel):
    storage_backend: str
    storage_config: str # Should be a JSON string


class LinkWalletRequest(BaseModel):
    message: dict
    signature: str


# Properties to receive via API on creation
class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[EmailStr] = None
    bio: Optional[str] = None
    profile_picture_url: Optional[str] = None
    did: Optional[str] = None
    wallet_address: Optional[str] = None
    storage_backend: Optional[str] = None
    storage_config: Optional[str] = None


# Properties to return to client
class User(UserBase):
    id: UUID
    status: str
    created_at: datetime.datetime
    updated_at: Optional[datetime.datetime] = None

    class Config:
        from_attributes = True


# Pydantic models for tokens
class Token(BaseModel):
    access_token: str
    token_type: str
    refresh_token: str


class TokenData(BaseModel):
    email: Optional[str] = None


class PasswordlessLoginRequest(BaseModel):
    email: EmailStr


class PasswordlessLoginToken(BaseModel):
    email: EmailStr
    temp_token: str


class TokenExchangeRequest(BaseModel):
    temp_token: str
    email: EmailStr


class RefreshTokenRequest(BaseModel):
    refresh_token: str
