import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr


# Base model for User properties
class UserBase(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    did: Optional[str] = None # Added for DID


# Properties to receive via API on creation
class UserCreate(UserBase):
    pass


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    did: Optional[str] = None # Added for DID


# Properties to return to client
class User(UserBase):
    id: UUID
    status: str
    created_at: datetime.datetime
    updated_at: Optional[datetime.datetime] = None
    did: Optional[str] = None # New field for Decentralized Identifier

    class Config:
        orm_mode = True


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
