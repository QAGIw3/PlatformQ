from pydantic import BaseModel, EmailStr
from uuid import UUID
import datetime

class TenantBase(BaseModel):
    name: str

class TenantCreate(TenantBase):
    admin_email: EmailStr
    admin_full_name: str

class Tenant(TenantBase):
    id: UUID
    created_at: datetime.datetime

    class Config:
        orm_mode = True 