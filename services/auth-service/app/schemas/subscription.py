import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


# --- Usage Quota ---
class UsageQuotaBase(BaseModel):
    feature: str
    quota: int
    used: int = 0


class UsageQuotaCreate(UsageQuotaBase):
    subscription_id: UUID


class UsageQuota(UsageQuotaBase):
    subscription_id: UUID

    class Config:
        orm_mode = True


# --- Subscription ---
class SubscriptionBase(BaseModel):
    tier: str
    status: str = "active"


class SubscriptionCreate(SubscriptionBase):
    user_id: UUID


class SubscriptionUpdate(BaseModel):
    tier: Optional[str] = None
    status: Optional[str] = None


class Subscription(SubscriptionBase):
    id: UUID
    user_id: UUID
    billing_cycle_start: datetime.datetime
    billing_cycle_end: datetime.datetime
    quotas: List[UsageQuota] = []

    class Config:
        orm_mode = True
