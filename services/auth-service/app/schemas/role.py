from typing import List
from uuid import UUID

from pydantic import BaseModel


class RoleAssignmentRequest(BaseModel):
    user_id: UUID
    role: str


class UserRolesResponse(BaseModel):
    user_id: UUID
    roles: List[str]
