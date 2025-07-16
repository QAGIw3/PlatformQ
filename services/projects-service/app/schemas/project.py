from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

class Milestone(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    description: Optional[str] = None
    completed: bool = False
    completed_at: Optional[datetime] = None

class ProjectBase(BaseModel):
    title: str
    description: Optional[str] = None

class ProjectCreate(ProjectBase):
    milestones: List[Milestone] = []

class Project(ProjectBase):
    id: UUID
    owner_id: str
    milestones: List[Milestone] = []

    class Config:
        orm_mode = True 