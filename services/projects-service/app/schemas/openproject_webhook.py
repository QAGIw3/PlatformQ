from pydantic import BaseModel
from typing import Optional

class OpenProjectWebhookPayload(BaseModel):
    action: str
    work_package: dict

class WorkPackage(BaseModel):
    id: int
    subject: str
    type: str
    status: Optional[str] = None
    project: dict 