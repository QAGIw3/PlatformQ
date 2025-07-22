from pydantic import BaseModel
from typing import Optional, Dict

class FunctionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    image: str
    env: Optional[Dict[str, str]] = None
    
class FunctionDeploy(FunctionCreate):
    code: str
    requirements: str

class Function(BaseModel):
    name: str
    description: Optional[str] = None
    image: str
    env: Optional[Dict[str, str]] = None
    url: Optional[str] = None

    class Config:
        orm_mode = True 