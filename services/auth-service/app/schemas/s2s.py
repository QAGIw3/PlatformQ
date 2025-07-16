from pydantic import BaseModel

class S2STokenRequest(BaseModel):
    role_id: str
    secret_id: str 