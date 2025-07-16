from pydantic import BaseModel, Field
from typing import Dict, Any

class PresentationVerificationRequest(BaseModel):
    verifiable_presentation: Dict[str, Any] = Field(description="The Verifiable Presentation to be verified.")
    required_credential_type: str = Field(description="The type of credential required for access.")

class PresentationVerificationResponse(BaseModel):
    verified: bool = Field(description="Whether the presentation was successfully verified.")
    error: str | None = Field(None, description="Any error message if verification failed.") 