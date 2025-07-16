from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class DirectIssuanceRequest(BaseModel):
    subject_did: str = Field(description="The DID of the user to whom the credential will be issued.")
    credential_data: Dict[str, Any] = Field(description="The subject data for the credential.")
    credential_type: str = Field(description="The type of the credential, e.g., 'DAOMembershipCredential'.")
    issuer_did: Optional[str] = Field(None, description="The DID of the issuer. If not provided, a default will be used.")

class PresentationRequest(BaseModel):
    challenge: str
    request_url: str
    # This would be more complex in a real app, defining the presentation requirements
    # For now, we just need a way for the wallet to post back the signed credential. 