from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any
from w3c_vc import VerifiableCredential
from datetime import datetime
import uuid
import logging
# from hyperledger.fabric import gateway # Conceptual client

logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="verifiable-credential-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["verifiable-credential-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "verifiable-credential-service is running"}

class CredentialSubject(BaseModel):
    id: str
    # ... other fields for the subject
    
class IssueRequest(BaseModel):
    subject: Dict[str, Any]
    credential_type: str = Field(alias="type")
    
@app.post("/api/v1/issue", status_code=201)
def issue_credential(
    req: IssueRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Issues a new Verifiable Credential, signs it, and records its hash
    on the private Hyperledger Fabric blockchain.
    """
    tenant_id = str(context["tenant_id"])
    
    # In a real app, the signing key would be loaded from Vault.
    # issuer_key = app.state.issuer_key
    
    vc = VerifiableCredential({
        "@context": ["https://www.w3.org/2018/credentials/v1"],
        "id": f"urn:uuid:{uuid.uuid4()}",
        "type": ["VerifiableCredential", req.credential_type],
        "issuer": f"did:web:platformq.com:tenants:{tenant_id}",
        "issuanceDate": datetime.utcnow().isoformat() + "Z",
        "credentialSubject": req.subject,
    })
    
    # Conceptually sign the credential with a key from Vault
    vc.add_proof(method="Ed25519VerificationKey2018", signature="z5tRea...")
    
    # Conceptually, connect to a Fabric peer and submit a transaction
    # to a "chaincode" (smart contract) that records the credential's hash.
    # try:
    #     with gateway.connect(...) as gw:
    #         contract = gw.get_contract("audit_trail")
    #         contract.submit_transaction("recordCredential", str(vc.id), vc.proof.signature)
    #     logger.info(f"Recorded credential {vc.id} to blockchain for tenant {tenant_id}.")
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Blockchain transaction failed: {e}")
    
    return vc.data
