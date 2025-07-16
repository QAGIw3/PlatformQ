from fastapi import APIRouter, Depends, HTTPException, Request
from ..schemas.direct_issuance import DirectIssuanceRequest, PresentationRequest
import uuid

router = APIRouter()

@router.post("/issue-direct", response_model=PresentationRequest)
async def issue_credential_direct(
    req: DirectIssuanceRequest,
    request: Request,
):
    """
    Initiates a direct-to-wallet credential issuance flow.
    Returns a presentation request that the user's wallet can fulfill.
    """
    challenge = f"issue-{uuid.uuid4()}"
    # In a real app, we would store the issuance request details
    # against this challenge in a database or cache.
    
    # The request_url is where the wallet should post the signed credential back to.
    request_url = str(request.url_for('handle_issuance_response', challenge=challenge))
    
    return PresentationRequest(challenge=challenge, request_url=request_url)

@router.post("/issue-response/{challenge}")
async def handle_issuance_response(
    challenge: str,
    request: Request,
):
    """
    Handles the response from the user's wallet.
    This is where the signed credential would be processed.
    """
    # In a real app, you would:
    # 1. Look up the original request by the challenge.
    # 2. Verify the signature on the received credential.
    # 3. Anchor the credential to the blockchain, etc.
    # 4. Return a success response.
    return {"status": "success", "challenge": challenge} 