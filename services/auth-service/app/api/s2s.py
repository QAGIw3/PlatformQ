from fastapi import APIRouter, Depends, HTTPException
import hvac
import os
from ..schemas.s2s import S2STokenRequest
from ..core.security import create_access_token
from datetime import timedelta

router = APIRouter()

@router.post("/internal/s2s-token")
def get_s2s_token(
    req: S2STokenRequest,
):
    """
    Internal endpoint for services to get a JWT.
    Authenticates the service with Vault using its AppRole credentials.
    """
    vault_addr = os.getenv("VAULT_ADDR")
    if not vault_addr:
        raise HTTPException(status_code=500, detail="VAULT_ADDR not configured.")

    try:
        client = hvac.Client(url=vault_addr)
        client.auth.approle.login(
            role_id=req.role_id,
            secret_id=req.secret_id,
        )
        assert client.is_authenticated()

        # The service is now authenticated.
        # We can issue a JWT with a short expiration.
        # The 'sub' can be the role_id to identify the service.
        access_token = create_access_token(
            data={"sub": req.role_id},
            expires_delta=timedelta(minutes=15)
        )
        return {"access_token": access_token, "token_type": "bearer"}

    except Exception as e:
        raise HTTPException(status_code=401, detail=f"S2S authentication failed: {e}") 