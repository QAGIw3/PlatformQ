from fastapi import APIRouter, Depends, HTTPException, Body
from cassandra.cluster import Session
from siwe import SiweMessage
from web3 import Web3
from datetime import datetime, timedelta
import secrets

from .deps import get_db_session
from ..crud import crud_user, crud_tenant, crud_siwe, crud_role
from ..core.security import create_access_token
from ..schemas.user import UserCreate

router = APIRouter()

@router.get("/siwe/nonce")
async def get_nonce(db: Session = Depends(get_db_session)):
    nonce = secrets.token_hex(16)
    crud_siwe.store_nonce(db, nonce)
    return {"nonce": nonce}

@router.post("/siwe/login")
async def siwe_login(message: dict = Body(...), signature: str = Body(...), db: Session = Depends(get_db_session)):
    siwe_message = SiweMessage(message=message)
    
    if not crud_siwe.use_nonce(db, siwe_message.nonce):
        raise HTTPException(status_code=422, detail="Invalid nonce.")

    try:
        siwe_message.verify(signature)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid signature: {e}")

    # The wallet address becomes the unique identifier
    wallet_address = siwe_message.address
    user = crud_user.get_user_by_wallet_address(db, wallet_address=wallet_address)

    if not user:
        # Auto-create a tenant and user on first login
        tenant = crud_tenant.create_tenant(db, name=f"Tenant for {wallet_address[:8]}")
        # Create a user with wallet_address as the primary identifier and a placeholder email
        user_create = UserCreate(
            email=f"{wallet_address}@platformq.eth", # Placeholder email
            full_name=wallet_address,
            wallet_address=wallet_address,
            did=f"did:ethr:{wallet_address}"
        )
        user = crud_user.create_user(db, tenant_id=tenant.id, user=user_create)

    roles = crud_role.get_roles_for_user(db, user_id=user.id, tenant_id=user.tenant_id)
    access_token = create_access_token(
        data={"sub": str(user.id), "tid": str(user.tenant_id)},
        groups=roles,
    )
    return {"access_token": access_token, "token_type": "bearer"} 