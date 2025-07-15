from fastapi import APIRouter, Depends, HTTPException, Body
from cassandra.cluster import Session
from siwe import SiweMessage
from web3 import Web3
from datetime import datetime, timedelta
import secrets

from .deps import get_db_session
from ..crud import crud_user, crud_tenant
from ..core.security import create_access_token

router = APIRouter()

def store_nonce(db: Session, nonce: str):
    query = "INSERT INTO siwe_nonces (nonce, created_at) VALUES (%s, %s)"
    db.execute(db.prepare(query), [nonce, datetime.now()])

def use_nonce(db: Session, nonce: str) -> bool:
    query = "SELECT nonce FROM siwe_nonces WHERE nonce = %s"
    row = db.execute(db.prepare(query), [nonce]).one()
    if not row: return False
    delete_query = "DELETE FROM siwe_nonces WHERE nonce = %s"
    db.execute(db.prepare(delete_query), [nonce])
    return True

@router.get("/siwe/nonce")
async def get_nonce(db: Session = Depends(get_db_session)):
    nonce = secrets.token_hex(16)
    store_nonce(db, nonce)
    return {"nonce": nonce}

@router.post("/siwe/login")
async def siwe_login(message: dict = Body(...), signature: str = Body(...), db: Session = Depends(get_db_session)):
    siwe_message = SiweMessage(message=message)
    
    if not use_nonce(db, siwe_message.nonce):
        raise HTTPException(status_code=422, detail="Invalid nonce.")

    try:
        siwe_message.verify(signature)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid signature: {e}")

    # The wallet address becomes the unique identifier
    wallet_address = siwe_message.address
    user = crud_user.get_user_by_email(db, email=f"{wallet_address}@platformq.eth") # Use a derived email

    if not user:
        # Auto-create a tenant and user on first login
        tenant = crud_tenant.create_tenant(db, name=f"Tenant for {wallet_address[:8]}")
        user_create = {"email": f"{wallet_address}@platformq.eth", "full_name": wallet_address}
        user = crud_user.create_user(db, tenant_id=tenant['id'], user=user_create)
        crud_user.create_user_credential(db, tenant_id=tenant['id'], user_id=user.id, provider="ethereum", provider_user_id=wallet_address)

    access_token = create_access_token(
        data={"sub": str(user.id), "tid": str(user.tenant_id)}
    )
    return {"access_token": access_token, "token_type": "bearer"} 