from cassandra.cluster import Session
from uuid import UUID
import datetime
import secrets

from ..core.security import get_password_hash, verify_password

def get_oidc_client(db: Session, client_id: str):
    """Fetches an OIDC client by its ID."""
    query = "SELECT client_id, client_secret_hash, client_name, redirect_uris, allowed_scopes FROM oidc_clients WHERE client_id = %s"
    prepared = db.prepare(query)
    row = db.execute(prepared, [client_id]).one()
    return row

def save_authorization_code(db: Session, code: str, client_id: str, user_id: UUID, redirect_uri: str, scope: str):
    """Hashes and saves a new authorization code."""
    hashed_code = get_password_hash(code)
    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
    
    query = "INSERT INTO authorization_codes (code_hash, client_id, user_id, redirect_uri, scope, expires_at, is_used) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(prepared, [hashed_code, client_id, user_id, redirect_uri, scope, expires, False])

def get_and_use_authorization_code(db: Session, code: str, client_id: str):
    """
    Validates an authorization code. If valid, marks it as used and returns its data.
    This is inefficient and needs optimization for production.
    """
    query = "SELECT code_hash, client_id, user_id, redirect_uri, scope, expires_at, is_used FROM authorization_codes"
    rows = db.execute(query)

    for row in rows:
        if row.client_id == client_id and verify_password(code, row.code_hash):
            if row.is_used or row.expires_at < datetime.datetime.now(datetime.timezone.utc):
                return None # Code already used or expired
            
            # Mark as used
            update_query = "UPDATE authorization_codes SET is_used = true WHERE code_hash = %s"
            update_prepared = db.prepare(update_query)
            db.execute(update_prepared, [row.code_hash])
            return row
            
    return None 