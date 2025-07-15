import datetime
import secrets
from typing import List, Tuple
from uuid import UUID

from cassandra.cluster import Session

from ..core.security import get_password_hash
from ..schemas.api_key import ApiKeyCreate

PREFIX_BASE = "pk_test"  # In a real app, could be 'pk_live', 'sk_test', etc.
KEY_BYTES = 32


def generate_api_key() -> Tuple[str, str, str]:
    """
    Generates a new API key and returns the full key, its secret part, and its unique prefix.
    """
    prefix = f"{PREFIX_BASE}_{secrets.token_hex(8)}"
    secret = secrets.token_urlsafe(KEY_BYTES)
    full_key = f"{prefix}_{secret}"
    return full_key, secret, prefix


def create_api_key_for_user(db: Session, user_id: UUID, key_in: ApiKeyCreate) -> dict:
    """
    Creates and stores a new API key for a user.
    """
    full_key, secret, prefix = generate_api_key()
    hashed_key = get_password_hash(secret)  # We can reuse password hashing

    now = datetime.datetime.now(datetime.timezone.utc)

    query = """
    INSERT INTO api_keys (key_prefix, hashed_key, user_id, created_at, expires_at, is_active)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    prepared = db.prepare(query)
    db.execute(prepared, [prefix, hashed_key, user_id, now, key_in.expires_at, True])

    # Return the key details along with the full key for the response
    key_data = get_api_key_by_prefix(db, prefix=prefix)
    key_data["full_key"] = full_key
    return key_data


def get_api_keys_for_user(db: Session, user_id: UUID) -> List[dict]:
    """
    Retrieves all API key metadata for a user.
    """
    query = "SELECT key_prefix, user_id, is_active, created_at, expires_at, last_used_at FROM api_keys WHERE user_id = %s"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [user_id])
    return [dict(row._asdict()) for row in rows]


def get_api_key_by_prefix(db: Session, prefix: str) -> dict:
    """
    Retrieves a single API key by its prefix.
    """
    query = "SELECT key_prefix, hashed_key, user_id, is_active, created_at, expires_at, last_used_at FROM api_keys WHERE key_prefix = %s"
    prepared = db.prepare(query)
    row = db.execute(prepared, [prefix]).one()
    return dict(row._asdict()) if row else None


def revoke_api_key(db: Session, prefix: str, user_id: UUID) -> bool:
    """
    Revokes an API key by setting its 'is_active' flag to false.
    Ensures a user can only revoke their own keys.
    """
    key_info = get_api_key_by_prefix(db, prefix)
    if not key_info or key_info["user_id"] != user_id:
        return False

    query = "UPDATE api_keys SET is_active = false WHERE key_prefix = %s"
    prepared = db.prepare(query)
    db.execute(prepared, [prefix])
    return True
