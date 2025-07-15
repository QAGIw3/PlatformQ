import datetime
import secrets
from uuid import UUID

from cassandra.cluster import Session

from ..core.security import get_password_hash, verify_password

REFRESH_TOKEN_EXPIRE_DAYS = 60
REFRESH_TOKEN_BYTES = 32


def create_refresh_token(db: Session, user_id: UUID) -> str:
    """
    Creates, hashes, and stores a new refresh token for a user.
    Returns the raw (unhashed) token.
    """
    token = secrets.token_urlsafe(REFRESH_TOKEN_BYTES)
    hashed_token = get_password_hash(token)

    now = datetime.datetime.now(datetime.timezone.utc)
    expires = now + datetime.timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    query = """
    INSERT INTO refresh_tokens (token_hash, user_id, created_at, expires_at, is_revoked)
    VALUES (%s, %s, %s, %s, %s)
    """
    prepared = db.prepare(query)
    db.execute(prepared, [hashed_token, user_id, now, expires, False])

    return token


def validate_refresh_token(db: Session, token: str) -> dict:
    """
    Validates a refresh token.
    Returns the token data if valid, otherwise None.
    """
    # This is inefficient as it requires scanning the table.
    # In a real high-performance system, we might need a different approach,
    # but for this service, we accept the limitation. We cannot query by the
    # hashed token directly without a "token salt" system.
    query = "SELECT token_hash, user_id, expires_at, is_revoked FROM refresh_tokens"
    rows = db.execute(query)

    for row in rows:
        if verify_password(token, row.token_hash):
            if row.is_revoked:
                return None  # Token was revoked
            if row.expires_at < datetime.datetime.now(datetime.timezone.utc):
                return None  # Token has expired

            return dict(row._asdict())

    return None


def revoke_refresh_token(db: Session, token: str):
    """
    Revokes a refresh token.
    """
    token_data = validate_refresh_token(db, token)
    if token_data:
        query = "UPDATE refresh_tokens SET is_revoked = true WHERE token_hash = %s"
        prepared = db.prepare(query)
        db.execute(prepared, [token_data["token_hash"]])
        return True
    return False
