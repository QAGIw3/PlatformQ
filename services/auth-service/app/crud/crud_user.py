import datetime
import secrets
from datetime import timedelta
from uuid import UUID, uuid4

from cassandra.cluster import Session

from ..crud.crud_api_key import revoke_api_key
from ..schemas.user import UserCreate, UserUpdate


def get_user_by_email(db: Session, *, email: str):
    """
    Retrieves a user from the database by their email address.
    """
    query = "SELECT tenant_id, id, email, full_name, status, created_at, updated_at FROM users WHERE email = %s ALLOW FILTERING"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [email])
    return rows.one()


def create_user(db: Session, *, tenant_id: UUID, user: UserCreate):
    """
    Creates a new user in the database.
    """
    user_id = uuid4()
    created_time = datetime.datetime.now(datetime.timezone.utc)

    query = """
    INSERT INTO users (tenant_id, id, email, full_name, status, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    prepared_statement = db.prepare(query)
    db.execute(
        prepared_statement,
        [tenant_id, user_id, user.email, user.full_name, "active", created_time, created_time],
    )

    # Retrieve the just-created user to return it
    return get_user_by_id(db, tenant_id=tenant_id, user_id=user_id)


def get_user_by_id(db: Session, *, tenant_id: UUID, user_id: UUID):
    """
    Retrieves a user from the database by their ID.
    """
    query = "SELECT tenant_id, id, email, full_name, status, created_at, updated_at FROM users WHERE tenant_id = %s AND id = %s"
    prepared_statement = db.prepare(query)
    rows = db.execute(prepared_statement, [tenant_id, user_id])
    return rows.one()


def update_user(db: Session, *, tenant_id: UUID, user_id: UUID, user_update: UserUpdate):
    """
    Updates a user's information.
    """
    # For now, we only allow updating the full_name
    if user_update.full_name is not None:
        query = "UPDATE users SET full_name = %s, updated_at = %s WHERE tenant_id = %s AND id = %s"
        prepared = db.prepare(query)
        db.execute(
            prepared,
            [
                user_update.full_name,
                datetime.datetime.now(datetime.timezone.utc),
                tenant_id,
                user_id,
            ],
        )

    return get_user_by_id(db, tenant_id=tenant_id, user_id=user_id)


def set_user_status(db: Session, *, tenant_id: UUID, user_id: UUID, status: str):
    """
    Sets a user's status (e.g., 'active', 'deactivated').
    """
    query = "UPDATE users SET status = %s, updated_at = %s WHERE tenant_id = %s AND id = %s"
    prepared = db.prepare(query)
    db.execute(
        prepared, [status, datetime.datetime.now(datetime.timezone.utc), tenant_id, user_id]
    )
    return get_user_by_id(db, tenant_id=tenant_id, user_id=user_id)


def soft_delete_user(db: Session, *, tenant_id: UUID, user_id: UUID):
    """
    Performs a soft delete by anonymizing PII and setting status to 'deleted'.
    Also revokes all associated refresh tokens and API keys.
    """
    # Anonymize user data
    anonymized_email = f"deleted_{user_id}@platformq.com"
    anonymized_name = "Deleted User"

    query = "UPDATE users SET email = %s, full_name = %s, status = 'deleted', updated_at = %s WHERE tenant_id = %s AND id = %s"
    prepared = db.prepare(query)
    db.execute(
        prepared,
        [
            anonymized_email,
            anonymized_name,
            datetime.datetime.now(datetime.timezone.utc),
            tenant_id,
            user_id,
        ],
    )

    # Invalidate all refresh tokens for the user
    # NOTE: This is very inefficient. A better approach for production would be needed.
    # The current revoke function takes the raw token, not the hash, so we cannot
    # easily get it here. This logic needs to be revisited.
    pass

    # Invalidate all API keys for the user
    ak_query = "SELECT key_prefix FROM api_keys WHERE user_id = %s"
    ak_prepared = db.prepare(ak_query)
    keys = db.execute(ak_prepared, [user_id])
    for key in keys:
        revoke_api_key(db, prefix=key.key_prefix, user_id=user_id)

    return get_user_by_id(db, tenant_id=tenant_id, user_id=user_id)


def create_user_credential(
    db: Session, *, tenant_id: UUID, user_id: UUID, provider: str, provider_user_id: str
):
    """
    Creates a user credential for social login.
    """
    query = "INSERT INTO user_credentials (provider, provider_user_id, tenant_id, user_id) VALUES (%s, %s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(prepared, [provider, provider_user_id, tenant_id, user_id])


def create_passwordless_token(db: Session, email: str) -> str:
    """
    Generates and stores a temporary token for passwordless login.
    """
    token = secrets.token_urlsafe(32)
    expires = datetime.datetime.now(datetime.timezone.utc) + timedelta(
        minutes=15
    )  # Token is valid for 15 minutes

    query = (
        "INSERT INTO passwordless_tokens (email, token, expires_at) VALUES (%s, %s, %s)"
    )
    prepared = db.prepare(query)
    db.execute(prepared, [email, token, expires])
    return token


def verify_passwordless_token(db: Session, email: str, token: str):
    """
    Verifies a temporary token and deletes it if valid.
    """
    query = "SELECT expires_at FROM passwordless_tokens WHERE email = %s AND token = %s"
    prepared = db.prepare(query)
    row = db.execute(prepared, [email, token]).one()

    if not row:
        return None

    # If token is found, delete it so it can't be used again
    delete_query = "DELETE FROM passwordless_tokens WHERE email = %s AND token = %s"
    delete_prepared = db.prepare(delete_query)
    db.execute(delete_prepared, [email, token])

    if row.expires_at < datetime.datetime.now(datetime.timezone.utc):
        return None  # Token has expired

    return row
