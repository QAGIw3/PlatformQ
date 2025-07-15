import datetime
import secrets
from uuid import UUID

from cassandra.cluster import Session

from ..core.security import get_password_hash, verify_password
from ..schemas.invitation import InvitationCreate

INVITE_TOKEN_EXPIRE_DAYS = 7
INVITE_TOKEN_BYTES = 32


def create_invitation(
    db: Session, invite_in: InvitationCreate, invited_by: UUID
) -> str:
    """
    Creates, hashes, and stores a new invitation token.
    Returns the raw (unhashed) token.
    """
    token = secrets.token_urlsafe(INVITE_TOKEN_BYTES)
    hashed_token = get_password_hash(token)

    now = datetime.datetime.now(datetime.timezone.utc)
    expires = now + datetime.timedelta(days=INVITE_TOKEN_EXPIRE_DAYS)

    query = """
    INSERT INTO invitations (token_hash, email_invited, invited_by_user_id, created_at, expires_at, is_accepted)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    prepared = db.prepare(query)
    db.execute(
        prepared,
        [hashed_token, invite_in.email_to_invite, invited_by, now, expires, False],
    )

    return token


def get_invitation_by_token(db: Session, token: str) -> dict:
    """
    Validates an invitation token and returns its data.
    """
    # This is inefficient as it requires scanning the table.
    query = "SELECT token_hash, email_invited, invited_by_user_id, expires_at, is_accepted FROM invitations"
    rows = db.execute(query)

    for row in rows:
        if verify_password(token, row.token_hash):
            return dict(row._asdict())

    return None


def mark_invitation_as_accepted(db: Session, token: str):
    """
    Marks an invitation token as accepted so it cannot be used again.
    """
    token_data = get_invitation_by_token(db, token)
    if token_data:
        query = "UPDATE invitations SET is_accepted = true WHERE token_hash = %s"
        prepared = db.prepare(query)
        db.execute(prepared, [token_data["token_hash"]])
        return True
    return False
