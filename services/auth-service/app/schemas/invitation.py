from uuid import UUID

from pydantic import BaseModel, EmailStr


class InvitationCreate(BaseModel):
    email_to_invite: EmailStr


class InvitationResponse(BaseModel):
    email_invited: EmailStr
    invited_by_user_id: UUID
    # The token is returned for testing purposes. In production, this would be emailed.
    invitation_token: str


class InvitationAccept(BaseModel):
    invitation_token: str
    full_name: str
