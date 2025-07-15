import datetime
import time
from typing import List
from uuid import UUID

from cassandra.cluster import Session
from fastapi import APIRouter, Depends, HTTPException, Request, status, Response
from shared_lib.event_publisher import EventPublisher

from ..core.oidc_server import create_authorization_server, generate_user_info
from ..api.deps import get_current_user_from_trusted_header
from ..crud import (
    crud_api_key,
    crud_audit,
    crud_invitation,
    crud_refresh_token,
    crud_role,
    crud_subscription,
    crud_user,
    crud_oidc,
    crud_tenant
)
from ..db.session import get_db_session
from ..schemas.api_key import ApiKeyCreate, ApiKeyCreateResponse, ApiKeyInfo
from ..schemas.invitation import InvitationAccept, InvitationCreate, InvitationResponse
from ..schemas.role import RoleAssignmentRequest, UserRolesResponse
from ..schemas.subscription import Subscription, SubscriptionCreate, SubscriptionUpdate
from ..schemas.user import (
    PasswordlessLoginRequest,
    PasswordlessLoginToken,
    RefreshTokenRequest,
    Token,
    TokenExchangeRequest,
    User,
    UserCreate,
    UserUpdate,
)
from ..schemas.tenant import Tenant, TenantCreate
from .deps import get_current_tenant_and_user, require_role

router = APIRouter()

# --- Tenant Management ---
@router.post("/tenants", response_model=Tenant, status_code=201)
def create_tenant_endpoint(
    tenant_in: TenantCreate,
    db: Session = Depends(get_db_session),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """
    Public endpoint to create a new tenant. This is the first step for a new organization.
    It also creates the first admin user for that tenant.
    """
    new_tenant = crud_tenant.create_tenant(db, name=tenant_in.name)
    
    # Create the first admin user for this new tenant
    admin_user = UserCreate(email=tenant_in.admin_email, full_name=tenant_in.admin_full_name)
    new_user = crud_user.create_user(db, tenant_id=new_tenant['id'], user=admin_user)
    crud_role.assign_role_to_user(db, tenant_id=new_tenant['id'], user_id=new_user.id, role='admin')
    
    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    crud_subscription.create_subscription_for_user(db, subscription=sub_create)

    # Create audit log
    crud_audit.create_audit_log(
        db,
        user_id=new_user.id,
        event_type="USER_CREATED",
        details=f"User created with email {new_user.email}",
    )

    # Publish user created event to the new tenant's topic
    publisher.publish(
        topic_base='user-events',
        tenant_id=str(new_tenant['id']),
        schema_path='schemas/user_created.avsc',
        data={
            'user_id': str(new_user.id),
            'email': new_user.email,
            'full_name': new_user.full_name,
            'event_timestamp': int(time.time() * 1000)
        }
    )
    return new_tenant

# --- OIDC Endpoints ---

@router.get('/.well-known/openid-configuration')
async def openid_configuration():
    return {
        "issuer": "http://localhost:8000/auth", # Replace with actual public URL
        "authorization_endpoint": "http://localhost:8000/auth/api/v1/authorize",
        "token_endpoint": "http://localhost:8000/auth/api/v1/token",
        "userinfo_endpoint": "http://localhost:8000/auth/api/v1/userinfo",
        "jwks_uri": "http://localhost:8000/auth/api/v1/jwks", # Not implemented yet
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["HS256"],
    }

@router.get('/authorize')
async def authorize(request: Request, db: Session = Depends(get_db_session)):
    # The dependency logic needs to be slightly different here
    # to handle the UI redirect flow, but it will still be tenant-aware.
    # We will assume a tenant_id is passed in the query params for now.
    tenant_id = request.query_params.get("tenant_id")
    try:
        user = get_current_user_from_trusted_header(user_id=request.headers.get("X-User-ID"), db=db)
    except HTTPException:
        # Redirect to a login page or return an error
        return Response("Not logged in", status_code=401)
    
    server = create_authorization_server(db, tenant_id) # OIDC server becomes tenant-aware
    return await server.create_authorization_response(request, user)

@router.post('/token')
async def issue_token(request: Request, db: Session = Depends(get_db_session)):
    server = create_authorization_server(db)
    return await server.create_token_response(request)

@router.get('/userinfo')
async def userinfo(request: Request, db: Session = Depends(get_db_session)):
    # This is a placeholder as Authlib requires a BearerTokenValidator class
    # to protect this route, which we will implement if needed.
    user = get_current_user_from_trusted_header(user_id=request.headers.get("X-User-ID"), db=db)
    return generate_user_info(user, "openid profile email")


# --- User and Auth Endpoints ---


@router.post("/users/", response_model=User, status_code=201)
def create_user_endpoint(
    user: UserCreate,
    db: Session = Depends(get_db_session),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """
    Create a new user, with a default role and subscription.
    """
    db_user = crud_user.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(
            status_code=400,
            detail="Email already registered",
        )

    new_user = crud_user.create_user(db=db, user=user)

    # Assign a default 'member' role
    crud_role.assign_role_to_user(db, user_id=new_user.id, role="member")

    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    crud_subscription.create_subscription_for_user(db, subscription=sub_create)

    # Create audit log
    crud_audit.create_audit_log(
        db,
        user_id=new_user.id,
        event_type="USER_CREATED",
        details=f"User created with email {new_user.email}",
    )

    # Publish user created event
    publisher.publish(
        topic="user-events",
        schema_path="schemas/user_created.avsc",
        data={
            "user_id": str(new_user.id),
            "email": new_user.email,
            "full_name": new_user.full_name,
            "event_timestamp": int(time.time() * 1000),
        },
    )

    return new_user


@router.get("/users/me", response_model=User)
def read_users_me(context: dict = Depends(get_current_tenant_and_user)):
    """
    Get the current logged-in user's details.
    """
    return context["user"]


@router.put("/users/me", response_model=User)
def update_current_user(
    user_in: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Update current user's profile.
    """
    user = crud_user.update_user(db, user_id=current_user.id, user_update=user_in)
    return user


@router.post("/users/me/deactivate", response_model=User)
def deactivate_current_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Deactivate the current user's account.
    """
    user = crud_user.set_user_status(db, user_id=current_user.id, status="deactivated")
    # Add audit log
    crud_audit.create_audit_log(
        db,
        user_id=current_user.id,
        event_type="USER_DEACTIVATED",
        details="User deactivated their account.",
    )
    return user


@router.delete("/users/me", response_model=User)
def delete_current_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Delete the current user's account.
    """
    user = crud_user.soft_delete_user(db, user_id=current_user.id)
    crud_audit.create_audit_log(
        db,
        user_id=current_user.id,
        event_type="USER_DELETED",
        details="User deleted their account.",
    )
    # Here you would publish a user_deleted event
    return user


@router.delete(
    "/users/{user_id}",
    response_model=User,
    dependencies=[Depends(require_role("admin"))],
)
def delete_user_by_admin(user_id: UUID, db: Session = Depends(get_db_session)):
    """
    Delete a user's account (Admin only).
    """
    user = crud_user.soft_delete_user(db, user_id=user_id)
    crud_audit.create_audit_log(
        db,
        user_id=user_id,
        event_type="ADMIN_USER_DELETED",
        details=f"Admin deleted account for user {user_id}.",
    )
    return user


# --- Invitation Endpoints ---


@router.post("/users/invite", response_model=InvitationResponse)
def create_invitation_endpoint(
    invite_in: InvitationCreate,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """Creates an invitation for a new user within the current tenant."""
    # Check if the invited email already exists
    if crud_user.get_user_by_email(db, email=invite_in.email_to_invite):
        raise HTTPException(status_code=400, detail="Email already registered")

    token = crud_invitation.create_invitation(
        db, tenant_id=context["tenant_id"], invite_in=invite_in, invited_by=context["user"].id
    )
    crud_audit.create_audit_log(
        db,
        user_id=context["user"].id,
        event_type="INVITATION_CREATED",
        details=f"Invitation sent to {invite_in.email_to_invite}",
    )
    return {"invitation_token": token, "email_invited": invite_in.email_to_invite, "invited_by_user_id": context["user"].id}


@router.post("/invitations/accept", response_model=User, status_code=201)
def accept_invitation_endpoint(
    accept_in: InvitationAccept,
    db: Session = Depends(get_db_session),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Accept an invitation. Tenant is determined by the token."""
    # The get_invitation_by_token function must be updated to not require tenant_id
    # as the token is the only piece of info the new user has.
    invite_data = crud_invitation.get_invitation_by_token(db, token=accept_in.invitation_token)

    if not invite_data:
        raise HTTPException(status_code=404, detail="Invitation not found or invalid.")
    if invite_data["is_accepted"]:
        raise HTTPException(
            status_code=400, detail="Invitation has already been accepted."
        )
    if invite_data["expires_at"] < datetime.datetime.now(datetime.timezone.utc):
        raise HTTPException(status_code=400, detail="Invitation has expired.")

    # Mark invitation as used
    crud_invitation.mark_invitation_as_accepted(db, token=accept_in.invitation_token)

    # Create the new user in the correct tenant
    user_create = UserCreate(email=invite_data['email_invited'], full_name=accept_in.full_name)
    new_user = crud_user.create_user(db, tenant_id=invite_data['tenant_id'], user=user_create)
    
    # Assign a default 'member' role
    crud_role.assign_role_to_user(db, user_id=new_user.id, role="member")

    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    crud_subscription.create_subscription_for_user(db, subscription=sub_create)

    # Create audit log
    crud_audit.create_audit_log(
        db,
        user_id=new_user.id,
        event_type="USER_CREATED",
        details=f"User created with email {new_user.email}",
    )

    # Publish user created event
    publisher.publish(
        topic="user-events",
        schema_path="schemas/user_created.avsc",
        data={
            "user_id": str(new_user.id),
            "email": new_user.email,
            "full_name": new_user.full_name,
            "event_timestamp": int(time.time() * 1000),
        },
    )

    return new_user


# --- Role Management Endpoints ---


@router.post(
    "/roles/assign",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_role("admin"))],
)
def assign_role(
    assignment: RoleAssignmentRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """
    Assign a role to a user.
    **Note:** This should be a protected endpoint in a real application.
    """
    # Check if user exists
    user = crud_user.get_user_by_id(db, user_id=assignment.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    crud_role.assign_role_to_user(
        db, tenant_id=context["tenant_id"], user_id=assignment.user_id, role=assignment.role
    )
    crud_audit.create_audit_log(
        db,
        event_type="ROLE_ASSIGNED",
        details=f"Role '{assignment.role}' assigned to user {assignment.user_id}",
    )
    return


@router.delete(
    "/roles/revoke",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_role("admin"))],
)
def revoke_role(
    assignment: RoleAssignmentRequest, db: Session = Depends(get_db_session)
):
    """
    Revoke a role from a user (Admin only).
    """
    user = crud_user.get_user_by_id(db, user_id=assignment.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    crud_role.remove_role_from_user(
        db, user_id=assignment.user_id, role=assignment.role
    )
    crud_audit.create_audit_log(
        db,
        event_type="ROLE_REVOKED",
        details=f"Role '{assignment.role}' revoked from user {assignment.user_id}",
    )
    return


@router.get("/users/{user_id}/roles", response_model=UserRolesResponse)
def get_user_roles(user_id: UUID, db: Session = Depends(get_db_session)):
    """
    Get all roles for a specific user.
    """
    user = crud_user.get_user_by_id(db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    roles = crud_role.get_roles_for_user(db, user_id=user_id)
    return {"user_id": user_id, "roles": roles}


# --- Subscription Management Endpoints ---


@router.put(
    "/users/{user_id}/subscription",
    response_model=Subscription,
    dependencies=[Depends(require_role("admin"))],
)
def update_user_subscription(
    user_id: UUID,
    sub_update: SubscriptionUpdate,
    db: Session = Depends(get_db_session),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """
    Update a user's subscription tier or status (Admin only).
    """
    updated_sub = crud_subscription.update_subscription_for_user(
        db, user_id=user_id, sub_update=sub_update
    )
    if not updated_sub:
        raise HTTPException(
            status_code=404, detail="Subscription not found for this user"
        )

    # Create audit log
    crud_audit.create_audit_log(
        db,
        user_id=user_id,
        event_type="SUBSCRIPTION_UPDATED",
        details=f"Subscription updated for user {user_id}. New tier: {sub_update.tier}, new status: {sub_update.status}",
    )

    # Publish subscription changed event
    publisher.publish(
        topic="subscription-events",
        schema_path="schemas/subscription_changed.avsc",
        data={
            "user_id": str(updated_sub["user_id"]),
            "subscription_id": str(updated_sub["id"]),
            "new_tier": updated_sub["tier"],
            "new_status": updated_sub["status"],
            "event_timestamp": int(time.time() * 1000),
        },
    )

    return updated_sub


@router.get("/users/{user_id}/subscription", response_model=Subscription)
def get_user_subscription(
    user_id: UUID,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Get subscription details for a user.
    A user can only see their own subscription, unless they are an admin.
    """
    user_roles = crud_role.get_roles_for_user(db, user_id=current_user.id)
    if current_user.id != user_id and "admin" not in user_roles:
        raise HTTPException(
            status_code=403, detail="Not authorized to view this subscription"
        )

    subscription = crud_subscription.get_subscription_by_user_id(db, user_id=user_id)
    if not subscription:
        raise HTTPException(
            status_code=404, detail="Subscription not found for this user"
        )

    return subscription


# --- API Key Management Endpoints ---


@router.post("/api-keys", response_model=ApiKeyCreateResponse, status_code=201)
def create_api_key(
    key_in: ApiKeyCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Create a new API key for the current user.
    """
    api_key = crud_api_key.create_api_key_for_user(
        db, user_id=current_user.id, key_in=key_in
    )
    crud_audit.create_audit_log(
        db,
        user_id=current_user.id,
        event_type="API_KEY_CREATED",
        details=f"API key created with prefix {api_key['key_prefix']}",
    )
    return api_key


@router.get("/api-keys", response_model=List[ApiKeyInfo])
def get_api_keys(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Get all API keys for the current user.
    """
    return crud_api_key.get_api_keys_for_user(db, user_id=current_user.id)


@router.delete("/api-keys/{key_prefix}", status_code=204)
def revoke_api_key_endpoint(
    key_prefix: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Revoke an API key for the current user.
    """
    success = crud_api_key.revoke_api_key(
        db, prefix=key_prefix, user_id=current_user.id
    )
    if not success:
        raise HTTPException(
            status_code=404,
            detail="API key not found or you do not have permission to revoke it.",
        )

    crud_audit.create_audit_log(
        db,
        user_id=current_user.id,
        event_type="API_KEY_REVOKED",
        details=f"API key with prefix {key_prefix} revoked.",
    )
    return


@router.post("/login/passwordless", response_model=PasswordlessLoginToken)
def request_passwordless_login(
    request: PasswordlessLoginRequest, db: Session = Depends(get_db_session)
):
    """
    Initiate a passwordless login.
    This checks if the user exists and generates a temporary token.
    """
    user = crud_user.get_user_by_email(db, email=request.email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail="User with this email does not exist.",
        )

    temp_token = crud_user.create_passwordless_token(db, email=request.email)

    # In a real app, you would email this token to the user.
    # For this example, we return it directly.
    return {"email": request.email, "temp_token": temp_token}


@router.post("/token/passwordless", response_model=Token)
def get_access_token_from_passwordless(
    request: TokenExchangeRequest, db: Session = Depends(get_db_session)
):
    """
    Exchange a temporary passwordless token for a JWT access token.
    """
    valid_token = crud_user.verify_passwordless_token(
        db, email=request.email, token=request.temp_token
    )
    if not valid_token:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired token.",
        )

    user = crud_user.get_user_by_email(db, email=request.email)

    access_token = create_access_token(data={"sub": request.email})
    refresh_token = crud_refresh_token.create_refresh_token(db, user_id=user.id)

    crud_audit.create_audit_log(
        db,
        user_id=user.id,
        event_type="USER_LOGIN_PASSWORDLESS",
        details=f"User {request.email} successfully logged in via passwordless token.",
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token": refresh_token,
    }


@router.post("/token/refresh", response_model=Token)
def refresh_access_token(
    request: RefreshTokenRequest, db: Session = Depends(get_db_session)
):
    """
    Get a new access token from a valid refresh token.
    """
    token_data = crud_refresh_token.validate_refresh_token(
        db, token=request.refresh_token
    )
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    user = crud_user.get_user_by_id(db, user_id=token_data["user_id"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    new_access_token = create_access_token(data={"sub": user.email})

    return {
        "access_token": new_access_token,
        "token_type": "bearer",
        "refresh_token": request.refresh_token,  # Return the same refresh token
    }


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    request: RefreshTokenRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),  # Ensures user is logged in
):
    """
    Revoke the user's refresh token, effectively logging them out.
    """
    crud_refresh_token.revoke_refresh_token(db, token=request.refresh_token)
    crud_audit.create_audit_log(
        db,
        user_id=current_user.id,
        event_type="USER_LOGOUT",
        details=f"User {current_user.email} logged out.",
    )
    return
