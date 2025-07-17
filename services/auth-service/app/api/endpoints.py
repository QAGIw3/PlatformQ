import datetime
import time
from typing import List
from uuid import UUID

import httpx
from cassandra.cluster import Session
from fastapi import APIRouter, Depends, HTTPException, Request, status, Response
from platformq_shared.event_publisher import EventPublisher

from ..api.deps import (
    get_current_user_from_trusted_header,
    get_user_repository,
    get_api_key_repository,
    get_invitation_repository,
    get_role_repository,
    get_subscription_repository,
    get_tenant_repository,
    get_oidc_repository,
    get_siwe_repository,
    get_audit_repository,
    get_refresh_token_repository
)
from ..repository import (
    UserRepository,
    APIKeyRepository,
    InvitationRepository,
    RoleRepository,
    SubscriptionRepository,
    TenantRepository,
    OIDCRepository,
    SIWERepository,
    AuditRepository,
    RefreshTokenRepository
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
    LinkWalletRequest,
    StorageConfigUpdate,
)
from ..schemas.tenant import Tenant, TenantCreate
from .deps import get_current_tenant_and_user, require_role, get_current_user, require_service_token, get_event_publisher
from ..services.user_service import UserService
from platformq_shared.events import UserCreatedEvent, SubscriptionChangedEvent
from siwe import SiweMessage
from jose import jwt
from jose.constants import ALGORITHMS
from platformq_shared.jwt import create_access_token
from ..core.config import settings

router = APIRouter()

# ==============================================================================
# Tenant & User Management Endpoints
# ==============================================================================
# These are the core administrative endpoints for managing the platform's tenants and users.


@router.post("/tenants", response_model=Tenant, status_code=201, tags=["Tenants"])
async def create_tenant_endpoint(
    tenant_in: TenantCreate,
    tenant_repo: TenantRepository = Depends(get_tenant_repository),
    user_repo: UserRepository = Depends(get_user_repository),
    role_repo: RoleRepository = Depends(get_role_repository),
    subscription_repo: SubscriptionRepository = Depends(get_subscription_repository),
    audit_repo: AuditRepository = Depends(get_audit_repository),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """
    Public endpoint to create a new tenant. This is the first step for a new 
    organization to join the platform. It creates the tenant and then calls the
    provisioning service to set up all necessary resources.
    """
    new_tenant = tenant_repo.create_tenant(name=tenant_in.name)
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{settings.provisioning_service_url}/provision",
                json={"tenant_id": str(new_tenant['id']), "tenant_name": new_tenant['name']},
                timeout=30.0
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            # If provisioning fails, we should ideally roll back the tenant creation.
            # For now, we'll just log the error and raise an exception.
            print(f"Failed to provision tenant {new_tenant['id']}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to provision tenant: {str(e)}"
            )

    user_service = UserService(user_repo)
    
    # Create the first admin user for this new tenant
    admin_user_in = UserCreate(email=tenant_in.admin_email, full_name=tenant_in.admin_full_name, did=tenant_in.admin_did)
    new_user = user_service.create_full_user(tenant_id=new_tenant['id'], user_in=admin_user_in)
    role_repo.assign_role_to_user(tenant_id=new_tenant['id'], user_id=new_user.id, role='admin')
    
    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    subscription_repo.create_subscription_for_user(subscription=sub_create)

    # Create audit log
    audit_repo.create_audit_log(
        user_id=new_user.id,
        event_type="USER_CREATED",
        details=f"User created with email {new_user.email}",
    )

    # Publish user created event using the schema class
    publisher.publish(
        topic_base='user-events',
        tenant_id=str(new_tenant['id']),
        schema_class=UserCreatedEvent,
        data=UserCreatedEvent(
            tenant_id=str(new_tenant['id']),
            user_id=str(new_user.id),
            email=new_user.email,
            full_name=new_user.full_name,
        )
    )
    return new_tenant


@router.post("/users/", response_model=User, status_code=201)
def create_user_endpoint(
    user: UserCreate,
    user_repo: UserRepository = Depends(get_user_repository),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """
    Create a new user, with a default role and subscription.
    """
    db_user = user_repo.get_by_email(email=user.email)
    if db_user:
        raise HTTPException(
            status_code=400,
            detail="Email already registered",
        )

    new_user = user_repo.add(user=user, tenant_id=user.tenant_id)

    # Assign a default 'member' role
    role_repo.assign_role_to_user(user_id=new_user.id, role="member")

    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    subscription_repo.create_subscription_for_user(subscription=sub_create)

    # Create audit log
    audit_repo.create_audit_log(
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


@router.get("/users/me", response_model=User, tags=["Users"])
def read_users_me(context: dict = Depends(get_current_tenant_and_user)):
    """
    Get the current logged-in user's details.
    """
    return context["user"]


@router.put("/users/me", response_model=User)
def update_current_user(
    user_in: UserUpdate,
    current_user: User = Depends(get_current_user),
    user_repo: UserRepository = Depends(get_user_repository),
):
    """
    Update current user's profile.
    """
    user = user_repo.update(user_id=current_user.id, user_update=user_in)
    return user


@router.put("/users/me/storage", response_model=User)
def update_storage_config(
    storage_in: StorageConfigUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Update the current user's storage configuration.
    """
    # In a real app, you would add validation for the storage_config JSON
    # and ensure the storage_backend is a valid choice.
    user_update = UserUpdate(
        storage_backend=storage_in.storage_backend,
        storage_config=storage_in.storage_config
    )
    updated_user = user_repo.update_user(user_id=current_user.id, user_update=user_update)
    return updated_user


@router.post("/users/me/link-wallet", response_model=User)
def link_wallet(
    request: LinkWalletRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Link a wallet to the current user's account using SIWE.
    """
    siwe_message = SiweMessage(message=request.message)

    if not siwe_repo.use_nonce(db, siwe_message.nonce):
        raise HTTPException(status_code=422, detail="Invalid nonce.")

    try:
        siwe_message.verify(request.signature)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid signature: {e}")

    wallet_address = siwe_message.address
    
    # Check if this wallet is already linked to another user
    existing_user = user_repo.get_user_by_wallet_address(db, wallet_address=wallet_address)
    if existing_user and existing_user.id != current_user.id:
        raise HTTPException(status_code=400, detail="Wallet is already linked to another account.")

    # Update the user with wallet address and DID
    did = f"did:ethr:{wallet_address}"
    user_update = UserUpdate(wallet_address=wallet_address, did=did)
    
    updated_user = user_repo.update_user(user_id=current_user.id, user_update=user_update)
    return updated_user


@router.post("/users/me/deactivate", response_model=User)
def deactivate_current_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """
    Deactivate the current user's account.
    """
    user = user_repo.set_user_status(db, user_id=current_user.id, status="deactivated")
    # Add audit log
    audit_repo.create_audit_log(
        user_id=current_user.id,
        event_type="USER_DEACTIVATED",
        details="User deactivated their account.",
    )
    return user


@router.delete("/users/me", response_model=User)
def delete_current_user(
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """
    Delete the current user's account by calling the UserService.
    """
    user_service = UserService(user_repo)
    deleted_user = user_service.soft_delete_full_user(
        tenant_id=context["tenant_id"], user_id=context["user"].id
    )
    audit_repo.create_audit_log(
        user_id=context["user"].id,
        event_type="USER_DELETED",
        details="User deleted their account.",
    )
    # Here you would publish a user_deleted event
    return deleted_user


@router.delete(
    "/users/{user_id}",
    response_model=User,
    dependencies=[Depends(require_role("admin"))],
)
def delete_user_by_admin(user_id: UUID, db: Session = Depends(get_db_session)):
    """
    Delete a user's account (Admin only).
    """
    user = user_repo.soft_delete_user(db, user_id=user_id)
    audit_repo.create_audit_log(
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
    if user_repo.get_user_by_email(db, email=invite_in.email_to_invite):
        raise HTTPException(status_code=400, detail="Email already registered")

    token = invitation_repo.create_invitation(
        db, tenant_id=context["tenant_id"], invite_in=invite_in, invited_by=context["user"].id
    )
    audit_repo.create_audit_log(
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
    invite_data = invitation_repo.get_invitation_by_token(db, token=accept_in.invitation_token)

    if not invite_data:
        raise HTTPException(status_code=404, detail="Invitation not found or invalid.")
    if invite_data["is_accepted"]:
        raise HTTPException(
            status_code=400, detail="Invitation has already been accepted."
        )
    if invite_data["expires_at"] < datetime.datetime.now(datetime.timezone.utc):
        raise HTTPException(status_code=400, detail="Invitation has expired.")

    # Mark invitation as used
    invitation_repo.mark_invitation_as_accepted(db, token=accept_in.invitation_token)

    # Create the new user in the correct tenant
    user_create = UserCreate(email=invite_data['email_invited'], full_name=accept_in.full_name)
    new_user = user_repo.create_user(db, tenant_id=invite_data['tenant_id'], user=user_create)
    
    # Assign a default 'member' role
    role_repo.assign_role_to_user(user_id=new_user.id, role="member")

    # Assign a default 'free' subscription tier
    sub_create = SubscriptionCreate(user_id=new_user.id, tier="free")
    subscription_repo.create_subscription_for_user(subscription=sub_create)

    # Create audit log
    audit_repo.create_audit_log(
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
    user = user_repo.get_user_by_id(db, user_id=assignment.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    role_repo.assign_role_to_user(
        db, tenant_id=context["tenant_id"], user_id=assignment.user_id, role=assignment.role
    )
    audit_repo.create_audit_log(
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
    user = user_repo.get_user_by_id(db, user_id=assignment.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    role_repo.remove_role_from_user(
        db, user_id=assignment.user_id, role=assignment.role
    )
    audit_repo.create_audit_log(
        event_type="ROLE_REVOKED",
        details=f"Role '{assignment.role}' revoked from user {assignment.user_id}",
    )
    return


@router.get("/users/{user_id}/roles", response_model=UserRolesResponse)
def get_user_roles(user_id: UUID, db: Session = Depends(get_db_session)):
    """
    Get all roles for a specific user.
    """
    user = user_repo.get_user_by_id(db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    roles = role_repo.get_roles_for_user(db, user_id=user_id)
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
    updated_sub = subscription_repo.update_subscription_for_user(
        db, user_id=user_id, sub_update=sub_update
    )
    if not updated_sub:
        raise HTTPException(
            status_code=404, detail="Subscription not found for this user"
        )

    # Create audit log
    audit_repo.create_audit_log(
        user_id=user_id,
        event_type="SUBSCRIPTION_UPDATED",
        details=f"Subscription updated for user {user_id}. New tier: {sub_update.tier}, new status: {sub_update.status}",
    )

    # Publish subscription changed event using the schema class
    publisher.publish(
        topic_base='subscription-events',
        tenant_id=str(updated_sub['tenant_id']),
        schema_class=SubscriptionChangedEvent,
        data=SubscriptionChangedEvent(
            user_id=str(updated_sub['user_id']),
            subscription_id=str(updated_sub['id']),
            new_tier=updated_sub['tier'],
            new_status=updated_sub['status'],
        )
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
    user_roles = role_repo.get_roles_for_user(db, user_id=current_user.id)
    if current_user.id != user_id and "admin" not in user_roles:
        raise HTTPException(
            status_code=403, detail="Not authorized to view this subscription"
        )

    subscription = subscription_repo.get_subscription_by_user_id(db, user_id=user_id)
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
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository),
):
    """
    Create a new API key for the current user.
    """
    api_key = api_key_repo.add(
        user_id=current_user.id, key_in=key_in
    )
    audit_repo.create_audit_log(
        user_id=current_user.id,
        event_type="API_KEY_CREATED",
        details=f"API key created with prefix {api_key['key_prefix']}",
    )
    return api_key


@router.get("/api-keys", response_model=List[ApiKeyInfo])
def get_api_keys(
    current_user: User = Depends(get_current_user),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository),
):
    """
    Get all API keys for the current user.
    """
    return api_key_repo.list(user_id=current_user.id)


@router.delete("/api-keys/{key_prefix}", status_code=204)
def revoke_api_key_endpoint(
    key_prefix: str,
    current_user: User = Depends(get_current_user),
    api_key_repo: APIKeyRepository = Depends(get_api_key_repository),
):
    """
    Revoke an API key for the current user.
    """
    api_key_repo.delete(
        prefix=key_prefix, user_id=current_user.id
    )
    audit_repo.create_audit_log(
        user_id=current_user.id,
        event_type="API_KEY_REVOKED",
        details=f"API key with prefix {key_prefix} revoked.",
    )
    return


# ==============================================================================
# Internal Service-to-Service Endpoints
# ==============================================================================

@router.get("/internal/users/{user_id}", response_model=User)
def get_user_details_for_s2s(
    user_id: UUID,
    db: Session = Depends(get_db_session),
    service_token: dict = Depends(require_service_token),
):
    """
    Internal endpoint for services to retrieve user details.
    """
    user = user_repo.get_user_by_id(db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# ==============================================================================
# Decentralized Identity Endpoints (Sign-In with Ethereum)
# ==============================================================================
# These endpoints provide an alternative, Web3-native authentication method.


@router.get("/siwe/nonce", tags=["SIWE"])
def get_siwe_nonce(request: Request, db: Session = Depends(get_db_session)):
    """
    Generate a nonce for a SIWE (Sign-In with Ethereum) login request.
    This nonce is used to prevent replay attacks.
    """
    # In a real app, you would generate a unique nonce per request.
    # For this example, we'll just return a placeholder.
    return {"nonce": "your_nonce_here"}


@router.post("/siwe/login", tags=["SIWE"])
def siwe_login(request: Request, db: Session = Depends(get_db_session)):
    """
    Process a SIWE login request.
    This endpoint expects a POST request with a 'message' field containing
    the signed message and a 'signature' field.
    """
    # In a real app, you would parse the message, verify the signature,
    # and then generate a JWT access token.
    # For this example, we'll just return a placeholder.
    return {"message": "SIWE login successful", "token": "your_access_token_here"}


@router.post("/login/passwordless", response_model=PasswordlessLoginToken)
def request_passwordless_login(
    request: PasswordlessLoginRequest, db: Session = Depends(get_db_session)
):
    """
    Initiate a passwordless login.
    This checks if the user exists and generates a temporary token.
    """
    user = user_repo.get_user_by_email(db, email=request.email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail="User with this email does not exist.",
        )

    temp_token = user_repo.create_passwordless_token(db, email=request.email)

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
    valid_token = user_repo.verify_passwordless_token(
        db, email=request.email, token=request.temp_token
    )
    if not valid_token:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired token.",
        )

    user = user_repo.get_user_by_email(db, email=request.email)
    roles = role_repo.get_roles_for_user(db, user_id=user.id, tenant_id=user.tenant_id)
    subscription = subscription_repo.get_subscription_by_user_id(db, user_id=user.id)

    # Add roles and subscription tier to the token
    token_data = {
        "sub": str(user.id),
        "tid": str(user.tenant_id),
        "roles": roles,
        "tier": subscription.tier if subscription else "free",
    }
    
    access_token = create_access_token(data=token_data)
    refresh_token = refresh_token_repo.create_refresh_token(db, user_id=user.id)

    audit_repo.create_audit_log(
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
    token_data = refresh_token_repo.validate_refresh_token(
        db, token=request.refresh_token
    )
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    user = user_repo.get_user_by_id(db, user_id=token_data["user_id"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
        
    roles = role_repo.get_roles_for_user(db, user_id=user.id, tenant_id=user.tenant_id)
    subscription = subscription_repo.get_subscription_by_user_id(db, user_id=user.id)

    # Add roles and subscription tier to the new token
    token_data = {
        "sub": str(user.id),
        "tid": str(user.tenant_id),
        "roles": roles,
        "tier": subscription.tier if subscription else "free",
    }

    new_access_token = create_access_token(data=token_data)

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
    refresh_token_repo.revoke_refresh_token(db, token=request.refresh_token)
    audit_repo.create_audit_log(
        user_id=current_user.id,
        event_type="USER_LOGOUT",
        details=f"User {current_user.email} logged out.",
    )
    return
