"""
OAuth2/OIDC Endpoints for PlatformQ Auth Service

Implements:
- OAuth2 Authorization Code Flow
- OpenID Connect Discovery
- Token endpoint
- UserInfo endpoint
- JWKS endpoint
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, List
from uuid import UUID, uuid4
import json
import secrets
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Form, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from platformq_consul import create_service_config
from ..db.session import get_db_session
from ..core.config import settings
from ..core.security import create_access_token, get_password_hash, verify_password
from ..core.oidc_server import create_authorization_server, generate_user_info
from ..schemas.oauth2 import (
    TokenResponse,
    AuthorizationRequest,
    TokenRequest,
    UserInfoResponse,
    OIDCConfiguration,
    JWKSResponse
)
from ..crud import crud_user, crud_oidc
from ..api.deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/oauth2", tags=["OAuth2/OIDC"])

# In-memory storage for authorization codes (in production, use Redis/Ignite)
authorization_codes = {}

# Generate RSA key pair for JWT signing (in production, store in Vault)
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
    backend=default_backend()
)
public_key = private_key.public_key()

# Service configuration
service_config = create_service_config("auth-service")


@router.get("/.well-known/openid-configuration", response_model=OIDCConfiguration)
async def openid_configuration(request: Request):
    """
    OpenID Connect Discovery endpoint
    
    Returns metadata about the authorization server
    """
    base_url = str(request.base_url).rstrip('/')
    
    return {
        "issuer": f"{base_url}/oauth2",
        "authorization_endpoint": f"{base_url}/oauth2/authorize",
        "token_endpoint": f"{base_url}/oauth2/token",
        "userinfo_endpoint": f"{base_url}/oauth2/userinfo",
        "jwks_uri": f"{base_url}/oauth2/jwks",
        "registration_endpoint": f"{base_url}/oauth2/register",
        "scopes_supported": [
            "openid", "profile", "email", "offline_access",
            "trade:read", "trade:write", "analytics:read", "ml:access"
        ],
        "response_types_supported": ["code", "token", "id_token", "code id_token"],
        "grant_types_supported": [
            "authorization_code", "refresh_token", "client_credentials"
        ],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "token_endpoint_auth_methods_supported": [
            "client_secret_basic", "client_secret_post"
        ],
        "claims_supported": [
            "sub", "name", "email", "email_verified", "roles", "tenant_id"
        ],
        "code_challenge_methods_supported": ["S256", "plain"]
    }


@router.get("/authorize")
async def authorize(
    client_id: str = Query(...),
    response_type: str = Query(...),
    redirect_uri: str = Query(...),
    scope: str = Query(...),
    state: Optional[str] = Query(None),
    code_challenge: Optional[str] = Query(None),
    code_challenge_method: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """
    OAuth2 Authorization endpoint
    
    Handles the authorization request and returns an authorization code
    """
    # Validate client
    client = crud_oidc.get_oidc_client(db, client_id=client_id)
    if not client:
        raise HTTPException(status_code=400, detail="Invalid client_id")
    
    # Validate redirect URI
    if redirect_uri not in client.redirect_uris:
        raise HTTPException(status_code=400, detail="Invalid redirect_uri")
    
    # Validate response type
    if response_type not in ["code", "token"]:
        raise HTTPException(status_code=400, detail="Unsupported response_type")
    
    # Generate authorization code
    code = secrets.token_urlsafe(32)
    
    # Store authorization code with metadata
    authorization_codes[code] = {
        "client_id": client_id,
        "user_id": current_user["sub"],
        "redirect_uri": redirect_uri,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "expires_at": datetime.utcnow() + timedelta(minutes=10)
    }
    
    # Log authorization
    await service_config.discovery.get_service_config(
        f"audit/oauth2/authorize/{current_user['sub']}"
    )
    
    # Build redirect URL
    redirect_url = f"{redirect_uri}?code={code}"
    if state:
        redirect_url += f"&state={state}"
    
    return RedirectResponse(url=redirect_url, status_code=302)


@router.get("/authorize/consent")
async def consent_page(
    client_id: str = Query(...),
    scope: str = Query(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """
    Display consent page for user authorization
    """
    client = crud_oidc.get_oidc_client(db, client_id=client_id)
    if not client:
        raise HTTPException(status_code=400, detail="Invalid client_id")
    
    scopes = scope.split(" ")
    
    # Simple HTML consent page (in production, use templates)
    html_content = f"""
    <html>
    <head>
        <title>Authorize {client.client_name}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .container {{ max-width: 600px; margin: 0 auto; }}
            .scopes {{ margin: 20px 0; }}
            .scope-item {{ margin: 10px 0; }}
            .buttons {{ margin-top: 30px; }}
            button {{ padding: 10px 20px; margin: 0 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Authorize {client.client_name}</h1>
            <p>{client.client_name} is requesting access to your account.</p>
            
            <div class="scopes">
                <h3>This application will be able to:</h3>
                {''.join(f'<div class="scope-item">• {s}</div>' for s in scopes)}
            </div>
            
            <form method="post" action="/oauth2/authorize/confirm">
                <input type="hidden" name="client_id" value="{client_id}">
                <input type="hidden" name="scope" value="{scope}">
                <div class="buttons">
                    <button type="submit" name="action" value="allow">Allow</button>
                    <button type="submit" name="action" value="deny">Deny</button>
                </div>
            </form>
        </div>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)


@router.post("/token", response_model=TokenResponse)
async def token(
    grant_type: str = Form(...),
    code: Optional[str] = Form(None),
    redirect_uri: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    client_secret: Optional[str] = Form(None),
    refresh_token: Optional[str] = Form(None),
    scope: Optional[str] = Form(None),
    code_verifier: Optional[str] = Form(None),
    db: Session = Depends(get_db_session)
):
    """
    OAuth2 Token endpoint
    
    Exchanges authorization code or refresh token for access token
    """
    if grant_type == "authorization_code":
        if not code:
            raise HTTPException(status_code=400, detail="Missing authorization code")
        
        # Validate authorization code
        auth_code_data = authorization_codes.get(code)
        if not auth_code_data:
            raise HTTPException(status_code=400, detail="Invalid authorization code")
        
        # Check expiration
        if datetime.utcnow() > auth_code_data["expires_at"]:
            authorization_codes.pop(code, None)
            raise HTTPException(status_code=400, detail="Authorization code expired")
        
        # Validate client
        if client_id != auth_code_data["client_id"]:
            raise HTTPException(status_code=400, detail="Invalid client_id")
        
        # Validate redirect URI
        if redirect_uri != auth_code_data["redirect_uri"]:
            raise HTTPException(status_code=400, detail="Invalid redirect_uri")
        
        # Validate PKCE if used
        if auth_code_data.get("code_challenge"):
            if not code_verifier:
                raise HTTPException(status_code=400, detail="Missing code_verifier")
            # Verify code challenge (simplified, implement proper PKCE verification)
        
        # Get user
        user = crud_user.get_user_by_id(db, user_id=auth_code_data["user_id"])
        if not user:
            raise HTTPException(status_code=400, detail="User not found")
        
        # Generate tokens
        access_token_data = {
            "sub": str(user.id),
            "email": user.email,
            "roles": [role.name for role in user.roles],
            "tenant_id": str(user.tenant_id),
            "scope": auth_code_data["scope"]
        }
        
        access_token = create_access_token(data=access_token_data)
        refresh_token_value = secrets.token_urlsafe(32)
        
        # Store refresh token (in production, use database)
        
        # Generate ID token if openid scope is requested
        id_token = None
        if "openid" in auth_code_data["scope"]:
            id_token_data = {
                "iss": f"{settings.API_V1_STR}/oauth2",
                "sub": str(user.id),
                "aud": client_id,
                "exp": datetime.utcnow() + timedelta(hours=1),
                "iat": datetime.utcnow(),
                "auth_time": int(datetime.utcnow().timestamp()),
                "email": user.email,
                "email_verified": user.email_verified
            }
            id_token = jwt.encode(id_token_data, private_key, algorithm="RS256")
        
        # Remove used authorization code
        authorization_codes.pop(code, None)
        
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": refresh_token_value,
            "id_token": id_token,
            "scope": auth_code_data["scope"]
        }
    
    elif grant_type == "refresh_token":
        if not refresh_token:
            raise HTTPException(status_code=400, detail="Missing refresh token")
        
        # Validate refresh token and get new access token
        # Implementation depends on refresh token storage
        
        raise HTTPException(status_code=501, detail="Refresh token grant not implemented")
    
    elif grant_type == "client_credentials":
        # Validate client credentials
        client = crud_oidc.get_oidc_client(db, client_id=client_id)
        if not client or not verify_password(client_secret, client.client_secret_hash):
            raise HTTPException(status_code=401, detail="Invalid client credentials")
        
        # Generate service account token
        access_token_data = {
            "sub": client_id,
            "client_id": client_id,
            "scope": scope or "service",
            "grant_type": "client_credentials"
        }
        
        access_token = create_access_token(data=access_token_data)
        
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": scope or "service"
        }
    
    else:
        raise HTTPException(status_code=400, detail="Unsupported grant type")


@router.get("/userinfo", response_model=UserInfoResponse)
async def userinfo(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """
    OpenID Connect UserInfo endpoint
    
    Returns claims about the authenticated user
    """
    user = crud_user.get_user_by_id(db, user_id=current_user["sub"])
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Return user info based on requested scopes
    userinfo_data = {
        "sub": str(user.id),
        "email": user.email,
        "email_verified": user.email_verified,
        "updated_at": int(user.updated_at.timestamp())
    }
    
    # Add profile claims if requested
    if "profile" in current_user.get("scope", ""):
        userinfo_data.update({
            "name": user.full_name,
            "preferred_username": user.username,
            "locale": user.locale or "en-US",
            "zoneinfo": user.timezone or "UTC"
        })
    
    # Add custom claims
    userinfo_data.update({
        "roles": current_user.get("roles", []),
        "tenant_id": current_user.get("tenant_id"),
        "reputation_score": user.reputation_score
    })
    
    return userinfo_data


@router.get("/jwks", response_model=JWKSResponse)
async def jwks():
    """
    JSON Web Key Set endpoint
    
    Returns public keys used to verify JWT signatures
    """
    # Export public key in JWK format
    public_numbers = public_key.public_numbers()
    
    # Convert to base64url encoded values
    import base64
    
    def to_base64url(num: int, size: int) -> str:
        """Convert integer to base64url encoded string"""
        return base64.urlsafe_b64encode(
            num.to_bytes(size, byteorder='big')
        ).decode('utf-8').rstrip('=')
    
    jwk = {
        "kty": "RSA",
        "use": "sig",
        "kid": "1",  # Key ID
        "alg": "RS256",
        "n": to_base64url(public_numbers.n, 256),  # Modulus
        "e": to_base64url(public_numbers.e, 3)     # Exponent
    }
    
    return {"keys": [jwk]}


@router.post("/introspect")
async def introspect(
    token: str = Form(...),
    token_type_hint: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    client_secret: Optional[str] = Form(None),
    db: Session = Depends(get_db_session)
):
    """
    OAuth2 Token Introspection endpoint
    
    Validates and returns information about a token
    """
    try:
        # Decode token without verification first to get claims
        unverified = jwt.get_unverified_claims(token)
        
        # Verify token signature
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"verify_exp": True}
        )
        
        # Token is valid
        return {
            "active": True,
            "scope": payload.get("scope", ""),
            "client_id": payload.get("client_id"),
            "username": payload.get("email"),
            "token_type": "Bearer",
            "exp": payload.get("exp"),
            "iat": payload.get("iat"),
            "sub": payload.get("sub"),
            "aud": payload.get("aud"),
            "iss": payload.get("iss"),
            "roles": payload.get("roles", []),
            "tenant_id": payload.get("tenant_id")
        }
        
    except JWTError:
        # Token is invalid or expired
        return {"active": False}


@router.post("/revoke")
async def revoke(
    token: str = Form(...),
    token_type_hint: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    client_secret: Optional[str] = Form(None),
    db: Session = Depends(get_db_session)
):
    """
    OAuth2 Token Revocation endpoint
    
    Revokes an access or refresh token
    """
    # Validate client credentials if provided
    if client_id and client_secret:
        client = crud_oidc.get_oidc_client(db, client_id=client_id)
        if not client or not verify_password(client_secret, client.client_secret_hash):
            raise HTTPException(status_code=401, detail="Invalid client credentials")
    
    # In production, implement token revocation
    # This would involve:
    # 1. Adding token to a revocation list
    # 2. Checking revocation list during token validation
    # 3. Removing refresh tokens from storage
    
    return Response(status_code=200)


# Mount the router in the main app
def init_oauth2_endpoints(app):
    """Initialize OAuth2/OIDC endpoints"""
    app.include_router(router) 