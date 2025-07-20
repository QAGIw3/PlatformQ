"""
OAuth2/OIDC Schemas for PlatformQ Auth Service
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime


class TokenResponse(BaseModel):
    """OAuth2 Token Response"""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None
    scope: Optional[str] = None


class AuthorizationRequest(BaseModel):
    """OAuth2 Authorization Request"""
    response_type: str
    client_id: str
    redirect_uri: HttpUrl
    scope: str
    state: Optional[str] = None
    code_challenge: Optional[str] = None
    code_challenge_method: Optional[str] = None
    nonce: Optional[str] = None
    prompt: Optional[str] = None
    max_age: Optional[int] = None


class TokenRequest(BaseModel):
    """OAuth2 Token Request"""
    grant_type: str
    code: Optional[str] = None
    redirect_uri: Optional[HttpUrl] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    code_verifier: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class UserInfoResponse(BaseModel):
    """OpenID Connect UserInfo Response"""
    sub: str
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    name: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    middle_name: Optional[str] = None
    nickname: Optional[str] = None
    preferred_username: Optional[str] = None
    profile: Optional[HttpUrl] = None
    picture: Optional[HttpUrl] = None
    website: Optional[HttpUrl] = None
    gender: Optional[str] = None
    birthdate: Optional[str] = None
    zoneinfo: Optional[str] = None
    locale: Optional[str] = None
    phone_number: Optional[str] = None
    phone_number_verified: Optional[bool] = None
    address: Optional[Dict[str, str]] = None
    updated_at: Optional[int] = None
    
    # Custom claims
    roles: Optional[List[str]] = None
    tenant_id: Optional[str] = None
    reputation_score: Optional[int] = None
    permissions: Optional[List[str]] = None


class OIDCConfiguration(BaseModel):
    """OpenID Connect Discovery Configuration"""
    issuer: HttpUrl
    authorization_endpoint: HttpUrl
    token_endpoint: HttpUrl
    userinfo_endpoint: HttpUrl
    jwks_uri: HttpUrl
    registration_endpoint: Optional[HttpUrl] = None
    scopes_supported: List[str]
    response_types_supported: List[str]
    response_modes_supported: Optional[List[str]] = None
    grant_types_supported: Optional[List[str]] = None
    acr_values_supported: Optional[List[str]] = None
    subject_types_supported: List[str]
    id_token_signing_alg_values_supported: List[str]
    id_token_encryption_alg_values_supported: Optional[List[str]] = None
    id_token_encryption_enc_values_supported: Optional[List[str]] = None
    userinfo_signing_alg_values_supported: Optional[List[str]] = None
    userinfo_encryption_alg_values_supported: Optional[List[str]] = None
    userinfo_encryption_enc_values_supported: Optional[List[str]] = None
    request_object_signing_alg_values_supported: Optional[List[str]] = None
    request_object_encryption_alg_values_supported: Optional[List[str]] = None
    request_object_encryption_enc_values_supported: Optional[List[str]] = None
    token_endpoint_auth_methods_supported: Optional[List[str]] = None
    token_endpoint_auth_signing_alg_values_supported: Optional[List[str]] = None
    display_values_supported: Optional[List[str]] = None
    claim_types_supported: Optional[List[str]] = None
    claims_supported: Optional[List[str]] = None
    service_documentation: Optional[HttpUrl] = None
    claims_locales_supported: Optional[List[str]] = None
    ui_locales_supported: Optional[List[str]] = None
    claims_parameter_supported: Optional[bool] = False
    request_parameter_supported: Optional[bool] = False
    request_uri_parameter_supported: Optional[bool] = True
    require_request_uri_registration: Optional[bool] = False
    op_policy_uri: Optional[HttpUrl] = None
    op_tos_uri: Optional[HttpUrl] = None
    revocation_endpoint: Optional[HttpUrl] = None
    revocation_endpoint_auth_methods_supported: Optional[List[str]] = None
    revocation_endpoint_auth_signing_alg_values_supported: Optional[List[str]] = None
    introspection_endpoint: Optional[HttpUrl] = None
    introspection_endpoint_auth_methods_supported: Optional[List[str]] = None
    introspection_endpoint_auth_signing_alg_values_supported: Optional[List[str]] = None
    code_challenge_methods_supported: Optional[List[str]] = None


class JWK(BaseModel):
    """JSON Web Key"""
    kty: str  # Key Type (RSA, EC, etc.)
    use: Optional[str] = None  # Key Use (sig, enc)
    key_ops: Optional[List[str]] = None  # Key Operations
    alg: Optional[str] = None  # Algorithm
    kid: Optional[str] = None  # Key ID
    x5u: Optional[HttpUrl] = None  # X.509 URL
    x5c: Optional[List[str]] = None  # X.509 Certificate Chain
    x5t: Optional[str] = None  # X.509 Certificate SHA-1 Thumbprint
    x5t_S256: Optional[str] = Field(None, alias="x5t#S256")  # X.509 Certificate SHA-256 Thumbprint
    
    # RSA Key Parameters
    n: Optional[str] = None  # Modulus
    e: Optional[str] = None  # Exponent
    d: Optional[str] = None  # Private Exponent
    p: Optional[str] = None  # First Prime Factor
    q: Optional[str] = None  # Second Prime Factor
    dp: Optional[str] = None  # First Factor CRT Exponent
    dq: Optional[str] = None  # Second Factor CRT Exponent
    qi: Optional[str] = None  # First CRT Coefficient
    
    # EC Key Parameters
    crv: Optional[str] = None  # Curve
    x: Optional[str] = None  # X Coordinate
    y: Optional[str] = None  # Y Coordinate


class JWKSResponse(BaseModel):
    """JSON Web Key Set Response"""
    keys: List[JWK]


class ClientRegistrationRequest(BaseModel):
    """OAuth2 Dynamic Client Registration Request"""
    redirect_uris: List[HttpUrl]
    token_endpoint_auth_method: Optional[str] = "client_secret_basic"
    grant_types: Optional[List[str]] = ["authorization_code"]
    response_types: Optional[List[str]] = ["code"]
    client_name: Optional[str] = None
    client_uri: Optional[HttpUrl] = None
    logo_uri: Optional[HttpUrl] = None
    scope: Optional[str] = None
    contacts: Optional[List[str]] = None
    tos_uri: Optional[HttpUrl] = None
    policy_uri: Optional[HttpUrl] = None
    jwks_uri: Optional[HttpUrl] = None
    jwks: Optional[Dict[str, Any]] = None
    software_id: Optional[str] = None
    software_version: Optional[str] = None


class ClientRegistrationResponse(BaseModel):
    """OAuth2 Dynamic Client Registration Response"""
    client_id: str
    client_secret: Optional[str] = None
    registration_access_token: Optional[str] = None
    registration_client_uri: Optional[HttpUrl] = None
    client_id_issued_at: Optional[int] = None
    client_secret_expires_at: Optional[int] = None
    redirect_uris: List[HttpUrl]
    token_endpoint_auth_method: str
    grant_types: List[str]
    response_types: List[str]
    client_name: Optional[str] = None
    client_uri: Optional[HttpUrl] = None
    logo_uri: Optional[HttpUrl] = None
    scope: Optional[str] = None
    contacts: Optional[List[str]] = None
    tos_uri: Optional[HttpUrl] = None
    policy_uri: Optional[HttpUrl] = None
    jwks_uri: Optional[HttpUrl] = None
    jwks: Optional[Dict[str, Any]] = None
    software_id: Optional[str] = None
    software_version: Optional[str] = None


class TokenIntrospectionRequest(BaseModel):
    """OAuth2 Token Introspection Request"""
    token: str
    token_type_hint: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class TokenIntrospectionResponse(BaseModel):
    """OAuth2 Token Introspection Response"""
    active: bool
    scope: Optional[str] = None
    client_id: Optional[str] = None
    username: Optional[str] = None
    token_type: Optional[str] = None
    exp: Optional[int] = None
    iat: Optional[int] = None
    nbf: Optional[int] = None
    sub: Optional[str] = None
    aud: Optional[List[str]] = None
    iss: Optional[str] = None
    jti: Optional[str] = None
    
    # Custom claims
    roles: Optional[List[str]] = None
    tenant_id: Optional[str] = None
    permissions: Optional[List[str]] = None


class TokenRevocationRequest(BaseModel):
    """OAuth2 Token Revocation Request"""
    token: str
    token_type_hint: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None 