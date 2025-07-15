from authlib.integrations.starlette_oauth2 import AuthorizationServer
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6750 import BearerTokenValidator
from authlib.oidc.core import UserInfo
from authlib.oidc.core.grants import OpenIDCode
from cassandra.cluster import Session
import logging
from uuid import UUID

from ..crud import crud_oidc, crud_user, crud_role
from ..core.security import create_access_token

logger = logging.getLogger(__name__)

# --- Grant Types ---

class AuthorizationCodeGrant(grants.AuthorizationCodeGrant):
    def save_authorization_code(self, code, request):
        db: Session = request.state.db
        client = request.client
        crud_oidc.save_authorization_code(
            db,
            code=code,
            client_id=client.client_id,
            user_id=request.user.id,
            redirect_uri=request.redirect_uri,
            scope=request.scope,
        )

    def query_authorization_code(self, code, client):
        db: Session = client.context.get("db")
        return crud_oidc.get_and_use_authorization_code(
            db, code=code, client_id=client.client_id
        )

    def delete_authorization_code(self, authorization_code):
        # We mark it as used in the query step, so no action needed here.
        pass

    def authenticate_user(self, authorization_code):
        db: Session = authorization_code.client.context.get("db")
        return crud_user.get_user_by_id(db, user_id=authorization_code.user_id)


# --- OIDC Server Setup ---

def generate_user_info(user, scope):
    """Generate claims for the id_token and userinfo endpoint."""
    return UserInfo(
        sub=str(user.id),
        name=user.full_name,
        email=user.email,
        # Add other claims based on scope
    )

def create_authorization_server(db: Session, tenant_id: UUID):
    server = AuthorizationServer(
        client_model=crud_oidc.get_oidc_client,
        # We pass the db session via a context dictionary
        client_context={"db": db},
    )

    def generate_bearer_token(client, grant_type, user, scope):
        # Fetch user's roles to include in the token
        roles = crud_role.get_roles_for_user(db, tenant_id=tenant_id, user_id=user.id)
        
        return create_access_token(
            data={"sub": str(user.id), "scope": scope, "tid": str(tenant_id), "roles": roles}
        )

    server.generate_token = generate_bearer_token
    server.register_grant(AuthorizationCodeGrant, [OpenIDCode(require_nonce=False)])
    
    return server 