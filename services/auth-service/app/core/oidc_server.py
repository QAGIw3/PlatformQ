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
# This section defines the custom logic for how our OIDC provider handles
# the standard "Authorization Code" grant flow. We are telling Authlib
# how to save, query, and authenticate based on our Cassandra database.

class AuthorizationCodeGrant(grants.AuthorizationCodeGrant):
    def save_authorization_code(self, code, request):
        """
        Called by Authlib after it generates an authorization code.
        We persist this code to our Cassandra database.
        """
        db: Session = request.state.db
        client = request.client
        crud_oidc.save_authorization_code(
            db,
            tenant_id=request.user.tenant_id, # Tenant context is critical
            code=code,
            client_id=client.client_id,
            user_id=request.user.id,
            redirect_uri=request.redirect_uri,
            scope=request.scope,
        )

    def query_authorization_code(self, code, client):
        """
        Called by Authlib during the token exchange step.
        It provides the code from the client, and we must look it up in our database.
        """
        db: Session = client.context.get("db")
        return crud_oidc.get_and_use_authorization_code(
            db, code=code, client_id=client.client_id
        )

    def delete_authorization_code(self, authorization_code):
        """
        Called by Authlib after the code is used. Our implementation marks the
        code as used in the `query_authorization_code` step, so no action is needed here.
        """
        pass

    def authenticate_user(self, authorization_code):
        """

        Called by Authlib after a code is successfully exchanged.
        It associates the final access token with the user who originally authorized the code.
        """
        db: Session = authorization_code.client.context.get("db")
        return crud_user.get_user_by_id(
            db, tenant_id=authorization_code.tenant_id, user_id=authorization_code.user_id
        )

# --- OIDC Server Setup ---

def generate_user_info(user, scope):
    """
    Generates the claims for the `id_token` and the `/userinfo` endpoint.
    This defines the identity information we share with client applications.
    """
    return UserInfo(
        sub=str(user.id),
        name=user.full_name,
        email=user.email,
        # In a real system, you could add more claims based on the 'scope' requested
    )

def create_authorization_server(db: Session, tenant_id: UUID):
    """
    Factory function to create and configure the OIDC Authorization Server.
    """
    # We pass the Cassandra session via a context dictionary so our custom
    # grant class methods can access it.
    server = AuthorizationServer(
        client_model=crud_oidc.get_oidc_client,
        client_context={"db": db},
    )

    def generate_bearer_token(client, grant_type, user, scope):
        """
        This function is called by Authlib when it needs to generate the final
        access token. We override the default behavior to issue our own custom JWT,
        which includes our critical `tid` and `roles` claims.
        """
        roles = crud_role.get_roles_for_user(db, tenant_id=tenant_id, user_id=user.id)
        
        return create_access_token(
            data={"sub": str(user.id), "scope": scope, "tid": str(tenant_id), "roles": roles}
        )

    server.generate_token = generate_bearer_token
    # Register our custom AuthorizationCodeGrant to handle the 'code' and 'openid' grant types.
    server.register_grant(AuthorizationCodeGrant, [OpenIDCode(require_nonce=False)])
    
    return server 