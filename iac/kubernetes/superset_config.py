import os
from flask_appbuilder.security.manager import AUTH_OAUTH

# --- Basic Superset Config ---
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "a_default_secret_key_for_superset")
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_DB_URL")

# --- OIDC Configuration ---
AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [
    {
        "name": "platformq",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "superset",
            "client_secret": "superset-secret", # This should come from Vault
            "server_metadata_url": "http://platformq-kong-proxy/auth/api/v1/.well-known/openid-configuration",
            "api_base_url": "http://platformq-kong-proxy/auth/api/v1/",
            "client_kwargs": {"scope": "openid profile email roles"},
        },
    }
]

# Map OIDC roles to Superset roles
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"
AUTH_ROLES_MAPPING = {
    "admin": ["Admin"],
    "member": ["Gamma"], # Gamma is a standard Superset role with dashboard access
} 