from fastapi import Request, Depends
from cassandra.cluster import Session
from platformq_shared import security as shared_security

def get_db_session(request: Request) -> Session:
    session = request.app.state.db_manager.get_session()
    session.set_keyspace('auth_keyspace') # This should be configured
    yield session

# For a new service, these dependencies are placeholders.
# A real service would define its own CRUD modules and pass them here.
def get_api_key_crud_placeholder(): return None
def get_user_crud_placeholder(): return None
def get_password_verifier_placeholder(): return None

get_current_tenant_and_user = shared_security.get_current_user_from_trusted_header
