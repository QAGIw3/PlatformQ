from fastapi import Request, Depends
from cassandra.cluster import Session
from shared_lib import security as shared_security

# This service only needs a db session and the trusted header user
# The actual logic is in the shared library.

def get_db_session(request: Request) -> Session:
    session = request.app.state.db_manager.get_session()
    # In a real app, we would need a way to get the keyspace name
    # from config for this service.
    session.set_keyspace('auth_keyspace')
    yield session

get_current_tenant_and_user = shared_security.get_current_user_from_trusted_header 