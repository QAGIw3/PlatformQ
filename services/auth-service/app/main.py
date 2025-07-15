from platformq_shared.base_service import create_base_app
from .api import endpoints, siwe_endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud, 
    get_user_crud, 
    get_password_verifier
)

app = create_base_app(
    service_name="auth-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["auth"])
app.include_router(siwe_endpoints.router, prefix="/api/v1", tags=["siwe"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "Authentication Service is running"}
