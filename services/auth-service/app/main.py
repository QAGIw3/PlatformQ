from platformq_shared.base_service import create_base_app
from .api import endpoints, siwe_endpoints, s2s
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
app.include_router(endpoints.router, prefix="/api/v1", tags=["Users"])
app.include_router(siwe_endpoints.router, prefix="/api/v1", tags=["SIWE"])
app.include_router(s2s.router, prefix="/api/v1", tags=["S2S"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "Authentication Service is running"}
