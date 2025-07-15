from platformq_shared.base_service import create_base_app
from .api import endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder
)

app = create_base_app(
    service_name="simulation-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["simulation-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "simulation-service is running"}
