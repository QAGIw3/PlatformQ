from platformq_shared.base_service import create_base_app
from .api import endpoints
from .postgres_db import get_db_session
from . import crud

# The database tables are now managed by Alembic migrations.
# No need for Base.metadata.create_all(bind=engine) here anymore.

# TODO: Create real CRUD functions and a real password verifier.
def get_api_key_crud_placeholder():
    return None

def get_user_crud_placeholder():
    return None
    
def get_password_verifier_placeholder():
    return None

app = create_base_app(
    service_name="digital-asset-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include the API router
app.include_router(endpoints.router, prefix="/api/v1", tags=["Digital Assets"])

@app.get("/")
def read_root():
    return {"message": "digital-asset-service is running"}
