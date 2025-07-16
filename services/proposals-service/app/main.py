from platformq.shared.base_service import create_base_app
from platformq.shared.nextcloud_client import NextcloudClient
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel
import tempfile
from sqlalchemy.orm import Session

from .api.deps import get_current_tenant_and_user, get_db_session
from .api import endpoints
from .crud import crud_proposal
from .messaging.pulsar_consumer import start_consumer, stop_consumer
from .core.config import settings

app = create_base_app(
    service_name="proposals-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
)

@app.on_event("startup")
async def startup_event():
    start_consumer()
    app.state.nextcloud_client = NextcloudClient(
        nextcloud_url=settings.nextcloud_url,
        admin_user=settings.nextcloud_user,
        admin_pass=settings.nextcloud_password
    )

@app.on_event("shutdown")
async def shutdown_event():
    stop_consumer()

def get_nextcloud_client(request: Request) -> NextcloudClient:
    return request.app.state.nextcloud_client

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["proposals-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "proposals-service is running"}

class ProposalCreateRequest(BaseModel):
    customer_name: str

@app.post("/api/v1/proposals", status_code=201)
def create_proposal_endpoint(
    proposal_in: ProposalCreateRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
    nextcloud_client: NextcloudClient = Depends(get_nextcloud_client),
):
    """
    Creates a new proposal, which involves creating a backing document
    in Nextcloud and a metadata record in our database.
    """
    tenant_id = context["tenant_id"]
    user = context["user"]

    # 1. Create the file in Nextcloud
    document_path = f"Proposals/{proposal_in.customer_name}_{user.id}.docx"
    
    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
        # Create an empty file to upload
        tmp.write(b"")
        tmp_path = tmp.name

    success = nextcloud_client.upload_file(tmp_path, document_path)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to create document in Nextcloud")

    # 2. Create the metadata record in our database
    new_proposal = crud_proposal.create_proposal(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        customer_name=proposal_in.customer_name,
        document_path=document_path,
    )

    return new_proposal
