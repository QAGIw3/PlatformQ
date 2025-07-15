from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException, UploadFile, File, Response
import ipfshttpclient
from .api import endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder,
    get_current_tenant_and_user
)

app = create_base_app(
    service_name="storage-proxy-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["storage-proxy-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "storage-proxy-service is running"}

# In a real app, this would come from config/vault
IPFS_API_URL = "/dns/platformq-ipfs-kubo/tcp/5001/http"

@app.post("/upload")
async def upload_to_ipfs(
    context: dict = Depends(get_current_tenant_and_user),
    file: UploadFile = File(...)
):
    """
    Uploads a file to the IPFS node and returns its CID.
    """
    try:
        with ipfshttpclient.connect(IPFS_API_URL) as client:
            result = client.add(file.file, pin=True)
            cid = result['Hash']
            # In a real app, we would store the mapping from
            # (tenant_id, file_path) -> cid in a database.
            return {"cid": cid, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to IPFS: {e}")

@app.get("/download/{cid}")
async def download_from_ipfs(cid: str):
    """
    Downloads a file from IPFS by its CID.
    """
    try:
        with ipfshttpclient.connect(IPFS_API_URL) as client:
            file_content = client.cat(cid)
            return Response(content=file_content, media_type="application/octet-stream")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to download from IPFS: {e}")
