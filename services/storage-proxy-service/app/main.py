from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException, UploadFile, File, Response, Query
import ipfshttpclient
from .api import endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder,
    get_current_tenant_and_user
)
from typing import Optional
import httpx
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

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

async def check_license_validity(
    asset_id: str,
    user_address: str,
    license_id: Optional[str] = None
) -> dict:
    """
    Check if user has a valid license for the asset.
    Returns license info if valid, raises exception otherwise.
    """
    # In production, this would call the verifiable-credential-service
    # to check on-chain license validity
    vc_service_url = "http://verifiable-credential-service/api/v1"
    
    try:
        async with httpx.AsyncClient() as client:
            # Check for active licenses
            response = await client.get(
                f"{vc_service_url}/licenses/check",
                params={
                    "asset_id": asset_id,
                    "user_address": user_address,
                    "license_id": license_id
                }
            )
            
            if response.status_code == 200:
                license_data = response.json()
                if license_data.get("valid"):
                    return license_data
                else:
                    raise HTTPException(
                        status_code=403,
                        detail=f"License expired or invalid: {license_data.get('reason')}"
                    )
            else:
                raise HTTPException(
                    status_code=403,
                    detail="No valid license found for this asset"
                )
    except httpx.RequestError as e:
        logger.error(f"Failed to check license: {e}")
        # Fail closed - deny access if we can't verify
        raise HTTPException(
            status_code=503,
            detail="Unable to verify license at this time"
        )

@app.get("/download/{cid}")
async def download_from_ipfs(
    cid: str,
    asset_id: Optional[str] = Query(None, description="Asset ID for license verification"),
    license_id: Optional[str] = Query(None, description="Specific license ID to use"),
    context: dict = Depends(get_current_tenant_and_user),
    require_license: bool = Query(False, description="Enforce license check")
):
    """
    Downloads a file from IPFS by its CID.
    If asset_id is provided and require_license is True, enforces license check.
    """
    # Check license if required
    if require_license and asset_id:
        user = context.get("user")
        if not user or not hasattr(user, "blockchain_address"):
            raise HTTPException(
                status_code=401,
                detail="User must have blockchain address for licensed content"
            )
        
        # Verify license
        license_info = await check_license_validity(
            asset_id=asset_id,
            user_address=user.blockchain_address,
            license_id=license_id
        )
        
        # Record usage if license has usage limits
        if license_info.get("max_usage") and license_info.get("max_usage") > 0:
            # In production, this would call smart contract to record usage
            logger.info(f"Recording usage for license {license_info.get('license_id')}")
    
    try:
        with ipfshttpclient.connect(IPFS_API_URL) as client:
            file_content = client.cat(cid)
            
            # Add license headers if applicable
            headers = {}
            if require_license and asset_id:
                headers["X-License-Type"] = license_info.get("license_type", "standard")
                headers["X-License-Expires"] = str(license_info.get("expires_at", ""))
            
            return Response(
                content=file_content, 
                media_type="application/octet-stream",
                headers=headers
            )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to download from IPFS: {e}")
