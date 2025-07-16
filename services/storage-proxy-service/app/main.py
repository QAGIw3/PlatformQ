from fastapi import Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from typing import Optional
import httpx
import logging
from datetime import datetime

from platformq_shared.base_service import create_base_app
from .api.deps import get_current_tenant_and_user
from .storage.base import BaseStorage
from .storage.factory import get_storage_backend

logger = logging.getLogger(__name__)

app = create_base_app()

@app.post("/upload")
async def upload_file(
    context: dict = Depends(get_current_tenant_and_user),
    file: UploadFile = File(...),
    storage: BaseStorage = Depends(get_storage_backend)
):
    """
    Uploads a file to the configured storage backend.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found in context.")
        
    identifier = await storage.upload(file, tenant_id)
    return {"identifier": identifier, "filename": file.filename}

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

@app.get("/download/{identifier}")
async def download_file(
    identifier: str,
    asset_id: Optional[str] = Query(None, description="Asset ID for license verification"),
    license_id: Optional[str] = Query(None, description="Specific license ID to use"),
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend),
    require_license: bool = Query(False, description="Enforce license check")
):
    """
    Downloads a file from the configured storage backend by its identifier.
    If asset_id is provided and require_license is True, enforces license check.
    """
    headers = {}
    # Check license if required
    if require_license and asset_id:
        user = context.get("user", {}) # Use .get for safety
        user_blockchain_address = user.get("blockchain_address")

        if not user_blockchain_address:
            raise HTTPException(
                status_code=401,
                detail="User must have blockchain address for licensed content"
            )
        
        # Verify license
        license_info = await check_license_validity(
            asset_id=asset_id,
            user_address=user_blockchain_address,
            license_id=license_id
        )
        
        # Record usage if license has usage limits
        if license_info.get("max_usage") and license_info.get("max_usage") > 0:
            # In production, this would call smart contract to record usage
            logger.info(f"Recording usage for license {license_info.get('license_id')}")

        headers["X-License-Type"] = license_info.get("license_type", "standard")
        headers["X-License-Expires"] = str(license_info.get("expires_at", ""))

    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found in context.")

    return StreamingResponse(
        storage.download(identifier, tenant_id),
        media_type="application/octet-stream",
        headers=headers
    )

@app.get("/")
def read_root():
    return {"message": "storage-proxy-service is running"}
