from fastapi import APIRouter
import requests
import os
from typing import List, Dict, Any
from pydantic import BaseModel
from fastapi import Depends, HTTPException
from app.core.config import logger
from app.api.deps import get_current_tenant_and_user

router = APIRouter()

# Additional endpoints can be added here
# The main endpoints are defined in main.py for now 

@router.get("/api/v1/dids/{did}/credentials", response_model=List[Dict[str, Any]])
def get_user_credentials(
    did: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Retrieve all verifiable credentials issued to a specific DID.
    """
    tenant_id = str(context["tenant_id"])
    
    # This endpoint queries the search service to find credentials.
    # The search service is responsible for indexing credentials as they are issued.
    search_query = {
        "query": "",
        "search_type": "verifiable_credential",
        "filters": {
            "credential_subject.id.keyword": did
        },
        "size": 100 # Return up to 100 credentials
    }
    
    try:
        # In a real system, you'd use a shared HTTP client with proper resilience (retry, timeout).
        # We also need to pass the auth context (e.g., a service-to-service JWT).
        # For now, we'll make a simple request.
        headers = {"Authorization": f"Bearer mock-service-token-for-tenant-{tenant_id}"}
        response = requests.post(
            f"{SEARCH_SERVICE_URL}/api/v1/search",
            json=search_query,
            headers=headers
        )
        response.raise_for_status()
        
        search_results = response.json()
        
        # Extract the '_source' from each hit
        credentials = [hit["source"] for hit in search_results.get("hits", [])]
        
        return credentials
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to query search service for credentials: {e}")
        raise HTTPException(status_code=503, detail="The search service is currently unavailable.") 