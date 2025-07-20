"""
DID API Endpoints
"""

from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.did_manager import DIDManager
from app import main

router = APIRouter()


# Request/Response models
class CreateDIDRequest(BaseModel):
    """Request to create a new DID"""
    method: str = Field(
        description="DID method to use",
        example="key",
        default="key"
    )
    options: Optional[Dict[str, Any]] = Field(
        description="Method-specific options",
        default=None
    )
    key_type: Optional[str] = Field(
        description="Key type for cryptographic operations",
        example="Ed25519",
        default="Ed25519"
    )
    services: Optional[List[Dict[str, Any]]] = Field(
        description="Service endpoints to include in DID document",
        default=None
    )
    metadata: Optional[Dict[str, Any]] = Field(
        description="Additional metadata",
        default=None
    )


class UpdateDIDRequest(BaseModel):
    """Request to update a DID document"""
    add_keys: Optional[List[Dict[str, Any]]] = Field(
        description="Keys to add to the DID document",
        default=None
    )
    remove_keys: Optional[List[str]] = Field(
        description="Key IDs to remove from the DID document",
        default=None
    )
    add_services: Optional[List[Dict[str, Any]]] = Field(
        description="Services to add to the DID document",
        default=None
    )
    remove_services: Optional[List[str]] = Field(
        description="Service IDs to remove from the DID document",
        default=None
    )
    update_metadata: Optional[Dict[str, Any]] = Field(
        description="Metadata to update",
        default=None
    )


class DIDResponse(BaseModel):
    """DID operation response"""
    did: str = Field(description="Decentralized Identifier")
    did_document: Dict[str, Any] = Field(description="DID Document")
    metadata: Optional[Dict[str, Any]] = Field(
        description="Additional metadata",
        default=None
    )
    created_at: Optional[datetime] = Field(
        description="Creation timestamp",
        default=None
    )
    updated_at: Optional[datetime] = Field(
        description="Last update timestamp",
        default=None
    )


class DIDListResponse(BaseModel):
    """List of DIDs response"""
    dids: List[DIDResponse] = Field(description="List of DIDs")
    total: int = Field(description="Total count")
    page: int = Field(description="Current page")
    page_size: int = Field(description="Page size")


class DIDMethodsResponse(BaseModel):
    """Supported DID methods response"""
    methods: List[Dict[str, Any]] = Field(
        description="List of supported DID methods with their capabilities"
    )


# Dependency to get DID manager
def get_did_manager() -> DIDManager:
    """Get DID manager instance"""
    if not main.did_manager:
        raise HTTPException(
            status_code=503,
            detail="DID manager not initialized"
        )
    return main.did_manager


# API Endpoints
@router.post("/dids", response_model=DIDResponse)
async def create_did(
    request: CreateDIDRequest,
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Create a new DID
    
    Creates a new decentralized identifier using the specified method.
    Supports multiple DID methods including did:key, did:web, did:platformq, and did:ethr.
    """
    try:
        # Validate DID method
        if request.method not in settings.supported_did_methods:
            raise ValueError(f"Unsupported DID method: {request.method}")
        
        # Validate key type
        if request.key_type and request.key_type not in settings.allowed_key_types:
            raise ValueError(f"Unsupported key type: {request.key_type}")
        
        # Create DID
        result = await did_manager.create_did(
            method=request.method,
            options=request.options,
            key_type=request.key_type,
            services=request.services,
            metadata=request.metadata
        )
        
        return DIDResponse(
            did=result["did"],
            did_document=result["did_document"],
            metadata=result.get("metadata"),
            created_at=result.get("created_at"),
            updated_at=result.get("updated_at")
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create DID: {str(e)}")


@router.get("/dids/{did:path}", response_model=DIDResponse)
async def resolve_did(
    did: str,
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Resolve a DID to its DID document
    
    Resolves a decentralized identifier and returns the associated DID document.
    Supports caching for improved performance.
    """
    try:
        # Resolve DID
        result = await did_manager.resolve_did(did)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"DID not found: {did}"
            )
        
        return DIDResponse(
            did=did,
            did_document=result["did_document"],
            metadata=result.get("metadata"),
            created_at=result.get("created_at"),
            updated_at=result.get("updated_at")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to resolve DID: {str(e)}"
        )


@router.put("/dids/{did:path}", response_model=DIDResponse)
async def update_did(
    did: str,
    request: UpdateDIDRequest,
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Update a DID document
    
    Updates an existing DID document by adding/removing keys and services.
    Only the controller of the DID can perform updates.
    """
    try:
        # Update DID
        result = await did_manager.update_did(
            did=did,
            add_keys=request.add_keys,
            remove_keys=request.remove_keys,
            add_services=request.add_services,
            remove_services=request.remove_services,
            update_metadata=request.update_metadata
        )
        
        return DIDResponse(
            did=did,
            did_document=result["did_document"],
            metadata=result.get("metadata"),
            created_at=result.get("created_at"),
            updated_at=result.get("updated_at")
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update DID: {str(e)}"
        )


@router.delete("/dids/{did:path}")
async def deactivate_did(
    did: str,
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Deactivate a DID
    
    Marks a DID as deactivated. The DID document will still be resolvable
    but will include a deactivation flag.
    """
    try:
        await did_manager.deactivate_did(did)
        
        return JSONResponse(
            content={
                "message": f"DID {did} deactivated successfully"
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to deactivate DID: {str(e)}"
        )


@router.get("/dids", response_model=DIDListResponse)
async def list_dids(
    method: Optional[str] = Query(
        None,
        description="Filter by DID method"
    ),
    controller: Optional[str] = Query(
        None,
        description="Filter by controller DID"
    ),
    active_only: bool = Query(
        True,
        description="Only return active DIDs"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        20,
        ge=1,
        le=100,
        description="Items per page"
    ),
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    List DIDs
    
    Lists DIDs with optional filtering by method and controller.
    Supports pagination.
    """
    try:
        # Get DIDs
        result = await did_manager.list_dids(
            method=method,
            controller=controller,
            active_only=active_only,
            page=page,
            page_size=page_size
        )
        
        # Format response
        dids = []
        for item in result["dids"]:
            dids.append(DIDResponse(
                did=item["did"],
                did_document=item["did_document"],
                metadata=item.get("metadata"),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at")
            ))
        
        return DIDListResponse(
            dids=dids,
            total=result["total"],
            page=page,
            page_size=page_size
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list DIDs: {str(e)}"
        )


@router.get("/methods", response_model=DIDMethodsResponse)
async def get_supported_methods(
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Get supported DID methods
    
    Returns a list of supported DID methods and their capabilities.
    """
    try:
        methods = await did_manager.get_supported_methods()
        
        return DIDMethodsResponse(methods=methods)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get supported methods: {str(e)}"
        )


@router.post("/dids/{did:path}/verify")
async def verify_did_signature(
    did: str,
    message: str = Body(..., description="Message to verify"),
    signature: str = Body(..., description="Signature to verify"),
    key_id: Optional[str] = Body(
        None,
        description="Specific key ID to use for verification"
    ),
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Verify a signature using a DID
    
    Verifies a signature against a message using the public keys
    in the DID document.
    """
    try:
        result = await did_manager.verify_signature(
            did=did,
            message=message,
            signature=signature,
            key_id=key_id
        )
        
        return JSONResponse(
            content={
                "valid": result["valid"],
                "key_id": result.get("key_id"),
                "algorithm": result.get("algorithm")
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify signature: {str(e)}"
        )


@router.post("/dids/{did:path}/rotate-keys")
async def rotate_did_keys(
    did: str,
    key_ids: Optional[List[str]] = Body(
        None,
        description="Specific key IDs to rotate, or None to rotate all"
    ),
    did_manager: DIDManager = Depends(get_did_manager)
):
    """
    Rotate DID keys
    
    Rotates the cryptographic keys associated with a DID.
    Old keys are marked as revoked and new keys are generated.
    """
    try:
        result = await did_manager.rotate_keys(
            did=did,
            key_ids=key_ids
        )
        
        return JSONResponse(
            content={
                "did": did,
                "rotated_keys": result["rotated_keys"],
                "new_keys": result["new_keys"]
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to rotate keys: {str(e)}"
        ) 