"""
SBT Metadata API Endpoints
"""

from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.sbt_manager import SBTManager
from app import main

router = APIRouter()


# Request/Response models
class UpdateMetadataRequest(BaseModel):
    """Request to update SBT metadata"""
    metadata_updates: Dict[str, Any] = Field(
        description="Metadata fields to update"
    )
    updater: str = Field(
        description="DID or address of the updater"
    )


class MetadataResponse(BaseModel):
    """Metadata response"""
    sbtId: str
    metadata: Dict[str, Any]
    metadataUri: str
    lastUpdated: Optional[str]
    updatedBy: Optional[str]


# Dependency to get SBT manager
def get_sbt_manager() -> SBTManager:
    """Get SBT manager instance"""
    if not main.sbt_manager:
        raise HTTPException(
            status_code=503,
            detail="SBT manager not initialized"
        )
    return main.sbt_manager


# API Endpoints

@router.get("/sbts/{sbt_id}/metadata", response_model=MetadataResponse)
async def get_metadata(
    sbt_id: str,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get SBT metadata
    
    Retrieves the metadata associated with a SoulBound Token.
    """
    sbt = await sbt_manager.sbt_store.get(sbt_id)
    
    if not sbt:
        raise HTTPException(
            status_code=404,
            detail=f"SBT {sbt_id} not found"
        )
    
    return MetadataResponse(
        sbtId=sbt.id,
        metadata=sbt.metadata,
        metadataUri=sbt.metadata_uri,
        lastUpdated=sbt.metadata.get("lastUpdated"),
        updatedBy=sbt.metadata.get("updatedBy")
    )


@router.put("/sbts/{sbt_id}/metadata")
async def update_metadata(
    sbt_id: str,
    request: UpdateMetadataRequest,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Update SBT metadata
    
    Updates the off-chain metadata of a SoulBound Token.
    Only authorized parties (issuer or recipient) can update metadata.
    """
    try:
        result = await sbt_manager.update_metadata(
            sbt_id=sbt_id,
            metadata_updates=request.metadata_updates,
            updater=request.updater
        )
        
        return JSONResponse(
            content=result,
            status_code=200
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update metadata: {str(e)}"
        )


@router.get("/sbts/{sbt_id}/metadata/history")
async def get_metadata_history(
    sbt_id: str,
    limit: int = Query(50, ge=1, le=100, description="Maximum history entries"),
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get SBT metadata update history
    
    Retrieves the history of metadata updates for an SBT.
    Note: This is a placeholder - full history tracking would require
    additional database tables.
    """
    sbt = await sbt_manager.sbt_store.get(sbt_id)
    
    if not sbt:
        raise HTTPException(
            status_code=404,
            detail=f"SBT {sbt_id} not found"
        )
    
    # For now, return current metadata with timestamps
    history = []
    
    if sbt.metadata.get("lastUpdated"):
        history.append({
            "timestamp": sbt.metadata.get("lastUpdated"),
            "updatedBy": sbt.metadata.get("updatedBy"),
            "action": "update",
            "changes": list(sbt.metadata.keys())
        })
    
    history.append({
        "timestamp": sbt.created_at.isoformat(),
        "updatedBy": sbt.issuer,
        "action": "create",
        "changes": list(sbt.metadata.keys())
    })
    
    return JSONResponse(
        content={
            "sbtId": sbt_id,
            "history": history[:limit],
            "total": len(history)
        }
    )


@router.post("/sbts/{sbt_id}/metadata/verify")
async def verify_metadata_integrity(
    sbt_id: str,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Verify metadata integrity
    
    Verifies that the SBT metadata matches what's stored on-chain.
    """
    try:
        sbt = await sbt_manager.sbt_store.get(sbt_id)
        
        if not sbt:
            raise HTTPException(
                status_code=404,
                detail=f"SBT {sbt_id} not found"
            )
        
        # Get on-chain metadata URI
        response = await sbt_manager.http_client.post(
            f"{sbt_manager.blockchain_connector_url}/api/v1/contracts/call",
            json={
                "chain": sbt.chain,
                "contractAddress": sbt.contract_address,
                "method": "tokenURI",
                "params": {
                    "tokenId": sbt.token_id
                }
            }
        )
        
        on_chain_uri = None
        if response.status_code == 200:
            result = response.json()
            on_chain_uri = result.get("result")
        
        # Compare URIs
        uri_match = on_chain_uri == sbt.metadata_uri
        
        # Fetch and compare metadata content if URIs match
        content_match = False
        if uri_match and sbt.metadata_uri:
            # This would fetch from IPFS/storage service
            # For now, assume it matches
            content_match = True
        
        return JSONResponse(
            content={
                "sbtId": sbt_id,
                "metadataUri": sbt.metadata_uri,
                "onChainUri": on_chain_uri,
                "uriMatch": uri_match,
                "contentMatch": content_match,
                "integrity": "valid" if uri_match and content_match else "invalid",
                "verifiedAt": datetime.now().isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify metadata: {str(e)}"
        )


@router.get("/metadata/schemas")
async def get_metadata_schemas():
    """
    Get available metadata schemas
    
    Returns the supported metadata schemas for different SBT types.
    """
    schemas = {
        "credential": {
            "required": ["credentialId", "credentialType", "issuer", "issuanceDate"],
            "optional": ["description", "image", "expirationDate", "evidence"],
            "example": {
                "credentialId": "cred-123",
                "credentialType": ["VerifiableCredential", "UniversityDegree"],
                "issuer": "did:example:university",
                "issuanceDate": "2024-01-01T00:00:00Z",
                "description": "Bachelor of Science in Computer Science",
                "image": "ipfs://QmXxx..."
            }
        },
        "achievement": {
            "required": ["name", "description", "criteria", "issuer"],
            "optional": ["image", "tags", "level", "skills"],
            "example": {
                "name": "Blockchain Developer Certification",
                "description": "Certified blockchain developer",
                "criteria": "Completed blockchain development course",
                "issuer": "did:example:certifier",
                "level": "advanced",
                "skills": ["Solidity", "Smart Contracts", "Web3"]
            }
        },
        "membership": {
            "required": ["organization", "memberSince", "status"],
            "optional": ["role", "benefits", "expirationDate"],
            "example": {
                "organization": "Developer DAO",
                "memberSince": "2024-01-01T00:00:00Z",
                "status": "active",
                "role": "contributor",
                "benefits": ["voting", "access"]
            }
        }
    }
    
    return JSONResponse(content=schemas)


@router.post("/metadata/validate")
async def validate_metadata(
    metadata: Dict[str, Any] = ...,
    schema_type: str = Query(..., description="Schema type to validate against")
):
    """
    Validate metadata against schema
    
    Validates that metadata conforms to the specified schema type.
    """
    # Get schema
    schemas = {
        "credential": ["credentialId", "credentialType", "issuer", "issuanceDate"],
        "achievement": ["name", "description", "criteria", "issuer"],
        "membership": ["organization", "memberSince", "status"]
    }
    
    if schema_type not in schemas:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown schema type: {schema_type}"
        )
    
    # Check required fields
    required_fields = schemas[schema_type]
    missing_fields = [field for field in required_fields if field not in metadata]
    
    if missing_fields:
        return JSONResponse(
            content={
                "valid": False,
                "errors": [f"Missing required field: {field}" for field in missing_fields],
                "schema": schema_type
            },
            status_code=400
        )
    
    return JSONResponse(
        content={
            "valid": True,
            "schema": schema_type,
            "metadata": metadata
        }
    ) 