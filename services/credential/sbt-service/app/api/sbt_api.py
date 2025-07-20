"""
SoulBound Token API Endpoints
"""

from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.sbt_manager import SBTManager, SBTStatus
from app import main

router = APIRouter()


# Request/Response models
class MintSBTRequest(BaseModel):
    """Request to mint a new SBT"""
    credential_id: str = Field(
        description="ID of the credential to bind as SBT"
    )
    recipient: str = Field(
        description="Wallet address to receive the SBT"
    )
    chain: str = Field(
        description="Blockchain to mint on",
        example="ethereum"
    )
    metadata: Dict[str, Any] = Field(
        description="Additional metadata for the SBT",
        default={}
    )
    issuer: str = Field(
        description="DID or address of the issuer"
    )


class RevokeSBTRequest(BaseModel):
    """Request to revoke an SBT"""
    reason: str = Field(
        description="Reason for revocation"
    )
    revoker: str = Field(
        description="DID or address of the revoker"
    )


class BurnSBTRequest(BaseModel):
    """Request to burn an SBT"""
    burner: str = Field(
        description="Address requesting the burn (must be owner)"
    )


class SBTResponse(BaseModel):
    """SBT response model"""
    id: str
    tokenId: Optional[str]
    credentialId: str
    chain: str
    contractAddress: str
    recipient: str
    issuer: str
    metadataUri: str
    metadata: Dict[str, Any]
    status: str
    mintedAt: Optional[str]
    transactionHash: Optional[str]


class SBTListResponse(BaseModel):
    """List of SBTs response"""
    sbts: List[SBTResponse]
    total: int
    page: int
    limit: int


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

@router.post("/sbts", response_model=SBTResponse)
async def mint_sbt(
    request: MintSBTRequest,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Mint a new SoulBound Token
    
    Creates a non-transferable token bound to a credential and recipient.
    The SBT is permanently associated with the recipient's address.
    """
    try:
        result = await sbt_manager.mint_sbt(
            credential_id=request.credential_id,
            recipient=request.recipient,
            chain=request.chain,
            metadata=request.metadata,
            issuer=request.issuer
        )
        
        return SBTResponse(
            id=result["id"],
            tokenId=result["tokenId"],
            credentialId=request.credential_id,
            chain=result["chain"],
            contractAddress=result["contractAddress"],
            recipient=result["recipient"],
            issuer=request.issuer,
            metadataUri=result["metadataUri"],
            metadata=request.metadata,
            status=result["status"],
            mintedAt=datetime.now().isoformat(),
            transactionHash=result["transactionHash"]
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to mint SBT: {str(e)}"
        )


@router.get("/sbts/{sbt_id}", response_model=SBTResponse)
async def get_sbt(
    sbt_id: str,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get SBT by ID
    
    Retrieves details of a specific SoulBound Token.
    """
    sbt = await sbt_manager.sbt_store.get(sbt_id)
    
    if not sbt:
        raise HTTPException(
            status_code=404,
            detail=f"SBT {sbt_id} not found"
        )
    
    return SBTResponse(
        id=sbt.id,
        tokenId=sbt.token_id,
        credentialId=sbt.credential_id,
        chain=sbt.chain,
        contractAddress=sbt.contract_address,
        recipient=sbt.recipient,
        issuer=sbt.issuer,
        metadataUri=sbt.metadata_uri,
        metadata=sbt.metadata,
        status=sbt.status,
        mintedAt=sbt.minted_at.isoformat() if sbt.minted_at else None,
        transactionHash=sbt.transaction_hash
    )


@router.post("/sbts/{sbt_id}/revoke")
async def revoke_sbt(
    sbt_id: str,
    request: RevokeSBTRequest,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Revoke a SoulBound Token
    
    Revokes an SBT, marking it as invalid. Only authorized parties
    (typically the issuer) can revoke an SBT.
    """
    try:
        result = await sbt_manager.revoke_sbt(
            sbt_id=sbt_id,
            reason=request.reason,
            revoker=request.revoker
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
            detail=f"Failed to revoke SBT: {str(e)}"
        )


@router.post("/sbts/{sbt_id}/burn")
async def burn_sbt(
    sbt_id: str,
    request: BurnSBTRequest,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Burn a SoulBound Token
    
    Permanently destroys an SBT. Only the token owner can burn their SBT.
    This operation is irreversible.
    """
    try:
        result = await sbt_manager.burn_sbt(
            sbt_id=sbt_id,
            burner=request.burner
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
            detail=f"Failed to burn SBT: {str(e)}"
        )


@router.get("/sbts/credential/{credential_id}", response_model=Optional[SBTResponse])
async def get_sbt_by_credential(
    credential_id: str,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get SBT by credential ID
    
    Retrieves the SBT associated with a specific credential.
    """
    sbt_data = await sbt_manager.get_sbt_by_credential(credential_id)
    
    if not sbt_data:
        return None
    
    return SBTResponse(**sbt_data)


@router.get("/sbts", response_model=SBTListResponse)
async def list_sbts(
    recipient: Optional[str] = Query(None, description="Filter by recipient address"),
    issuer: Optional[str] = Query(None, description="Filter by issuer"),
    chain: Optional[str] = Query(None, description="Filter by blockchain"),
    status: Optional[SBTStatus] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    List SBTs with filters
    
    Retrieves a paginated list of SBTs based on various filters.
    """
    sbts = []
    
    if recipient:
        sbts = await sbt_manager.get_sbts_by_recipient(recipient, chain, status)
    elif issuer:
        sbts = await sbt_manager.get_sbts_by_issuer(issuer, chain, status)
    else:
        # Get all SBTs (with pagination)
        offset = (page - 1) * limit
        all_sbts = await sbt_manager.sbt_store.search(
            query="",
            filters={
                "chain": chain,
                "status": status
            } if chain or status else None,
            limit=limit,
            offset=offset
        )
        
        sbts = [sbt_manager._format_sbt(sbt) for sbt in all_sbts]
    
    # Convert to response format
    sbt_responses = []
    for sbt in sbts:
        sbt_responses.append(SBTResponse(
            id=sbt["id"],
            tokenId=sbt.get("tokenId"),
            credentialId=sbt["credentialId"],
            chain=sbt["chain"],
            contractAddress=sbt["contractAddress"],
            recipient=sbt["recipient"],
            issuer=sbt["issuer"],
            metadataUri=sbt["metadataUri"],
            metadata=sbt["metadata"],
            status=sbt["status"],
            mintedAt=sbt.get("mintedAt"),
            transactionHash=sbt.get("transactionHash")
        ))
    
    return SBTListResponse(
        sbts=sbt_responses,
        total=len(sbt_responses),
        page=page,
        limit=limit
    )


@router.post("/sbts/{sbt_id}/verify-ownership")
async def verify_ownership(
    sbt_id: str,
    address: str = Body(..., description="Address to verify"),
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Verify SBT ownership
    
    Verifies if a specific address owns the SBT.
    """
    try:
        is_owner = await sbt_manager.verify_sbt_ownership(sbt_id, address)
        
        return JSONResponse(
            content={
                "sbtId": sbt_id,
                "address": address,
                "isOwner": is_owner,
                "verifiedAt": datetime.now().isoformat()
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify ownership: {str(e)}"
        )


@router.get("/stats")
async def get_statistics(
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """Get SBT service statistics"""
    stats = await sbt_manager.get_statistics()
    return JSONResponse(content=stats) 