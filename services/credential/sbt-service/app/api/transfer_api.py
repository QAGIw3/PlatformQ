"""
SBT Transfer API Endpoints
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.sbt_manager import SBTManager, TransferAttemptResult
from app import main

router = APIRouter()


# Request/Response models
class RecordTransferAttemptRequest(BaseModel):
    """Request to record a transfer attempt"""
    sbt_id: str = Field(
        description="SBT that was attempted to transfer"
    )
    from_address: str = Field(
        description="Source address"
    )
    to_address: str = Field(
        description="Destination address"
    )
    transaction_hash: Optional[str] = Field(
        description="Transaction hash if available",
        default=None
    )


class TransferAttemptResponse(BaseModel):
    """Transfer attempt response"""
    attemptId: str
    sbtId: str
    tokenId: Optional[str]
    from_address: str = Field(alias="from")
    to_address: str = Field(alias="to")
    result: str
    timestamp: str


class TransferAttemptsListResponse(BaseModel):
    """List of transfer attempts"""
    attempts: List[TransferAttemptResponse]
    total: int


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

@router.post("/transfer-attempts", response_model=TransferAttemptResponse)
async def record_transfer_attempt(
    request: RecordTransferAttemptRequest,
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Record a blocked transfer attempt
    
    Records when an attempt was made to transfer a SoulBound Token.
    SBTs are non-transferable, so all attempts are blocked.
    """
    try:
        result = await sbt_manager.record_transfer_attempt(
            sbt_id=request.sbt_id,
            from_address=request.from_address,
            to_address=request.to_address,
            transaction_hash=request.transaction_hash
        )
        
        return TransferAttemptResponse(
            attemptId=result["attemptId"],
            sbtId=result["sbtId"],
            tokenId=result["tokenId"],
            **{"from": result["from"], "to": result["to"]},
            result=result["result"],
            timestamp=result["timestamp"]
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to record transfer attempt: {str(e)}"
        )


@router.get("/transfer-attempts/{sbt_id}", response_model=TransferAttemptsListResponse)
async def get_transfer_attempts(
    sbt_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Maximum attempts to return"),
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get transfer attempts for an SBT
    
    Retrieves the history of blocked transfer attempts for a specific SBT.
    """
    # Verify SBT exists
    sbt = await sbt_manager.sbt_store.get(sbt_id)
    if not sbt:
        raise HTTPException(
            status_code=404,
            detail=f"SBT {sbt_id} not found"
        )
    
    # Get transfer attempts
    attempts = await sbt_manager.sbt_store.get_transfer_attempts(sbt_id, limit)
    
    # Format response
    attempt_responses = []
    for attempt in attempts:
        attempt_responses.append(TransferAttemptResponse(
            attemptId=attempt.id,
            sbtId=attempt.sbt_id,
            tokenId=sbt.token_id,
            **{"from": attempt.from_address, "to": attempt.to_address},
            result=attempt.result,
            timestamp=attempt.timestamp.isoformat()
        ))
    
    return TransferAttemptsListResponse(
        attempts=attempt_responses,
        total=len(attempt_responses)
    )


@router.post("/transfer-protection/verify")
async def verify_transfer_protection(
    chain: str = Query(..., description="Blockchain to verify on"),
    token_id: str = Query(..., description="Token ID to verify"),
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Verify transfer protection is active
    
    Verifies that a token on the blockchain has transfer protection enabled.
    This confirms the token is truly non-transferable.
    """
    try:
        # Get SBT by chain and token ID
        sbt = await sbt_manager.sbt_store.get_by_token_id(chain, token_id)
        
        if not sbt:
            raise HTTPException(
                status_code=404,
                detail=f"SBT not found for token {token_id} on {chain}"
            )
        
        # Verify on blockchain that transfer is blocked
        # This would call the smart contract to verify transfer protection
        response = await sbt_manager.http_client.post(
            f"{sbt_manager.blockchain_connector_url}/api/v1/contracts/call",
            json={
                "chain": chain,
                "contractAddress": sbt.contract_address,
                "method": "isTransferrable",
                "params": {
                    "tokenId": token_id
                }
            }
        )
        
        is_transferrable = False
        if response.status_code == 200:
            result = response.json()
            is_transferrable = result.get("result", False)
        
        return JSONResponse(
            content={
                "sbtId": sbt.id,
                "tokenId": token_id,
                "chain": chain,
                "isTransferrable": is_transferrable,
                "transferProtection": "active" if not is_transferrable else "inactive",
                "verifiedAt": datetime.now(timezone.utc).isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify transfer protection: {str(e)}"
        )


@router.get("/transfer-statistics")
async def get_transfer_statistics(
    sbt_manager: SBTManager = Depends(get_sbt_manager)
):
    """
    Get transfer attempt statistics
    
    Returns statistics about blocked transfer attempts across all SBTs.
    """
    stats = await sbt_manager.get_statistics()
    
    return JSONResponse(
        content={
            "total_transfer_attempts": stats.get("total_transfer_attempts", 0),
            "database_stats": stats.get("database_stats", {}).get("total_transfer_attempts", 0),
            "average_attempts_per_sbt": (
                stats.get("total_transfer_attempts", 0) / 
                stats.get("database_stats", {}).get("total_sbts", 1)
            ) if stats.get("database_stats", {}).get("total_sbts", 0) > 0 else 0
        }
    ) 