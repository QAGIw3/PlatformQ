"""
Transaction Processor API endpoints
"""

from typing import List, Optional
from datetime import datetime, timedelta
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from ..models.transaction import (
    Transaction, TransactionRequest, TransactionResult,
    TransactionStatus, TransactionEvent
)
from ..core.transaction_processor import TransactionProcessor

router = APIRouter(prefix="/api/v1", tags=["transactions"])


# Dependency to get transaction processor
def get_processor(request) -> TransactionProcessor:
    """Get transaction processor instance"""
    return request.app.state.transaction_processor


@router.post("/transactions")
async def submit_transaction(
    request: TransactionRequest,
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Submit a new transaction for processing"""
    # Create transaction model
    transaction = Transaction(
        id=str(uuid4()),
        chain=request.chain,
        type=request.type,
        from_address=request.from_address,
        to_address=request.to_address,
        value=request.value or "0",
        data=request.data,
        gas_limit=request.gas_limit,
        gas_price=request.gas_price,
        max_fee_per_gas=request.max_fee_per_gas,
        max_priority_fee_per_gas=request.max_priority_fee_per_gas,
        priority=request.priority,
        tags=request.tags,
        created_at=datetime.utcnow()
    )
    
    # Set expiration if specified
    if request.expires_in_seconds:
        transaction.expires_at = datetime.utcnow() + timedelta(
            seconds=request.expires_in_seconds
        )
        
    # Submit for processing
    transaction_id = await processor.submit_transaction(transaction)
    
    return {
        "transaction_id": transaction_id,
        "status": TransactionStatus.QUEUED,
        "message": "Transaction queued for processing"
    }


@router.get("/transactions/{transaction_id}")
async def get_transaction_status(
    transaction_id: str,
    processor: TransactionProcessor = Depends(get_processor)
) -> TransactionResult:
    """Get transaction status and details"""
    result = await processor.get_transaction_status(transaction_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
        
    return result


@router.get("/transactions")
async def list_transactions(
    chain: Optional[str] = Query(None, description="Filter by chain"),
    status: Optional[TransactionStatus] = Query(None, description="Filter by status"),
    from_address: Optional[str] = Query(None, description="Filter by sender"),
    limit: int = Query(50, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """List transactions with filtering"""
    # TODO: Implement filtering and pagination
    # This would query the Ignite cache with filters
    
    return {
        "transactions": [],
        "total": 0,
        "limit": limit,
        "offset": offset
    }


@router.post("/transactions/{transaction_id}/cancel")
async def cancel_transaction(
    transaction_id: str,
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Cancel a pending transaction"""
    result = await processor.get_transaction_status(transaction_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
        
    if result.status not in [TransactionStatus.PENDING, TransactionStatus.QUEUED]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel transaction in status: {result.status}"
        )
        
    # TODO: Implement cancellation logic
    # This would update the transaction status and potentially send a replacement
    # transaction with higher gas to cancel the original
    
    return {
        "transaction_id": transaction_id,
        "status": TransactionStatus.CANCELLED,
        "message": "Transaction cancelled"
    }


@router.post("/transactions/{transaction_id}/retry")
async def retry_transaction(
    transaction_id: str,
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Retry a failed transaction"""
    result = await processor.get_transaction_status(transaction_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
        
    if result.status != TransactionStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Can only retry failed transactions, current status: {result.status}"
        )
        
    # TODO: Implement retry logic
    # This would resubmit the transaction with updated gas settings
    
    return {
        "transaction_id": transaction_id,
        "message": "Transaction resubmitted for processing"
    }


@router.get("/nonces/{chain}/{address}")
async def get_current_nonce(
    chain: str,
    address: str,
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Get current nonce for an address"""
    try:
        # Get nonce from nonce manager
        nonce = await processor.nonce_manager.get_nonce(chain, address)
        
        # Release it immediately since this is just a query
        await processor.nonce_manager.release_nonce(chain, address, nonce)
        
        return {
            "chain": chain,
            "address": address,
            "nonce": nonce
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/nonces/{chain}/{address}/reset")
async def reset_nonce(
    chain: str,
    address: str,
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Reset nonce for an address (admin operation)"""
    try:
        await processor.nonce_manager.reset_nonce(chain, address)
        
        return {
            "chain": chain,
            "address": address,
            "message": "Nonce reset successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gas/prices")
async def get_gas_prices(
    chain: str = Query(..., description="Blockchain identifier"),
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Get current gas prices for a chain"""
    try:
        gas_prices = await processor.gas_manager._get_gas_prices(chain)
        
        return {
            "chain": chain,
            "prices": gas_prices,
            "updated_at": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_processing_stats(
    processor: TransactionProcessor = Depends(get_processor)
) -> dict:
    """Get transaction processing statistics"""
    # TODO: Implement stats aggregation from metrics
    
    return {
        "active_transactions": len(processor._processing_tasks),
        "max_concurrent": processor.settings.MAX_CONCURRENT_TRANSACTIONS,
        "stats": {
            "total_processed": 0,
            "success_rate": 0.0,
            "average_duration": 0.0
        }
    }


@router.websocket("/ws/transactions/{transaction_id}")
async def transaction_updates(websocket, transaction_id: str):
    """WebSocket endpoint for real-time transaction updates"""
    # TODO: Implement WebSocket for real-time status updates
    # This would subscribe to the transaction status topic and
    # forward relevant events to the connected client
    pass 