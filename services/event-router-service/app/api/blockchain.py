"""
Blockchain Event API endpoints for Event Router Service.
"""

import logging
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from ..core.blockchain_event_handler import BlockchainEventHandler, BlockchainEventType

logger = logging.getLogger(__name__)

router = APIRouter()


class BlockchainEventRequest(BaseModel):
    """Request model for blockchain events"""
    event_type: str = Field(..., description="Type of blockchain event")
    chain: str = Field(..., description="Source blockchain")
    transaction_hash: Optional[str] = Field(None, description="Transaction hash")
    block_number: Optional[int] = Field(None, description="Block number")
    contract_address: Optional[str] = Field(None, description="Contract address")
    event_data: Dict[str, Any] = Field(..., description="Event-specific data")


class EventMappingUpdate(BaseModel):
    """Request to update event routing"""
    event_type: str = Field(..., description="Event type to update")
    target_topics: List[str] = Field(..., description="Target Pulsar topics")


def get_blockchain_handler(request) -> BlockchainEventHandler:
    """Dependency to get blockchain event handler"""
    return request.app.state.blockchain_event_handler


@router.post("/events")
async def handle_blockchain_event(
    request: BlockchainEventRequest,
    background_tasks: BackgroundTasks,
    handler: BlockchainEventHandler = Depends(get_blockchain_handler)
):
    """
    Handle incoming blockchain event and route to appropriate topics.
    
    This endpoint receives blockchain events from various sources and routes them
    to the appropriate Pulsar topics based on event type.
    """
    try:
        # Enrich event data with common fields
        event_data = {
            "tx_hash": request.transaction_hash,
            "block_number": request.block_number,
            "contract_address": request.contract_address,
            **request.event_data
        }
        
        # Process event
        result = await handler.handle_blockchain_event(
            event_type=request.event_type,
            chain=request.chain,
            event_data=event_data
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process event: {result.get('error')}"
            )
            
        return {
            "success": True,
            "event_id": result["event_id"],
            "routed_to": result["routed_to"]
        }
        
    except Exception as e:
        logger.error(f"Error handling blockchain event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/event-types")
async def get_supported_event_types():
    """Get list of supported blockchain event types"""
    return {
        "event_types": [event.value for event in BlockchainEventType]
    }


@router.get("/mappings")
async def get_event_mappings(
    handler: BlockchainEventHandler = Depends(get_blockchain_handler)
):
    """Get current event type to topic mappings"""
    mappings = await handler.get_event_mappings()
    return {"mappings": mappings}


@router.put("/mappings")
async def update_event_mapping(
    request: EventMappingUpdate,
    handler: BlockchainEventHandler = Depends(get_blockchain_handler)
):
    """Update routing configuration for a specific event type"""
    try:
        await handler.update_event_mapping(
            event_type=request.event_type,
            topics=request.target_topics
        )
        
        return {
            "success": True,
            "message": f"Updated mapping for {request.event_type}"
        }
        
    except Exception as e:
        logger.error(f"Error updating event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_blockchain_event_stats(
    handler: BlockchainEventHandler = Depends(get_blockchain_handler)
):
    """Get statistics about blockchain events processed"""
    try:
        stats = await handler.get_blockchain_event_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error getting blockchain event stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch")
async def handle_blockchain_events_batch(
    events: List[BlockchainEventRequest],
    background_tasks: BackgroundTasks,
    handler: BlockchainEventHandler = Depends(get_blockchain_handler)
):
    """
    Handle multiple blockchain events in batch.
    
    Useful for processing historical events or bulk updates.
    """
    results = []
    
    for event in events:
        try:
            event_data = {
                "tx_hash": event.transaction_hash,
                "block_number": event.block_number,
                "contract_address": event.contract_address,
                **event.event_data
            }
            
            result = await handler.handle_blockchain_event(
                event_type=event.event_type,
                chain=event.chain,
                event_data=event_data
            )
            
            results.append({
                "event": event.dict(),
                "result": result
            })
            
        except Exception as e:
            logger.error(f"Error processing batch event: {e}")
            results.append({
                "event": event.dict(),
                "error": str(e)
            })
            
    successful = sum(1 for r in results if "result" in r and r["result"].get("success"))
    failed = len(results) - successful
    
    return {
        "total": len(results),
        "successful": successful,
        "failed": failed,
        "results": results
    } 