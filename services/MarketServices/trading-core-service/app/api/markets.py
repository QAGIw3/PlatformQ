"""Market management API endpoints."""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from decimal import Decimal

from ..models.market import Market, MarketStatus, MarketType, ProductType
from ..models.orderbook import OrderBookSnapshot
from ..core import MarketManager, MatchingEngine
from ..dependencies import get_market_manager, get_matching_engine, require_admin


router = APIRouter(prefix="/markets", tags=["markets"])


@router.post("/", response_model=Market)
async def create_market(
    market: Market,
    admin_check: None = Depends(require_admin),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Create a new market (admin only)."""
    try:
        created_market = await market_manager.create_market(market)
        return created_market
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create market: {str(e)}")


@router.put("/{market_id}")
async def update_market(
    market_id: str,
    updates: Dict[str, Any],
    admin_check: None = Depends(require_admin),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Update market configuration (admin only)."""
    try:
        updated_market = await market_manager.update_market(market_id, updates)
        return updated_market
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update market: {str(e)}")


@router.post("/{market_id}/open")
async def open_market(
    market_id: str,
    admin_check: None = Depends(require_admin),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Open a market for trading (admin only)."""
    success = await market_manager.open_market(market_id)
    if success:
        return JSONResponse(content={"message": f"Market {market_id} opened"})
    else:
        raise HTTPException(status_code=400, detail="Failed to open market")


@router.post("/{market_id}/close")
async def close_market(
    market_id: str,
    admin_check: None = Depends(require_admin),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Close a market (admin only)."""
    success = await market_manager.close_market(market_id)
    if success:
        return JSONResponse(content={"message": f"Market {market_id} closed"})
    else:
        raise HTTPException(status_code=400, detail="Failed to close market")


@router.post("/{market_id}/halt")
async def halt_market(
    market_id: str,
    reason: str,
    duration_seconds: Optional[int] = None,
    admin_check: None = Depends(require_admin),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Halt trading in a market (admin only)."""
    success = await market_manager.halt_market(market_id, reason, duration_seconds)
    if success:
        return JSONResponse(content={
            "message": f"Market {market_id} halted",
            "reason": reason,
            "duration": duration_seconds
        })
    else:
        raise HTTPException(status_code=400, detail="Failed to halt market")


@router.get("/{market_id}", response_model=Market)
async def get_market(
    market_id: str,
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Get market details."""
    market = await market_manager.get_market(market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return market


@router.get("/", response_model=List[Market])
async def list_markets(
    market_type: Optional[MarketType] = Query(None, description="Filter by market type"),
    product_type: Optional[ProductType] = Query(None, description="Filter by product type"),
    status: Optional[MarketStatus] = Query(None, description="Filter by status"),
    active_only: bool = Query(True, description="Show only active markets"),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """List available markets."""
    markets = await market_manager.list_markets(
        market_type=market_type,
        product_type=product_type,
        status=status,
        active_only=active_only
    )
    return markets


@router.get("/{market_id}/orderbook", response_model=OrderBookSnapshot)
async def get_orderbook(
    market_id: str,
    depth: int = Query(20, ge=1, le=100, description="Order book depth"),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get current order book for a market."""
    orderbook = matching_engine.order_books.get(market_id)
    if not orderbook:
        raise HTTPException(status_code=404, detail="Order book not found")
    
    snapshot = orderbook.get_snapshot(depth=depth)
    return snapshot


@router.get("/{market_id}/ticker")
async def get_ticker(
    market_id: str,
    market_manager: MarketManager = Depends(get_market_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get market ticker data."""
    market = await market_manager.get_market(market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    
    orderbook = matching_engine.order_books.get(market_id)
    if not orderbook:
        raise HTTPException(status_code=404, detail="Order book not found")
    
    best_bid, best_ask = orderbook.get_best_bid_ask()
    
    # This would normally come from market stats
    return {
        "market_id": market_id,
        "symbol": market.symbol,
        "best_bid": str(best_bid) if best_bid else None,
        "best_ask": str(best_ask) if best_ask else None,
        "spread": str(best_ask - best_bid) if best_bid and best_ask else None,
        "mid_price": str((best_bid + best_ask) / 2) if best_bid and best_ask else None,
        "last_price": None,  # Would come from last trade
        "volume_24h": "0",
        "high_24h": None,
        "low_24h": None,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/search")
async def search_markets(
    query: str = Query(..., min_length=1, description="Search query"),
    market_manager: MarketManager = Depends(get_market_manager)
):
    """Search markets by symbol or name."""
    all_markets = await market_manager.list_markets(active_only=True)
    
    # Simple search implementation
    query_lower = query.lower()
    matching_markets = [
        m for m in all_markets
        if query_lower in m.symbol.lower() or query_lower in m.name.lower()
    ]
    
    return matching_markets[:10]  # Limit results


@router.get("/stats/summary")
async def get_market_summary(
    market_manager: MarketManager = Depends(get_market_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get summary statistics for all markets."""
    markets = await market_manager.list_markets(status=MarketStatus.OPEN)
    
    summary = {
        "total_markets": len(markets),
        "open_markets": len([m for m in markets if m.status == MarketStatus.OPEN]),
        "halted_markets": len([m for m in markets if m.status == MarketStatus.HALTED]),
        "market_types": {},
        "product_types": {},
        "engine_metrics": matching_engine.get_metrics()
    }
    
    # Count by market type
    for market in markets:
        market_type = market.market_type.value
        summary["market_types"][market_type] = summary["market_types"].get(market_type, 0) + 1
        
        product_type = market.product_type.value
        summary["product_types"][product_type] = summary["product_types"].get(product_type, 0) + 1
    
    return summary


# Import for datetime
from datetime import datetime 