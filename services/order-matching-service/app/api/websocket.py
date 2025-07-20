from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from typing import Dict, Set, List
import asyncio
import json
import time
from decimal import Decimal

from ..core.matching_engine import MatchingEngine
from ..dependencies import get_matching_engine


router = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    """Manages WebSocket connections"""
    
    def __init__(self):
        # market_id -> set of websockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # websocket -> set of subscribed markets
        self.connection_subscriptions: Dict[WebSocket, Set[str]] = {}
        
    async def connect(self, websocket: WebSocket, market_id: str):
        """Accept new connection"""
        await websocket.accept()
        
        # Add to market subscribers
        if market_id not in self.active_connections:
            self.active_connections[market_id] = set()
        self.active_connections[market_id].add(websocket)
        
        # Track subscriptions
        if websocket not in self.connection_subscriptions:
            self.connection_subscriptions[websocket] = set()
        self.connection_subscriptions[websocket].add(market_id)
        
    def disconnect(self, websocket: WebSocket):
        """Remove connection"""
        # Get all subscriptions for this connection
        if websocket in self.connection_subscriptions:
            for market_id in self.connection_subscriptions[websocket]:
                if market_id in self.active_connections:
                    self.active_connections[market_id].discard(websocket)
                    # Clean up empty sets
                    if not self.active_connections[market_id]:
                        del self.active_connections[market_id]
            
            del self.connection_subscriptions[websocket]
    
    async def subscribe(self, websocket: WebSocket, market_id: str):
        """Subscribe to additional market"""
        if market_id not in self.active_connections:
            self.active_connections[market_id] = set()
        self.active_connections[market_id].add(websocket)
        
        if websocket not in self.connection_subscriptions:
            self.connection_subscriptions[websocket] = set()
        self.connection_subscriptions[websocket].add(market_id)
        
    async def unsubscribe(self, websocket: WebSocket, market_id: str):
        """Unsubscribe from market"""
        if market_id in self.active_connections:
            self.active_connections[market_id].discard(websocket)
            
        if websocket in self.connection_subscriptions:
            self.connection_subscriptions[websocket].discard(market_id)
    
    async def send_to_market(self, market_id: str, message: dict):
        """Send message to all subscribers of a market"""
        if market_id in self.active_connections:
            # Create tasks for parallel sending
            tasks = []
            dead_connections = []
            
            for websocket in self.active_connections[market_id]:
                try:
                    tasks.append(websocket.send_json(message))
                except Exception:
                    dead_connections.append(websocket)
            
            # Send all messages in parallel
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Clean up dead connections
            for websocket in dead_connections:
                self.disconnect(websocket)
    
    async def broadcast_orderbook_update(self, market_id: str, orderbook: dict):
        """Broadcast order book update"""
        message = {
            "type": "orderbook",
            "market_id": market_id,
            "data": orderbook,
            "timestamp": time.time_ns()
        }
        await self.send_to_market(market_id, message)
    
    async def broadcast_trade(self, market_id: str, trade: dict):
        """Broadcast new trade"""
        message = {
            "type": "trade",
            "market_id": market_id,
            "data": trade,
            "timestamp": time.time_ns()
        }
        await self.send_to_market(market_id, message)


# Global connection manager
manager = ConnectionManager()


@router.websocket("/market/{market_id}")
async def websocket_market_data(
    websocket: WebSocket,
    market_id: str,
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """WebSocket endpoint for real-time market data"""
    await manager.connect(websocket, market_id)
    
    try:
        # Send initial order book snapshot
        orderbook = matching_engine.get_order_book(market_id, depth=20)
        if orderbook:
            await websocket.send_json({
                "type": "snapshot",
                "market_id": market_id,
                "data": orderbook
            })
        
        # Keep connection alive and handle messages
        while True:
            try:
                # Wait for messages with timeout
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=30.0  # 30 second timeout
                )
                
                # Handle different message types
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                    
                elif message.get("type") == "subscribe":
                    new_market = message.get("market_id")
                    if new_market:
                        await manager.subscribe(websocket, new_market)
                        # Send snapshot for new market
                        orderbook = matching_engine.get_order_book(new_market, depth=20)
                        if orderbook:
                            await websocket.send_json({
                                "type": "snapshot",
                                "market_id": new_market,
                                "data": orderbook
                            })
                            
                elif message.get("type") == "unsubscribe":
                    old_market = message.get("market_id")
                    if old_market:
                        await manager.unsubscribe(websocket, old_market)
                        
            except asyncio.TimeoutError:
                # Send ping on timeout
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    break
                    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)


@router.websocket("/trades")
async def websocket_all_trades(
    websocket: WebSocket,
    markets: str = Query(default="", description="Comma-separated market IDs")
):
    """WebSocket endpoint for all trades across markets"""
    await websocket.accept()
    
    # Parse markets
    market_list = [m.strip() for m in markets.split(",") if m.strip()] if markets else []
    
    # Subscribe to markets
    for market_id in market_list:
        await manager.connect(websocket, market_id)
    
    try:
        while True:
            message = await websocket.receive_json()
            
            if message.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# Background task to publish market data updates
async def market_data_publisher(matching_engine: MatchingEngine):
    """Publish market data updates from Pulsar to WebSocket clients"""
    # This would subscribe to Pulsar topics and forward to WebSocket
    # For now, simplified implementation
    
    while True:
        try:
            # Get all active markets
            for market_id in matching_engine.active_markets:
                # Get order book snapshot
                orderbook = matching_engine.get_order_book(market_id, depth=5)
                if orderbook:
                    await manager.broadcast_orderbook_update(market_id, orderbook)
            
            # Sleep briefly
            await asyncio.sleep(0.1)  # 100ms updates
            
        except Exception as e:
            print(f"Error in market data publisher: {e}")
            await asyncio.sleep(1) 