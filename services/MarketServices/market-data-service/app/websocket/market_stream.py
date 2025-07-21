"""WebSocket endpoints for real-time market data streaming"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from typing import Dict, Set, List, Optional
import asyncio
import json
import time
import uuid
from collections import defaultdict

from ..core.aggregator import MarketDataAggregator
from ..dependencies import get_aggregator
from ..config import MarketDataConfig


router = APIRouter(prefix="/ws", tags=["websocket"])


class StreamManager:
    """Manages WebSocket connections and data streams"""
    
    def __init__(self):
        # Connection management
        self.connections: Dict[str, WebSocket] = {}  # connection_id -> websocket
        self.connection_subscriptions: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
        # connection_id -> {stream_type -> set of markets}
        
        # Reverse mapping for efficient broadcasting
        self.stream_subscribers: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
        # stream_type -> {market_id -> set of connection_ids}
        
        # Rate limiting
        self.connection_message_counts: Dict[str, int] = defaultdict(int)
        self.last_rate_limit_reset = time.time()
    
    async def connect(self, websocket: WebSocket) -> str:
        """Accept new connection and return connection ID"""
        await websocket.accept()
        connection_id = str(uuid.uuid4())
        self.connections[connection_id] = websocket
        return connection_id
    
    def disconnect(self, connection_id: str):
        """Remove connection and all its subscriptions"""
        # Remove from connections
        self.connections.pop(connection_id, None)
        
        # Remove all subscriptions
        if connection_id in self.connection_subscriptions:
            for stream_type, markets in self.connection_subscriptions[connection_id].items():
                for market_id in markets:
                    self.stream_subscribers[stream_type][market_id].discard(connection_id)
            
            del self.connection_subscriptions[connection_id]
        
        # Clean up rate limiting
        self.connection_message_counts.pop(connection_id, None)
    
    async def subscribe(self, connection_id: str, stream_type: str, markets: List[str]):
        """Subscribe connection to data streams"""
        if connection_id not in self.connections:
            return
        
        # Add subscriptions
        for market_id in markets:
            self.connection_subscriptions[connection_id][stream_type].add(market_id)
            self.stream_subscribers[stream_type][market_id].add(connection_id)
        
        # Send confirmation
        await self.send_to_connection(connection_id, {
            "type": "subscribed",
            "stream": stream_type,
            "markets": markets,
            "timestamp": time.time_ns()
        })
    
    async def unsubscribe(self, connection_id: str, stream_type: str, markets: List[str]):
        """Unsubscribe connection from data streams"""
        if connection_id not in self.connections:
            return
        
        # Remove subscriptions
        for market_id in markets:
            self.connection_subscriptions[connection_id][stream_type].discard(market_id)
            self.stream_subscribers[stream_type][market_id].discard(connection_id)
        
        # Send confirmation
        await self.send_to_connection(connection_id, {
            "type": "unsubscribed",
            "stream": stream_type,
            "markets": markets,
            "timestamp": time.time_ns()
        })
    
    async def send_to_connection(self, connection_id: str, message: dict):
        """Send message to specific connection"""
        websocket = self.connections.get(connection_id)
        if websocket:
            try:
                # Check rate limit
                if self._check_rate_limit(connection_id):
                    await websocket.send_json(message)
                    self.connection_message_counts[connection_id] += 1
            except:
                # Connection error - remove it
                self.disconnect(connection_id)
    
    async def broadcast_price_update(self, market_id: str, price_data: dict):
        """Broadcast price update to subscribers"""
        subscribers = self.stream_subscribers["ticker"][market_id]
        if not subscribers:
            return
        
        message = {
            "type": "ticker",
            "market_id": market_id,
            "data": price_data,
            "timestamp": time.time_ns()
        }
        
        # Send to all subscribers in parallel
        tasks = [
            self.send_to_connection(conn_id, message)
            for conn_id in subscribers
        ]
        await asyncio.gather(*tasks)
    
    async def broadcast_orderbook_update(self, market_id: str, orderbook_data: dict):
        """Broadcast order book update to subscribers"""
        subscribers = self.stream_subscribers["orderbook"][market_id]
        if not subscribers:
            return
        
        message = {
            "type": "orderbook",
            "market_id": market_id,
            "data": orderbook_data,
            "timestamp": time.time_ns()
        }
        
        tasks = [
            self.send_to_connection(conn_id, message)
            for conn_id in subscribers
        ]
        await asyncio.gather(*tasks)
    
    async def broadcast_trade(self, market_id: str, trade_data: dict):
        """Broadcast new trade to subscribers"""
        subscribers = self.stream_subscribers["trades"][market_id]
        if not subscribers:
            return
        
        message = {
            "type": "trade",
            "market_id": market_id,
            "data": trade_data,
            "timestamp": time.time_ns()
        }
        
        tasks = [
            self.send_to_connection(conn_id, message)
            for conn_id in subscribers
        ]
        await asyncio.gather(*tasks)
    
    def _check_rate_limit(self, connection_id: str, max_messages_per_minute: int = 1000) -> bool:
        """Check if connection has exceeded rate limit"""
        # Reset counters every minute
        current_time = time.time()
        if current_time - self.last_rate_limit_reset > 60:
            self.connection_message_counts.clear()
            self.last_rate_limit_reset = current_time
        
        return self.connection_message_counts[connection_id] < max_messages_per_minute


# Global stream manager
stream_manager = StreamManager()


@router.websocket("/stream")
async def websocket_stream(
    websocket: WebSocket,
    aggregator: MarketDataAggregator = Depends(get_aggregator)
):
    """WebSocket endpoint for real-time market data streaming"""
    connection_id = await stream_manager.connect(websocket)
    
    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "connection_id": connection_id,
            "timestamp": time.time_ns(),
            "available_streams": ["ticker", "orderbook", "trades", "candles"]
        })
        
        # Handle messages
        while True:
            try:
                # Wait for messages with timeout
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=30.0
                )
                
                await handle_message(connection_id, message, aggregator)
                
            except asyncio.TimeoutError:
                # Send ping on timeout
                await websocket.send_json({"type": "ping"})
                
    except WebSocketDisconnect:
        stream_manager.disconnect(connection_id)
    except Exception as e:
        print(f"WebSocket error: {e}")
        stream_manager.disconnect(connection_id)


async def handle_message(
    connection_id: str,
    message: dict,
    aggregator: MarketDataAggregator
):
    """Handle incoming WebSocket message"""
    msg_type = message.get("type")
    
    if msg_type == "ping":
        await stream_manager.send_to_connection(connection_id, {"type": "pong"})
    
    elif msg_type == "subscribe":
        stream_type = message.get("stream")
        markets = message.get("markets", [])
        
        if stream_type and markets:
            await stream_manager.subscribe(connection_id, stream_type, markets)
            
            # Send initial data
            if stream_type == "ticker":
                for market_id in markets:
                    state = await aggregator.get_market_state(market_id)
                    if state:
                        await stream_manager.send_to_connection(connection_id, {
                            "type": "ticker",
                            "market_id": market_id,
                            "data": {
                                "price": str(state.last_price),
                                "volume_24h": str(state.volume_24h),
                                "high_24h": str(state.high_24h),
                                "low_24h": str(state.low_24h)
                            },
                            "timestamp": time.time_ns()
                        })
            
            elif stream_type == "orderbook":
                for market_id in markets:
                    orderbook = await aggregator.get_orderbook(market_id)
                    if orderbook:
                        await stream_manager.send_to_connection(connection_id, {
                            "type": "orderbook_snapshot",
                            "market_id": market_id,
                            "data": orderbook.to_dict(),
                            "timestamp": time.time_ns()
                        })
    
    elif msg_type == "unsubscribe":
        stream_type = message.get("stream")
        markets = message.get("markets", [])
        
        if stream_type and markets:
            await stream_manager.unsubscribe(connection_id, stream_type, markets)
    
    else:
        await stream_manager.send_to_connection(connection_id, {
            "type": "error",
            "message": f"Unknown message type: {msg_type}"
        })


# Background task to publish market data updates
async def market_data_publisher(aggregator: MarketDataAggregator):
    """Publish market data updates to WebSocket clients"""
    # This would be integrated with the aggregator
    # For now, simplified implementation
    
    while True:
        try:
            # Publish updates for all active markets
            for market_id, state in aggregator.market_states.items():
                # Ticker updates
                ticker_data = {
                    "price": str(state.last_price),
                    "best_bid": str(state.best_bid) if state.best_bid else None,
                    "best_ask": str(state.best_ask) if state.best_ask else None,
                    "volume_24h": str(state.volume_24h)
                }
                await stream_manager.broadcast_price_update(market_id, ticker_data)
            
            await asyncio.sleep(1)  # Update every second
            
        except Exception as e:
            print(f"Error in market data publisher: {e}")
            await asyncio.sleep(1) 