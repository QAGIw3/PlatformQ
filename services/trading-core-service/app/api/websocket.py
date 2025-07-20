"""WebSocket API for real-time market data and updates."""

import json
import asyncio
from typing import Dict, Set, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from datetime import datetime
import logging

from ..core import MatchingEngine, MarketManager
from ..dependencies import get_matching_engine, get_market_manager, verify_ws_token


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    """Manages WebSocket connections and subscriptions."""
    
    def __init__(self):
        # Active connections by client
        self.active_connections: Dict[str, WebSocket] = {}
        
        # Subscriptions by channel
        self.subscriptions: Dict[str, Set[str]] = {
            "orderbook": set(),
            "trades": set(),
            "ticker": set(),
            "positions": set(),
            "orders": set()
        }
        
        # Market subscriptions
        self.market_subscriptions: Dict[str, Dict[str, Set[str]]] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept new connection."""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"Client {client_id} connected")
    
    def disconnect(self, client_id: str):
        """Remove connection and clean up subscriptions."""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            
            # Clean up subscriptions
            for channel, clients in self.subscriptions.items():
                clients.discard(client_id)
            
            # Clean up market subscriptions
            for market_id, channels in self.market_subscriptions.items():
                for channel, clients in channels.items():
                    clients.discard(client_id)
            
            logger.info(f"Client {client_id} disconnected")
    
    async def subscribe(self, client_id: str, channel: str, market_id: Optional[str] = None):
        """Subscribe client to a channel."""
        if channel not in self.subscriptions:
            return False
        
        if market_id:
            # Market-specific subscription
            if market_id not in self.market_subscriptions:
                self.market_subscriptions[market_id] = {
                    "orderbook": set(),
                    "trades": set(),
                    "ticker": set()
                }
            
            if channel in self.market_subscriptions[market_id]:
                self.market_subscriptions[market_id][channel].add(client_id)
        else:
            # Global subscription
            self.subscriptions[channel].add(client_id)
        
        return True
    
    async def unsubscribe(self, client_id: str, channel: str, market_id: Optional[str] = None):
        """Unsubscribe client from a channel."""
        if market_id and market_id in self.market_subscriptions:
            if channel in self.market_subscriptions[market_id]:
                self.market_subscriptions[market_id][channel].discard(client_id)
        elif channel in self.subscriptions:
            self.subscriptions[channel].discard(client_id)
    
    async def send_to_client(self, client_id: str, message: dict):
        """Send message to specific client."""
        if client_id in self.active_connections:
            websocket = self.active_connections[client_id]
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error sending to client {client_id}: {e}")
                self.disconnect(client_id)
    
    async def broadcast_to_channel(self, channel: str, message: dict, market_id: Optional[str] = None):
        """Broadcast message to all subscribers of a channel."""
        if market_id and market_id in self.market_subscriptions:
            # Market-specific broadcast
            if channel in self.market_subscriptions[market_id]:
                clients = self.market_subscriptions[market_id][channel].copy()
                for client_id in clients:
                    await self.send_to_client(client_id, message)
        elif channel in self.subscriptions:
            # Global broadcast
            clients = self.subscriptions[channel].copy()
            for client_id in clients:
                await self.send_to_client(client_id, message)


# Global connection manager
manager = ConnectionManager()


@router.websocket("/market")
async def websocket_market_data(
    websocket: WebSocket,
    token: Optional[str] = Query(None, description="Authentication token")
):
    """WebSocket endpoint for real-time market data."""
    # Verify token and get client ID
    client_id = await verify_ws_token(token) if token else f"anonymous_{id(websocket)}"
    
    await manager.connect(websocket, client_id)
    
    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()
            
            # Handle different message types
            message_type = data.get("type")
            
            if message_type == "subscribe":
                channel = data.get("channel")
                market_id = data.get("market_id")
                
                success = await manager.subscribe(client_id, channel, market_id)
                
                await manager.send_to_client(client_id, {
                    "type": "subscription",
                    "channel": channel,
                    "market_id": market_id,
                    "status": "subscribed" if success else "failed"
                })
                
            elif message_type == "unsubscribe":
                channel = data.get("channel")
                market_id = data.get("market_id")
                
                await manager.unsubscribe(client_id, channel, market_id)
                
                await manager.send_to_client(client_id, {
                    "type": "subscription",
                    "channel": channel,
                    "market_id": market_id,
                    "status": "unsubscribed"
                })
                
            elif message_type == "ping":
                await manager.send_to_client(client_id, {
                    "type": "pong",
                    "timestamp": datetime.utcnow().isoformat()
                })
                
            else:
                await manager.send_to_client(client_id, {
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                })
    
    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
        manager.disconnect(client_id)


async def broadcast_orderbook_update(market_id: str, orderbook_snapshot: dict):
    """Broadcast order book update to subscribers."""
    message = {
        "type": "orderbook",
        "market_id": market_id,
        "data": orderbook_snapshot,
        "timestamp": datetime.utcnow().isoformat()
    }
    await manager.broadcast_to_channel("orderbook", message, market_id)


async def broadcast_trade(market_id: str, trade: dict):
    """Broadcast new trade to subscribers."""
    message = {
        "type": "trade",
        "market_id": market_id,
        "data": trade,
        "timestamp": datetime.utcnow().isoformat()
    }
    await manager.broadcast_to_channel("trades", message, market_id)


async def broadcast_ticker_update(market_id: str, ticker: dict):
    """Broadcast ticker update to subscribers."""
    message = {
        "type": "ticker",
        "market_id": market_id,
        "data": ticker,
        "timestamp": datetime.utcnow().isoformat()
    }
    await manager.broadcast_to_channel("ticker", message, market_id)


async def send_position_update(user_id: str, position: dict):
    """Send position update to user."""
    message = {
        "type": "position",
        "data": position,
        "timestamp": datetime.utcnow().isoformat()
    }
    # Would need to map user_id to client_id
    # For now, broadcast to all position subscribers
    await manager.broadcast_to_channel("positions", message)


async def send_order_update(user_id: str, order: dict):
    """Send order update to user."""
    message = {
        "type": "order",
        "data": order,
        "timestamp": datetime.utcnow().isoformat()
    }
    # Would need to map user_id to client_id
    # For now, broadcast to all order subscribers
    await manager.broadcast_to_channel("orders", message) 