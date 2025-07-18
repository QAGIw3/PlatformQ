from fastapi import WebSocket
from typing import Dict, Set
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

class MarketDataWebSocket:
    """
    Manages WebSocket connections for real-time market data streaming
    """
    def __init__(self, ignite_cache, pulsar_publisher):
        self.ignite = ignite_cache
        self.pulsar = pulsar_publisher
        self.connections: Dict[str, Set[WebSocket]] = {}
        self._running = False
        
    async def connect(self, websocket: WebSocket, market_id: str):
        """Connect a WebSocket to a market data stream"""
        await websocket.accept()
        if market_id not in self.connections:
            self.connections[market_id] = set()
        self.connections[market_id].add(websocket)
        logger.info(f"WebSocket connected for market {market_id}")
        
    def disconnect(self, websocket: WebSocket, market_id: str):
        """Disconnect a WebSocket from market data stream"""
        if market_id in self.connections:
            self.connections[market_id].discard(websocket)
            if not self.connections[market_id]:
                del self.connections[market_id]
        logger.info(f"WebSocket disconnected from market {market_id}")
        
    async def handle_message(self, websocket: WebSocket, message: str):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            # Handle subscription changes or other commands
            if data.get("action") == "subscribe":
                market_id = data.get("market_id")
                if market_id:
                    await self.connect(websocket, market_id)
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
            
    async def broadcast_market_data(self, market_id: str, data: dict):
        """Broadcast market data to all connected clients for a market"""
        if market_id in self.connections:
            disconnected = set()
            for websocket in self.connections[market_id]:
                try:
                    await websocket.send_json(data)
                except Exception:
                    disconnected.add(websocket)
            
            # Clean up disconnected websockets
            for ws in disconnected:
                self.disconnect(ws, market_id)
                
    async def start_broadcasting(self):
        """Start the background task for broadcasting market updates"""
        self._running = True
        while self._running:
            try:
                # Subscribe to market data updates from Pulsar
                # This is a simplified implementation
                await asyncio.sleep(0.1)  # Prevent busy loop
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")
                await asyncio.sleep(1)
                
    async def stop(self):
        """Stop the broadcasting loop"""
        self._running = False 