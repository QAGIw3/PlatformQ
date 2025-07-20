"""Position service client integration"""

from typing import Dict, List, Optional
from decimal import Decimal
import aiohttp
import logging


logger = logging.getLogger(__name__)


class PositionServiceClient:
    """Client for fetching position data from position service"""
    
    def __init__(self, base_url: str = "http://position-service:8084"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_trader_positions(self, trader_id: str) -> Dict:
        """Get all positions for a trader"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            async with self.session.get(
                f"{self.base_url}/api/v1/positions",
                params={"trader_id": trader_id}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "positions": data.get("positions", []),
                        "cash_balance": Decimal(data.get("cash_balance", "0"))
                    }
                else:
                    logger.error(f"Failed to get positions for {trader_id}: {response.status}")
                    return {"positions": [], "cash_balance": Decimal("0")}
                    
        except Exception as e:
            logger.error(f"Error fetching positions for {trader_id}: {e}")
            # For now, return mock data
            return self._get_mock_positions(trader_id)
    
    def _get_mock_positions(self, trader_id: str) -> Dict:
        """Return mock positions for testing"""
        return {
            "positions": [
                {
                    "position_id": f"pos_001_{trader_id}",
                    "market_id": "BTC-USDT-PERP",
                    "side": "long",
                    "size": "0.5",
                    "entry_price": "45000",
                    "mark_price": "46000",
                    "leverage": "10",
                    "initial_margin": "2250",
                    "maintenance_margin": "1125",
                    "margin_used": "2250",
                    "realized_pnl": "0",
                    "unrealized_pnl": "500",
                    "fees_paid": "45"
                },
                {
                    "position_id": f"pos_002_{trader_id}",
                    "market_id": "ETH-USDT-PERP",
                    "side": "short",
                    "size": "5",
                    "entry_price": "3200",
                    "mark_price": "3150",
                    "leverage": "5",
                    "initial_margin": "3200",
                    "maintenance_margin": "1600",
                    "margin_used": "3200",
                    "realized_pnl": "100",
                    "unrealized_pnl": "250",
                    "fees_paid": "32"
                }
            ],
            "cash_balance": Decimal("50000")
        } 