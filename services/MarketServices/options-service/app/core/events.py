"""Event publishing for options service."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List
import logging
import json

logger = logging.getLogger(__name__)


class OptionsEventPublisher:
    """Publishes options-related events."""
    
    def __init__(self, settings):
        self.settings = settings
        self._events = []  # Simple event store for now
        
    async def publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish a generic event."""
        event = {
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        self._events.append(event)
        logger.info(f"Published event: {event_type}")
        # In production, would publish to Pulsar
        
    async def publish_option_created(self, option: Dict[str, Any]):
        """Publish option created event."""
        await self.publish_event("option.created", option)
        
    async def publish_order_placed(self, order: Any):
        """Publish order placed event."""
        order_data = {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side,
            "size": str(order.size),
            "price": str(order.price) if order.price else None,
            "order_type": order.order_type,
            "status": order.status
        }
        await self.publish_event("option.order.placed", order_data)
        
    async def publish_option_chain_created(
        self,
        underlying_asset: str,
        expiry_date: datetime,
        strikes: List[Decimal]
    ):
        """Publish option chain created event."""
        data = {
            "underlying_asset": underlying_asset,
            "expiry_date": expiry_date.isoformat(),
            "strikes": [str(s) for s in strikes],
            "num_contracts": len(strikes) * 2  # Calls and puts
        }
        await self.publish_event("option.chain.created", data)
        
    async def publish_volatility_surface_updated(
        self,
        underlying: str,
        surface_data: Dict[str, Any]
    ):
        """Publish volatility surface updated event."""
        data = {
            "underlying": underlying,
            "updated_at": datetime.utcnow().isoformat(),
            "atm_vol": surface_data.get("atm_vol"),
            "num_strikes": len(surface_data.get("strikes", []))
        }
        await self.publish_event("option.volatility.updated", data) 