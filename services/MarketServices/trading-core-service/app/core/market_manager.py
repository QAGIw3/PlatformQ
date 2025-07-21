"""Market manager for managing market configurations and status."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any

from ..models.market import Market, MarketStatus, MarketType, ProductType, MarketStats
from ..state import IgniteStateManager, CacheType
from ..events import FlinkEventProcessor, MarketEvent, EventType


logger = logging.getLogger(__name__)


class MarketManager:
    """Manages market configurations and lifecycle."""
    
    def __init__(
        self,
        state_manager: IgniteStateManager,
        event_processor: FlinkEventProcessor
    ):
        self.state_manager = state_manager
        self.event_processor = event_processor
        
        # Market configuration validators by product type
        self.validators: Dict[str, Any] = {}
        
        # Circuit breaker state
        self.circuit_breakers: Dict[str, Dict[str, Any]] = {}
        
        # Metrics
        self.metrics = {
            "markets_created": 0,
            "markets_opened": 0,
            "markets_closed": 0,
            "circuit_breakers_triggered": 0
        }
    
    async def create_market(self, market: Market) -> Market:
        """Create a new market."""
        # Validate market configuration
        validation_result = await self._validate_market_config(market)
        if not validation_result["valid"]:
            raise ValueError(f"Invalid market configuration: {validation_result['reason']}")
        
        # Check for duplicate
        existing = await self.state_manager.get_market(market.market_id)
        if existing:
            raise ValueError(f"Market {market.market_id} already exists")
        
        # Set creation timestamp
        market.created_at = datetime.utcnow()
        market.updated_at = market.created_at
        
        # Store market
        await self.state_manager.put_market(market.market_id, market.dict())
        
        # Publish market creation event
        await self._publish_market_event(market, "created")
        
        self.metrics["markets_created"] += 1
        logger.info(f"Created market {market.market_id}")
        
        return market
    
    async def update_market(
        self,
        market_id: str,
        updates: Dict[str, Any]
    ) -> Market:
        """Update market configuration."""
        # Get existing market
        market_data = await self.state_manager.get_market(market_id)
        if not market_data:
            raise ValueError(f"Market {market_id} not found")
        
        market = Market(**market_data)
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(market, key):
                setattr(market, key, value)
        
        market.updated_at = datetime.utcnow()
        
        # Validate updated configuration
        validation_result = await self._validate_market_config(market)
        if not validation_result["valid"]:
            raise ValueError(f"Invalid market update: {validation_result['reason']}")
        
        # Store updated market
        await self.state_manager.put_market(market.market_id, market.dict())
        
        # Publish update event
        await self._publish_market_event(market, "updated", updates)
        
        return market
    
    async def open_market(self, market_id: str) -> bool:
        """Open a market for trading."""
        market_data = await self.state_manager.get_market(market_id)
        if not market_data:
            return False
        
        market = Market(**market_data)
        
        if market.status == MarketStatus.OPEN:
            return True
        
        # Check if market can be opened
        if market.status == MarketStatus.HALTED:
            logger.warning(f"Cannot open halted market {market_id}")
            return False
        
        # Update status
        market.status = MarketStatus.OPEN
        market.updated_at = datetime.utcnow()
        
        # Clear circuit breaker if any
        if market_id in self.circuit_breakers:
            del self.circuit_breakers[market_id]
        
        # Store updated market
        await self.state_manager.put_market(market.market_id, market.dict())
        
        # Publish status change event
        await self._publish_market_event(market, "status_change", {"status": "open"})
        
        self.metrics["markets_opened"] += 1
        logger.info(f"Opened market {market_id}")
        
        return True
    
    async def close_market(self, market_id: str) -> bool:
        """Close a market."""
        market_data = await self.state_manager.get_market(market_id)
        if not market_data:
            return False
        
        market = Market(**market_data)
        
        if market.status == MarketStatus.CLOSED:
            return True
        
        # Update status
        market.status = MarketStatus.CLOSED
        market.updated_at = datetime.utcnow()
        
        # Store updated market
        await self.state_manager.put_market(market.market_id, market.dict())
        
        # Publish status change event
        await self._publish_market_event(market, "status_change", {"status": "closed"})
        
        self.metrics["markets_closed"] += 1
        logger.info(f"Closed market {market_id}")
        
        return True
    
    async def halt_market(
        self,
        market_id: str,
        reason: str,
        duration_seconds: Optional[int] = None
    ) -> bool:
        """Halt trading in a market."""
        market_data = await self.state_manager.get_market(market_id)
        if not market_data:
            return False
        
        market = Market(**market_data)
        
        # Update status
        market.status = MarketStatus.HALTED
        market.updated_at = datetime.utcnow()
        
        # Store updated market
        await self.state_manager.put_market(market.market_id, market.dict())
        
        # Publish halt event
        await self._publish_market_event(
            market,
            "halted",
            {
                "reason": reason,
                "duration": duration_seconds
            }
        )
        
        logger.warning(f"Halted market {market_id}: {reason}")
        
        return True
    
    async def trigger_circuit_breaker(
        self,
        market_id: str,
        trigger_type: str,
        trigger_value: Any
    ) -> bool:
        """Trigger circuit breaker for a market."""
        market_data = await self.state_manager.get_market(market_id)
        if not market_data:
            return False
        
        market = Market(**market_data)
        
        if not market.circuit_breaker_enabled:
            return False
        
        # Record circuit breaker trigger
        self.circuit_breakers[market_id] = {
            "triggered_at": datetime.utcnow(),
            "trigger_type": trigger_type,
            "trigger_value": trigger_value,
            "duration": market_data.get("circuit_breaker_duration_seconds", 300)
        }
        
        # Halt market
        await self.halt_market(
            market_id,
            f"Circuit breaker triggered: {trigger_type}",
            market_data.get("circuit_breaker_duration_seconds", 300)
        )
        
        self.metrics["circuit_breakers_triggered"] += 1
        
        return True
    
    async def get_market(self, market_id: str) -> Optional[Market]:
        """Get market configuration."""
        market_data = await self.state_manager.get_market(market_id)
        if market_data:
            return Market(**market_data)
        return None
    
    async def list_markets(
        self,
        market_type: Optional[MarketType] = None,
        product_type: Optional[ProductType] = None,
        status: Optional[MarketStatus] = None,
        active_only: bool = True
    ) -> List[Market]:
        """List markets based on criteria."""
        # Get all active markets
        markets_data = await self.state_manager.get_active_markets()
        
        markets = [Market(**data) for data in markets_data]
        
        # Apply filters
        if market_type:
            markets = [m for m in markets if m.market_type == market_type]
        
        if product_type:
            markets = [m for m in markets if m.product_type == product_type]
        
        if status:
            markets = [m for m in markets if m.status == status]
        
        if active_only:
            markets = [m for m in markets if m.is_active]
        
        return markets
    
    async def update_market_stats(
        self,
        market_id: str,
        stats: MarketStats
    ):
        """Update market statistics."""
        # Store in cache with TTL
        # This would be stored separately from market config
        logger.info(f"Updated stats for market {market_id}")
    
    async def check_circuit_breakers(self):
        """Check and reset expired circuit breakers."""
        current_time = datetime.utcnow()
        expired_breakers = []
        
        for market_id, breaker_info in self.circuit_breakers.items():
            triggered_at = breaker_info["triggered_at"]
            duration = breaker_info["duration"]
            
            if (current_time - triggered_at).total_seconds() >= duration:
                expired_breakers.append(market_id)
        
        # Reset expired circuit breakers
        for market_id in expired_breakers:
            del self.circuit_breakers[market_id]
            await self.open_market(market_id)
            logger.info(f"Circuit breaker reset for market {market_id}")
    
    async def _validate_market_config(self, market: Market) -> Dict[str, Any]:
        """Validate market configuration."""
        # Basic validations
        if market.tick_size <= 0:
            return {"valid": False, "reason": "Invalid tick size"}
        
        if market.lot_size <= 0:
            return {"valid": False, "reason": "Invalid lot size"}
        
        if market.min_notional <= 0:
            return {"valid": False, "reason": "Invalid minimum notional"}
        
        if market.initial_margin_rate >= 1 or market.initial_margin_rate <= 0:
            return {"valid": False, "reason": "Invalid initial margin rate"}
        
        if market.maintenance_margin_rate >= market.initial_margin_rate:
            return {"valid": False, "reason": "Maintenance margin must be less than initial margin"}
        
        # Product-specific validation
        if market.product_type.value in self.validators:
            validator = self.validators[market.product_type.value]
            result = await validator(market)
            if not result["valid"]:
                return result
        
        return {"valid": True, "reason": None}
    
    async def _publish_market_event(
        self,
        market: Market,
        update_type: str,
        update_data: Optional[Dict[str, Any]] = None
    ):
        """Publish market event."""
        event = MarketEvent(
            event_id=f"{market.market_id}:{update_type}",
            event_type=EventType.MARKET_UPDATE,
            market_id=market.market_id,
            update_type=update_type,
            market_data={
                "market": market.dict(),
                "update": update_data or {}
            }
        )
        
        # Would publish to Pulsar
        # await self.event_processor.publish(event)
    
    def register_validator(self, product_type: str, validator: Any):
        """Register a product-specific market validator."""
        self.validators[product_type] = validator
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get market manager metrics."""
        return {
            **self.metrics,
            "active_circuit_breakers": len(self.circuit_breakers)
        } 