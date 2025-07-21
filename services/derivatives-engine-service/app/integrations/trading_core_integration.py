"""
Trading Core Integration for Derivatives Engine

Integrates derivatives-engine-service with the unified trading-core-service
for order matching and compute market functionality.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from platformq_shared import ServiceClient
from app.models.market import MarketType

logger = logging.getLogger(__name__)


class TradingCoreIntegration:
    """Integration with unified trading-core-service"""
    
    def __init__(self):
        self.trading_core_client = ServiceClient(
            service_name="trading-core-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        # Cache of registered derivatives markets
        self.registered_markets: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize integration"""
        logger.info("Initializing trading-core integration")
        
        # Verify connectivity
        try:
            health = await self.trading_core_client.request(
                method="GET",
                path="/health"
            )
            logger.info(f"Trading-core service health: {health}")
        except Exception as e:
            logger.error(f"Failed to connect to trading-core service: {e}")
            raise
    
    async def register_derivatives_market(
        self,
        market_id: str,
        product_type: str,
        contract_specs: Dict[str, Any]
    ) -> bool:
        """Register a derivatives market with trading-core"""
        try:
            # Use the derivatives adapter endpoint
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/derivatives/register-market",
                json={
                    "market_id": market_id,
                    "product_type": product_type,
                    "contract_specs": contract_specs
                }
            )
            
            if result.get("success"):
                self.registered_markets[market_id] = {
                    "product_type": product_type,
                    "contract_specs": contract_specs,
                    "registered_at": datetime.utcnow()
                }
                
            return result.get("success", False)
            
        except Exception as e:
            logger.error(f"Failed to register derivatives market {market_id}: {e}")
            return False
    
    async def submit_order(
        self,
        order_data: Dict[str, Any],
        neuromorphic_hint: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit order through trading-core's derivatives adapter"""
        try:
            # Ensure market is registered
            market_id = order_data.get("market_id")
            if market_id not in self.registered_markets:
                logger.warning(f"Market {market_id} not registered, attempting registration")
                # Try to register based on order type
                await self.register_derivatives_market(
                    market_id=market_id,
                    product_type=order_data.get("product_type", "derivatives"),
                    contract_specs={}
                )
            
            # Submit through derivatives adapter
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/derivatives/submit-order",
                json={
                    "order_data": order_data,
                    "neuromorphic_hint": neuromorphic_hint
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to submit order: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_orderbook(
        self,
        market_id: str,
        depth: int = 20
    ) -> Optional[Dict[str, Any]]:
        """Get orderbook with derivatives enhancements"""
        try:
            result = await self.trading_core_client.request(
                method="GET",
                path="/internal/derivatives/orderbook",
                params={
                    "market_id": market_id,
                    "depth": depth
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get orderbook for {market_id}: {e}")
            return None
    
    async def trigger_settlement(
        self,
        market_id: str,
        settlement_price: Decimal
    ) -> bool:
        """Trigger settlement for expired contracts"""
        try:
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/derivatives/settlement",
                json={
                    "market_id": market_id,
                    "settlement_price": str(settlement_price)
                }
            )
            
            return result.get("success", False)
            
        except Exception as e:
            logger.error(f"Failed to trigger settlement for {market_id}: {e}")
            return False
    
    async def register_compute_market(
        self,
        resource_type: str,
        market_type: str,
        specifications: Dict[str, Any]
    ) -> Optional[str]:
        """Register a compute market through trading-core"""
        try:
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/compute/create-market",
                json={
                    "resource_type": resource_type,
                    "market_type": market_type,
                    "specifications": specifications
                }
            )
            
            return result.get("market_id")
            
        except Exception as e:
            logger.error(f"Failed to register compute market: {e}")
            return None
    
    async def submit_compute_order(
        self,
        user_id: str,
        resource_type: str,
        market_type: str,
        quantity: str,
        duration_hours: Optional[int] = None,
        specifications: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit compute order through trading-core"""
        try:
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/compute/submit-order",
                json={
                    "user_id": user_id,
                    "resource_type": resource_type,
                    "market_type": market_type,
                    "quantity": quantity,
                    "duration_hours": duration_hours,
                    "specifications": specifications
                },
                headers={
                    "X-User-ID": user_id
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to submit compute order: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def register_compute_provider(
        self,
        provider_id: str,
        resources: Dict[str, Dict[str, Any]]
    ) -> bool:
        """Register compute provider through trading-core"""
        try:
            result = await self.trading_core_client.request(
                method="POST",
                path="/internal/compute/register-provider",
                json={
                    "provider_id": provider_id,
                    "resources": resources
                }
            )
            
            return result.get("success", False)
            
        except Exception as e:
            logger.error(f"Failed to register compute provider: {e}")
            return False
    
    async def get_compute_metrics(self) -> Dict[str, Any]:
        """Get compute market metrics"""
        try:
            result = await self.trading_core_client.request(
                method="GET",
                path="/internal/compute/metrics"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get compute metrics: {e}")
            return {}
    
    async def get_matching_engine_metrics(self) -> Dict[str, Any]:
        """Get matching engine performance metrics"""
        try:
            result = await self.trading_core_client.request(
                method="GET",
                path="/api/v1/orders/metrics/summary"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get matching engine metrics: {e}")
            return {} 