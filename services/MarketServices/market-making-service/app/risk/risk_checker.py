"""Risk checker with direct communication for ultra-fast checks."""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime
import asyncio

from platformq_direct_comm import DirectCommunicator, MessageType

from ..config import Settings
from ..core.dependencies import ServiceClients

logger = logging.getLogger(__name__)


class RiskChecker:
    """Performs risk checks using direct communication when available."""
    
    def __init__(
        self,
        direct_communicator: Optional[DirectCommunicator],
        service_clients: ServiceClients,
        settings: Settings
    ):
        self.direct_comm = direct_communicator
        self.service_clients = service_clients
        self.settings = settings
        self.use_direct = settings.ENABLE_DIRECT_COMM and direct_communicator is not None
        
        # Cache for risk limits
        self._risk_limits_cache = {}
        self._cache_ttl = 300  # 5 minutes
        
        # Performance tracking
        self._stats = {
            "direct_checks": 0,
            "http_checks": 0,
            "direct_latency_us": 0,
            "http_latency_ms": 0,
            "cache_hits": 0
        }
    
    async def check_pre_trade_risk(
        self,
        user_id: str,
        new_position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform pre-trade risk check."""
        start_time = datetime.utcnow()
        
        if self.use_direct:
            # Use direct communication for ultra-low latency
            try:
                result = await self._direct_risk_check(user_id, new_position)
                self._stats["direct_checks"] += 1
                return result
            except Exception as e:
                logger.warning(f"Direct risk check failed, falling back to HTTP: {e}")
                # Fall back to HTTP
        
        # Use HTTP API
        result = await self._http_risk_check(user_id, new_position)
        self._stats["http_checks"] += 1
        
        # Track latency
        latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        self._stats["http_latency_ms"] = (self._stats["http_latency_ms"] + latency_ms) / 2
        
        return result
    
    async def check_margin(
        self,
        user_id: str,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check margin requirements."""
        if self.use_direct:
            try:
                # Direct margin check
                result = await self.direct_comm.send_direct(
                    target_service="risk-engine-001",
                    msg_type=MessageType.MARGIN_CHECK,
                    data={
                        "user_id": user_id,
                        "position": position,
                        "market_data": market_data
                    },
                    wait_response=True,
                    timeout_ms=self.settings.DIRECT_COMM_TIMEOUT_MS
                )
                
                if result and result.get("status") == "success":
                    return result
                    
            except Exception as e:
                logger.warning(f"Direct margin check failed: {e}")
        
        # Fall back to HTTP
        return await self.service_clients.call_risk_engine(
            "POST",
            f"/api/v1/margin/check",
            json={
                "user_id": user_id,
                "position": position,
                "market_data": market_data
            }
        )
    
    async def get_position_risk(
        self,
        position_id: str
    ) -> Dict[str, Any]:
        """Get risk metrics for a position."""
        if self.use_direct:
            try:
                # Direct position risk query
                result = await self.direct_comm.send_direct(
                    target_service="risk-engine-001",
                    msg_type=MessageType.POSITION_RISK,
                    data={"position_id": position_id},
                    wait_response=True,
                    timeout_ms=self.settings.DIRECT_COMM_TIMEOUT_MS
                )
                
                if result and result.get("status") == "success":
                    return result
                    
            except Exception as e:
                logger.warning(f"Direct position risk check failed: {e}")
        
        # Fall back to HTTP
        return await self.service_clients.call_risk_engine(
            "GET",
            f"/api/v1/risk/position/{position_id}"
        )
    
    async def check_strategy_risk(
        self,
        strategy_id: str,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Check risk for an entire strategy."""
        # Calculate aggregate metrics
        total_value = Decimal("0")
        total_risk = Decimal("0")
        max_leverage = Decimal("0")
        
        for position in positions:
            market_id = position.get("market_id")
            if market_id in market_data:
                price = Decimal(str(market_data[market_id].get("price", "0")))
                quantity = Decimal(str(position.get("quantity", "0")))
                value = abs(quantity * price)
                total_value += value
                
                # Simple risk calculation
                volatility = Decimal(str(market_data[market_id].get("volatility", "0.02")))
                position_risk = value * volatility
                total_risk += position_risk
                
                # Track leverage
                collateral = Decimal(str(position.get("collateral", "1")))
                leverage = value / collateral if collateral > 0 else Decimal("0")
                max_leverage = max(max_leverage, leverage)
        
        # Check against limits
        risk_ratio = total_risk / total_value if total_value > 0 else Decimal("0")
        
        return {
            "strategy_id": strategy_id,
            "total_value": str(total_value),
            "total_risk": str(total_risk),
            "risk_ratio": str(risk_ratio),
            "max_leverage": str(max_leverage),
            "risk_score": self._calculate_risk_score(risk_ratio, max_leverage),
            "approved": risk_ratio < Decimal("0.1") and max_leverage < self.settings.MAX_LEVERAGE
        }
    
    async def get_risk_limits(self, user_id: str) -> Dict[str, Any]:
        """Get risk limits for a user (cached)."""
        cache_key = f"limits_{user_id}"
        cached = self._risk_limits_cache.get(cache_key)
        
        if cached and (datetime.utcnow() - cached["timestamp"]).total_seconds() < self._cache_ttl:
            self._stats["cache_hits"] += 1
            return cached["data"]
        
        # Fetch from risk engine
        limits = await self.service_clients.call_risk_engine(
            "GET",
            f"/api/v1/limits/{user_id}"
        )
        
        # Cache the result
        self._risk_limits_cache[cache_key] = {
            "data": limits,
            "timestamp": datetime.utcnow()
        }
        
        return limits
    
    async def _direct_risk_check(
        self,
        user_id: str,
        new_position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform risk check using direct communication."""
        start_time = datetime.utcnow()
        
        # Send direct risk check request
        result = await self.direct_comm.send_direct(
            target_service="risk-engine-001",
            msg_type=MessageType.RISK_CHECK,
            data={
                "user_id": user_id,
                "check_type": "pre_trade",
                "new_position": new_position
            },
            wait_response=True,
            timeout_ms=self.settings.DIRECT_COMM_TIMEOUT_MS,
            priority=1  # High priority for pre-trade checks
        )
        
        if not result:
            raise Exception("No response from risk engine")
        
        if result.get("status") != "success":
            raise Exception(f"Risk check failed: {result.get('error', 'Unknown error')}")
        
        # Track latency
        latency_us = int((datetime.utcnow() - start_time).total_seconds() * 1_000_000)
        self._stats["direct_latency_us"] = (self._stats["direct_latency_us"] + latency_us) / 2
        
        # Add latency to result
        result["latency_us"] = latency_us
        
        return result
    
    async def _http_risk_check(
        self,
        user_id: str,
        new_position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform risk check using HTTP API."""
        return await self.service_clients.call_risk_engine(
            "POST",
            "/api/v1/risk/calculate",
            json={
                "position": new_position,
                "user_id": user_id
            }
        )
    
    def _calculate_risk_score(self, risk_ratio: Decimal, leverage: Decimal) -> int:
        """Calculate a simple risk score (0-100)."""
        score = 0
        
        # Risk ratio component (0-50)
        if risk_ratio > Decimal("0.2"):
            score += 50
        elif risk_ratio > Decimal("0.15"):
            score += 40
        elif risk_ratio > Decimal("0.1"):
            score += 30
        elif risk_ratio > Decimal("0.05"):
            score += 20
        else:
            score += 10
        
        # Leverage component (0-50)
        if leverage > Decimal("20"):
            score += 50
        elif leverage > Decimal("15"):
            score += 40
        elif leverage > Decimal("10"):
            score += 30
        elif leverage > Decimal("5"):
            score += 20
        else:
            score += 10
        
        return min(score, 100)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        stats = self._stats.copy()
        
        # Calculate success rate
        total_checks = stats["direct_checks"] + stats["http_checks"]
        if total_checks > 0:
            stats["direct_check_ratio"] = stats["direct_checks"] / total_checks
        else:
            stats["direct_check_ratio"] = 0
        
        # Add cache metrics
        stats["cache_size"] = len(self._risk_limits_cache)
        
        return stats 