"""Direct risk handler for ultra-low latency risk checks."""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime
import msgpack

from platformq_direct_comm import DirectCommunicator, MessageType

from ..core import RiskCalculator, MarginCalculator
from ..state import StateManager

logger = logging.getLogger(__name__)


class DirectRiskHandler:
    """Handles direct risk check requests via low-latency communication."""
    
    def __init__(
        self,
        communicator: DirectCommunicator,
        risk_calculator: RiskCalculator,
        margin_calculator: MarginCalculator,
        state_manager: StateManager
    ):
        self.communicator = communicator
        self.risk_calculator = risk_calculator
        self.margin_calculator = margin_calculator
        self.state_manager = state_manager
        
        # Cache for frequently accessed data
        self._position_cache = {}
        self._market_data_cache = {}
        self._cache_ttl = 1000  # 1 second in ms
    
    async def initialize(self):
        """Initialize direct risk handler."""
        # Register message handlers
        await self.communicator.register_handler(
            MessageType.RISK_CHECK,
            self._handle_risk_check
        )
        
        await self.communicator.register_handler(
            MessageType.MARGIN_CHECK,
            self._handle_margin_check
        )
        
        await self.communicator.register_handler(
            MessageType.POSITION_RISK,
            self._handle_position_risk
        )
        
        logger.info("Direct risk handler initialized")
    
    async def _handle_risk_check(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle real-time risk check request."""
        start_time = datetime.utcnow()
        
        try:
            # Extract request data
            user_id = message.get("user_id")
            position_id = message.get("position_id")
            check_type = message.get("check_type", "pre_trade")
            
            # For pre-trade checks
            if check_type == "pre_trade":
                new_position = message.get("new_position")
                if not new_position:
                    return {
                        "status": "error",
                        "error": "New position data required for pre-trade check"
                    }
                
                # Quick risk assessment
                result = await self._quick_risk_assessment(user_id, new_position)
                
            # For position updates
            elif check_type == "position_update":
                result = await self._position_risk_update(user_id, position_id)
                
            else:
                return {
                    "status": "error",
                    "error": f"Unknown check type: {check_type}"
                }
            
            # Calculate latency
            latency_us = int((datetime.utcnow() - start_time).total_seconds() * 1_000_000)
            result["latency_us"] = latency_us
            
            return result
            
        except Exception as e:
            logger.error(f"Risk check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _handle_margin_check(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle margin check request."""
        try:
            user_id = message.get("user_id")
            position_data = message.get("position")
            market_data = message.get("market_data")
            
            if not all([user_id, position_data, market_data]):
                return {
                    "status": "error",
                    "error": "Missing required data"
                }
            
            # Calculate margin requirements
            margin_result = await self.margin_calculator.calculate_margin(
                position=position_data,
                market_data=market_data
            )
            
            # Get user balance (cached)
            balance = await self._get_cached_balance(user_id)
            
            # Calculate available margin
            available_margin = balance - margin_result["initial_margin"]
            margin_sufficient = available_margin >= 0
            
            return {
                "status": "success",
                "margin_sufficient": margin_sufficient,
                "initial_margin": str(margin_result["initial_margin"]),
                "maintenance_margin": str(margin_result["maintenance_margin"]),
                "available_margin": str(available_margin),
                "margin_ratio": str(margin_result["margin_ratio"]),
                "liquidation_price": str(margin_result.get("liquidation_price", 0))
            }
            
        except Exception as e:
            logger.error(f"Margin check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _handle_position_risk(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle position risk calculation request."""
        try:
            position_id = message.get("position_id")
            
            # Get position from cache or state
            position = await self._get_cached_position(position_id)
            if not position:
                return {
                    "status": "error",
                    "error": "Position not found"
                }
            
            # Get market data
            market_id = position.get("market_id")
            market_data = await self._get_cached_market_data(market_id)
            
            # Calculate position risk
            position_risk = await self.risk_calculator.calculate_position_risk(
                position=position,
                market_data=market_data
            )
            
            return {
                "status": "success",
                "position_id": position_id,
                "notional_value": str(position_risk.notional_value),
                "unrealized_pnl": str(position_risk.unrealized_pnl),
                "margin_ratio": str(position_risk.margin_ratio),
                "var_1d": str(position_risk.var_1d),
                "leverage": str(position_risk.leverage),
                "risk_score": position_risk.risk_score,
                "alerts": [
                    {
                        "type": alert.alert_type,
                        "severity": alert.severity,
                        "message": alert.message
                    }
                    for alert in position_risk.alerts
                ] if hasattr(position_risk, 'alerts') else []
            }
            
        except Exception as e:
            logger.error(f"Position risk calculation failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _quick_risk_assessment(self, user_id: str, new_position: Dict[str, Any]) -> Dict[str, Any]:
        """Perform quick pre-trade risk assessment."""
        # Get current positions
        positions = await self.state_manager.get_user_positions(user_id)
        
        # Get market data
        market_id = new_position.get("market_id")
        market_data = await self._get_cached_market_data(market_id)
        
        if not market_data:
            return {
                "status": "error",
                "error": "Market data not available"
            }
        
        # Quick checks
        checks = {
            "position_limit": await self._check_position_limit(user_id, positions),
            "leverage_limit": await self._check_leverage_limit(user_id, new_position, market_data),
            "concentration_limit": await self._check_concentration_limit(user_id, new_position, positions),
            "margin_available": await self._check_margin_available(user_id, new_position, market_data)
        }
        
        # All checks must pass
        all_passed = all(check["passed"] for check in checks.values())
        
        return {
            "status": "success",
            "approved": all_passed,
            "checks": checks,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def _position_risk_update(self, user_id: str, position_id: str) -> Dict[str, Any]:
        """Update risk metrics for a position."""
        # Get position
        position = await self._get_cached_position(position_id)
        if not position:
            return {
                "status": "error",
                "error": "Position not found"
            }
        
        # Verify ownership
        if position.get("user_id") != user_id:
            return {
                "status": "error",
                "error": "Unauthorized"
            }
        
        # Get market data
        market_id = position.get("market_id")
        market_data = await self._get_cached_market_data(market_id)
        
        # Calculate updated risk
        position_risk = await self.risk_calculator.calculate_position_risk(
            position=position,
            market_data=market_data
        )
        
        # Check for alerts
        alerts = []
        if position_risk.margin_ratio < Decimal("1.5"):
            alerts.append({
                "type": "margin_warning",
                "severity": "high",
                "message": f"Margin ratio low: {position_risk.margin_ratio}"
            })
        
        if position_risk.leverage > Decimal("15"):
            alerts.append({
                "type": "leverage_warning",
                "severity": "medium",
                "message": f"High leverage: {position_risk.leverage}x"
            })
        
        return {
            "status": "success",
            "position_id": position_id,
            "risk_metrics": {
                "margin_ratio": str(position_risk.margin_ratio),
                "leverage": str(position_risk.leverage),
                "var_1d": str(position_risk.var_1d),
                "unrealized_pnl": str(position_risk.unrealized_pnl),
                "risk_score": position_risk.risk_score
            },
            "alerts": alerts
        }
    
    # Cache management
    async def _get_cached_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position from cache or fetch from state."""
        cache_key = f"pos_{position_id}"
        cached = self._position_cache.get(cache_key)
        
        if cached and (datetime.utcnow() - cached["timestamp"]).total_seconds() * 1000 < self._cache_ttl:
            return cached["data"]
        
        # Fetch from state
        position = await self.state_manager.get_position(position_id)
        if position:
            self._position_cache[cache_key] = {
                "data": position,
                "timestamp": datetime.utcnow()
            }
        
        return position
    
    async def _get_cached_market_data(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get market data from cache or fetch from state."""
        cache_key = f"mkt_{market_id}"
        cached = self._market_data_cache.get(cache_key)
        
        if cached and (datetime.utcnow() - cached["timestamp"]).total_seconds() * 1000 < self._cache_ttl:
            return cached["data"]
        
        # Fetch from state
        market_data = await self.state_manager.get_market_data(market_id)
        if market_data:
            self._market_data_cache[cache_key] = {
                "data": market_data,
                "timestamp": datetime.utcnow()
            }
        
        return market_data
    
    async def _get_cached_balance(self, user_id: str) -> Decimal:
        """Get cached user balance."""
        # In production, implement proper caching
        return await self.state_manager.get_user_balance(user_id)
    
    # Risk checks
    async def _check_position_limit(self, user_id: str, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check position count limit."""
        limits = await self.state_manager.get_risk_limits(user_id)
        max_positions = limits.get("max_open_positions", 50) if limits else 50
        
        current_count = len(positions)
        passed = current_count < max_positions
        
        return {
            "passed": passed,
            "current": current_count,
            "limit": max_positions,
            "message": f"Position count: {current_count}/{max_positions}"
        }
    
    async def _check_leverage_limit(self, user_id: str, new_position: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check leverage limit."""
        limits = await self.state_manager.get_risk_limits(user_id)
        max_leverage = limits.get("max_leverage", Decimal("10")) if limits else Decimal("10")
        
        # Calculate position leverage
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = Decimal(str(new_position.get("quantity", "0")))
        collateral = Decimal(str(new_position.get("collateral", "1")))
        
        notional = abs(quantity) * mark_price
        leverage = notional / collateral if collateral > 0 else Decimal("999")
        
        passed = leverage <= max_leverage
        
        return {
            "passed": passed,
            "current": str(leverage),
            "limit": str(max_leverage),
            "message": f"Leverage: {leverage:.1f}x/{max_leverage}x"
        }
    
    async def _check_concentration_limit(self, user_id: str, new_position: Dict[str, Any], positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check concentration limit."""
        limits = await self.state_manager.get_risk_limits(user_id)
        max_concentration = limits.get("max_concentration", Decimal("0.3")) if limits else Decimal("0.3")
        
        # Calculate concentration with new position
        market_values = {}
        total_value = Decimal("0")
        
        # Add existing positions
        for pos in positions:
            market_id = pos.get("market_id")
            value = Decimal(str(pos.get("notional_value", "0")))
            market_values[market_id] = market_values.get(market_id, Decimal("0")) + value
            total_value += value
        
        # Add new position
        new_market_id = new_position.get("market_id")
        new_value = Decimal(str(new_position.get("notional_value", "0")))
        market_values[new_market_id] = market_values.get(new_market_id, Decimal("0")) + new_value
        total_value += new_value
        
        # Calculate max concentration
        if total_value > 0:
            max_market_concentration = max(v / total_value for v in market_values.values())
        else:
            max_market_concentration = Decimal("0")
        
        passed = max_market_concentration <= max_concentration
        
        return {
            "passed": passed,
            "current": str(max_market_concentration),
            "limit": str(max_concentration),
            "message": f"Max concentration: {max_market_concentration:.1%}/{max_concentration:.0%}"
        }
    
    async def _check_margin_available(self, user_id: str, new_position: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if sufficient margin is available."""
        # Calculate margin requirement for new position
        margin_req = await self.margin_calculator.calculate_margin(
            position=new_position,
            market_data=market_data
        )
        
        # Get available margin
        balance = await self._get_cached_balance(user_id)
        
        # Get current margin usage
        positions = await self.state_manager.get_user_positions(user_id)
        current_margin = Decimal("0")
        
        for pos in positions:
            # Simplified - in production would calculate actual margin
            current_margin += Decimal(str(pos.get("initial_margin", "0")))
        
        available = balance - current_margin
        required = margin_req["initial_margin"]
        passed = available >= required
        
        return {
            "passed": passed,
            "available": str(available),
            "required": str(required),
            "message": f"Margin available: ${available:.2f} (need ${required:.2f})"
        } 