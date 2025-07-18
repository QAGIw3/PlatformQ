"""
Risk Limits System

Implements pre-trade and post-trade risk limits with real-time monitoring.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import logging

from app.integrations import IgniteCache, PulsarEventPublisher
from app.models.position import Position
from app.models.order import Order

logger = logging.getLogger(__name__)


class LimitType(Enum):
    """Types of risk limits"""
    POSITION_SIZE = "position_size"
    NOTIONAL_EXPOSURE = "notional_exposure"
    DELTA_LIMIT = "delta_limit"
    GAMMA_LIMIT = "gamma_limit"
    VEGA_LIMIT = "vega_limit"
    VAR_LIMIT = "var_limit"
    LOSS_LIMIT = "loss_limit"
    CONCENTRATION = "concentration"
    LEVERAGE = "leverage"
    MARGIN_USAGE = "margin_usage"


class LimitAction(Enum):
    """Actions when limit is breached"""
    BLOCK = "block"  # Block new trades
    WARN = "warn"  # Allow but warn
    REDUCE_ONLY = "reduce_only"  # Only allow reducing positions
    LIQUIDATE = "liquidate"  # Force liquidation


@dataclass
class RiskLimit:
    """Individual risk limit configuration"""
    limit_id: str
    limit_type: LimitType
    limit_value: Decimal
    current_value: Decimal = Decimal("0")
    
    # Thresholds
    warning_threshold: Decimal = Decimal("0.8")  # 80% of limit
    critical_threshold: Decimal = Decimal("0.95")  # 95% of limit
    
    # Actions
    warning_action: LimitAction = LimitAction.WARN
    breach_action: LimitAction = LimitAction.BLOCK
    
    # Scope
    scope: str = "user"  # user, account, market, global
    scope_id: Optional[str] = None
    
    # Status
    is_active: bool = True
    last_checked: datetime = field(default_factory=datetime.utcnow)
    breach_count: int = 0
    
    def utilization_percent(self) -> Decimal:
        """Calculate limit utilization percentage"""
        if self.limit_value == 0:
            return Decimal("0")
        return (self.current_value / self.limit_value) * 100
        
    def is_breached(self) -> bool:
        """Check if limit is breached"""
        return self.current_value >= self.limit_value
        
    def is_warning(self) -> bool:
        """Check if at warning level"""
        return self.current_value >= (self.limit_value * self.warning_threshold)
        
    def is_critical(self) -> bool:
        """Check if at critical level"""
        return self.current_value >= (self.limit_value * self.critical_threshold)


@dataclass
class UserRiskLimits:
    """Risk limits for a user"""
    user_id: str
    tier: str = "retail"  # retail, professional, institutional
    
    # Position limits
    max_positions: int = 50
    max_position_size: Dict[str, Decimal] = field(default_factory=dict)
    
    # Exposure limits
    max_notional_exposure: Decimal = Decimal("1000000")  # $1M
    max_leverage: Decimal = Decimal("10")  # 10x
    
    # Greeks limits
    max_delta: Decimal = Decimal("100000")
    max_gamma: Decimal = Decimal("10000")
    max_vega: Decimal = Decimal("50000")
    
    # Loss limits
    daily_loss_limit: Decimal = Decimal("50000")
    weekly_loss_limit: Decimal = Decimal("100000")
    monthly_loss_limit: Decimal = Decimal("200000")
    
    # VaR limit
    var_limit_95: Decimal = Decimal("100000")
    var_limit_99: Decimal = Decimal("200000")
    
    # Concentration limits
    max_concentration_percent: Decimal = Decimal("0.3")  # 30% in single market
    
    # Active limits
    active_limits: Dict[str, RiskLimit] = field(default_factory=dict)


class RiskLimitsEngine:
    """
    Main risk limits engine
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        
        # User limits
        self.user_limits: Dict[str, UserRiskLimits] = {}
        
        # Global limits
        self.global_limits: Dict[str, RiskLimit] = {}
        
        # Breach tracking
        self.active_breaches: Set[str] = set()
        self.breach_history: List[Dict] = []
        
        # Default tiers
        self.tier_defaults = {
            "retail": {
                "max_notional_exposure": Decimal("1000000"),
                "max_leverage": Decimal("10"),
                "daily_loss_limit": Decimal("50000")
            },
            "professional": {
                "max_notional_exposure": Decimal("10000000"),
                "max_leverage": Decimal("20"),
                "daily_loss_limit": Decimal("200000")
            },
            "institutional": {
                "max_notional_exposure": Decimal("100000000"),
                "max_leverage": Decimal("50"),
                "daily_loss_limit": Decimal("1000000")
            }
        }
        
    async def start(self):
        """Start risk limits monitoring"""
        asyncio.create_task(self._monitoring_loop())
        asyncio.create_task(self._limit_reset_loop())
        logger.info("Risk limits engine started")
        
    async def check_pre_trade_limits(
        self,
        user_id: str,
        order: Order,
        current_positions: List[Position]
    ) -> Tuple[bool, List[str]]:
        """
        Check if order passes pre-trade risk limits
        Returns (is_allowed, reasons)
        """
        violations = []
        user_limits = await self._get_user_limits(user_id)
        
        # 1. Check position count limit
        if len(current_positions) >= user_limits.max_positions:
            if not self._is_reducing_position(order, current_positions):
                violations.append(f"Maximum positions limit reached: {user_limits.max_positions}")
                
        # 2. Check position size limit
        position_size_limit = user_limits.max_position_size.get(
            order.market_id,
            Decimal("100000")  # Default
        )
        if order.size > position_size_limit:
            violations.append(f"Order size {order.size} exceeds limit {position_size_limit}")
            
        # 3. Check notional exposure
        current_exposure = await self._calculate_total_exposure(current_positions)
        order_notional = order.size * order.price if order.price else order.size * Decimal("100")
        
        if current_exposure + order_notional > user_limits.max_notional_exposure:
            violations.append(f"Would exceed notional exposure limit: {user_limits.max_notional_exposure}")
            
        # 4. Check leverage
        collateral = await self._get_user_collateral(user_id)
        if collateral > 0:
            new_leverage = (current_exposure + order_notional) / collateral
            if new_leverage > user_limits.max_leverage:
                violations.append(f"Would exceed leverage limit: {user_limits.max_leverage}x")
                
        # 5. Check Greeks limits (for options)
        if await self._is_options_order(order):
            greeks_check = await self._check_greeks_limits(user_id, order, current_positions)
            violations.extend(greeks_check)
            
        # 6. Check loss limits
        loss_check = await self._check_loss_limits(user_id)
        violations.extend(loss_check)
        
        # 7. Check concentration limits
        concentration_check = await self._check_concentration_limits(
            user_id, order, current_positions
        )
        violations.extend(concentration_check)
        
        # Record check
        await self._record_limit_check(user_id, order, violations)
        
        return (len(violations) == 0, violations)
        
    async def update_position_limits(
        self,
        user_id: str,
        positions: List[Position]
    ):
        """Update current values for position-based limits"""
        user_limits = await self._get_user_limits(user_id)
        
        # Update exposure
        total_exposure = await self._calculate_total_exposure(positions)
        self._update_limit(user_limits, LimitType.NOTIONAL_EXPOSURE, total_exposure)
        
        # Update leverage
        collateral = await self._get_user_collateral(user_id)
        if collateral > 0:
            leverage = total_exposure / collateral
            self._update_limit(user_limits, LimitType.LEVERAGE, leverage)
            
        # Update Greeks
        total_greeks = await self._calculate_portfolio_greeks(positions)
        self._update_limit(user_limits, LimitType.DELTA_LIMIT, abs(total_greeks["delta"]))
        self._update_limit(user_limits, LimitType.GAMMA_LIMIT, abs(total_greeks["gamma"]))
        self._update_limit(user_limits, LimitType.VEGA_LIMIT, abs(total_greeks["vega"]))
        
        # Check for breaches
        await self._check_and_handle_breaches(user_id, user_limits)
        
    async def set_user_limits(
        self,
        user_id: str,
        tier: str,
        custom_limits: Optional[Dict[str, Any]] = None
    ) -> UserRiskLimits:
        """Set risk limits for a user"""
        # Start with tier defaults
        defaults = self.tier_defaults.get(tier, self.tier_defaults["retail"])
        
        user_limits = UserRiskLimits(
            user_id=user_id,
            tier=tier,
            max_notional_exposure=defaults["max_notional_exposure"],
            max_leverage=defaults["max_leverage"],
            daily_loss_limit=defaults["daily_loss_limit"]
        )
        
        # Apply custom limits
        if custom_limits:
            for key, value in custom_limits.items():
                if hasattr(user_limits, key):
                    setattr(user_limits, key, value)
                    
        # Create limit objects
        self._create_limit_objects(user_limits)
        
        # Store
        self.user_limits[user_id] = user_limits
        await self.ignite.set(f"risk_limits:{user_id}", user_limits)
        
        return user_limits
        
    async def override_limit(
        self,
        user_id: str,
        limit_type: LimitType,
        new_value: Decimal,
        reason: str,
        approved_by: str
    ):
        """Override a specific limit temporarily"""
        user_limits = await self._get_user_limits(user_id)
        
        if limit_type.value in user_limits.active_limits:
            limit = user_limits.active_limits[limit_type.value]
            old_value = limit.limit_value
            limit.limit_value = new_value
            
            # Record override
            await self.pulsar.publish(
                "risk-limit-override",
                {
                    "user_id": user_id,
                    "limit_type": limit_type.value,
                    "old_value": str(old_value),
                    "new_value": str(new_value),
                    "reason": reason,
                    "approved_by": approved_by,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def get_limit_utilization(self, user_id: str) -> Dict[str, Any]:
        """Get current limit utilization for a user"""
        user_limits = await self._get_user_limits(user_id)
        
        utilization = {}
        for limit_type, limit in user_limits.active_limits.items():
            utilization[limit_type] = {
                "current_value": str(limit.current_value),
                "limit_value": str(limit.limit_value),
                "utilization_percent": f"{limit.utilization_percent():.1f}%",
                "status": self._get_limit_status(limit),
                "is_breached": limit.is_breached()
            }
            
        return {
            "user_id": user_id,
            "tier": user_limits.tier,
            "limits": utilization,
            "active_breaches": list(self.active_breaches),
            "last_updated": datetime.utcnow().isoformat()
        }
        
    async def _monitoring_loop(self):
        """Continuously monitor risk limits"""
        while True:
            try:
                # Check all users
                for user_id, user_limits in self.user_limits.items():
                    # Update real-time metrics
                    positions = await self._get_user_positions(user_id)
                    await self.update_position_limits(user_id, positions)
                    
                    # Check VaR limits
                    await self._check_var_limits(user_id, positions)
                    
            except Exception as e:
                logger.error(f"Error in risk monitoring: {e}")
                
            await asyncio.sleep(10)  # Check every 10 seconds
            
    async def _limit_reset_loop(self):
        """Reset time-based limits (daily, weekly, monthly)"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                # Daily reset at midnight UTC
                if current_time.hour == 0 and current_time.minute < 1:
                    await self._reset_daily_limits()
                    
                # Weekly reset on Monday
                if current_time.weekday() == 0 and current_time.hour == 0:
                    await self._reset_weekly_limits()
                    
                # Monthly reset on 1st
                if current_time.day == 1 and current_time.hour == 0:
                    await self._reset_monthly_limits()
                    
            except Exception as e:
                logger.error(f"Error in limit reset: {e}")
                
            await asyncio.sleep(60)  # Check every minute
            
    async def _get_user_limits(self, user_id: str) -> UserRiskLimits:
        """Get user limits, creating defaults if needed"""
        if user_id not in self.user_limits:
            # Try to load from cache
            cached = await self.ignite.get(f"risk_limits:{user_id}")
            if cached:
                self.user_limits[user_id] = cached
            else:
                # Create default retail limits
                await self.set_user_limits(user_id, "retail")
                
        return self.user_limits[user_id]
        
    def _create_limit_objects(self, user_limits: UserRiskLimits):
        """Create RiskLimit objects for UserRiskLimits"""
        # Notional exposure limit
        user_limits.active_limits[LimitType.NOTIONAL_EXPOSURE.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_notional",
            limit_type=LimitType.NOTIONAL_EXPOSURE,
            limit_value=user_limits.max_notional_exposure,
            breach_action=LimitAction.BLOCK
        )
        
        # Leverage limit
        user_limits.active_limits[LimitType.LEVERAGE.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_leverage",
            limit_type=LimitType.LEVERAGE,
            limit_value=user_limits.max_leverage,
            breach_action=LimitAction.REDUCE_ONLY
        )
        
        # Greeks limits
        user_limits.active_limits[LimitType.DELTA_LIMIT.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_delta",
            limit_type=LimitType.DELTA_LIMIT,
            limit_value=user_limits.max_delta,
            breach_action=LimitAction.WARN
        )
        
        user_limits.active_limits[LimitType.GAMMA_LIMIT.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_gamma",
            limit_type=LimitType.GAMMA_LIMIT,
            limit_value=user_limits.max_gamma,
            breach_action=LimitAction.BLOCK
        )
        
        user_limits.active_limits[LimitType.VEGA_LIMIT.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_vega",
            limit_type=LimitType.VEGA_LIMIT,
            limit_value=user_limits.max_vega,
            breach_action=LimitAction.WARN
        )
        
        # Loss limits
        user_limits.active_limits[LimitType.LOSS_LIMIT.value] = RiskLimit(
            limit_id=f"{user_limits.user_id}_daily_loss",
            limit_type=LimitType.LOSS_LIMIT,
            limit_value=user_limits.daily_loss_limit,
            breach_action=LimitAction.BLOCK
        )
        
    def _update_limit(
        self,
        user_limits: UserRiskLimits,
        limit_type: LimitType,
        new_value: Decimal
    ):
        """Update current value for a limit"""
        if limit_type.value in user_limits.active_limits:
            limit = user_limits.active_limits[limit_type.value]
            limit.current_value = new_value
            limit.last_checked = datetime.utcnow()
            
    async def _check_and_handle_breaches(
        self,
        user_id: str,
        user_limits: UserRiskLimits
    ):
        """Check for limit breaches and take action"""
        for limit_type, limit in user_limits.active_limits.items():
            breach_key = f"{user_id}:{limit_type}"
            
            if limit.is_breached():
                # New breach
                if breach_key not in self.active_breaches:
                    self.active_breaches.add(breach_key)
                    limit.breach_count += 1
                    
                    # Take action based on breach type
                    await self._handle_breach(user_id, limit)
                    
                    # Record breach
                    breach_record = {
                        "user_id": user_id,
                        "limit_type": limit_type,
                        "limit_value": str(limit.limit_value),
                        "current_value": str(limit.current_value),
                        "action": limit.breach_action.value,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    self.breach_history.append(breach_record)
                    await self.pulsar.publish("risk-limit-breach", breach_record)
                    
            else:
                # Breach cleared
                if breach_key in self.active_breaches:
                    self.active_breaches.remove(breach_key)
                    
                    await self.pulsar.publish(
                        "risk-limit-cleared",
                        {
                            "user_id": user_id,
                            "limit_type": limit_type,
                            "current_value": str(limit.current_value),
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    
    async def _handle_breach(self, user_id: str, limit: RiskLimit):
        """Handle a limit breach based on configured action"""
        if limit.breach_action == LimitAction.WARN:
            # Just log warning
            logger.warning(f"Risk limit warning for {user_id}: {limit.limit_type.value}")
            
        elif limit.breach_action == LimitAction.BLOCK:
            # Block new trades
            await self._block_trading(user_id, f"Risk limit breached: {limit.limit_type.value}")
            
        elif limit.breach_action == LimitAction.REDUCE_ONLY:
            # Set user to reduce-only mode
            await self._set_reduce_only(user_id)
            
        elif limit.breach_action == LimitAction.LIQUIDATE:
            # Initiate liquidation
            await self._initiate_liquidation(user_id, limit)
            
    def _is_reducing_position(
        self,
        order: Order,
        positions: List[Position]
    ) -> bool:
        """Check if order reduces existing position"""
        for position in positions:
            if position.market_id == order.market_id:
                # Opposite side = reducing
                if (position.side == "long" and order.side == "sell") or \
                   (position.side == "short" and order.side == "buy"):
                    return True
        return False
        
    async def _calculate_total_exposure(self, positions: List[Position]) -> Decimal:
        """Calculate total notional exposure"""
        total = Decimal("0")
        for position in positions:
            # Use mark price for current exposure
            notional = position.size * position.mark_price
            total += abs(notional)
        return total
        
    async def _get_user_collateral(self, user_id: str) -> Decimal:
        """Get user's total collateral value"""
        # Would integrate with collateral engine
        return Decimal("100000")  # Placeholder
        
    async def _is_options_order(self, order: Order) -> bool:
        """Check if order is for options market"""
        # Would check market type
        return "OPT" in order.market_id
        
    async def _check_greeks_limits(
        self,
        user_id: str,
        order: Order,
        positions: List[Position]
    ) -> List[str]:
        """Check Greeks limits for options"""
        violations = []
        user_limits = await self._get_user_limits(user_id)
        
        # Estimate order Greeks (simplified)
        order_delta = order.size * Decimal("0.5")  # Placeholder
        
        # Current portfolio Greeks
        current_greeks = await self._calculate_portfolio_greeks(positions)
        
        # Check if new order would breach limits
        new_delta = abs(current_greeks["delta"] + order_delta)
        
        if new_delta > user_limits.max_delta:
            violations.append(f"Would exceed delta limit: {user_limits.max_delta}")
            
        return violations
        
    async def _check_loss_limits(self, user_id: str) -> List[str]:
        """Check P&L based loss limits"""
        violations = []
        user_limits = await self._get_user_limits(user_id)
        
        # Get user's P&L
        daily_pnl = await self._get_user_pnl(user_id, timedelta(days=1))
        
        if daily_pnl < -user_limits.daily_loss_limit:
            violations.append(f"Daily loss limit breached: -{user_limits.daily_loss_limit}")
            
        return violations
        
    async def _check_concentration_limits(
        self,
        user_id: str,
        order: Order,
        positions: List[Position]
    ) -> List[str]:
        """Check position concentration limits"""
        violations = []
        user_limits = await self._get_user_limits(user_id)
        
        # Calculate concentration by market
        total_exposure = await self._calculate_total_exposure(positions)
        if total_exposure == 0:
            return violations
            
        # Market concentration
        market_exposure = Decimal("0")
        for position in positions:
            if position.market_id == order.market_id:
                market_exposure += abs(position.size * position.mark_price)
                
        # Add order
        order_notional = order.size * (order.price or Decimal("100"))
        new_concentration = (market_exposure + order_notional) / (total_exposure + order_notional)
        
        if new_concentration > user_limits.max_concentration_percent:
            violations.append(
                f"Would exceed concentration limit: {user_limits.max_concentration_percent:.0%}"
            )
            
        return violations
        
    async def _check_var_limits(self, user_id: str, positions: List[Position]):
        """Check Value at Risk limits"""
        if not positions:
            return
            
        user_limits = await self._get_user_limits(user_id)
        
        # Calculate VaR (simplified)
        # In practice would use historical simulation or parametric VaR
        portfolio_value = sum(p.size * p.mark_price for p in positions)
        volatility = Decimal("0.02")  # 2% daily vol assumption
        
        var_95 = portfolio_value * volatility * Decimal("1.65")  # 95% confidence
        var_99 = portfolio_value * volatility * Decimal("2.33")  # 99% confidence
        
        # Update VaR limit values
        if LimitType.VAR_LIMIT.value + "_95" in user_limits.active_limits:
            limit_95 = user_limits.active_limits[LimitType.VAR_LIMIT.value + "_95"]
            limit_95.current_value = var_95
            
        if LimitType.VAR_LIMIT.value + "_99" in user_limits.active_limits:
            limit_99 = user_limits.active_limits[LimitType.VAR_LIMIT.value + "_99"]
            limit_99.current_value = var_99
            
    async def _calculate_portfolio_greeks(self, positions: List[Position]) -> Dict[str, Decimal]:
        """Calculate total portfolio Greeks"""
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_vega = Decimal("0")
        total_theta = Decimal("0")
        
        for position in positions:
            # Would get actual Greeks from position
            total_delta += position.size * Decimal("0.5")  # Placeholder
            
        return {
            "delta": total_delta,
            "gamma": total_gamma,
            "vega": total_vega,
            "theta": total_theta
        }
        
    async def _get_user_positions(self, user_id: str) -> List[Position]:
        """Get user's current positions"""
        # Would query from position service
        return []  # Placeholder
        
    async def _get_user_pnl(self, user_id: str, period: timedelta) -> Decimal:
        """Get user's P&L for period"""
        # Would calculate from trade history
        return Decimal("-10000")  # Placeholder
        
    async def _record_limit_check(
        self,
        user_id: str,
        order: Order,
        violations: List[str]
    ):
        """Record pre-trade limit check"""
        await self.ignite.set(
            f"limit_check:{user_id}:{order.id}",
            {
                "order_id": order.id,
                "market_id": order.market_id,
                "size": str(order.size),
                "violations": violations,
                "passed": len(violations) == 0,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    def _get_limit_status(self, limit: RiskLimit) -> str:
        """Get human-readable limit status"""
        if limit.is_breached():
            return "BREACHED"
        elif limit.is_critical():
            return "CRITICAL"
        elif limit.is_warning():
            return "WARNING"
        else:
            return "OK"
            
    async def _reset_daily_limits(self):
        """Reset daily loss limits"""
        logger.info("Resetting daily loss limits")
        
        for user_id, user_limits in self.user_limits.items():
            if LimitType.LOSS_LIMIT.value in user_limits.active_limits:
                limit = user_limits.active_limits[LimitType.LOSS_LIMIT.value]
                limit.current_value = Decimal("0")
                
    async def _reset_weekly_limits(self):
        """Reset weekly limits"""
        logger.info("Resetting weekly limits")
        # Reset weekly loss limits if implemented
        
    async def _reset_monthly_limits(self):
        """Reset monthly limits"""
        logger.info("Resetting monthly limits")
        # Reset monthly loss limits if implemented
        
    async def _block_trading(self, user_id: str, reason: str):
        """Block user from trading"""
        await self.ignite.set(
            f"trading_blocked:{user_id}",
            {
                "blocked": True,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    async def _set_reduce_only(self, user_id: str):
        """Set user to reduce-only mode"""
        await self.ignite.set(
            f"reduce_only:{user_id}",
            {
                "reduce_only": True,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    async def _initiate_liquidation(self, user_id: str, limit: RiskLimit):
        """Initiate position liquidation"""
        await self.pulsar.publish(
            "initiate-liquidation",
            {
                "user_id": user_id,
                "reason": f"Risk limit breach: {limit.limit_type.value}",
                "limit_value": str(limit.limit_value),
                "current_value": str(limit.current_value),
                "timestamp": datetime.utcnow().isoformat()
            }
        ) 