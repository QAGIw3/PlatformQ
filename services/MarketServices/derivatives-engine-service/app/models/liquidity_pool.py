from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class PoolType(Enum):
    """Liquidity pool types"""
    AMM = "amm"  # Automated Market Maker
    ORDER_BOOK = "order_book"
    HYBRID = "hybrid"
    CONCENTRATED = "concentrated"  # Concentrated liquidity
    INSURANCE = "insurance"
    LENDING = "lending"


class PoolStatus(Enum):
    """Pool status"""
    ACTIVE = "active"
    PAUSED = "paused"
    MIGRATING = "migrating"
    DEPRECATED = "deprecated"
    LIQUIDATED = "liquidated"


@dataclass
class PoolConfig:
    """
    Configuration for a liquidity pool
    """
    # Pool type and curve
    pool_type: PoolType = PoolType.AMM
    curve_type: str = "constant_product"  # constant_product, stableswap, concentrated
    
    # Fee configuration
    base_fee: Decimal = Decimal("0.003")  # 0.3%
    protocol_fee_share: Decimal = Decimal("0.2")  # 20% of fees to protocol
    
    # Slippage protection
    max_slippage: Decimal = Decimal("0.1")  # 10%
    max_price_impact: Decimal = Decimal("0.05")  # 5%
    
    # Concentrated liquidity params (if applicable)
    tick_spacing: Optional[int] = None
    min_tick: Optional[int] = None
    max_tick: Optional[int] = None
    
    # Risk parameters
    max_leverage_provided: Decimal = Decimal("10")  # Max leverage for traders
    maintenance_margin_ratio: Decimal = Decimal("0.05")  # 5%
    
    # Pool limits
    max_pool_size: Optional[Decimal] = None
    min_liquidity: Decimal = Decimal("1000")  # $1000 minimum
    
    # Oracle configuration
    use_external_pricing: bool = True
    max_oracle_deviation: Decimal = Decimal("0.02")  # 2%
    
    # Auto-rebalancing
    auto_rebalance: bool = False
    rebalance_threshold: Decimal = Decimal("0.1")  # 10% imbalance
    
    # Incentives
    liquidity_mining_enabled: bool = True
    base_apy: Decimal = Decimal("0.05")  # 5% base APY
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "pool_type": self.pool_type.value,
            "curve_type": self.curve_type,
            "base_fee": str(self.base_fee),
            "protocol_fee_share": str(self.protocol_fee_share),
            "max_slippage": str(self.max_slippage),
            "max_price_impact": str(self.max_price_impact),
            "tick_spacing": self.tick_spacing,
            "min_tick": self.min_tick,
            "max_tick": self.max_tick,
            "max_leverage_provided": str(self.max_leverage_provided),
            "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
            "max_pool_size": str(self.max_pool_size) if self.max_pool_size else None,
            "min_liquidity": str(self.min_liquidity),
            "use_external_pricing": self.use_external_pricing,
            "max_oracle_deviation": str(self.max_oracle_deviation),
            "auto_rebalance": self.auto_rebalance,
            "rebalance_threshold": str(self.rebalance_threshold),
            "liquidity_mining_enabled": self.liquidity_mining_enabled,
            "base_apy": str(self.base_apy)
        }


@dataclass
class LiquidityPool:
    """
    Liquidity pool for derivative markets
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str = ""
    name: str = ""
    status: PoolStatus = PoolStatus.ACTIVE
    
    # Configuration
    config: PoolConfig = field(default_factory=PoolConfig)
    
    # Liquidity tracking
    total_liquidity: Decimal = Decimal("0")
    available_liquidity: Decimal = Decimal("0")
    utilized_liquidity: Decimal = Decimal("0")
    utilization_rate: Decimal = Decimal("0")
    
    # Token balances (for AMM pools)
    token_balances: Dict[str, Decimal] = field(default_factory=dict)
    
    # LP tokens
    lp_token_supply: Decimal = Decimal("0")
    lp_token_price: Decimal = Decimal("1")
    
    # Liquidity providers
    liquidity_providers: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # address -> LP info
    unique_lps: int = 0
    
    # Fee accumulation
    accumulated_fees: Dict[str, Decimal] = field(default_factory=dict)  # token -> amount
    total_fees_usd: Decimal = Decimal("0")
    unclaimed_fees: Dict[str, Decimal] = field(default_factory=dict)  # LP -> fees
    
    # Performance metrics
    volume_24h: Decimal = Decimal("0")
    volume_7d: Decimal = Decimal("0")
    volume_total: Decimal = Decimal("0")
    trades_24h: int = 0
    trades_total: int = 0
    
    # PnL tracking
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    impermanent_loss: Decimal = Decimal("0")
    
    # APY metrics
    current_apy: Decimal = Decimal("0")
    avg_apy_7d: Decimal = Decimal("0")
    avg_apy_30d: Decimal = Decimal("0")
    
    # Risk metrics
    var_95: Decimal = Decimal("0")  # Value at Risk 95%
    max_drawdown: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    
    # Insurance (if applicable)
    insurance_fund: Decimal = Decimal("0")
    insurance_claims_paid: Decimal = Decimal("0")
    
    # Governance
    governance_token: Optional[str] = None
    min_lp_for_governance: Decimal = Decimal("100")  # Min LP tokens for voting
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    last_rebalance: Optional[datetime] = None
    
    # Metadata
    creator: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_liquidity(self, provider: str, amounts: Dict[str, Decimal]) -> Decimal:
        """Add liquidity to pool and mint LP tokens"""
        # Calculate LP tokens to mint
        if self.lp_token_supply == 0:
            # First deposit
            lp_tokens = sum(amounts.values())  # Simplified
        else:
            # Proportional to existing liquidity
            value_added = sum(amounts.values())  # Simplified - should use prices
            lp_tokens = (value_added / self.total_liquidity) * self.lp_token_supply
            
        # Update balances
        for token, amount in amounts.items():
            self.token_balances[token] = self.token_balances.get(token, Decimal("0")) + amount
            
        # Update liquidity tracking
        liquidity_value = sum(amounts.values())  # Simplified
        self.total_liquidity += liquidity_value
        self.available_liquidity += liquidity_value
        
        # Update LP tracking
        if provider not in self.liquidity_providers:
            self.unique_lps += 1
            self.liquidity_providers[provider] = {
                "lp_tokens": Decimal("0"),
                "deposits": [],
                "withdrawals": [],
                "fees_earned": Decimal("0")
            }
            
        self.liquidity_providers[provider]["lp_tokens"] += lp_tokens
        self.liquidity_providers[provider]["deposits"].append({
            "timestamp": datetime.utcnow().isoformat(),
            "amounts": {k: str(v) for k, v in amounts.items()},
            "lp_tokens": str(lp_tokens)
        })
        
        self.lp_token_supply += lp_tokens
        self.updated_at = datetime.utcnow()
        
        return lp_tokens
        
    def remove_liquidity(self, provider: str, lp_tokens: Decimal) -> Dict[str, Decimal]:
        """Remove liquidity from pool and burn LP tokens"""
        if provider not in self.liquidity_providers:
            return {}
            
        provider_info = self.liquidity_providers[provider]
        if lp_tokens > provider_info["lp_tokens"]:
            lp_tokens = provider_info["lp_tokens"]
            
        # Calculate share of pool
        pool_share = lp_tokens / self.lp_token_supply
        
        # Calculate amounts to return
        amounts_out = {}
        for token, balance in self.token_balances.items():
            amount = balance * pool_share
            amounts_out[token] = amount
            self.token_balances[token] -= amount
            
        # Update liquidity tracking
        liquidity_removed = self.total_liquidity * pool_share
        self.total_liquidity -= liquidity_removed
        self.available_liquidity -= min(liquidity_removed, self.available_liquidity)
        
        # Update LP tracking
        provider_info["lp_tokens"] -= lp_tokens
        provider_info["withdrawals"].append({
            "timestamp": datetime.utcnow().isoformat(),
            "lp_tokens": str(lp_tokens),
            "amounts": {k: str(v) for k, v in amounts_out.items()}
        })
        
        # Distribute accumulated fees
        if provider in self.unclaimed_fees:
            for token, fee_amount in self.unclaimed_fees[provider].items():
                amounts_out[token] = amounts_out.get(token, Decimal("0")) + fee_amount
            del self.unclaimed_fees[provider]
            
        self.lp_token_supply -= lp_tokens
        
        # Remove provider if no more tokens
        if provider_info["lp_tokens"] == 0:
            self.unique_lps -= 1
            
        self.updated_at = datetime.utcnow()
        
        return amounts_out
        
    def utilize_liquidity(self, amount: Decimal) -> bool:
        """Mark liquidity as utilized for positions"""
        if amount > self.available_liquidity:
            return False
            
        self.available_liquidity -= amount
        self.utilized_liquidity += amount
        self.utilization_rate = self.utilized_liquidity / self.total_liquidity if self.total_liquidity > 0 else Decimal("0")
        self.updated_at = datetime.utcnow()
        
        return True
        
    def release_liquidity(self, amount: Decimal):
        """Release utilized liquidity back to available"""
        amount = min(amount, self.utilized_liquidity)
        self.utilized_liquidity -= amount
        self.available_liquidity += amount
        self.utilization_rate = self.utilized_liquidity / self.total_liquidity if self.total_liquidity > 0 else Decimal("0")
        self.updated_at = datetime.utcnow()
        
    def record_trade(self, volume: Decimal, fee: Decimal, token: str = "USDC"):
        """Record a trade and its fees"""
        self.volume_24h += volume  # Simplified - should track time windows
        self.volume_7d += volume
        self.volume_total += volume
        self.trades_24h += 1
        self.trades_total += 1
        
        # Record fees
        self.accumulated_fees[token] = self.accumulated_fees.get(token, Decimal("0")) + fee
        self.total_fees_usd += fee  # Simplified - should convert to USD
        
        # Distribute fees to LPs proportionally
        for provider, info in self.liquidity_providers.items():
            if info["lp_tokens"] > 0:
                provider_share = info["lp_tokens"] / self.lp_token_supply
                provider_fee = fee * provider_share * (Decimal("1") - self.config.protocol_fee_share)
                
                if provider not in self.unclaimed_fees:
                    self.unclaimed_fees[provider] = {}
                self.unclaimed_fees[provider][token] = (
                    self.unclaimed_fees[provider].get(token, Decimal("0")) + provider_fee
                )
                info["fees_earned"] += provider_fee
                
        self.updated_at = datetime.utcnow()
        
    def update_metrics(self):
        """Update pool metrics"""
        # Update LP token price
        if self.lp_token_supply > 0:
            self.lp_token_price = self.total_liquidity / self.lp_token_supply
            
        # Calculate APY (simplified)
        if self.total_liquidity > 0 and self.volume_24h > 0:
            daily_fees = self.volume_24h * self.config.base_fee * (Decimal("1") - self.config.protocol_fee_share)
            daily_return = daily_fees / self.total_liquidity
            self.current_apy = daily_return * Decimal("365")
            
        self.updated_at = datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "market_id": self.market_id,
            "name": self.name,
            "status": self.status.value,
            "config": self.config.to_dict(),
            "total_liquidity": str(self.total_liquidity),
            "available_liquidity": str(self.available_liquidity),
            "utilized_liquidity": str(self.utilized_liquidity),
            "utilization_rate": str(self.utilization_rate),
            "token_balances": {k: str(v) for k, v in self.token_balances.items()},
            "lp_token_supply": str(self.lp_token_supply),
            "lp_token_price": str(self.lp_token_price),
            "unique_lps": self.unique_lps,
            "accumulated_fees": {k: str(v) for k, v in self.accumulated_fees.items()},
            "total_fees_usd": str(self.total_fees_usd),
            "volume_24h": str(self.volume_24h),
            "volume_7d": str(self.volume_7d),
            "volume_total": str(self.volume_total),
            "trades_24h": self.trades_24h,
            "trades_total": self.trades_total,
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "impermanent_loss": str(self.impermanent_loss),
            "current_apy": str(self.current_apy),
            "avg_apy_7d": str(self.avg_apy_7d),
            "avg_apy_30d": str(self.avg_apy_30d),
            "var_95": str(self.var_95),
            "max_drawdown": str(self.max_drawdown),
            "sharpe_ratio": str(self.sharpe_ratio),
            "insurance_fund": str(self.insurance_fund),
            "insurance_claims_paid": str(self.insurance_claims_paid),
            "governance_token": self.governance_token,
            "min_lp_for_governance": str(self.min_lp_for_governance),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_rebalance": self.last_rebalance.isoformat() if self.last_rebalance else None,
            "creator": self.creator,
            "metadata": self.metadata
        } 