"""AMM Service dependencies for FastAPI."""

from typing import Annotated, Optional
import uuid

from fastapi import Depends, Header, HTTPException

from app.config import Settings
from app.pools.concentrated_liquidity import ConcentratedLiquidityAMM
from app.pools.stableswap import StableSwapAMM
from app.fees.dynamic_fee_manager import DynamicFeeManager
from app.models.amm import LiquidityPool, LiquidityPosition, PoolType
from platformq_consul import ServiceMeshClient


# Global instances (initialized in main.py)
_settings: Optional[Settings] = None
_concentrated_amm: Optional[ConcentratedLiquidityAMM] = None
_stableswap_amm: Optional[StableSwapAMM] = None
_fee_manager: Optional[DynamicFeeManager] = None
_service_mesh_client: Optional[ServiceMeshClient] = None


def init_dependencies(
    settings: Settings,
    concentrated_amm: ConcentratedLiquidityAMM,
    stableswap_amm: StableSwapAMM,
    fee_manager: DynamicFeeManager,
    service_mesh_client: Optional[ServiceMeshClient] = None
):
    """Initialize global dependencies."""
    global _settings, _concentrated_amm, _stableswap_amm, _fee_manager, _service_mesh_client
    _settings = settings
    _concentrated_amm = concentrated_amm
    _stableswap_amm = stableswap_amm
    _fee_manager = fee_manager
    _service_mesh_client = service_mesh_client


def get_settings() -> Settings:
    """Get application settings."""
    if _settings is None:
        raise RuntimeError("Dependencies not initialized")
    return _settings


async def get_concentrated_amm() -> ConcentratedLiquidityAMM:
    """Get concentrated liquidity AMM instance."""
    if _concentrated_amm is None:
        raise RuntimeError("Dependencies not initialized")
    return _concentrated_amm


async def get_stableswap_amm() -> StableSwapAMM:
    """Get StableSwap AMM instance."""
    if _stableswap_amm is None:
        raise RuntimeError("Dependencies not initialized")
    return _stableswap_amm


async def get_fee_manager() -> DynamicFeeManager:
    """Get fee manager instance."""
    if _fee_manager is None:
        raise RuntimeError("Dependencies not initialized")
    return _fee_manager


async def get_service_mesh_client() -> Optional[ServiceMeshClient]:
    """Get service mesh client instance."""
    return _service_mesh_client


async def get_pool_manager():
    """Get pool manager instance with service mesh support."""
    # In production, return actual pool manager with database/cache
    # For now, return a mock
    class MockPoolManager:
        _pools = {}
        _positions = {}
        _swaps = []
        
        def __init__(self):
            self.mesh_client = _service_mesh_client
        
        async def find_pool(self, base_asset: str, quote_asset: str, pool_type: PoolType):
            for pool in self._pools.values():
                if (pool.base_asset == base_asset and 
                    pool.quote_asset == quote_asset and 
                    pool.pool_type == pool_type):
                    return pool
            return None
        
        async def get_pool(self, pool_id: str) -> Optional[LiquidityPool]:
            return self._pools.get(pool_id)
        
        async def store_pool(self, pool: LiquidityPool):
            self._pools[pool.pool_id] = pool
            
            # Notify other services via events
            if self.mesh_client:
                try:
                    await self.mesh_client.post(
                        "event-router-service",
                        "/api/v1/events",
                        json={
                            "event_type": "pool.created",
                            "data": {
                                "pool_id": pool.pool_id,
                                "pool_type": pool.pool_type.value,
                                "base_asset": pool.base_asset,
                                "quote_asset": pool.quote_asset
                            }
                        }
                    )
                except Exception as e:
                    # Log but don't fail
                    import logging
                    logging.error(f"Failed to publish pool creation event: {e}")
        
        async def update_pool(self, pool: LiquidityPool):
            self._pools[pool.pool_id] = pool
        
        async def list_pools(
            self, 
            base_asset: Optional[str] = None,
            quote_asset: Optional[str] = None,
            pool_type: Optional[PoolType] = None,
            active_only: bool = True,
            limit: int = 100,
            offset: int = 0
        ):
            pools = list(self._pools.values())
            
            # Apply filters
            if base_asset:
                pools = [p for p in pools if p.base_asset == base_asset]
            if quote_asset:
                pools = [p for p in pools if p.quote_asset == quote_asset]
            if pool_type:
                pools = [p for p in pools if p.pool_type == pool_type]
            if active_only:
                pools = [p for p in pools if p.is_active]
            
            # Apply pagination
            return pools[offset:offset + limit]
        
        async def get_or_create_position(self, pool_id: str, provider: str) -> LiquidityPosition:
            key = f"{pool_id}:{provider}"
            if key not in self._positions:
                self._positions[key] = LiquidityPosition(
                    position_id=f"pos_{uuid.uuid4().hex[:8]}",
                    pool_id=pool_id,
                    provider=provider,
                    liquidity=0,
                    base_amount=0,
                    quote_amount=0
                )
            return self._positions[key]
        
        async def get_position(self, position_id: str) -> Optional[LiquidityPosition]:
            for pos in self._positions.values():
                if pos.position_id == position_id:
                    return pos
            return None
        
        async def update_position(self, position: LiquidityPosition):
            key = f"{position.pool_id}:{position.provider}"
            self._positions[key] = position
        
        async def get_user_positions(self, user_id: str):
            return [
                pos for pos in self._positions.values() 
                if pos.provider == user_id
            ]
        
        async def store_swap(self, swap):
            self._swaps.append(swap)
            
            # Get price from oracle service if available
            if self.mesh_client:
                try:
                    pool = self._pools.get(swap.pool_id)
                    if pool:
                        response = await self.mesh_client.get(
                            "oracle-service",
                            f"/api/v1/price/{pool.base_asset}/{pool.quote_asset}"
                        )
                        if response.status_code == 200:
                            oracle_price = response.json().get("price")
                            # Compare with swap execution price
                            price_diff = abs(float(swap.execution_price) - oracle_price) / oracle_price
                            if price_diff > 0.05:  # 5% difference
                                import logging
                                logging.warning(
                                    f"Large price difference detected: "
                                    f"Swap price {swap.execution_price}, Oracle price {oracle_price}"
                                )
                except Exception as e:
                    # Log but don't fail
                    import logging
                    logging.error(f"Failed to check oracle price: {e}")
        
        async def get_pool_metrics(self, pool_id: str, period: str):
            pool = self._pools.get(pool_id)
            if not pool:
                return None
            
            # Mock metrics
            from app.models.amm import PoolMetrics
            from decimal import Decimal
            
            return PoolMetrics(
                pool_id=pool_id,
                period=period,
                volume_base=pool.volume_24h / 2,
                volume_quote=pool.volume_24h / 2,
                volume_usd=pool.volume_24h,
                fees_base=pool.fees_collected_24h / 2,
                fees_quote=pool.fees_collected_24h / 2,
                fees_usd=pool.fees_collected_24h,
                avg_fee_rate=Decimal(str(pool.base_fee_bps)) / 10000,
                avg_liquidity=pool.total_liquidity,
                liquidity_utilization=Decimal("0.5"),
                open_price=pool.current_price,
                close_price=pool.current_price,
                high_price=pool.current_price * Decimal("1.01"),
                low_price=pool.current_price * Decimal("0.99"),
                price_volatility=Decimal("0.02"),
                total_trades=pool.trades_24h,
                unique_traders=pool.unique_traders_24h,
                avg_trade_size=pool.volume_24h / max(pool.trades_24h, 1),
                price_impact_avg=Decimal("0.001"),
                slippage_avg=Decimal("0.001")
            )
    
    return MockPoolManager()


async def get_current_user(
    x_user_id: Annotated[Optional[str], Header()] = None
) -> str:
    """Get current user from headers."""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="User ID header required")
    return x_user_id 