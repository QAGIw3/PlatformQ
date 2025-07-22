"""
Strategy Runner for Market Making Service

Manages the lifecycle of market making strategies and ensures they are
properly initialized with all required dependencies including risk checks.
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from datetime import datetime

from app.core.dependencies import (
    get_ignite_client,
    get_pulsar_client,
    get_service_clients,
    get_risk_checker
)
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.risk import RiskChecker
from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.compute_futures_engine import ComputeFuturesEngine
from app.engines.compute_options_engine import ComputeOptionsEngine

from .grid_trading_strategy import GridTradingStrategy, GridConfig, GridType
from .cross_market_arbitrage import CrossMarketArbitrage, CrossMarketArbConfig
from .delta_neutral_options_mm import DeltaNeutralOptionsMM, MarketMakingConfig
from .volatility_arbitrage_bot import VolatilityArbitrageBot, VolArbConfig

logger = logging.getLogger(__name__)


class StrategyRunner:
    """Manages the lifecycle and execution of market making strategies"""
    
    def __init__(self):
        self.strategies: Dict[str, Any] = {}
        self.strategy_tasks: Dict[str, asyncio.Task] = {}
        self._initialized = False
        
        # Core dependencies
        self.ignite_cache: Optional[IgniteCache] = None
        self.pulsar_publisher: Optional[PulsarEventPublisher] = None
        self.oracle_client: Optional[OracleAggregatorClient] = None
        self.risk_checker: Optional[RiskChecker] = None
        
        # Market engines
        self.spot_market: Optional[ComputeSpotMarket] = None
        self.futures_engine: Optional[ComputeFuturesEngine] = None
        self.options_engine: Optional[ComputeOptionsEngine] = None
        
    async def initialize(self):
        """Initialize all dependencies"""
        if self._initialized:
            return
            
        try:
            # Get core dependencies
            ignite_client = await get_ignite_client()
            pulsar_client = await get_pulsar_client()
            service_clients = await get_service_clients()
            self.risk_checker = await get_risk_checker()
            
            # Initialize wrappers
            self.ignite_cache = IgniteCache(ignite_client)
            self.pulsar_publisher = PulsarEventPublisher(pulsar_client)
            self.oracle_client = OracleAggregatorClient(service_clients)
            
            # Initialize market engines
            self.spot_market = ComputeSpotMarket()
            self.futures_engine = ComputeFuturesEngine(
                ignite=self.ignite_cache,
                pulsar=self.pulsar_publisher
            )
            self.options_engine = ComputeOptionsEngine(
                ignite=self.ignite_cache,
                pulsar=self.pulsar_publisher
            )
            
            # Initialize engines
            await self.spot_market.initialize()
            await self.futures_engine.initialize()
            await self.options_engine.initialize()
            
            self._initialized = True
            logger.info("Strategy runner initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize strategy runner: {e}")
            raise
            
    async def create_grid_strategy(self,
                                 strategy_id: str,
                                 config: GridConfig,
                                 user_id: str) -> str:
        """Create and start a grid trading strategy"""
        if not self._initialized:
            await self.initialize()
            
        # Create strategy instance with risk checker
        strategy = GridTradingStrategy(
            ignite=self.ignite_cache,
            pulsar=self.pulsar_publisher,
            oracle=self.oracle_client,
            risk_checker=self.risk_checker  # Pass risk checker
        )
        
        # Start strategy
        await strategy.start()
        
        # Create grid
        grid_id = await strategy.create_grid(config, user_id)
        
        # Store strategy
        self.strategies[strategy_id] = {
            "type": "grid",
            "instance": strategy,
            "grid_id": grid_id,
            "user_id": user_id,
            "created_at": datetime.utcnow()
        }
        
        logger.info(f"Created grid strategy {strategy_id} with grid {grid_id}")
        return grid_id
        
    async def create_arbitrage_strategy(self,
                                      strategy_id: str,
                                      config: CrossMarketArbConfig,
                                      user_id: str) -> str:
        """Create and start a cross-market arbitrage strategy"""
        if not self._initialized:
            await self.initialize()
            
        # Create strategy instance with risk checker
        strategy = CrossMarketArbitrage(
            spot_market=self.spot_market,
            futures_engine=self.futures_engine,
            options_engine=self.options_engine,
            ignite=self.ignite_cache,
            pulsar=self.pulsar_publisher,
            oracle=self.oracle_client,
            risk_checker=self.risk_checker  # Pass risk checker
        )
        
        # Start strategy
        await strategy.start()
        
        # Create bot
        bot_id = await strategy.create_bot(config)
        
        # Store strategy
        self.strategies[strategy_id] = {
            "type": "arbitrage",
            "instance": strategy,
            "bot_id": bot_id,
            "user_id": user_id,
            "created_at": datetime.utcnow()
        }
        
        logger.info(f"Created arbitrage strategy {strategy_id} with bot {bot_id}")
        return bot_id
        
    async def create_delta_neutral_strategy(self,
                                          strategy_id: str,
                                          config: MarketMakingConfig,
                                          user_id: str) -> str:
        """Create and start a delta-neutral options market making strategy"""
        if not self._initialized:
            await self.initialize()
            
        # Create strategy instance with risk checker
        strategy = DeltaNeutralOptionsMM(
            spot_market=self.spot_market,
            options_engine=self.options_engine,
            ignite=self.ignite_cache,
            pulsar=self.pulsar_publisher,
            oracle=self.oracle_client,
            risk_checker=self.risk_checker  # Pass risk checker
        )
        
        # Start strategy
        await strategy.start()
        
        # Create market maker
        mm_id = await strategy.create_market_maker(
            config=config,
            user_id=user_id
        )
        
        # Store strategy
        self.strategies[strategy_id] = {
            "type": "delta_neutral",
            "instance": strategy,
            "mm_id": mm_id,
            "user_id": user_id,
            "created_at": datetime.utcnow()
        }
        
        logger.info(f"Created delta-neutral strategy {strategy_id} with MM {mm_id}")
        return mm_id
        
    async def stop_strategy(self, strategy_id: str) -> Dict[str, Any]:
        """Stop a running strategy"""
        if strategy_id not in self.strategies:
            raise ValueError(f"Strategy {strategy_id} not found")
            
        strategy_data = self.strategies[strategy_id]
        strategy = strategy_data["instance"]
        
        # Stop strategy based on type
        result = {}
        
        if strategy_data["type"] == "grid":
            result = await strategy.stop_grid(
                strategy_data["grid_id"],
                liquidate=True
            )
        elif strategy_data["type"] == "arbitrage":
            result = await strategy.stop_bot(strategy_data["bot_id"])
        elif strategy_data["type"] == "delta_neutral":
            result = await strategy.stop_market_maker(strategy_data["mm_id"])
            
        # Stop the strategy instance
        await strategy.stop()
        
        # Remove from active strategies
        del self.strategies[strategy_id]
        
        logger.info(f"Stopped strategy {strategy_id}")
        return result
        
    async def get_strategy_status(self, strategy_id: str) -> Dict[str, Any]:
        """Get status of a running strategy"""
        if strategy_id not in self.strategies:
            raise ValueError(f"Strategy {strategy_id} not found")
            
        strategy_data = self.strategies[strategy_id]
        strategy = strategy_data["instance"]
        
        # Get status based on type
        if strategy_data["type"] == "grid":
            return await strategy.get_grid_status(strategy_data["grid_id"])
        elif strategy_data["type"] == "arbitrage":
            return await strategy.get_bot_status(strategy_data["bot_id"])
        elif strategy_data["type"] == "delta_neutral":
            return await strategy.get_market_maker_status(strategy_data["mm_id"])
            
        return {}
        
    async def cleanup(self):
        """Clean up all running strategies"""
        # Stop all strategies
        for strategy_id in list(self.strategies.keys()):
            try:
                await self.stop_strategy(strategy_id)
            except Exception as e:
                logger.error(f"Error stopping strategy {strategy_id}: {e}")
                
        # Clean up market engines
        if self.spot_market:
            await self.spot_market.cleanup()
        if self.futures_engine:
            await self.futures_engine.cleanup()
        if self.options_engine:
            await self.options_engine.cleanup()
            
        self._initialized = False
        logger.info("Strategy runner cleaned up")


# Global instance
_strategy_runner: Optional[StrategyRunner] = None


async def get_strategy_runner() -> StrategyRunner:
    """Get or create the strategy runner instance"""
    global _strategy_runner
    
    if _strategy_runner is None:
        _strategy_runner = StrategyRunner()
        await _strategy_runner.initialize()
        
    return _strategy_runner 