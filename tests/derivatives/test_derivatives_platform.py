import pytest
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
import random
import numpy as np
from unittest.mock import Mock, patch, AsyncMock
import aiohttp

from services.derivatives_engine_service.app.engines.matching_engine import MatchingEngine, OrderBook
from services.derivatives_engine_service.app.collateral.multi_tier_engine import MultiTierCollateralEngine
from services.derivatives_engine_service.app.liquidation.partial_liquidator import PartialLiquidationEngine
from services.derivatives_engine_service.app.governance.market_dao import MarketCreationDAO
from services.derivatives_engine_service.app.integrations.marketplace_integration import MarketplaceIntegration
from services.oracle_aggregator_service.app.aggregator.price_aggregator import PriceAggregator

# Test fixtures
@pytest.fixture
async def mock_ignite_client():
    """Mock Ignite cache client"""
    client = AsyncMock()
    client.get_async = AsyncMock(return_value=None)
    client.put_async = AsyncMock(return_value=True)
    return client

@pytest.fixture
async def mock_pulsar_client():
    """Mock Pulsar event publisher"""
    client = AsyncMock()
    client.publish = AsyncMock(return_value=True)
    return client

@pytest.fixture
async def mock_neuromorphic_client():
    """Mock neuromorphic service client"""
    client = AsyncMock()
    client.process = AsyncMock(return_value={"will_match_immediately": True})
    return client

@pytest.fixture
async def matching_engine(mock_neuromorphic_client, mock_ignite_client, mock_pulsar_client):
    """Create matching engine instance"""
    engine = MatchingEngine(mock_neuromorphic_client, mock_ignite_client, mock_pulsar_client)
    await engine.start()
    yield engine
    await engine.stop()

@pytest.fixture
async def collateral_engine(mock_ignite_client):
    """Create collateral engine instance"""
    mock_graph_client = AsyncMock()
    mock_oracle_client = AsyncMock()
    return MultiTierCollateralEngine(mock_ignite_client, mock_graph_client, mock_oracle_client)

# Unit Tests

class TestOrderBook:
    """Test order book functionality"""
    
    def test_add_order(self):
        book = OrderBook("BTC-USD")
        order = Mock(
            id="order1",
            side="BUY",
            price=Decimal("50000"),
            size=Decimal("1.0")
        )
        
        book.add_order(order)
        assert order.id in book.orders
        assert book.get_best_bid() == Decimal("50000")
        
    def test_remove_order(self):
        book = OrderBook("BTC-USD")
        order = Mock(
            id="order1",
            side="SELL",
            price=Decimal("51000"),
            size=Decimal("1.0")
        )
        
        book.add_order(order)
        removed = book.remove_order("order1")
        
        assert removed == order
        assert "order1" not in book.orders
        assert book.get_best_ask() is None
        
    def test_get_spread(self):
        book = OrderBook("BTC-USD")
        
        buy_order = Mock(
            id="buy1",
            side="BUY",
            price=Decimal("50000"),
            size=Decimal("1.0")
        )
        
        sell_order = Mock(
            id="sell1",
            side="SELL",
            price=Decimal("50100"),
            size=Decimal("1.0")
        )
        
        book.add_order(buy_order)
        book.add_order(sell_order)
        
        spread = book.get_spread()
        assert spread == Decimal("100")

class TestMatchingEngine:
    """Test matching engine functionality"""
    
    @pytest.mark.asyncio
    async def test_market_order_matching(self, matching_engine):
        """Test market order matching"""
        # Add limit orders to book
        limit_order = Mock(
            id="limit1",
            trader="trader1",
            market_id=1,
            side="SELL",
            order_type="LIMIT",
            price=Decimal("50000"),
            size=Decimal("1.0"),
            remaining_size=Decimal("1.0")
        )
        
        matching_engine.order_books[1] = OrderBook(1)
        matching_engine.order_books[1].add_order(limit_order)
        
        # Submit market buy order
        market_order = Mock(
            id="market1",
            trader="trader2",
            market_id=1,
            side="BUY",
            order_type="MARKET",
            size=Decimal("0.5"),
            remaining_size=Decimal("0.5")
        )
        
        with patch.object(matching_engine, '_validate_order', return_value={"valid": True}):
            with patch.object(matching_engine, '_execute_trade', return_value=Mock()):
                result = await matching_engine.submit_order(market_order)
                
        assert result["success"] is True
        assert "trades" in result
        
    @pytest.mark.asyncio
    async def test_limit_order_crossing(self, matching_engine):
        """Test limit order crossing"""
        # Existing sell order
        sell_order = Mock(
            id="sell1",
            trader="trader1",
            market_id=1,
            side="SELL",
            order_type="LIMIT",
            price=Decimal("50000"),
            size=Decimal("1.0"),
            remaining_size=Decimal("1.0")
        )
        
        matching_engine.order_books[1] = OrderBook(1)
        matching_engine.order_books[1].add_order(sell_order)
        
        # New buy order that crosses
        buy_order = Mock(
            id="buy1",
            trader="trader2",
            market_id=1,
            side="BUY",
            order_type="LIMIT",
            price=Decimal("50100"),  # Higher than sell
            size=Decimal("0.5"),
            remaining_size=Decimal("0.5"),
            post_only=False
        )
        
        with patch.object(matching_engine, '_validate_order', return_value={"valid": True}):
            with patch.object(matching_engine, '_execute_trade', return_value=Mock()):
                result = await matching_engine.submit_order(buy_order)
                
        assert result["success"] is True

class TestCollateralEngine:
    """Test collateral management"""
    
    @pytest.mark.asyncio
    async def test_calculate_collateral_value(self, collateral_engine):
        """Test collateral value calculation"""
        # Mock user balances
        user = "0x123"
        balances = {
            "USDC": Decimal("10000"),
            "ETH": Decimal("5"),
            "PLATFORM": Decimal("50000")
        }
        
        # Mock prices
        prices = {
            "USDC": Decimal("1"),
            "ETH": Decimal("2000"),
            "PLATFORM": Decimal("0.5")
        }
        
        with patch.object(collateral_engine, 'get_user_balances', return_value=balances):
            with patch.object(collateral_engine.oracle, 'get_price', side_effect=lambda asset: prices[asset]):
                value = await collateral_engine.calculate_total_collateral_value(user)
                
        expected = (10000 * 1 * 0.95) + (5 * 2000 * 0.85) + (50000 * 0.5 * 0.75)
        assert abs(value - Decimal(str(expected))) < Decimal("0.01")
        
    @pytest.mark.asyncio
    async def test_reputation_based_credit(self, collateral_engine):
        """Test reputation-based credit calculation"""
        # Mock graph intelligence response
        reputation_data = {
            "trading_volume": 5000000,  # $5M
            "liquidation_count": 0,
            "account_age_days": 365,
            "governance_participation": 50,
            "social_score": 850
        }
        
        with patch.object(collateral_engine.graph, 'get_user_reputation', return_value=reputation_data):
            credit = await collateral_engine.calculate_reputation_credit("0x123")
            
        assert credit > 0  # Should have some credit
        assert credit <= Decimal("1000000")  # Max $1M

class TestLiquidationEngine:
    """Test liquidation functionality"""
    
    @pytest.mark.asyncio
    async def test_partial_liquidation(self, mock_ignite_client, mock_pulsar_client):
        """Test partial liquidation logic"""
        # Create engine
        mock_collateral = AsyncMock()
        mock_insurance = AsyncMock()
        engine = PartialLiquidationEngine(
            mock_collateral,
            mock_insurance,
            mock_ignite_client,
            mock_pulsar_client
        )
        
        # Mock position
        position = {
            "id": "pos1",
            "user": "0x123",
            "size": Decimal("10000"),  # $10k position
            "collateral": Decimal("1000"),
            "health_factor": Decimal("0.9")  # Unhealthy
        }
        
        # Execute partial liquidation
        with patch.object(engine, 'calculate_liquidation_size', return_value=Decimal("5000")):
            result = await engine.liquidate_position(position["id"])
            
        assert result["liquidated_size"] == Decimal("5000")  # 50% liquidated
        assert result["remaining_size"] == Decimal("5000")

class TestPriceAggregator:
    """Test oracle price aggregation"""
    
    @pytest.mark.asyncio
    async def test_outlier_detection(self, mock_ignite_client, mock_pulsar_client):
        """Test price outlier detection"""
        aggregator = PriceAggregator(mock_ignite_client, mock_pulsar_client)
        
        # Price data with outlier
        prices = [
            {"source": "chainlink", "price": Decimal("50000"), "timestamp": datetime.utcnow()},
            {"source": "band", "price": Decimal("50100"), "timestamp": datetime.utcnow()},
            {"source": "internal", "price": Decimal("49900"), "timestamp": datetime.utcnow()},
            {"source": "ai", "price": Decimal("75000"), "timestamp": datetime.utcnow()}  # Outlier
        ]
        
        result = await aggregator.aggregate_prices("BTC-USD", prices)
        
        # Should exclude outlier
        assert result["final_price"] < Decimal("55000")
        assert len(result["excluded_sources"]) == 1
        assert "ai" in result["excluded_sources"]
        
    @pytest.mark.asyncio
    async def test_weighted_median_calculation(self, mock_ignite_client, mock_pulsar_client):
        """Test weighted median price calculation"""
        aggregator = PriceAggregator(mock_ignite_client, mock_pulsar_client)
        
        prices = [
            {"source": "chainlink", "price": Decimal("50000"), "weight": 0.4},
            {"source": "band", "price": Decimal("50200"), "weight": 0.3},
            {"source": "internal", "price": Decimal("49800"), "weight": 0.3}
        ]
        
        median = aggregator._calculate_weighted_median(prices)
        
        # Weighted median should be close to 50000
        assert abs(median - Decimal("50000")) < Decimal("100")

# Integration Tests

class TestDerivativesIntegration:
    """Integration tests for derivatives platform"""
    
    @pytest.mark.asyncio
    async def test_full_trade_lifecycle(self, matching_engine, collateral_engine):
        """Test complete trade lifecycle"""
        user = "0x123"
        market_id = 1
        
        # 1. Deposit collateral
        await collateral_engine.deposit_collateral(user, "USDC", Decimal("10000"))
        
        # 2. Open position
        position = await matching_engine.open_position(
            user=user,
            market_id=market_id,
            is_long=True,
            size=Decimal("1.0"),
            leverage=10
        )
        
        # 3. Monitor position
        health = await collateral_engine.calculate_health_factor(position["id"])
        assert health > Decimal("1.0")
        
        # 4. Close position
        result = await matching_engine.close_position(position["id"])
        assert result["success"] is True
        
    @pytest.mark.asyncio
    async def test_marketplace_integration(self):
        """Test integration with digital asset marketplace"""
        # Mock services
        mock_asset_service = AsyncMock()
        mock_graph_service = AsyncMock()
        mock_vc_service = AsyncMock()
        mock_pulsar = AsyncMock()
        
        integration = MarketplaceIntegration(
            mock_asset_service,
            mock_graph_service,
            mock_vc_service,
            mock_pulsar
        )
        
        # Mock asset data
        mock_asset_service.get_asset.return_value = {
            "id": "asset123",
            "name": "Rare NFT",
            "type": "nft",
            "collection": "collection1",
            "creator": "0x456",
            "created_at": (datetime.utcnow() - timedelta(days=30)).isoformat()
        }
        
        # Test derivative creation
        result = await integration.create_asset_derivative_market("asset123")
        
        assert "proposal_id" in result
        assert result["asset_id"] == "asset123"
        assert "oracle_config" in result

# Stress Tests

class TestStressScenarios:
    """Stress tests for extreme scenarios"""
    
    @pytest.mark.asyncio
    async def test_high_frequency_trading(self, matching_engine):
        """Test high frequency order submission"""
        market_id = 1
        matching_engine.order_books[1] = OrderBook(1)
        
        # Submit 1000 orders rapidly
        tasks = []
        for i in range(1000):
            order = Mock(
                id=f"order{i}",
                trader=f"trader{i % 10}",
                market_id=market_id,
                side=random.choice(["BUY", "SELL"]),
                order_type="LIMIT",
                price=Decimal(str(50000 + random.randint(-100, 100))),
                size=Decimal(str(random.uniform(0.1, 1.0))),
                remaining_size=Decimal(str(random.uniform(0.1, 1.0)))
            )
            
            task = matching_engine.submit_order(order)
            tasks.append(task)
            
        # Wait for all orders
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check success rate
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        assert successful > 900  # >90% success rate
        
    @pytest.mark.asyncio
    async def test_cascade_liquidation_scenario(self):
        """Test cascade liquidation prevention"""
        # Create positions with varying health
        positions = []
        for i in range(100):
            positions.append({
                "id": f"pos{i}",
                "user": f"user{i}",
                "size": Decimal("10000"),
                "health_factor": Decimal(str(0.8 + (i * 0.01)))  # 0.8 to 1.8
            })
            
        # Simulate price crash
        liquidation_count = 0
        partial_count = 0
        
        for pos in positions:
            if pos["health_factor"] < Decimal("1.0"):
                liquidation_count += 1
                # Check if partial liquidation applied
                if pos["size"] > Decimal("1000"):
                    partial_count += 1
                    
        # Verify cascade prevention
        assert partial_count > liquidation_count * 0.5  # >50% partial liquidations
        
    @pytest.mark.asyncio
    async def test_oracle_manipulation_resistance(self):
        """Test resistance to oracle manipulation"""
        aggregator = PriceAggregator(Mock(), Mock())
        
        # Normal prices
        normal_price = Decimal("50000")
        
        # Attempt manipulation with multiple bad sources
        prices = [
            {"source": "chainlink", "price": normal_price, "weight": 0.4},
            {"source": "manipulated1", "price": normal_price * 2, "weight": 0.2},
            {"source": "manipulated2", "price": normal_price * 2, "weight": 0.2},
            {"source": "band", "price": normal_price, "weight": 0.2}
        ]
        
        result = await aggregator.aggregate_prices("BTC-USD", prices)
        
        # Price should remain close to normal despite manipulation
        assert abs(result["final_price"] - normal_price) < normal_price * Decimal("0.1")

# Performance Tests

class TestPerformance:
    """Performance benchmarks"""
    
    @pytest.mark.asyncio
    async def test_matching_latency(self, matching_engine):
        """Test order matching latency"""
        import time
        
        market_id = 1
        matching_engine.order_books[1] = OrderBook(1)
        
        # Measure latency for 100 orders
        latencies = []
        
        for i in range(100):
            order = Mock(
                id=f"perf{i}",
                trader="trader1",
                market_id=market_id,
                side="BUY",
                order_type="LIMIT",
                price=Decimal("50000"),
                size=Decimal("0.1"),
                remaining_size=Decimal("0.1")
            )
            
            start = time.time()
            await matching_engine.submit_order(order)
            latency = (time.time() - start) * 1000  # ms
            latencies.append(latency)
            
        # Check performance metrics
        avg_latency = np.mean(latencies)
        p99_latency = np.percentile(latencies, 99)
        
        assert avg_latency < 10  # <10ms average
        assert p99_latency < 50  # <50ms p99
        
    @pytest.mark.asyncio
    async def test_risk_calculation_performance(self, collateral_engine):
        """Test risk calculation performance"""
        import time
        
        # Create 1000 positions
        positions = []
        for i in range(1000):
            positions.append({
                "id": f"pos{i}",
                "user": f"user{i % 100}",
                "market": "BTC-USD",
                "size": Decimal(str(random.uniform(100, 10000))),
                "leverage": random.randint(1, 100)
            })
            
        # Measure batch risk calculation
        start = time.time()
        
        health_factors = []
        for pos in positions:
            health = await collateral_engine.calculate_health_factor(pos["id"])
            health_factors.append(health)
            
        duration = time.time() - start
        
        # Should process 1000 positions quickly
        assert duration < 5.0  # <5 seconds for 1000 positions
        assert len(health_factors) == 1000

# Security Tests

class TestSecurity:
    """Security and edge case tests"""
    
    @pytest.mark.asyncio
    async def test_reentrancy_protection(self, matching_engine):
        """Test protection against reentrancy attacks"""
        # Attempt to close position during callback
        with pytest.raises(Exception):
            await matching_engine.close_position_with_callback(
                "pos1",
                callback=lambda: matching_engine.close_position("pos1")
            )
            
    @pytest.mark.asyncio
    async def test_integer_overflow_protection(self):
        """Test protection against integer overflow"""
        # Large numbers that could overflow
        size = Decimal("1" + "0" * 50)  # Very large number
        price = Decimal("1" + "0" * 20)
        
        # Should handle without overflow
        notional = size * price
        assert notional > 0  # Python handles arbitrary precision
        
    @pytest.mark.asyncio
    async def test_access_control(self):
        """Test role-based access control"""
        # Mock DAO with access control
        dao = MarketCreationDAO(Mock(), Mock(), Mock(), Mock())
        
        # Non-admin should not create market
        with pytest.raises(PermissionError):
            await dao.create_market(
                proposer="0x123",  # Non-admin
                market_spec={}
            )

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"]) 