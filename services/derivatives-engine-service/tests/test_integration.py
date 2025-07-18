"""
Integration Tests for Derivatives Engine Service

Tests the complete service functionality including all optimizations
"""

import pytest
import asyncio
import httpx
from decimal import Decimal
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List
import uuid
import random


class TestDerivativesEngineIntegration:
    """Integration tests for the derivatives engine"""
    
    @pytest.fixture(scope="class")
    async def client(self):
        """Create test client"""
        async with httpx.AsyncClient(
            base_url="http://localhost:8000",
            timeout=30.0
        ) as client:
            yield client
            
    @pytest.fixture(scope="class")
    async def test_user(self):
        """Create test user data"""
        return {
            "user_id": f"test_user_{uuid.uuid4()}",
            "account_id": f"test_account_{uuid.uuid4()}"
        }
        
    @pytest.mark.asyncio
    async def test_health_check(self, client):
        """Test service health endpoint"""
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        
    @pytest.mark.asyncio
    async def test_spot_market_flow(self, client, test_user):
        """Test complete spot market trading flow"""
        
        # 1. Get initial spot price
        response = await client.get("/api/v1/markets/spot-price/GPU_SPOT")
        assert response.status_code == 200
        initial_price = Decimal(response.json()["last_trade_price"])
        
        # 2. Place buy order
        buy_order = {
            "user_id": test_user["user_id"],
            "instrument_id": "GPU_SPOT",
            "side": "buy",
            "price": str(initial_price - 1),
            "quantity": "10",
            "order_type": "limit"
        }
        
        response = await client.post("/api/v1/trading/orders", json=buy_order)
        assert response.status_code == 200
        buy_order_id = response.json()["order_id"]
        
        # 3. Place matching sell order
        sell_order = {
            "user_id": f"test_seller_{uuid.uuid4()}",
            "instrument_id": "GPU_SPOT",
            "side": "sell",
            "price": str(initial_price - 1),
            "quantity": "10",
            "order_type": "limit"
        }
        
        response = await client.post("/api/v1/trading/orders", json=sell_order)
        assert response.status_code == 200
        
        # 4. Verify trade execution
        await asyncio.sleep(0.5)  # Allow batch processing
        
        response = await client.get(f"/api/v1/positions/{test_user['user_id']}")
        assert response.status_code == 200
        positions = response.json()
        
        # Should have GPU position
        gpu_position = next(
            (p for p in positions if p["instrument_id"] == "GPU_SPOT"), 
            None
        )
        assert gpu_position is not None
        assert Decimal(gpu_position["quantity"]) == Decimal("10")
        
    @pytest.mark.asyncio
    async def test_futures_trading_flow(self, client, test_user):
        """Test futures contract creation and trading"""
        
        # 1. Create futures contract
        futures_data = {
            "underlying": "GPU_COMPUTE",
            "expiry": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "contract_size": "100",
            "settlement_type": "physical"
        }
        
        response = await client.post(
            "/api/v1/markets/futures/create",
            json=futures_data
        )
        assert response.status_code == 200
        contract = response.json()
        contract_id = contract["contract_id"]
        
        # 2. Trade futures contract
        trade_data = {
            "user_id": test_user["user_id"],
            "contract_id": contract_id,
            "side": "buy",
            "quantity": "5",
            "order_type": "market"
        }
        
        response = await client.post(
            "/api/v1/trading/futures/trade",
            json=trade_data
        )
        assert response.status_code == 200
        
        # 3. Check position
        response = await client.get(
            f"/api/v1/positions/{test_user['user_id']}/futures"
        )
        assert response.status_code == 200
        futures_positions = response.json()
        assert len(futures_positions) > 0
        
    @pytest.mark.asyncio
    async def test_options_flow_with_margin(self, client, test_user):
        """Test options trading with margin checks"""
        
        # 1. Add collateral
        collateral_data = {
            "user_id": test_user["user_id"],
            "asset_type": "CASH",
            "asset_id": "USD",
            "amount": "10000"
        }
        
        response = await client.post(
            "/api/v1/margin/add-collateral",
            json=collateral_data
        )
        assert response.status_code == 200
        
        # 2. Create option
        option_data = {
            "underlying": "GPU_SPOT",
            "option_type": "call",
            "strike_price": "50",
            "expiry": (datetime.utcnow() + timedelta(days=7)).isoformat(),
            "contract_size": "100"
        }
        
        response = await client.post(
            "/api/v1/options/create",
            json=option_data
        )
        assert response.status_code == 200
        option = response.json()
        option_id = option["option_id"]
        
        # 3. Trade option with margin check
        trade_data = {
            "user_id": test_user["user_id"],
            "option_id": option_id,
            "quantity": "10",
            "side": "buy"
        }
        
        response = await client.post(
            "/api/v1/options/trade",
            json=trade_data
        )
        assert response.status_code == 200
        result = response.json()
        assert result["success"] is True
        
        # 4. Check margin account
        response = await client.get(
            f"/api/v1/margin/account/{test_user['user_id']}"
        )
        assert response.status_code == 200
        margin_account = response.json()
        assert Decimal(margin_account["used_margin"]) > 0
        
    @pytest.mark.asyncio
    async def test_performance_endpoints(self, client):
        """Test performance monitoring endpoints"""
        
        # 1. Cache statistics
        response = await client.get("/api/v1/performance/cache/stats")
        assert response.status_code == 200
        cache_stats = response.json()
        assert "performance_summary" in cache_stats
        assert "cache_stats" in cache_stats
        
        # 2. Database statistics
        response = await client.get("/api/v1/performance/database/stats")
        assert response.status_code == 200
        db_stats = response.json()
        assert len(db_stats) > 0
        
        # 3. System metrics
        response = await client.get("/api/v1/performance/system/metrics")
        assert response.status_code == 200
        metrics = response.json()
        assert "system" in metrics
        assert "cache" in metrics
        assert "database" in metrics
        
    @pytest.mark.asyncio
    async def test_batch_order_processing(self, client):
        """Test batch order processing performance"""
        
        # Place multiple orders quickly
        orders = []
        for i in range(50):
            order = {
                "user_id": f"batch_user_{i}",
                "instrument_id": "CPU_SPOT",
                "side": random.choice(["buy", "sell"]),
                "price": str(random.uniform(4, 6)),
                "quantity": str(random.randint(10, 100)),
                "order_type": "limit"
            }
            orders.append(order)
            
        # Send orders concurrently
        start_time = time.time()
        
        tasks = [
            client.post("/api/v1/trading/orders", json=order)
            for order in orders
        ]
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        successful = sum(
            1 for r in responses 
            if not isinstance(r, Exception) and r.status_code == 200
        )
        
        # Should process efficiently
        assert successful == len(orders)
        assert elapsed < 5.0  # Should complete within 5 seconds
        
        throughput = successful / elapsed
        print(f"Batch order throughput: {throughput:.2f} orders/second")
        
    @pytest.mark.asyncio
    async def test_volatility_surface(self, client):
        """Test volatility surface functionality"""
        
        # Add market data points
        for i in range(10):
            vol_point = {
                "instrument_id": "GPU_SPOT",
                "strike_price": str(45 + i * 2),
                "expiry_days": random.choice([7, 14, 30]),
                "implied_volatility": str(0.2 + random.uniform(-0.05, 0.05)),
                "option_type": "call"
            }
            
            response = await client.post(
                "/api/v1/markets/volatility/add-point",
                json=vol_point
            )
            assert response.status_code in [200, 201]
            
        # Query volatility
        response = await client.get(
            "/api/v1/markets/volatility/GPU_SPOT?strike=50&expiry_days=14"
        )
        assert response.status_code == 200
        vol_data = response.json()
        assert "implied_volatility" in vol_data
        assert 0.1 < float(vol_data["implied_volatility"]) < 0.5
        
    @pytest.mark.asyncio
    async def test_settlement_flow(self, client, test_user):
        """Test physical settlement process"""
        
        # 1. Create futures contract with near expiry
        futures_data = {
            "underlying": "GPU_COMPUTE",
            "expiry": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
            "contract_size": "10",
            "settlement_type": "physical"
        }
        
        response = await client.post(
            "/api/v1/markets/futures/create",
            json=futures_data
        )
        assert response.status_code == 200
        contract = response.json()
        
        # 2. Trade the contract
        trade_data = {
            "user_id": test_user["user_id"],
            "contract_id": contract["contract_id"],
            "side": "buy",
            "quantity": "1",
            "order_type": "market"
        }
        
        response = await client.post(
            "/api/v1/trading/futures/trade",
            json=trade_data
        )
        assert response.status_code == 200
        
        # 3. Trigger settlement check
        response = await client.post(
            f"/api/v1/settlements/check/{contract['contract_id']}"
        )
        assert response.status_code in [200, 202]
        
        # 4. Check settlement status
        response = await client.get(
            f"/api/v1/settlements/contract/{contract['contract_id']}"
        )
        assert response.status_code == 200
        settlement = response.json()
        assert settlement["status"] in ["pending", "provisioning", "active"]
        
    @pytest.mark.asyncio
    async def test_risk_limits(self, client, test_user):
        """Test risk limit enforcement"""
        
        # 1. Set position limit
        limit_data = {
            "user_id": test_user["user_id"],
            "instrument_id": "GPU_SPOT",
            "max_position": "100",
            "max_order_size": "20"
        }
        
        response = await client.post(
            "/api/v1/risk/limits/set",
            json=limit_data
        )
        assert response.status_code == 200
        
        # 2. Try to exceed limit
        large_order = {
            "user_id": test_user["user_id"],
            "instrument_id": "GPU_SPOT",
            "side": "buy",
            "price": "50",
            "quantity": "25",  # Exceeds max_order_size
            "order_type": "limit"
        }
        
        response = await client.post(
            "/api/v1/trading/orders",
            json=large_order
        )
        
        # Should be rejected
        assert response.status_code == 400
        error = response.json()
        assert "limit" in error["detail"].lower()
        
    @pytest.mark.asyncio
    async def test_market_making_flow(self, client):
        """Test market making functionality"""
        
        # 1. Register as market maker
        mm_data = {
            "user_id": "market_maker_1",
            "instruments": ["GPU_SPOT", "CPU_SPOT"],
            "min_spread": "0.01",
            "max_position": "1000"
        }
        
        response = await client.post(
            "/api/v1/market-making/register",
            json=mm_data
        )
        assert response.status_code == 200
        
        # 2. Submit quotes
        quote_data = {
            "market_maker_id": "market_maker_1",
            "instrument_id": "GPU_SPOT",
            "bid_price": "49.95",
            "bid_size": "100",
            "ask_price": "50.05",
            "ask_size": "100"
        }
        
        response = await client.post(
            "/api/v1/market-making/quote",
            json=quote_data
        )
        assert response.status_code == 200
        
        # 3. Check market depth
        response = await client.get(
            "/api/v1/markets/depth/GPU_SPOT"
        )
        assert response.status_code == 200
        depth = response.json()
        assert len(depth["buy_levels"]) > 0
        assert len(depth["sell_levels"]) > 0


class TestPerformanceOptimizations:
    """Test performance optimization features"""
    
    @pytest.fixture(scope="class")
    async def client(self):
        """Create test client"""
        async with httpx.AsyncClient(
            base_url="http://localhost:8000",
            timeout=30.0
        ) as client:
            yield client
            
    @pytest.mark.asyncio
    async def test_cache_warmup(self, client):
        """Test cache warmup functionality"""
        
        # Trigger cache optimization
        response = await client.post(
            "/api/v1/performance/cache/optimize/market_data"
        )
        assert response.status_code == 200
        
        # Verify cache is warmed up
        response = await client.get("/api/v1/performance/cache/stats")
        assert response.status_code == 200
        stats = response.json()
        
        market_data_stats = stats["cache_stats"].get("market_data", {})
        assert market_data_stats.get("size", 0) > 0
        
    @pytest.mark.asyncio
    async def test_query_optimization(self, client):
        """Test SQL query optimization"""
        
        test_query = """
        SELECT instrument_id, price, volume 
        FROM market_data 
        WHERE instrument_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 100
        """
        
        response = await client.get(
            "/api/v1/performance/query/analyze",
            params={"sql": test_query, "cache_name": "market_data"}
        )
        
        assert response.status_code == 200
        analysis = response.json()
        assert "query_plan" in analysis
        assert "optimization_hints" in analysis
        
    @pytest.mark.asyncio
    async def test_concurrent_performance(self, client):
        """Test system performance under concurrent load"""
        
        async def make_requests(request_type: str, count: int):
            """Make concurrent requests of a specific type"""
            tasks = []
            
            for i in range(count):
                if request_type == "price":
                    task = client.get("/api/v1/markets/spot-price/GPU_SPOT")
                elif request_type == "order":
                    task = client.post(
                        "/api/v1/trading/orders",
                        json={
                            "user_id": f"perf_user_{i}",
                            "instrument_id": "GPU_SPOT",
                            "side": "buy",
                            "price": "50",
                            "quantity": "1",
                            "order_type": "limit"
                        }
                    )
                elif request_type == "position":
                    task = client.get(f"/api/v1/positions/perf_user_{i}")
                    
                tasks.append(task)
                
            return await asyncio.gather(*tasks, return_exceptions=True)
            
        # Run different request types concurrently
        start_time = time.time()
        
        results = await asyncio.gather(
            make_requests("price", 100),
            make_requests("order", 50),
            make_requests("position", 50)
        )
        
        elapsed = time.time() - start_time
        
        # Count successful requests
        total_requests = 200
        successful_requests = sum(
            1 for result_group in results
            for r in result_group
            if not isinstance(r, Exception) and r.status_code in [200, 201]
        )
        
        success_rate = successful_requests / total_requests
        requests_per_second = total_requests / elapsed
        
        print(f"\nConcurrent test results:")
        print(f"  - Success rate: {success_rate*100:.1f}%")
        print(f"  - Requests/second: {requests_per_second:.2f}")
        print(f"  - Total time: {elapsed:.2f}s")
        
        # Should handle load efficiently
        assert success_rate > 0.95  # 95% success rate
        assert requests_per_second > 50  # At least 50 req/s


@pytest.mark.asyncio
async def test_end_to_end_trading_scenario(client):
    """Complete end-to-end trading scenario"""
    
    # Create test users
    trader1 = f"trader_{uuid.uuid4()}"
    trader2 = f"trader_{uuid.uuid4()}"
    
    # 1. Both traders add collateral
    for trader in [trader1, trader2]:
        response = await client.post(
            "/api/v1/margin/add-collateral",
            json={
                "user_id": trader,
                "asset_type": "CASH",
                "asset_id": "USD",
                "amount": "50000"
            }
        )
        assert response.status_code == 200
        
    # 2. Trader1 creates and sells call options
    option_data = {
        "underlying": "GPU_SPOT",
        "option_type": "call",
        "strike_price": "55",
        "expiry": (datetime.utcnow() + timedelta(days=30)).isoformat(),
        "contract_size": "100"
    }
    
    response = await client.post("/api/v1/options/create", json=option_data)
    assert response.status_code == 200
    option = response.json()
    
    # Sell options
    response = await client.post(
        "/api/v1/options/trade",
        json={
            "user_id": trader1,
            "option_id": option["option_id"],
            "quantity": "10",
            "side": "sell"
        }
    )
    assert response.status_code == 200
    
    # 3. Trader2 buys the options
    response = await client.post(
        "/api/v1/options/trade",
        json={
            "user_id": trader2,
            "option_id": option["option_id"],
            "quantity": "10",
            "side": "buy"
        }
    )
    assert response.status_code == 200
    
    # 4. Create futures position
    futures_data = {
        "underlying": "GPU_COMPUTE",
        "expiry": (datetime.utcnow() + timedelta(days=60)).isoformat(),
        "contract_size": "100",
        "settlement_type": "physical"
    }
    
    response = await client.post(
        "/api/v1/markets/futures/create",
        json=futures_data
    )
    assert response.status_code == 200
    futures = response.json()
    
    # 5. Trade futures
    for trader, side in [(trader1, "buy"), (trader2, "sell")]:
        response = await client.post(
            "/api/v1/trading/futures/trade",
            json={
                "user_id": trader,
                "contract_id": futures["contract_id"],
                "side": side,
                "quantity": "5",
                "order_type": "market"
            }
        )
        assert response.status_code == 200
        
    # 6. Check final positions
    for trader in [trader1, trader2]:
        # Options positions
        response = await client.get(f"/api/v1/positions/{trader}/options")
        assert response.status_code == 200
        options_positions = response.json()
        assert len(options_positions) > 0
        
        # Futures positions
        response = await client.get(f"/api/v1/positions/{trader}/futures")
        assert response.status_code == 200
        futures_positions = response.json()
        assert len(futures_positions) > 0
        
        # Margin status
        response = await client.get(f"/api/v1/margin/account/{trader}")
        assert response.status_code == 200
        margin = response.json()
        assert Decimal(margin["used_margin"]) > 0
        assert Decimal(margin["available_margin"]) > 0
        
    print("\n✅ End-to-end trading scenario completed successfully!")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "-s"]) 