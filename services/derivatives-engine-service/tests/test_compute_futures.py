"""
Tests for Compute Futures functionality
"""

import pytest
import pytest_asyncio
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock

from app.engines.compute_futures_engine import (
    ComputeFuturesEngine,
    DayAheadMarket,
    ComputeBid,
    ComputeOffer,
    MarketClearingResult
)
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient


@pytest_asyncio.fixture
async def compute_futures_engine():
    """Create test compute futures engine"""
    ignite = IgniteCache()
    pulsar = PulsarEventPublisher()
    oracle = OracleAggregatorClient()
    
    engine = ComputeFuturesEngine(ignite, pulsar, oracle)
    await engine.start()
    
    yield engine
    
    # Cleanup
    await engine.stop()


@pytest.mark.asyncio
async def test_day_ahead_market_clearing(compute_futures_engine):
    """Test day-ahead market clearing mechanism"""
    delivery_date = datetime.utcnow() + timedelta(days=1)
    dam = await compute_futures_engine.get_day_ahead_market(
        delivery_date=delivery_date,
        resource_type="gpu"
    )
    
    # Submit bids
    bid1 = await dam.submit_bid(
        user_id="buyer1",
        hour=14,  # 2 PM
        quantity=Decimal("100"),  # 100 GPU hours
        max_price=Decimal("50")  # $50/hour max
    )
    
    bid2 = await dam.submit_bid(
        user_id="buyer2",
        hour=14,
        quantity=Decimal("50"),
        max_price=Decimal("45")
    )
    
    # Submit offers
    offer1 = await dam.submit_offer(
        provider_id="provider1",
        hour=14,
        quantity=Decimal("80"),
        min_price=Decimal("40"),
        ramp_rate=Decimal("10"),
        location_zone="us-east-1"
    )
    
    offer2 = await dam.submit_offer(
        provider_id="provider2",
        hour=14,
        quantity=Decimal("70"),
        min_price=Decimal("42"),
        ramp_rate=Decimal("15"),
        location_zone="us-west-2"
    )
    
    # Run market clearing
    results = await dam.clear_market()
    result = results[14]  # Get result for hour 14
    
    assert result["clearing_price"] > Decimal("40")
    assert result["clearing_price"] <= Decimal("50")
    assert result["cleared_quantity"] == Decimal("150")  # All supply cleared
    assert result["accepted_bids"] == 2
    assert result["accepted_offers"] == 2


@pytest.mark.asyncio
async def test_capacity_auction(compute_futures_engine):
    """Test capacity auction for future compute resources"""
    auction = compute_futures_engine.capacity_auction
    
    # Create auction for next month's GPU capacity
    auction_id = await auction.create_auction(
        resource_type="gpu",
        delivery_month=datetime.utcnow() + timedelta(days=30),
        total_capacity=Decimal("10000"),  # 10,000 GPU hours
        reserve_price=Decimal("35")  # $35/hour minimum
    )
    
    # Submit bids
    await auction.submit_capacity_bid(
        auction_id=auction_id,
        bidder_id="bidder1",
        quantity=Decimal("5000"),
        price=Decimal("45")
    )
    
    await auction.submit_capacity_bid(
        auction_id=auction_id,
        bidder_id="bidder2",
        quantity=Decimal("3000"),
        price=Decimal("42")
    )
    
    await auction.submit_capacity_bid(
        auction_id=auction_id,
        bidder_id="bidder3",
        quantity=Decimal("4000"),
        price=Decimal("38")
    )
    
    # Run auction
    results = await auction.run_auction(auction_id)
    
    assert results["clearing_price"] >= Decimal("38")
    assert results["total_allocated"] <= Decimal("10000")
    assert len(results["winning_bids"]) >= 2


@pytest.mark.asyncio
async def test_ancillary_services(compute_futures_engine):
    """Test ancillary services market (redundancy, priority, etc.)"""
    ancillary = compute_futures_engine.ancillary_services
    
    # Register redundancy service
    service_id = await ancillary.register_service(
        provider_id="provider1",
        service_type="redundancy",
        capacity=Decimal("1000"),  # Can backup 1000 GPU hours
        price=Decimal("5")  # $5/hour premium
    )
    
    # User requests redundancy
    booking = await ancillary.book_service(
        user_id="user1",
        service_type="redundancy",
        quantity=Decimal("100"),
        duration_hours=24
    )
    
    assert booking["service_id"] == service_id
    assert booking["total_cost"] == Decimal("100") * Decimal("5") * 24
    
    # Test priority access
    priority_id = await ancillary.register_service(
        provider_id="provider2",
        service_type="priority",
        capacity=Decimal("500"),
        price=Decimal("10")  # $10/hour for priority
    )
    
    priority_booking = await ancillary.book_service(
        user_id="user2",
        service_type="priority",
        quantity=Decimal("50"),
        duration_hours=4
    )
    
    assert priority_booking["guaranteed_latency"] == "< 10ms"
    assert priority_booking["sla_penalty"] == "10x refund"


@pytest.mark.asyncio
async def test_compute_futures_api(compute_futures_engine, test_client):
    """Test compute futures API endpoints"""
    # Submit day-ahead bid
    response = await test_client.post(
        "/api/v1/compute-futures/day-ahead/bid",
        json={
            "resource_type": "gpu",
            "hour": 14,
            "quantity": "100",
            "max_price": "50"
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "bid_id" in data
    
    # Get market status
    response = await test_client.get(
        "/api/v1/compute-futures/day-ahead/status",
        params={"resource_type": "gpu", "hour": 14}
    )
    
    assert response.status_code == 200
    status = response.json()
    assert "total_bids" in status
    assert "total_offers" in status
    assert "indicative_price" in status
    
    # Submit capacity auction bid
    response = await test_client.post(
        "/api/v1/compute-futures/capacity/bid",
        json={
            "resource_type": "tpu",
            "delivery_month": "2024-02",
            "quantity": "5000",
            "price": "60"
        }
    )
    
    assert response.status_code == 200
    assert response.json()["success"] is True


@pytest.mark.asyncio
async def test_physical_delivery(compute_futures_engine):
    """Test physical delivery of compute resources"""
    # Create a settled future
    contract_id = "CF_GPU_2024_01_15_14"
    
    await compute_futures_engine.record_settlement(
        contract_id=contract_id,
        buyer_id="buyer1",
        provider_id="provider1",
        resource_type="gpu",
        quantity=Decimal("100"),
        price=Decimal("45"),
        delivery_time=datetime.utcnow() + timedelta(hours=1)
    )
    
    # Simulate delivery time
    await asyncio.sleep(0.1)  # In real test would wait or mock time
    
    # Provider marks resources as available
    await compute_futures_engine.mark_resources_available(
        contract_id=contract_id,
        provider_id="provider1",
        resource_ids=["gpu-001", "gpu-002", "gpu-003"]  # etc
    )
    
    # Buyer claims resources
    claim = await compute_futures_engine.claim_resources(
        contract_id=contract_id,
        buyer_id="buyer1"
    )
    
    assert claim["status"] == "allocated"
    assert len(claim["resource_ids"]) > 0
    assert claim["access_credentials"] is not None
    
    # Test SLA monitoring
    sla_status = await compute_futures_engine.check_sla_compliance(
        contract_id=contract_id
    )
    
    assert sla_status["uptime"] >= 0.99  # 99% uptime
    assert sla_status["performance_score"] >= 0.95


@pytest.mark.asyncio
async def test_market_analytics(compute_futures_engine):
    """Test market analytics and pricing signals"""
    analytics = await compute_futures_engine.get_market_analytics(
        resource_type="gpu",
        lookback_days=7
    )
    
    assert "average_price" in analytics
    assert "price_volatility" in analytics
    assert "supply_demand_ratio" in analytics
    assert "peak_hours" in analytics
    assert "price_forecast" in analytics
    
    # Test forward curve
    forward_curve = await compute_futures_engine.get_forward_curve(
        resource_type="gpu",
        periods=24  # Next 24 hours
    )
    
    assert len(forward_curve) == 24
    for hour_data in forward_curve:
        assert "hour" in hour_data
        assert "expected_price" in hour_data
        assert "confidence_interval" in hour_data


@pytest.mark.asyncio
async def test_cross_resource_optimization(compute_futures_engine):
    """Test optimization across different compute resources"""
    # User needs either GPU or TPU for ML training
    optimization = await compute_futures_engine.optimize_resource_mix(
        user_id="ml_user",
        workload_type="deep_learning",
        total_compute_hours=Decimal("1000"),
        budget=Decimal("50000"),
        deadline=datetime.utcnow() + timedelta(days=7)
    )
    
    assert "resource_allocation" in optimization
    assert "total_cost" in optimization
    assert optimization["total_cost"] <= Decimal("50000")
    assert "gpu_hours" in optimization["resource_allocation"]
    assert "tpu_hours" in optimization["resource_allocation"]
    assert "expected_completion" in optimization


@pytest.mark.asyncio
async def test_market_failures_handling(compute_futures_engine):
    """Test handling of market failures and edge cases"""
    dam = compute_futures_engine.day_ahead_market
    
    # Test no supply scenario
    await dam.submit_bid(
        user_id="desperate_buyer",
        hour=3,  # 3 AM
        resource_type="h100_gpu",  # High-end GPU
        quantity=Decimal("1000"),
        max_price=Decimal("1000")  # Very high price
    )
    
    # No offers submitted
    result = await dam.clear_market("h100_gpu", 3)
    
    assert result.clearing_price is None
    assert result.cleared_quantity == Decimal("0")
    assert len(result.unmatched_bids) == 1
    
    # Test circuit breaker
    # Submit many volatile prices
    for i in range(10):
        price = Decimal("50") * (1 + i * 0.5)  # Rapidly increasing prices
        await dam.submit_offer(
            provider_id=f"volatile_provider_{i}",
            hour=16,
            resource_type="gpu",
            quantity=Decimal("10"),
            min_price=price
        )
    
    # Should trigger circuit breaker
    with pytest.raises(Exception) as exc_info:
        await dam.clear_market("gpu", 16)
    
    assert "circuit breaker" in str(exc_info.value).lower() 