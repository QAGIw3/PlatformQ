#!/usr/bin/env python3
"""
Compute Futures Physical Settlement Demo

This demo showcases the complete workflow of:
1. Creating compute futures contracts
2. Day-ahead market bidding
3. Physical settlement with automated provisioning
4. Failover handling
5. SLA monitoring and penalties
6. Quality derivatives (latency futures, uptime swaps, performance bonds)
"""

import asyncio
import httpx
from datetime import datetime, timedelta
from decimal import Decimal
import random
import json


class ComputeFuturesDemo:
    """Demo class for compute futures settlement"""
    
    def __init__(self):
        self.derivatives_url = "http://localhost:8000"  # Derivatives engine service
        self.provisioning_url = "http://localhost:8001"  # Provisioning service
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Demo users
        self.buyer_id = "buyer_123"
        self.provider_id = "provider_aws"
        self.failover_providers = ["provider_rackspace", "provider_azure", "provider_onprem"]
        
    async def setup_failover_providers(self):
        """Register failover providers"""
        print("\n=== Setting up Failover Providers ===")
        
        for i, provider in enumerate(self.failover_providers):
            response = await self.client.post(
                f"{self.derivatives_url}/api/v1/compute-futures/failover/register-provider",
                json={
                    "resource_type": "gpu",
                    "provider_id": provider,
                    "priority": (i + 1) * 10  # 10, 20, 30
                }
            )
            print(f"Registered {provider} as failover with priority {(i + 1) * 10}")
            
    async def create_futures_contract(self):
        """Create a compute futures contract"""
        print("\n=== Creating Compute Futures Contract ===")
        
        response = await self.client.post(
            f"{self.derivatives_url}/api/v1/compute-futures/futures/create-contract",
            json={
                "resource_type": "gpu",
                "quantity": "10",
                "location_zone": "us-east-1",
                "quality_specs": {
                    "gpu_model": "nvidia-a100",
                    "memory_gb": 40
                },
                "duration_hours": 24,
                "start_time": (datetime.utcnow() + timedelta(days=7)).isoformat() + "Z",
                "contract_months": 3
            }
        )
        
        contract = response.json()
        print(f"Created contract: {contract['symbol']}")
        print(f"Contract ID: {contract['contract_id']}")
        print(f"Margin requirement: ${contract['margin_requirement']}")
        
        return contract["contract_id"]
        
    async def submit_day_ahead_bids(self):
        """Submit bids to day-ahead market"""
        print("\n=== Day-Ahead Market Bidding ===")
        
        delivery_date = datetime.utcnow() + timedelta(days=1)
        
        # Submit bids for different hours
        for hour in [10, 14, 18]:  # Peak hours
            response = await self.client.post(
                f"{self.derivatives_url}/api/v1/compute-futures/day-ahead/submit-bid",
                params={"delivery_date": delivery_date.isoformat() + "Z"},
                json={
                    "hour": hour,
                    "resource_type": "gpu",
                    "quantity": "5",
                    "max_price": "100",
                    "flexible": hour != 14  # 2pm is not flexible
                }
            )
            
            result = response.json()
            print(f"Hour {hour}: Bid {result['bid_id']}, "
                  f"Estimated price: ${result['estimated_clearing_price']}")
            
    async def initiate_settlement(self, contract_id: str):
        """Initiate physical settlement"""
        print("\n=== Initiating Physical Settlement ===")
        
        response = await self.client.post(
            f"{self.derivatives_url}/api/v1/compute-futures/settlement/initiate",
            json={
                "contract_id": contract_id,
                "provider_id": self.provider_id,
                "sla_strict": True
            }
        )
        
        settlement = response.json()
        print(f"Settlement ID: {settlement['settlement_id']}")
        print(f"Status: {settlement['status']}")
        print(f"Delivery start: {settlement['delivery_start']}")
        
        return settlement['settlement_id']
        
    async def simulate_provisioning_failure(self, settlement_id: str):
        """Simulate a provisioning failure to trigger failover"""
        print("\n=== Simulating Provisioning Failure ===")
        
        # First, check current status
        response = await self.client.get(
            f"{self.derivatives_url}/api/v1/compute-futures/settlement/{settlement_id}/status"
        )
        
        status = response.json()
        print(f"Current status: {status['status']}")
        
        # In a real scenario, the provisioning service would fail
        # and the compute futures engine would automatically failover
        print("Primary provider failed - initiating automatic failover...")
        
        # Wait for failover to complete
        await asyncio.sleep(2)
        
        # Check status again
        response = await self.client.get(
            f"{self.derivatives_url}/api/v1/compute-futures/settlement/{settlement_id}/status"
        )
        
        status = response.json()
        print(f"Failover used: {status['failover_used']}")
        print(f"Failover provider: {status['failover_provider']}")
        
    async def monitor_sla_compliance(self, settlement_id: str):
        """Monitor SLA compliance"""
        print("\n=== SLA Monitoring ===")
        
        # Simulate monitoring over time
        for i in range(3):
            await asyncio.sleep(1)
            
            # Get metrics
            response = await self.client.get(
                f"{self.provisioning_url}/api/v1/metrics/compute/{settlement_id}"
            )
            
            metrics = response.json()
            print(f"\nTime {i+1}:")
            print(f"  Uptime: {metrics['uptime_percent']}%")
            print(f"  Latency: {metrics['latency_ms']}ms")
            print(f"  Performance: {metrics['performance_score']}")
            
            # Check for violations
            response = await self.client.get(
                f"{self.derivatives_url}/api/v1/compute-futures/settlement/{settlement_id}/status"
            )
            
            status = response.json()
            if status['sla_violations']:
                print(f"  SLA Violations: {len(status['sla_violations'])}")
                print(f"  Penalties: ${status['penalty_amount']}")
                
    async def create_quality_derivatives(self):
        """Create quality derivatives for risk management"""
        print("\n=== Creating Quality Derivatives ===")
        
        # 1. Latency Future
        print("\n1. Latency Future (Network Performance Hedge)")
        response = await self.client.post(
            f"{self.derivatives_url}/api/v1/compute-futures/quality/latency-future",
            json={
                "source_region": "us-east-1",
                "dest_region": "eu-west-1",
                "strike_latency_ms": 50,
                "notional": "10000",
                "expiry_days": 30,
                "side": "buy"
            }
        )
        
        latency_future = response.json()
        print(f"  Contract: {latency_future['contract_id']}")
        print(f"  Strike: {latency_future['strike_latency_ms']}ms")
        print(f"  Notional: ${latency_future['notional']}")
        
        # 2. Uptime Swap
        print("\n2. Uptime Swap (Service Reliability Hedge)")
        response = await self.client.post(
            f"{self.derivatives_url}/api/v1/compute-futures/quality/uptime-swap",
            json={
                "service_id": "gpu-cluster-01",
                "fixed_uptime_percent": "99.9",
                "notional_per_hour": "100",
                "duration_days": 30,
                "side": "fixed_payer"
            }
        )
        
        uptime_swap = response.json()
        print(f"  Swap ID: {uptime_swap['swap_id']}")
        print(f"  Fixed rate: {uptime_swap['fixed_rate']}")
        print(f"  Notional/hour: ${uptime_swap['notional_per_hour']}")
        
        # 3. Performance Bond
        print("\n3. Performance Bond (Hardware Guarantee)")
        response = await self.client.post(
            f"{self.derivatives_url}/api/v1/compute-futures/quality/performance-bond",
            json={
                "gpu_model": "nvidia-a100",
                "guaranteed_performance_percent": "95",
                "bond_amount": "50000",
                "expiry_days": 90,
                "as_issuer": False
            }
        )
        
        performance_bond = response.json()
        print(f"  Bond ID: {performance_bond['bond_id']}")
        print(f"  Guaranteed: {performance_bond['guaranteed_performance']}")
        print(f"  Amount: ${performance_bond['bond_amount']}")
        
    async def check_market_clearing(self):
        """Check day-ahead market clearing results"""
        print("\n=== Market Clearing Results ===")
        
        delivery_date = datetime.utcnow() + timedelta(days=1)
        
        response = await self.client.get(
            f"{self.derivatives_url}/api/v1/compute-futures/day-ahead/market-clearing",
            params={
                "delivery_date": delivery_date.isoformat() + "Z",
                "resource_type": "gpu"
            }
        )
        
        results = response.json()
        print(f"Total cleared volume: {results['total_cleared_volume']} GPU-hours")
        print("\nHourly clearing prices:")
        
        for hour, price in results['clearing_prices'].items():
            print(f"  Hour {hour}: ${price}")
            
    async def get_realtime_imbalance(self):
        """Get real-time imbalance pricing"""
        print("\n=== Real-Time Imbalance Pricing ===")
        
        response = await self.client.get(
            f"{self.derivatives_url}/api/v1/compute-futures/real-time/imbalance-price",
            params={"resource_type": "gpu"}
        )
        
        imbalance = response.json()
        print(f"Current imbalance: {imbalance['current_imbalance_mw']} units")
        print(f"Direction: {imbalance['imbalance_direction']}")
        print(f"Real-time price: ${imbalance['realtime_price']}")
        print(f"Day-ahead price: ${imbalance['day_ahead_price']}")
        print(f"Price spread: ${imbalance['price_spread']}")
        
    async def run_demo(self):
        """Run the complete demo"""
        print("=" * 60)
        print("Compute Futures Physical Settlement Demo")
        print("=" * 60)
        
        try:
            # 1. Setup failover providers
            await self.setup_failover_providers()
            
            # 2. Create futures contract
            contract_id = await self.create_futures_contract()
            
            # 3. Submit day-ahead bids
            await self.submit_day_ahead_bids()
            
            # 4. Check market clearing
            await self.check_market_clearing()
            
            # 5. Get real-time imbalance
            await self.get_realtime_imbalance()
            
            # 6. Create quality derivatives
            await self.create_quality_derivatives()
            
            # 7. Initiate settlement
            settlement_id = await self.initiate_settlement(contract_id)
            
            # 8. Simulate provisioning failure and failover
            await self.simulate_provisioning_failure(settlement_id)
            
            # 9. Monitor SLA compliance
            await self.monitor_sla_compliance(settlement_id)
            
            print("\n" + "=" * 60)
            print("Demo completed successfully!")
            print("=" * 60)
            
        except Exception as e:
            print(f"\nError during demo: {e}")
            
        finally:
            await self.client.aclose()


async def main():
    """Main entry point"""
    demo = ComputeFuturesDemo()
    await demo.run_demo()


if __name__ == "__main__":
    asyncio.run(main()) 