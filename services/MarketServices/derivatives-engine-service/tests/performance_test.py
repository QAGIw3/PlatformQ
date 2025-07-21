#!/usr/bin/env python3
"""
Performance Testing Script for Optimized Derivatives Engine

Tests the performance improvements and generates metrics
"""

import asyncio
import time
import random
from decimal import Decimal
from datetime import datetime, timedelta
import httpx
import numpy as np
from typing import List, Dict
import json


class PerformanceTester:
    """Performance testing for derivatives engine"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)
        self.results = {
            "order_placement": [],
            "price_queries": [],
            "option_pricing": [],
            "cache_hits": [],
            "database_queries": []
        }
        
    async def test_order_placement_throughput(self, num_orders: int = 1000):
        """Test order placement throughput with batching"""
        
        print(f"\n🚀 Testing order placement throughput ({num_orders} orders)...")
        
        orders = []
        for i in range(num_orders):
            order = {
                "instrument_id": random.choice(["GPU_SPOT", "CPU_SPOT"]),
                "side": random.choice(["buy", "sell"]),
                "price": str(random.uniform(40, 60)),
                "quantity": str(random.randint(1, 100)),
                "order_type": "limit"
            }
            orders.append(order)
            
        # Test batch processing
        start_time = time.time()
        
        tasks = []
        batch_size = 100
        
        for i in range(0, len(orders), batch_size):
            batch = orders[i:i+batch_size]
            for order in batch:
                task = self.client.post(
                    f"{self.base_url}/api/v1/trading/orders",
                    json=order
                )
                tasks.append(task)
                
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        successful = sum(1 for r in responses if not isinstance(r, Exception))
        
        throughput = successful / elapsed
        
        print(f"✅ Order throughput: {throughput:.2f} orders/second")
        print(f"   Success rate: {successful/num_orders*100:.1f}%")
        print(f"   Total time: {elapsed:.2f}s")
        
        self.results["order_placement"].append({
            "throughput": throughput,
            "success_rate": successful/num_orders,
            "total_time": elapsed
        })
        
    async def test_price_query_performance(self, num_queries: int = 500):
        """Test price query performance with caching"""
        
        print(f"\n📊 Testing price query performance ({num_queries} queries)...")
        
        instruments = ["GPU_SPOT", "CPU_SPOT", "TPU_SPOT"]
        
        # First pass - cold cache
        cold_start = time.time()
        
        for _ in range(num_queries):
            instrument = random.choice(instruments)
            response = await self.client.get(
                f"{self.base_url}/api/v1/markets/spot-price/{instrument}"
            )
            
        cold_elapsed = time.time() - cold_start
        cold_qps = num_queries / cold_elapsed
        
        # Second pass - warm cache
        warm_start = time.time()
        
        for _ in range(num_queries):
            instrument = random.choice(instruments)
            response = await self.client.get(
                f"{self.base_url}/api/v1/markets/spot-price/{instrument}"
            )
            
        warm_elapsed = time.time() - warm_start
        warm_qps = num_queries / warm_elapsed
        
        cache_speedup = warm_qps / cold_qps
        
        print(f"✅ Cold cache QPS: {cold_qps:.2f}")
        print(f"✅ Warm cache QPS: {warm_qps:.2f}")
        print(f"✅ Cache speedup: {cache_speedup:.2f}x")
        
        self.results["price_queries"].append({
            "cold_qps": cold_qps,
            "warm_qps": warm_qps,
            "cache_speedup": cache_speedup
        })
        
    async def test_option_pricing_performance(self, num_options: int = 100):
        """Test option pricing performance"""
        
        print(f"\n💰 Testing option pricing performance ({num_options} options)...")
        
        start_time = time.time()
        
        tasks = []
        for _ in range(num_options):
            option_request = {
                "underlying": "GPU_SPOT",
                "option_type": random.choice(["call", "put"]),
                "strike_price": str(random.uniform(40, 60)),
                "expiry_days": random.randint(1, 30)
            }
            
            task = self.client.post(
                f"{self.base_url}/api/v1/options/price",
                json=option_request
            )
            tasks.append(task)
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        successful = sum(1 for r in responses if not isinstance(r, Exception))
        
        pricing_rate = successful / elapsed
        
        print(f"✅ Option pricing rate: {pricing_rate:.2f} options/second")
        print(f"   Average time per option: {elapsed/num_options*1000:.2f}ms")
        
        self.results["option_pricing"].append({
            "pricing_rate": pricing_rate,
            "avg_time_ms": elapsed/num_options*1000
        })
        
    async def test_cache_performance(self):
        """Test cache hit rates and performance"""
        
        print(f"\n🔥 Testing cache performance...")
        
        # Get cache statistics
        response = await self.client.get(
            f"{self.base_url}/api/v1/performance/cache/stats"
        )
        
        if response.status_code == 200:
            stats = response.json()
            
            # Calculate overall metrics
            overall_hit_rate = stats["performance_summary"]["overall_hit_rate"]
            
            print(f"✅ Overall cache hit rate: {overall_hit_rate:.1f}%")
            
            # Show cache-specific stats
            for cache_name, cache_stats in stats["cache_stats"].items():
                print(f"\n   {cache_name}:")
                print(f"     - Hit rate: {cache_stats['hit_rate']:.1f}%")
                print(f"     - Size: {cache_stats['size']}")
                print(f"     - Avg get time: {cache_stats['avg_get_time']:.3f}ms")
                
            self.results["cache_hits"] = stats
            
    async def test_database_performance(self):
        """Test database connection pool performance"""
        
        print(f"\n🗄️ Testing database performance...")
        
        # Get database statistics
        response = await self.client.get(
            f"{self.base_url}/api/v1/performance/database/stats"
        )
        
        if response.status_code == 200:
            stats = response.json()
            
            for db_name, db_stats in stats.items():
                print(f"\n   {db_name}:")
                print(f"     - Avg query time: {db_stats['avg_query_time_ms']:.2f}ms")
                print(f"     - Connection usage: {db_stats['active_connections']}/{db_stats['total_connections']}")
                print(f"     - Error rate: {db_stats['error_rate']*100:.2f}%")
                
            self.results["database_queries"] = stats
            
    async def stress_test_concurrent_users(self, num_users: int = 50, 
                                         duration_seconds: int = 30):
        """Simulate concurrent users trading"""
        
        print(f"\n👥 Stress testing with {num_users} concurrent users for {duration_seconds}s...")
        
        async def simulate_user(user_id: int):
            """Simulate a single user's activity"""
            actions_performed = 0
            errors = 0
            
            end_time = time.time() + duration_seconds
            
            while time.time() < end_time:
                action = random.choice([
                    "place_order",
                    "check_price",
                    "get_positions",
                    "price_option"
                ])
                
                try:
                    if action == "place_order":
                        await self.client.post(
                            f"{self.base_url}/api/v1/trading/orders",
                            json={
                                "instrument_id": f"{random.choice(['GPU', 'CPU'])}_SPOT",
                                "side": random.choice(["buy", "sell"]),
                                "price": str(random.uniform(40, 60)),
                                "quantity": str(random.randint(1, 10))
                            }
                        )
                    elif action == "check_price":
                        await self.client.get(
                            f"{self.base_url}/api/v1/markets/spot-price/GPU_SPOT"
                        )
                    elif action == "get_positions":
                        await self.client.get(
                            f"{self.base_url}/api/v1/positions/user{user_id}"
                        )
                    else:  # price_option
                        await self.client.post(
                            f"{self.base_url}/api/v1/options/price",
                            json={
                                "underlying": "GPU_SPOT",
                                "option_type": "call",
                                "strike_price": "50",
                                "expiry_days": 7
                            }
                        )
                        
                    actions_performed += 1
                    
                except Exception as e:
                    errors += 1
                    
                # Small delay between actions
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
            return actions_performed, errors
            
        # Run concurrent users
        start_time = time.time()
        
        tasks = [simulate_user(i) for i in range(num_users)]
        results = await asyncio.gather(*tasks)
        
        elapsed = time.time() - start_time
        
        total_actions = sum(r[0] for r in results)
        total_errors = sum(r[1] for r in results)
        
        actions_per_second = total_actions / elapsed
        error_rate = total_errors / total_actions if total_actions > 0 else 0
        
        print(f"✅ Total actions: {total_actions}")
        print(f"✅ Actions per second: {actions_per_second:.2f}")
        print(f"✅ Error rate: {error_rate*100:.2f}%")
        
    async def get_system_metrics(self):
        """Get overall system performance metrics"""
        
        print(f"\n📈 Getting system metrics...")
        
        response = await self.client.get(
            f"{self.base_url}/api/v1/performance/system/metrics"
        )
        
        if response.status_code == 200:
            metrics = response.json()
            
            print(f"\n   System:")
            print(f"     - CPU: {metrics['system']['cpu_percent']:.1f}%")
            print(f"     - Memory: {metrics['system']['memory_mb']:.1f} MB")
            print(f"     - Threads: {metrics['system']['threads']}")
            
            print(f"\n   Cache Performance:")
            cache_perf = metrics['cache']
            print(f"     - Total hits: {cache_perf['total_cache_hits']}")
            print(f"     - Hit rate: {cache_perf['overall_hit_rate']:.1f}%")
            
    def generate_report(self):
        """Generate performance test report"""
        
        print("\n" + "="*60)
        print("PERFORMANCE TEST REPORT")
        print("="*60)
        
        if self.results["order_placement"]:
            avg_throughput = np.mean([r["throughput"] for r in self.results["order_placement"]])
            print(f"\n📦 Order Placement:")
            print(f"   Average throughput: {avg_throughput:.2f} orders/second")
            
        if self.results["price_queries"]:
            avg_speedup = np.mean([r["cache_speedup"] for r in self.results["price_queries"]])
            print(f"\n💨 Cache Performance:")
            print(f"   Average cache speedup: {avg_speedup:.2f}x")
            
        if self.results["option_pricing"]:
            avg_time = np.mean([r["avg_time_ms"] for r in self.results["option_pricing"]])
            print(f"\n⚡ Option Pricing:")
            print(f"   Average pricing time: {avg_time:.2f}ms")
            
        # Save detailed results
        with open("performance_results.json", "w") as f:
            json.dump(self.results, f, indent=2, default=str)
            
        print(f"\n📄 Detailed results saved to performance_results.json")
        
    async def run_all_tests(self):
        """Run all performance tests"""
        
        print("🚀 Starting Performance Tests for Optimized Derivatives Engine")
        print("="*60)
        
        # Run tests
        await self.test_order_placement_throughput(1000)
        await self.test_price_query_performance(500)
        await self.test_option_pricing_performance(100)
        await self.test_cache_performance()
        await self.test_database_performance()
        await self.stress_test_concurrent_users(50, 30)
        await self.get_system_metrics()
        
        # Generate report
        self.generate_report()
        
        # Cleanup
        await self.client.aclose()


async def main():
    """Main test runner"""
    
    tester = PerformanceTester()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 