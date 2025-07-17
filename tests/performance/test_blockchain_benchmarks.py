import asyncio
import aiohttp
import time
import statistics
from typing import List, Dict, Any
import json
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import os

class BlockchainPerformanceBenchmarks:
    """Performance benchmarks for blockchain operations"""
    
    def __init__(self):
        self.api_url = "http://localhost:8001"  # blockchain-event-bridge
        self.results = {
            "operations": [],
            "summary": {}
        }
        
    async def benchmark_operation(self, operation_name: str, operation_func, iterations: int = 100):
        """Benchmark a single operation"""
        latencies = []
        errors = 0
        
        print(f"\n⏱️  Benchmarking {operation_name} ({iterations} iterations)...")
        
        for i in range(iterations):
            try:
                start = time.time()
                await operation_func()
                latency = (time.time() - start) * 1000  # Convert to ms
                latencies.append(latency)
                
                if (i + 1) % 10 == 0:
                    print(f"  Progress: {i + 1}/{iterations}")
                    
            except Exception as e:
                errors += 1
                print(f"  Error in iteration {i + 1}: {e}")
        
        # Calculate statistics
        if latencies:
            stats = {
                "operation": operation_name,
                "iterations": iterations,
                "errors": errors,
                "min_latency": min(latencies),
                "max_latency": max(latencies),
                "avg_latency": statistics.mean(latencies),
                "median_latency": statistics.median(latencies),
                "p95_latency": sorted(latencies)[int(len(latencies) * 0.95)],
                "p99_latency": sorted(latencies)[int(len(latencies) * 0.99)],
                "std_dev": statistics.stdev(latencies) if len(latencies) > 1 else 0
            }
            
            self.results["operations"].append(stats)
            self._print_stats(stats)
            
            return stats
        else:
            print(f"  ❌ No successful operations for {operation_name}")
            return None
    
    def _print_stats(self, stats: Dict[str, Any]):
        """Print benchmark statistics"""
        print(f"\n📊 Results for {stats['operation']}:")
        print(f"  ✓ Successful: {stats['iterations'] - stats['errors']}/{stats['iterations']}")
        print(f"  ✗ Errors: {stats['errors']}")
        print(f"  ⚡ Min latency: {stats['min_latency']:.2f}ms")
        print(f"  ⚡ Max latency: {stats['max_latency']:.2f}ms")
        print(f"  ⚡ Avg latency: {stats['avg_latency']:.2f}ms")
        print(f"  ⚡ Median latency: {stats['median_latency']:.2f}ms")
        print(f"  ⚡ P95 latency: {stats['p95_latency']:.2f}ms")
        print(f"  ⚡ P99 latency: {stats['p99_latency']:.2f}ms")
        print(f"  ⚡ Std deviation: {stats['std_dev']:.2f}ms")
    
    async def run_load_test(self, operation_func, concurrent_users: int = 50, duration_seconds: int = 60):
        """Run a load test with concurrent users"""
        print(f"\n🔥 Running load test: {concurrent_users} concurrent users for {duration_seconds}s")
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        operations_completed = 0
        errors = 0
        latencies = []
        
        async def user_simulation():
            nonlocal operations_completed, errors
            
            while time.time() < end_time:
                try:
                    op_start = time.time()
                    await operation_func()
                    latency = (time.time() - op_start) * 1000
                    latencies.append(latency)
                    operations_completed += 1
                except Exception:
                    errors += 1
                
                # Small delay between operations
                await asyncio.sleep(0.1)
        
        # Run concurrent users
        tasks = [user_simulation() for _ in range(concurrent_users)]
        await asyncio.gather(*tasks)
        
        # Calculate throughput
        total_time = time.time() - start_time
        throughput = operations_completed / total_time
        
        load_test_results = {
            "concurrent_users": concurrent_users,
            "duration": total_time,
            "operations_completed": operations_completed,
            "errors": errors,
            "throughput_ops_per_sec": throughput,
            "avg_latency": statistics.mean(latencies) if latencies else 0,
            "p95_latency": sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
        }
        
        print(f"\n📊 Load Test Results:")
        print(f"  Duration: {total_time:.2f}s")
        print(f"  Operations completed: {operations_completed}")
        print(f"  Errors: {errors}")
        print(f"  Throughput: {throughput:.2f} ops/sec")
        print(f"  Avg latency: {load_test_results['avg_latency']:.2f}ms")
        print(f"  P95 latency: {load_test_results['p95_latency']:.2f}ms")
        
        return load_test_results
    
    async def benchmark_nft_minting(self):
        """Benchmark NFT minting operation"""
        async with aiohttp.ClientSession() as session:
            async def mint_nft():
                async with session.post(
                    f"{self.api_url}/api/v1/marketplace/mint-nft",
                    json={
                        "asset_id": f"test-asset-{time.time()}",
                        "creator": "0x1234567890123456789012345678901234567890",
                        "royalty_fraction": 250,
                        "chain": "polygon"
                    }
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed with status {resp.status}")
                    return await resp.json()
            
            return await self.benchmark_operation("NFT Minting", mint_nft)
    
    async def benchmark_license_purchase(self):
        """Benchmark license purchase operation"""
        async with aiohttp.ClientSession() as session:
            async def purchase_license():
                async with session.post(
                    f"{self.api_url}/api/v1/marketplace/purchase-license",
                    json={
                        "license_id": f"test-license-{time.time()}",
                        "buyer": "0x0987654321098765432109876543210987654321",
                        "payment": "10",
                        "chain": "polygon"
                    }
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed with status {resp.status}")
                    return await resp.json()
            
            return await self.benchmark_operation("License Purchase", purchase_license)
    
    async def benchmark_auction_bid(self):
        """Benchmark auction bidding operation"""
        async with aiohttp.ClientSession() as session:
            async def place_bid():
                async with session.post(
                    f"{self.api_url}/api/v1/defi/place-bid",
                    json={
                        "auction_id": "test-auction-1",
                        "bidder": "0x0987654321098765432109876543210987654321",
                        "amount": str(time.time() % 100),
                        "chain": "polygon"
                    }
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed with status {resp.status}")
                    return await resp.json()
            
            return await self.benchmark_operation("Auction Bid", place_bid)
    
    async def benchmark_search_query(self):
        """Benchmark search operation"""
        async with aiohttp.ClientSession() as session:
            async def search():
                async with session.get(
                    f"{self.api_url}/api/v1/search",
                    params={"q": "machine learning model", "type": "asset"}
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed with status {resp.status}")
                    return await resp.json()
            
            return await self.benchmark_operation("Search Query", search, iterations=500)
    
    async def benchmark_multi_chain_operations(self):
        """Benchmark operations across different chains"""
        chains = ["ethereum", "polygon", "avalanche", "solana", "near"]
        chain_results = {}
        
        async with aiohttp.ClientSession() as session:
            for chain in chains:
                print(f"\n🔗 Benchmarking {chain.upper()} chain...")
                
                async def chain_operation():
                    async with session.post(
                        f"{self.api_url}/api/v1/marketplace/mint-nft",
                        json={
                            "asset_id": f"test-{chain}-{time.time()}",
                            "creator": "0x1234567890123456789012345678901234567890",
                            "royalty_fraction": 250,
                            "chain": chain
                        }
                    ) as resp:
                        if resp.status != 200:
                            raise Exception(f"Failed with status {resp.status}")
                        return await resp.json()
                
                stats = await self.benchmark_operation(f"{chain} NFT Mint", chain_operation, iterations=50)
                if stats:
                    chain_results[chain] = stats
        
        # Compare chain performance
        if chain_results:
            print("\n📊 Chain Performance Comparison:")
            print("-" * 60)
            print(f"{'Chain':<15} {'Avg Latency':<15} {'P95 Latency':<15} {'Errors':<10}")
            print("-" * 60)
            
            for chain, stats in chain_results.items():
                print(f"{chain:<15} {stats['avg_latency']:<15.2f} {stats['p95_latency']:<15.2f} {stats['errors']:<10}")
        
        return chain_results
    
    async def stress_test_royalty_distribution(self):
        """Stress test royalty distribution with many recipients"""
        async with aiohttp.ClientSession() as session:
            # Create a complex royalty structure
            recipients = [
                {"address": f"0x{i:040x}", "percentage": 100 // 10}
                for i in range(1, 11)  # 10 recipients
            ]
            
            async def distribute_royalty():
                async with session.post(
                    f"{self.api_url}/api/v1/marketplace/distribute-royalty",
                    json={
                        "token_id": "test-token-1",
                        "sale_amount": "1000",
                        "recipients": recipients,
                        "chain": "polygon"
                    }
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Failed with status {resp.status}")
                    return await resp.json()
            
            return await self.benchmark_operation("Complex Royalty Distribution", distribute_royalty, iterations=50)
    
    def generate_report(self):
        """Generate a performance report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = "performance_reports"
        os.makedirs(report_dir, exist_ok=True)
        
        # Create summary
        self.results["summary"] = {
            "timestamp": timestamp,
            "total_operations": sum(op["iterations"] for op in self.results["operations"]),
            "total_errors": sum(op["errors"] for op in self.results["operations"]),
            "fastest_operation": min(self.results["operations"], key=lambda x: x["avg_latency"])["operation"],
            "slowest_operation": max(self.results["operations"], key=lambda x: x["avg_latency"])["operation"]
        }
        
        # Save JSON report
        with open(f"{report_dir}/benchmark_report_{timestamp}.json", "w") as f:
            json.dump(self.results, f, indent=2)
        
        # Generate charts
        self._generate_charts(report_dir, timestamp)
        
        print(f"\n📄 Report generated: {report_dir}/benchmark_report_{timestamp}.json")
    
    def _generate_charts(self, report_dir: str, timestamp: str):
        """Generate performance charts"""
        if not self.results["operations"]:
            return
        
        # Create DataFrame
        df = pd.DataFrame(self.results["operations"])
        
        # 1. Latency comparison chart
        plt.figure(figsize=(12, 6))
        operations = df["operation"]
        x = range(len(operations))
        
        plt.bar(x, df["avg_latency"], alpha=0.7, label="Average")
        plt.bar(x, df["p95_latency"], alpha=0.5, label="P95")
        plt.bar(x, df["p99_latency"], alpha=0.3, label="P99")
        
        plt.xlabel("Operation")
        plt.ylabel("Latency (ms)")
        plt.title("Operation Latency Comparison")
        plt.xticks(x, operations, rotation=45, ha="right")
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{report_dir}/latency_comparison_{timestamp}.png")
        plt.close()
        
        # 2. Error rate chart
        plt.figure(figsize=(10, 6))
        error_rates = (df["errors"] / df["iterations"]) * 100
        
        plt.bar(x, error_rates, color="red", alpha=0.7)
        plt.xlabel("Operation")
        plt.ylabel("Error Rate (%)")
        plt.title("Operation Error Rates")
        plt.xticks(x, operations, rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(f"{report_dir}/error_rates_{timestamp}.png")
        plt.close()
        
        print(f"📊 Charts saved to {report_dir}/")


async def run_blockchain_benchmarks():
    """Run all blockchain performance benchmarks"""
    benchmarker = BlockchainPerformanceBenchmarks()
    
    print("\n🚀 Starting Blockchain Performance Benchmarks\n")
    
    # Individual operation benchmarks
    await benchmarker.benchmark_nft_minting()
    await benchmarker.benchmark_license_purchase()
    await benchmarker.benchmark_auction_bid()
    await benchmarker.benchmark_search_query()
    
    # Multi-chain comparison
    await benchmarker.benchmark_multi_chain_operations()
    
    # Stress tests
    await benchmarker.stress_test_royalty_distribution()
    
    # Load tests
    print("\n🔥 Starting Load Tests")
    
    # NFT minting load test
    async with aiohttp.ClientSession() as session:
        async def mint_operation():
            async with session.post(
                f"{benchmarker.api_url}/api/v1/marketplace/mint-nft",
                json={
                    "asset_id": f"load-test-{time.time()}",
                    "creator": "0x1234567890123456789012345678901234567890",
                    "royalty_fraction": 250,
                    "chain": "polygon"
                }
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed with status {resp.status}")
        
        await benchmarker.run_load_test(mint_operation, concurrent_users=25, duration_seconds=30)
    
    # Generate report
    benchmarker.generate_report()
    
    print("\n✅ Blockchain benchmarks completed!")


if __name__ == "__main__":
    asyncio.run(run_blockchain_benchmarks()) 