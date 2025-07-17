import asyncio
import aiohttp
import random
import time
from typing import List, Dict, Any, Callable
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import subprocess

class MarketplaceChaosTests:
    """Chaos engineering tests for marketplace resilience"""
    
    def __init__(self):
        self.api_base_url = "http://localhost:8000"
        self.services = {
            "blockchain-event-bridge": 8001,
            "digital-asset-service": 8002,
            "mlops-service": 8003,
            "graph-intelligence-service": 8004,
            "search-service": 8005,
            "ignite": 10800,
            "elasticsearch": 9200,
            "cassandra": 9042,
            "janusgraph": 8182,
            "pulsar": 6650
        }
        self.chaos_results = []
        
    async def inject_network_latency(self, service: str, latency_ms: int, duration_seconds: int):
        """Inject network latency for a service"""
        print(f"\n🌐 Injecting {latency_ms}ms latency to {service} for {duration_seconds}s...")
        
        # Using tc (traffic control) to add latency
        port = self.services.get(service)
        if not port:
            print(f"❌ Unknown service: {service}")
            return
        
        try:
            # Add latency rule
            subprocess.run([
                "sudo", "tc", "qdisc", "add", "dev", "lo", "root", "netem", 
                "delay", f"{latency_ms}ms", "port", str(port)
            ], check=True)
            
            # Test operations during latency
            start_time = time.time()
            operations_during_chaos = await self._test_operations_during_chaos(
                f"Network Latency - {service}", duration_seconds
            )
            
            # Remove latency rule
            subprocess.run([
                "sudo", "tc", "qdisc", "del", "dev", "lo", "root"
            ], check=True)
            
            self._record_chaos_result(
                "network_latency",
                service,
                {"latency_ms": latency_ms, "duration": duration_seconds},
                operations_during_chaos
            )
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to inject network latency: {e}")
            print("Note: This test requires sudo privileges and tc (traffic control) installed")
    
    async def inject_packet_loss(self, service: str, loss_percentage: int, duration_seconds: int):
        """Inject packet loss for a service"""
        print(f"\n📉 Injecting {loss_percentage}% packet loss to {service} for {duration_seconds}s...")
        
        port = self.services.get(service)
        if not port:
            print(f"❌ Unknown service: {service}")
            return
        
        try:
            # Add packet loss rule
            subprocess.run([
                "sudo", "tc", "qdisc", "add", "dev", "lo", "root", "netem",
                "loss", f"{loss_percentage}%", "port", str(port)
            ], check=True)
            
            # Test operations during packet loss
            operations_during_chaos = await self._test_operations_during_chaos(
                f"Packet Loss - {service}", duration_seconds
            )
            
            # Remove packet loss rule
            subprocess.run([
                "sudo", "tc", "qdisc", "del", "dev", "lo", "root"
            ], check=True)
            
            self._record_chaos_result(
                "packet_loss",
                service,
                {"loss_percentage": loss_percentage, "duration": duration_seconds},
                operations_during_chaos
            )
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to inject packet loss: {e}")
    
    async def kill_service(self, service: str, downtime_seconds: int):
        """Kill a service and test system behavior"""
        print(f"\n💀 Killing {service} for {downtime_seconds}s...")
        
        try:
            # Get container name (assuming Docker)
            container_name = f"platformq_{service}_1"
            
            # Stop the service
            subprocess.run(["docker", "stop", container_name], check=True)
            
            # Test operations during downtime
            operations_during_chaos = await self._test_operations_during_chaos(
                f"Service Down - {service}", downtime_seconds
            )
            
            # Restart the service
            subprocess.run(["docker", "start", container_name], check=True)
            
            # Wait for service to be healthy
            await self._wait_for_service_health(service)
            
            self._record_chaos_result(
                "service_kill",
                service,
                {"downtime": downtime_seconds},
                operations_during_chaos
            )
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to kill service: {e}")
    
    async def inject_cpu_stress(self, service: str, cpu_percentage: int, duration_seconds: int):
        """Inject CPU stress on a service"""
        print(f"\n🔥 Injecting {cpu_percentage}% CPU stress on {service} for {duration_seconds}s...")
        
        try:
            container_name = f"platformq_{service}_1"
            
            # Start stress tool in container
            stress_cmd = f"stress-ng --cpu 0 --cpu-load {cpu_percentage} --timeout {duration_seconds}s"
            proc = subprocess.Popen([
                "docker", "exec", "-d", container_name, "sh", "-c", stress_cmd
            ])
            
            # Test operations during CPU stress
            operations_during_chaos = await self._test_operations_during_chaos(
                f"CPU Stress - {service}", duration_seconds
            )
            
            self._record_chaos_result(
                "cpu_stress",
                service,
                {"cpu_percentage": cpu_percentage, "duration": duration_seconds},
                operations_during_chaos
            )
            
        except Exception as e:
            print(f"❌ Failed to inject CPU stress: {e}")
    
    async def inject_memory_stress(self, service: str, memory_mb: int, duration_seconds: int):
        """Inject memory stress on a service"""
        print(f"\n💾 Injecting {memory_mb}MB memory stress on {service} for {duration_seconds}s...")
        
        try:
            container_name = f"platformq_{service}_1"
            
            # Start memory stress in container
            stress_cmd = f"stress-ng --vm 1 --vm-bytes {memory_mb}M --timeout {duration_seconds}s"
            proc = subprocess.Popen([
                "docker", "exec", "-d", container_name, "sh", "-c", stress_cmd
            ])
            
            # Test operations during memory stress
            operations_during_chaos = await self._test_operations_during_chaos(
                f"Memory Stress - {service}", duration_seconds
            )
            
            self._record_chaos_result(
                "memory_stress",
                service,
                {"memory_mb": memory_mb, "duration": duration_seconds},
                operations_during_chaos
            )
            
        except Exception as e:
            print(f"❌ Failed to inject memory stress: {e}")
    
    async def simulate_byzantine_behavior(self, service: str, duration_seconds: int):
        """Simulate Byzantine behavior (service returning incorrect data)"""
        print(f"\n👹 Simulating Byzantine behavior for {service} for {duration_seconds}s...")
        
        # This would require a proxy or service modification to return bad data
        # For now, we'll simulate by testing with invalid inputs
        
        byzantine_operations = []
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            try:
                # Send requests with corrupted data
                async with aiohttp.ClientSession() as session:
                    # Try to mint NFT with invalid royalty
                    async with session.post(
                        f"{self.api_base_url}/api/v1/marketplace/mint-nft",
                        json={
                            "asset_id": "byzantine-test",
                            "creator": "invalid-address",
                            "royalty_fraction": 99999,  # Invalid royalty
                            "chain": "invalid-chain"
                        }
                    ) as resp:
                        byzantine_operations.append({
                            "operation": "invalid_mint",
                            "status": resp.status,
                            "handled_correctly": resp.status in [400, 422]
                        })
                    
                    # Try to purchase with negative amount
                    async with session.post(
                        f"{self.api_base_url}/api/v1/marketplace/purchase-license",
                        json={
                            "license_id": "byzantine-license",
                            "buyer": "0x0000",
                            "payment": "-100",
                            "chain": "polygon"
                        }
                    ) as resp:
                        byzantine_operations.append({
                            "operation": "negative_payment",
                            "status": resp.status,
                            "handled_correctly": resp.status in [400, 422]
                        })
                
            except Exception as e:
                byzantine_operations.append({
                    "operation": "byzantine_test",
                    "error": str(e),
                    "handled_correctly": False
                })
            
            await asyncio.sleep(1)
        
        # Check if all Byzantine behaviors were handled correctly
        correctly_handled = sum(1 for op in byzantine_operations if op.get("handled_correctly", False))
        total_operations = len(byzantine_operations)
        
        print(f"✓ Byzantine behavior handled correctly: {correctly_handled}/{total_operations}")
        
        self._record_chaos_result(
            "byzantine_behavior",
            service,
            {"duration": duration_seconds},
            byzantine_operations
        )
    
    async def cascade_failure_test(self):
        """Test cascading failures across multiple services"""
        print("\n🌊 Testing cascade failure scenario...")
        
        cascade_results = []
        
        # 1. Kill blockchain-event-bridge (critical service)
        print("Step 1: Killing blockchain-event-bridge...")
        subprocess.run(["docker", "stop", "platformq_blockchain-event-bridge_1"], capture_output=True)
        
        # 2. Test other services' behavior
        async with aiohttp.ClientSession() as session:
            # Test digital-asset-service
            try:
                async with session.post(
                    f"http://localhost:8002/api/v1/assets",
                    json={"name": "Test Asset", "description": "Test"},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    cascade_results.append({
                        "service": "digital-asset-service",
                        "status": resp.status,
                        "degraded_gracefully": resp.status == 503
                    })
            except Exception as e:
                cascade_results.append({
                    "service": "digital-asset-service",
                    "error": str(e),
                    "degraded_gracefully": "timeout" in str(e).lower()
                })
            
            # Test mlops-service
            try:
                async with session.get(
                    f"http://localhost:8003/api/v1/marketplace/models",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    cascade_results.append({
                        "service": "mlops-service",
                        "status": resp.status,
                        "degraded_gracefully": resp.status in [200, 503]
                    })
            except Exception as e:
                cascade_results.append({
                    "service": "mlops-service",
                    "error": str(e),
                    "degraded_gracefully": True
                })
        
        # 3. Restart blockchain-event-bridge
        subprocess.run(["docker", "start", "platformq_blockchain-event-bridge_1"], capture_output=True)
        await self._wait_for_service_health("blockchain-event-bridge")
        
        # 4. Test recovery
        print("Testing recovery after service restart...")
        recovery_start = time.time()
        recovered = False
        
        while time.time() - recovery_start < 60:  # 60 second recovery timeout
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://localhost:8001/health") as resp:
                        if resp.status == 200:
                            recovered = True
                            break
            except:
                pass
            await asyncio.sleep(2)
        
        cascade_results.append({
            "recovery": recovered,
            "recovery_time": time.time() - recovery_start if recovered else None
        })
        
        self._record_chaos_result(
            "cascade_failure",
            "multiple_services",
            {},
            cascade_results
        )
    
    async def _test_operations_during_chaos(self, chaos_type: str, duration_seconds: int) -> List[Dict]:
        """Test marketplace operations during chaos"""
        operations = []
        start_time = time.time()
        
        async def test_operation(operation_name: str, operation_func: Callable):
            op_start = time.time()
            try:
                result = await operation_func()
                latency = (time.time() - op_start) * 1000
                operations.append({
                    "operation": operation_name,
                    "success": True,
                    "latency_ms": latency,
                    "timestamp": time.time() - start_time
                })
            except Exception as e:
                operations.append({
                    "operation": operation_name,
                    "success": False,
                    "error": str(e),
                    "timestamp": time.time() - start_time
                })
        
        # Run operations continuously during chaos
        while time.time() - start_time < duration_seconds:
            async with aiohttp.ClientSession() as session:
                # Test various operations
                await test_operation(
                    "search",
                    lambda: session.get(f"{self.api_base_url}/api/v1/search?q=test")
                )
                
                await test_operation(
                    "mint_nft",
                    lambda: session.post(
                        f"{self.api_base_url}/api/v1/marketplace/mint-nft",
                        json={
                            "asset_id": f"chaos-test-{time.time()}",
                            "creator": "0x1234567890123456789012345678901234567890",
                            "royalty_fraction": 250
                        }
                    )
                )
                
                await test_operation(
                    "get_stats",
                    lambda: session.get(f"{self.api_base_url}/api/v1/marketplace/stats")
                )
            
            await asyncio.sleep(0.5)
        
        # Calculate success rate
        successful = sum(1 for op in operations if op.get("success", False))
        total = len(operations)
        success_rate = (successful / total * 100) if total > 0 else 0
        
        print(f"  Operations during {chaos_type}: {successful}/{total} ({success_rate:.1f}% success rate)")
        
        return operations
    
    async def _wait_for_service_health(self, service: str, timeout: int = 60):
        """Wait for a service to become healthy"""
        port = self.services.get(service, 8000)
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://localhost:{port}/health") as resp:
                        if resp.status == 200:
                            print(f"✓ {service} is healthy")
                            return True
            except:
                pass
            await asyncio.sleep(2)
        
        print(f"❌ {service} failed to become healthy within {timeout}s")
        return False
    
    def _record_chaos_result(self, chaos_type: str, target: str, parameters: Dict, results: List):
        """Record chaos test results"""
        self.chaos_results.append({
            "chaos_type": chaos_type,
            "target": target,
            "parameters": parameters,
            "results": results,
            "timestamp": datetime.now().isoformat()
        })
    
    def generate_chaos_report(self):
        """Generate chaos testing report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = "chaos_reports"
        os.makedirs(report_dir, exist_ok=True)
        
        # Calculate overall resilience metrics
        resilience_metrics = {
            "total_chaos_tests": len(self.chaos_results),
            "services_tested": list(set(r["target"] for r in self.chaos_results)),
            "chaos_types_tested": list(set(r["chaos_type"] for r in self.chaos_results)),
            "overall_success_rate": self._calculate_overall_success_rate()
        }
        
        report = {
            "timestamp": timestamp,
            "resilience_metrics": resilience_metrics,
            "chaos_results": self.chaos_results
        }
        
        # Save report
        with open(f"{report_dir}/chaos_report_{timestamp}.json", "w") as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print("\n📊 Chaos Testing Summary:")
        print("-" * 50)
        print(f"Total chaos tests: {resilience_metrics['total_chaos_tests']}")
        print(f"Services tested: {', '.join(resilience_metrics['services_tested'])}")
        print(f"Chaos types: {', '.join(resilience_metrics['chaos_types_tested'])}")
        print(f"Overall success rate: {resilience_metrics['overall_success_rate']:.1f}%")
        print(f"\n📄 Detailed report saved to: {report_dir}/chaos_report_{timestamp}.json")
    
    def _calculate_overall_success_rate(self) -> float:
        """Calculate overall success rate across all chaos tests"""
        total_operations = 0
        successful_operations = 0
        
        for chaos_test in self.chaos_results:
            if isinstance(chaos_test["results"], list):
                for result in chaos_test["results"]:
                    if isinstance(result, dict) and "success" in result:
                        total_operations += 1
                        if result["success"]:
                            successful_operations += 1
        
        return (successful_operations / total_operations * 100) if total_operations > 0 else 0


async def run_chaos_tests():
    """Run all chaos tests"""
    chaos_tester = MarketplaceChaosTests()
    
    print("\n💥 Starting Marketplace Chaos Tests\n")
    print("⚠️  WARNING: These tests will disrupt services!")
    print("⚠️  Ensure you're running in a test environment!")
    
    # Network chaos tests
    await chaos_tester.inject_network_latency("blockchain-event-bridge", 500, 30)
    await chaos_tester.inject_packet_loss("digital-asset-service", 10, 30)
    
    # Service failure tests
    await chaos_tester.kill_service("search-service", 20)
    await chaos_tester.kill_service("mlops-service", 20)
    
    # Resource stress tests
    await chaos_tester.inject_cpu_stress("blockchain-event-bridge", 80, 30)
    await chaos_tester.inject_memory_stress("digital-asset-service", 512, 30)
    
    # Byzantine behavior test
    await chaos_tester.simulate_byzantine_behavior("blockchain-event-bridge", 30)
    
    # Cascade failure test
    await chaos_tester.cascade_failure_test()
    
    # Generate report
    chaos_tester.generate_chaos_report()
    
    print("\n✅ Chaos tests completed!")


if __name__ == "__main__":
    asyncio.run(run_chaos_tests()) 