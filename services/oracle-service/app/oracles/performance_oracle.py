"""
Performance Benchmark Oracle

Verifies compute resource performance metrics for DeFi protocols.
Essential for performance guarantees, insurance claims, and quality verification.
"""

from typing import Dict, Any, List, Optional, Tuple, Set
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
import statistics
import hashlib
import json
from collections import defaultdict
from enum import Enum

from web3 import Web3
from fastapi import HTTPException
from prometheus_client import Counter, Gauge, Histogram, Summary
import numpy as np

from ..core.blockchain import BlockchainClient
from ..models.performance import BenchmarkResult, PerformanceMetric, TestSuite
from ..utils.signing import sign_oracle_data
from .quantum_oracle import QuantumOracle
from .ai_oracle import AIOracle
from .network_oracle import NetworkOracle

logger = logging.getLogger(__name__)

# Metrics
BENCHMARK_RUNS = Counter(
    'oracle_benchmark_runs_total',
    'Total benchmark runs',
    ['resource_type', 'benchmark_type', 'status']
)
PERFORMANCE_SCORE = Gauge(
    'oracle_performance_score',
    'Current performance score',
    ['resource_type', 'resource_id', 'metric']
)
BENCHMARK_DURATION = Histogram(
    'oracle_benchmark_duration_seconds',
    'Benchmark execution time',
    ['resource_type', 'benchmark_type']
)
PERFORMANCE_DEVIATION = Summary(
    'oracle_performance_deviation_percent',
    'Performance deviation from baseline',
    ['resource_type', 'metric']
)


class BenchmarkType(str, Enum):
    STANDARD = "standard"          # Standard performance test
    STRESS = "stress"             # Stress test
    ENDURANCE = "endurance"       # Long-running test
    SPECIALIZED = "specialized"   # Resource-specific test
    VERIFICATION = "verification" # Quick verification test


class PerformanceMetricType(str, Enum):
    # Quantum metrics
    GATE_SPEED = "gate_speed"
    CIRCUIT_DEPTH = "circuit_depth"
    ERROR_RATE = "error_rate"
    COHERENCE_TIME = "coherence_time"
    
    # AI metrics
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ACCURACY = "accuracy"
    POWER_EFFICIENCY = "power_efficiency"
    
    # Network metrics
    BANDWIDTH = "bandwidth"
    PACKET_LOSS = "packet_loss"
    JITTER = "jitter"
    CONNECTION_STABILITY = "connection_stability"


class PerformanceOracle:
    """Benchmarks and verifies compute resource performance"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        oracle_contract_address: str,
        signing_key: str,
        quantum_oracle: QuantumOracle,
        ai_oracle: AIOracle,
        network_oracle: NetworkOracle
    ):
        self.blockchain = blockchain_client
        self.oracle_contract_address = oracle_contract_address
        self.signing_key = signing_key
        
        # Resource-specific oracles
        self.quantum_oracle = quantum_oracle
        self.ai_oracle = ai_oracle
        self.network_oracle = network_oracle
        
        # Benchmark configurations
        self._benchmark_configs = {
            'quantum': {
                'standard': self._quantum_standard_benchmark,
                'stress': self._quantum_stress_benchmark,
                'verification': self._quantum_verification_benchmark
            },
            'ai': {
                'standard': self._ai_standard_benchmark,
                'stress': self._ai_stress_benchmark,
                'verification': self._ai_verification_benchmark
            },
            'network': {
                'standard': self._network_standard_benchmark,
                'stress': self._network_stress_benchmark,
                'verification': self._network_verification_benchmark
            }
        }
        
        # Performance baselines
        self._performance_baselines = {
            'quantum': {
                'gate_speed': 100,  # microseconds
                'circuit_depth': 1000,
                'error_rate': 0.001,  # 0.1%
                'coherence_time': 100  # microseconds
            },
            'ai': {
                'throughput': 100,  # TFLOPS
                'latency': 10,  # milliseconds
                'accuracy': 0.95,  # 95%
                'power_efficiency': 20  # TFLOPS/W
            },
            'network': {
                'bandwidth': 10000,  # Mbps
                'packet_loss': 0.001,  # 0.1%
                'jitter': 1,  # milliseconds
                'connection_stability': 0.999  # 99.9%
            }
        }
        
        # Performance history
        self._performance_history = defaultdict(list)  # resource_id -> [results]
        self._baseline_cache = {}  # resource_id -> baseline_metrics
        
        # Verification parameters
        self.max_deviation_threshold = 0.2  # 20% deviation allowed
        self.min_test_duration = 60  # seconds
        self.confidence_threshold = 0.95  # 95% confidence required
        
    async def run_benchmark(
        self,
        resource_id: int,
        resource_type: str,
        benchmark_type: BenchmarkType = BenchmarkType.STANDARD,
        custom_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run performance benchmark on a resource
        
        Args:
            resource_id: Resource identifier
            resource_type: Type of resource (quantum/ai/network)
            benchmark_type: Type of benchmark to run
            custom_config: Custom benchmark configuration
            
        Returns:
            Benchmark results
        """
        try:
            # Get benchmark function
            resource_benchmarks = self._benchmark_configs.get(resource_type.lower())
            if not resource_benchmarks:
                raise ValueError(f"Unknown resource type: {resource_type}")
            
            benchmark_func = resource_benchmarks.get(benchmark_type)
            if not benchmark_func:
                raise ValueError(f"Unknown benchmark type: {benchmark_type}")
            
            # Start benchmark timer
            start_time = datetime.utcnow()
            
            # Run benchmark
            with BENCHMARK_DURATION.labels(
                resource_type=resource_type,
                benchmark_type=benchmark_type
            ).time():
                results = await benchmark_func(resource_id, custom_config)
            
            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Validate results
            validated_results = await self._validate_results(
                resource_id,
                resource_type,
                results
            )
            
            # Calculate performance scores
            scores = self._calculate_performance_scores(
                resource_type,
                validated_results
            )
            
            # Update metrics
            for metric, score in scores.items():
                PERFORMANCE_SCORE.labels(
                    resource_type=resource_type,
                    resource_id=resource_id,
                    metric=metric
                ).set(score)
            
            # Record benchmark run
            BENCHMARK_RUNS.labels(
                resource_type=resource_type,
                benchmark_type=benchmark_type,
                status='success'
            ).inc()
            
            # Store in history
            benchmark_result = {
                'resource_id': resource_id,
                'resource_type': resource_type,
                'benchmark_type': benchmark_type,
                'results': validated_results,
                'scores': scores,
                'duration': duration,
                'timestamp': datetime.utcnow(),
                'confidence': self._calculate_confidence(validated_results)
            }
            
            self._update_history(resource_id, benchmark_result)
            
            return benchmark_result
            
        except Exception as e:
            logger.error(f"Benchmark failed: {e}")
            BENCHMARK_RUNS.labels(
                resource_type=resource_type,
                benchmark_type=benchmark_type,
                status='failed'
            ).inc()
            raise HTTPException(status_code=500, detail=str(e))
    
    async def verify_performance_claim(
        self,
        resource_id: int,
        resource_type: str,
        claimed_metrics: Dict[str, Any],
        tolerance: float = 0.1  # 10% tolerance
    ) -> Dict[str, Any]:
        """
        Verify performance claims for insurance/guarantees
        
        Args:
            resource_id: Resource identifier
            resource_type: Type of resource
            claimed_metrics: Claimed performance metrics
            tolerance: Acceptable tolerance
            
        Returns:
            Verification result
        """
        try:
            # Run verification benchmark
            benchmark_result = await self.run_benchmark(
                resource_id,
                resource_type,
                BenchmarkType.VERIFICATION
            )
            
            # Compare with claims
            verification_results = {}
            all_valid = True
            
            for metric, claimed_value in claimed_metrics.items():
                if metric in benchmark_result['results']:
                    actual_value = benchmark_result['results'][metric]
                    
                    # Calculate deviation
                    if isinstance(claimed_value, (int, float)) and claimed_value > 0:
                        deviation = abs(actual_value - claimed_value) / claimed_value
                        
                        valid = deviation <= tolerance
                        
                        verification_results[metric] = {
                            'claimed': claimed_value,
                            'actual': actual_value,
                            'deviation': deviation,
                            'valid': valid,
                            'within_tolerance': valid
                        }
                        
                        if not valid:
                            all_valid = False
                            
                        # Record deviation metric
                        PERFORMANCE_DEVIATION.labels(
                            resource_type=resource_type,
                            metric=metric
                        ).observe(deviation * 100)
                    else:
                        verification_results[metric] = {
                            'error': 'Invalid claimed value'
                        }
                        all_valid = False
                else:
                    verification_results[metric] = {
                        'error': 'Metric not measured'
                    }
                    all_valid = False
            
            return {
                'resource_id': resource_id,
                'resource_type': resource_type,
                'verification_valid': all_valid,
                'metrics': verification_results,
                'benchmark_confidence': benchmark_result['confidence'],
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Performance verification failed: {e}")
            raise
    
    async def get_performance_history(
        self,
        resource_id: int,
        hours: int = 24,
        metric_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get historical performance data
        
        Args:
            resource_id: Resource identifier
            hours: Number of hours of history
            metric_filter: Optional list of metrics to include
            
        Returns:
            Performance history
        """
        try:
            history = self._performance_history.get(resource_id, [])
            
            # Filter by time
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            filtered_history = [
                h for h in history
                if h['timestamp'] > cutoff_time
            ]
            
            if not filtered_history:
                return {
                    'resource_id': resource_id,
                    'history': [],
                    'statistics': {}
                }
            
            # Extract metrics
            metrics_data = defaultdict(list)
            
            for record in filtered_history:
                for metric, value in record['results'].items():
                    if not metric_filter or metric in metric_filter:
                        metrics_data[metric].append({
                            'value': value,
                            'timestamp': record['timestamp'],
                            'benchmark_type': record['benchmark_type']
                        })
            
            # Calculate statistics
            statistics = {}
            for metric, values in metrics_data.items():
                metric_values = [v['value'] for v in values if isinstance(v['value'], (int, float))]
                
                if metric_values:
                    statistics[metric] = {
                        'mean': statistics.mean(metric_values),
                        'median': statistics.median(metric_values),
                        'std_dev': statistics.stdev(metric_values) if len(metric_values) > 1 else 0,
                        'min': min(metric_values),
                        'max': max(metric_values),
                        'trend': self._calculate_trend(values)
                    }
            
            return {
                'resource_id': resource_id,
                'period_hours': hours,
                'metrics': dict(metrics_data),
                'statistics': statistics,
                'benchmark_count': len(filtered_history)
            }
            
        except Exception as e:
            logger.error(f"Failed to get performance history: {e}")
            raise
    
    async def sign_performance_data(
        self,
        resource_id: int,
        performance_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sign performance data for on-chain verification
        
        Args:
            resource_id: Resource identifier
            performance_data: Performance benchmark data
            
        Returns:
            Signed performance data
        """
        try:
            # Create compact representation
            metrics_hash = self._hash_metrics(performance_data['results'])
            
            oracle_data = {
                'resource_id': resource_id,
                'overall_score': int(performance_data['scores'].get('overall', 0) * 100),
                'metrics_hash': metrics_hash,
                'confidence': int(performance_data['confidence'] * 100),
                'timestamp': int(datetime.utcnow().timestamp())
            }
            
            # Sign the data
            signed_data = sign_oracle_data(
                oracle_data,
                self.signing_key,
                self.oracle_contract_address
            )
            
            return {
                'oracle_data': oracle_data,
                'signature': signed_data['signature'],
                'message_hash': signed_data['message_hash'],
                'signer': signed_data['signer'],
                'detailed_metrics': performance_data['results']  # Keep detailed data off-chain
            }
            
        except Exception as e:
            logger.error(f"Failed to sign performance data: {e}")
            raise
    
    # Quantum benchmark methods
    
    async def _quantum_standard_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run standard quantum benchmark"""
        
        # Run quantum-specific tests
        results = {}
        
        # Gate speed test
        gate_test = await self.quantum_oracle.test_gate_operations(
            resource_id,
            gate_count=config.get('gate_count', 1000) if config else 1000
        )
        results['gate_speed'] = gate_test.get('avg_gate_time', 0)
        
        # Circuit depth test
        depth_test = await self.quantum_oracle.test_circuit_depth(
            resource_id,
            max_depth=config.get('max_depth', 1000) if config else 1000
        )
        results['circuit_depth'] = depth_test.get('max_achievable_depth', 0)
        
        # Error rate test
        error_test = await self.quantum_oracle.measure_error_rates(resource_id)
        results['error_rate'] = error_test.get('average_error_rate', 0)
        
        # Coherence time
        coherence_test = await self.quantum_oracle.measure_coherence_time(resource_id)
        results['coherence_time'] = coherence_test.get('t2_star', 0)
        
        return results
    
    async def _quantum_stress_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run quantum stress test"""
        
        # Run intensive quantum workload
        results = {}
        
        # Maximum circuit complexity
        stress_config = config or {}
        stress_config['circuit_size'] = stress_config.get('circuit_size', 50)
        stress_config['iterations'] = stress_config.get('iterations', 100)
        
        stress_test = await self.quantum_oracle.run_stress_test(
            resource_id,
            stress_config
        )
        
        results.update(stress_test)
        
        return results
    
    async def _quantum_verification_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Quick quantum verification benchmark"""
        
        # Run minimal tests for quick verification
        results = {}
        
        # Quick gate test
        gate_test = await self.quantum_oracle.test_gate_operations(
            resource_id,
            gate_count=100  # Reduced for speed
        )
        results['gate_speed'] = gate_test.get('avg_gate_time', 0)
        
        # Quick error check
        error_test = await self.quantum_oracle.measure_error_rates(
            resource_id,
            samples=10
        )
        results['error_rate'] = error_test.get('average_error_rate', 0)
        
        return results
    
    # AI benchmark methods
    
    async def _ai_standard_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run standard AI benchmark"""
        
        results = {}
        
        # Throughput test
        throughput_test = await self.ai_oracle.benchmark_throughput(
            resource_id,
            workload_type=config.get('workload', 'mixed') if config else 'mixed'
        )
        results['throughput'] = throughput_test.get('tflops', 0)
        
        # Latency test
        latency_test = await self.ai_oracle.measure_inference_latency(
            resource_id,
            batch_sizes=config.get('batch_sizes', [1, 8, 32]) if config else [1, 8, 32]
        )
        results['latency'] = latency_test.get('avg_latency_ms', 0)
        
        # Accuracy test (with standard model)
        accuracy_test = await self.ai_oracle.test_model_accuracy(
            resource_id,
            test_model='resnet50'
        )
        results['accuracy'] = accuracy_test.get('top1_accuracy', 0)
        
        # Power efficiency
        power_test = await self.ai_oracle.measure_power_efficiency(resource_id)
        results['power_efficiency'] = power_test.get('tflops_per_watt', 0)
        
        return results
    
    async def _ai_stress_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run AI stress test"""
        
        # Maximum load test
        stress_config = config or {}
        stress_config['duration'] = stress_config.get('duration', 300)  # 5 minutes
        stress_config['batch_size'] = stress_config.get('batch_size', 128)
        
        stress_test = await self.ai_oracle.run_stress_test(
            resource_id,
            stress_config
        )
        
        return stress_test
    
    async def _ai_verification_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Quick AI verification benchmark"""
        
        results = {}
        
        # Quick throughput check
        throughput_test = await self.ai_oracle.benchmark_throughput(
            resource_id,
            duration=30  # 30 seconds
        )
        results['throughput'] = throughput_test.get('tflops', 0)
        
        # Quick latency check
        latency_test = await self.ai_oracle.measure_inference_latency(
            resource_id,
            batch_sizes=[1],
            iterations=10
        )
        results['latency'] = latency_test.get('avg_latency_ms', 0)
        
        return results
    
    # Network benchmark methods
    
    async def _network_standard_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run standard network benchmark"""
        
        results = {}
        
        # Bandwidth test
        bandwidth_test = await self.network_oracle.test_bandwidth(
            resource_id,
            duration=config.get('duration', 60) if config else 60
        )
        results['bandwidth'] = bandwidth_test.get('avg_bandwidth_mbps', 0)
        
        # Packet loss test
        packet_test = await self.network_oracle.test_packet_loss(
            resource_id,
            packet_count=config.get('packets', 1000) if config else 1000
        )
        results['packet_loss'] = packet_test.get('loss_rate', 0)
        
        # Jitter test
        jitter_test = await self.network_oracle.measure_jitter(
            resource_id,
            samples=config.get('samples', 100) if config else 100
        )
        results['jitter'] = jitter_test.get('avg_jitter_ms', 0)
        
        # Connection stability
        stability_test = await self.network_oracle.test_connection_stability(
            resource_id,
            duration=300  # 5 minutes
        )
        results['connection_stability'] = stability_test.get('uptime_ratio', 0)
        
        return results
    
    async def _network_stress_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run network stress test"""
        
        stress_config = config or {}
        stress_config['concurrent_streams'] = stress_config.get('streams', 100)
        stress_config['duration'] = stress_config.get('duration', 300)
        
        stress_test = await self.network_oracle.run_stress_test(
            resource_id,
            stress_config
        )
        
        return stress_test
    
    async def _network_verification_benchmark(
        self,
        resource_id: int,
        config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Quick network verification benchmark"""
        
        results = {}
        
        # Quick bandwidth check
        bandwidth_test = await self.network_oracle.test_bandwidth(
            resource_id,
            duration=10
        )
        results['bandwidth'] = bandwidth_test.get('avg_bandwidth_mbps', 0)
        
        # Quick latency check
        latency_test = await self.network_oracle.measure_latency(
            resource_id,
            samples=10
        )
        results['latency'] = latency_test.get('avg_latency_ms', 0)
        
        return results
    
    # Helper methods
    
    async def _validate_results(
        self,
        resource_id: int,
        resource_type: str,
        results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate benchmark results"""
        
        validated = {}
        baselines = self._performance_baselines.get(resource_type.lower(), {})
        
        for metric, value in results.items():
            if isinstance(value, (int, float)):
                # Check against reasonable bounds
                baseline = baselines.get(metric.lower())
                
                if baseline:
                    # Allow up to 10x better or 10x worse than baseline
                    if value > 0 and baseline / 10 <= value <= baseline * 10:
                        validated[metric] = value
                    else:
                        logger.warning(
                            f"Metric {metric} value {value} outside reasonable range"
                        )
                        validated[metric] = baseline  # Use baseline as fallback
                else:
                    validated[metric] = value
            else:
                validated[metric] = value
        
        return validated
    
    def _calculate_performance_scores(
        self,
        resource_type: str,
        results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate normalized performance scores"""
        
        scores = {}
        baselines = self._performance_baselines.get(resource_type.lower(), {})
        
        for metric, value in results.items():
            if isinstance(value, (int, float)) and metric.lower() in baselines:
                baseline = baselines[metric.lower()]
                
                # Different metrics have different "better" directions
                if metric in ['error_rate', 'packet_loss', 'jitter', 'latency']:
                    # Lower is better
                    score = (baseline / value) * 100 if value > 0 else 100
                else:
                    # Higher is better
                    score = (value / baseline) * 100 if baseline > 0 else 0
                
                scores[metric] = min(max(score, 0), 200)  # Cap at 200%
        
        # Calculate overall score
        if scores:
            scores['overall'] = statistics.mean(scores.values())
        
        return scores
    
    def _calculate_confidence(self, results: Dict[str, Any]) -> float:
        """Calculate confidence in benchmark results"""
        
        # Simple confidence based on completeness
        expected_metrics = 4  # Expected number of metrics per resource type
        actual_metrics = len([v for v in results.values() if v is not None])
        
        return min(actual_metrics / expected_metrics, 1.0)
    
    def _calculate_trend(self, values: List[Dict[str, Any]]) -> str:
        """Calculate performance trend"""
        
        if len(values) < 2:
            return "stable"
        
        # Extract numeric values
        numeric_values = [
            v['value'] for v in values
            if isinstance(v['value'], (int, float))
        ]
        
        if len(numeric_values) < 2:
            return "stable"
        
        # Simple linear regression
        x = list(range(len(numeric_values)))
        y = numeric_values
        
        # Calculate slope
        n = len(x)
        xy_sum = sum(x[i] * y[i] for i in range(n))
        x_sum = sum(x)
        y_sum = sum(y)
        x_squared_sum = sum(x[i]**2 for i in range(n))
        
        denominator = n * x_squared_sum - x_sum**2
        if denominator == 0:
            return "stable"
        
        slope = (n * xy_sum - x_sum * y_sum) / denominator
        
        # Determine trend based on slope
        avg_value = statistics.mean(y)
        relative_slope = slope / avg_value if avg_value != 0 else 0
        
        if relative_slope > 0.01:
            return "improving"
        elif relative_slope < -0.01:
            return "degrading"
        else:
            return "stable"
    
    def _hash_metrics(self, metrics: Dict[str, Any]) -> str:
        """Create hash of metrics data"""
        
        # Sort and serialize metrics
        sorted_metrics = {
            k: v for k, v in sorted(metrics.items())
            if isinstance(v, (int, float, str))
        }
        
        metrics_str = json.dumps(sorted_metrics, sort_keys=True)
        return hashlib.sha256(metrics_str.encode()).hexdigest()
    
    def _update_history(self, resource_id: int, benchmark_result: Dict[str, Any]):
        """Update performance history"""
        
        history = self._performance_history[resource_id]
        history.append(benchmark_result)
        
        # Keep only last 30 days
        cutoff_time = datetime.utcnow() - timedelta(days=30)
        self._performance_history[resource_id] = [
            h for h in history
            if h['timestamp'] > cutoff_time
        ]
    
    async def start_periodic_benchmarks(
        self,
        resource_registry: List[Dict[str, Any]],
        interval_hours: int = 6
    ):
        """Start periodic performance benchmarks"""
        
        while True:
            try:
                for resource in resource_registry:
                    try:
                        # Run verification benchmark
                        await self.run_benchmark(
                            resource['id'],
                            resource['type'],
                            BenchmarkType.VERIFICATION
                        )
                        
                    except Exception as e:
                        logger.error(
                            f"Failed to benchmark resource {resource['id']}: {e}"
                        )
                
                # Wait for next cycle
                await asyncio.sleep(interval_hours * 3600)
                
            except Exception as e:
                logger.error(f"Error in periodic benchmarks: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour 