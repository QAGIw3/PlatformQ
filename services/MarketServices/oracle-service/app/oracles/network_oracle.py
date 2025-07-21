"""
Network Resource Oracle Implementation
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import uuid
import subprocess
import platform
from pyignite import Client

from ..models.measurements import (
    NetworkMeasurement, NetworkQualityScore, MeasurementType,
    OracleSource, QualityStatus
)
from ..config import settings
from ..utils.aggregation import aggregate_measurements, detect_outliers


logger = logging.getLogger(__name__)


class NetworkOracle:
    """Oracle for network resource measurements and quality scoring"""
    
    def __init__(self):
        self.ignite_client = None
        self.measurement_cache = None
        self.quality_cache = None
        
    async def initialize(self):
        """Initialize oracle connections"""
        try:
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.measurement_cache = self.ignite_client.get_or_create_cache(
                f"{settings.IGNITE_CACHE_MEASUREMENTS}_network"
            )
            self.quality_cache = self.ignite_client.get_or_create_cache(
                f"{settings.IGNITE_CACHE_QUALITY_SCORES}_network"
            )
            
            logger.info("Network Oracle initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Network Oracle: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def measure_latency(
        self,
        path_id: str,
        source_node: str,
        destination_node: str,
        protocol: str = "icmp",
        packet_count: int = None
    ) -> NetworkMeasurement:
        """Measure network latency between nodes"""
        packet_count = packet_count or settings.NETWORK_PING_COUNT
        
        try:
            # Simulate latency measurement
            # In production, would use actual network probes
            
            # Base latency by distance (simplified)
            base_latency = 10.0  # ms
            
            # Add protocol overhead
            protocol_overhead = {
                "icmp": 0,
                "tcp": 2,
                "udp": 1
            }
            
            latency = base_latency + protocol_overhead.get(protocol, 0)
            
            # Simulate multiple measurements
            measurements = []
            for _ in range(packet_count):
                jitter = np.random.normal(0, latency * 0.1)
                measurements.append(max(0.1, latency + jitter))
            
            # Calculate statistics
            avg_latency = float(np.mean(measurements))
            min_latency = float(np.min(measurements))
            max_latency = float(np.max(measurements))
            std_dev = float(np.std(measurements))
            
            measurement = NetworkMeasurement(
                measurement_id=f"nm_{uuid.uuid4().hex[:8]}",
                resource_id=path_id,
                measurement_type=MeasurementType.NETWORK_LATENCY,
                value=avg_latency,
                unit="milliseconds",
                timestamp=datetime.utcnow(),
                source=OracleSource.NETWORK_PROBE,
                confidence=0.95,
                source_node=source_node,
                destination_node=destination_node,
                path_id=path_id,
                protocol=protocol,
                sample_count=packet_count,
                metadata={
                    "min_latency": min_latency,
                    "max_latency": max_latency,
                    "std_dev": std_dev,
                    "packet_loss": 0.0
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure latency: {e}")
            raise
    
    async def measure_bandwidth(
        self,
        path_id: str,
        source_node: str,
        destination_node: str,
        test_duration: int = None
    ) -> NetworkMeasurement:
        """Measure available bandwidth"""
        test_duration = test_duration or settings.NETWORK_BANDWIDTH_TEST_DURATION
        
        try:
            # Simulate bandwidth test
            # In production, would use iperf or similar tool
            
            # Base bandwidth capacity (Mbps)
            max_bandwidth = 10000  # 10 Gbps
            
            # Current utilization affects available bandwidth
            utilization = np.random.uniform(0.3, 0.8)
            available_bandwidth = max_bandwidth * (1 - utilization)
            
            # Add measurement variation
            available_bandwidth += np.random.normal(0, available_bandwidth * 0.05)
            available_bandwidth = max(100, available_bandwidth)
            
            measurement = NetworkMeasurement(
                measurement_id=f"nm_{uuid.uuid4().hex[:8]}",
                resource_id=path_id,
                measurement_type=MeasurementType.NETWORK_BANDWIDTH,
                value=float(available_bandwidth),
                unit="mbps",
                timestamp=datetime.utcnow(),
                source=OracleSource.NETWORK_PROBE,
                confidence=0.92,
                source_node=source_node,
                destination_node=destination_node,
                path_id=path_id,
                metadata={
                    "test_duration_seconds": test_duration,
                    "max_bandwidth_mbps": max_bandwidth,
                    "utilization_percent": utilization * 100,
                    "test_protocol": "tcp"
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure bandwidth: {e}")
            raise
    
    async def measure_packet_loss(
        self,
        path_id: str,
        source_node: str,
        destination_node: str,
        packet_count: int = 1000,
        packet_size: int = 1400
    ) -> NetworkMeasurement:
        """Measure packet loss rate"""
        try:
            # Simulate packet loss measurement
            # In production, would send actual test packets
            
            # Base packet loss rate
            if np.random.random() < 0.1:  # 10% chance of degraded path
                base_loss_rate = 0.01  # 1% loss
            else:
                base_loss_rate = 0.0001  # 0.01% loss
            
            # Add variation
            loss_rate = base_loss_rate + np.random.exponential(base_loss_rate * 0.1)
            loss_rate = min(loss_rate, 0.1)  # Cap at 10%
            
            # Calculate actual lost packets
            lost_packets = int(packet_count * loss_rate)
            actual_loss_rate = lost_packets / packet_count
            
            measurement = NetworkMeasurement(
                measurement_id=f"nm_{uuid.uuid4().hex[:8]}",
                resource_id=path_id,
                measurement_type=MeasurementType.NETWORK_PACKET_LOSS,
                value=float(actual_loss_rate),
                unit="fraction",
                timestamp=datetime.utcnow(),
                source=OracleSource.NETWORK_PROBE,
                confidence=0.94,
                source_node=source_node,
                destination_node=destination_node,
                path_id=path_id,
                packet_size_bytes=packet_size,
                sample_count=packet_count,
                metadata={
                    "packets_sent": packet_count,
                    "packets_lost": lost_packets,
                    "loss_percentage": actual_loss_rate * 100
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure packet loss: {e}")
            raise
    
    async def measure_jitter(
        self,
        path_id: str,
        source_node: str,
        destination_node: str,
        sample_count: int = None
    ) -> NetworkMeasurement:
        """Measure network jitter (latency variation)"""
        sample_count = sample_count or settings.JITTER_MEASUREMENT_SAMPLES
        
        try:
            # Simulate jitter measurement
            # Jitter is the variation in latency
            
            # Base latency
            base_latency = 10.0  # ms
            
            # Generate latency samples
            latencies = []
            for _ in range(sample_count):
                # Add time-varying component
                time_factor = np.sin(len(latencies) * 0.1) * 2
                latency = base_latency + time_factor + np.random.normal(0, 1)
                latencies.append(max(0.1, latency))
            
            # Calculate jitter (standard deviation of latency)
            jitter = float(np.std(latencies))
            
            # Also calculate inter-packet jitter
            inter_packet_jitters = []
            for i in range(1, len(latencies)):
                ipj = abs(latencies[i] - latencies[i-1])
                inter_packet_jitters.append(ipj)
            
            avg_ipj = float(np.mean(inter_packet_jitters)) if inter_packet_jitters else 0
            
            measurement = NetworkMeasurement(
                measurement_id=f"nm_{uuid.uuid4().hex[:8]}",
                resource_id=path_id,
                measurement_type=MeasurementType.NETWORK_JITTER,
                value=jitter,
                unit="milliseconds",
                timestamp=datetime.utcnow(),
                source=OracleSource.NETWORK_PROBE,
                confidence=0.93,
                source_node=source_node,
                destination_node=destination_node,
                path_id=path_id,
                sample_count=sample_count,
                metadata={
                    "avg_latency": float(np.mean(latencies)),
                    "min_latency": float(np.min(latencies)),
                    "max_latency": float(np.max(latencies)),
                    "inter_packet_jitter": avg_ipj
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure jitter: {e}")
            raise
    
    async def calculate_quality_score(
        self,
        path_id: str,
        source_node: str,
        destination_node: str,
        time_window_hours: int = 24
    ) -> NetworkQualityScore:
        """Calculate comprehensive quality score for a network path"""
        try:
            # Get recent measurements
            # In production, would query from cache/database
            
            # For now, generate sample measurements
            latency = await self.measure_latency(path_id, source_node, destination_node)
            bandwidth = await self.measure_bandwidth(path_id, source_node, destination_node)
            packet_loss = await self.measure_packet_loss(path_id, source_node, destination_node)
            jitter = await self.measure_jitter(path_id, source_node, destination_node)
            
            # Calculate component scores
            latency_score = self._calculate_latency_score(latency.value)
            bandwidth_score = self._calculate_bandwidth_score(bandwidth.value)
            packet_loss_score = self._calculate_packet_loss_score(packet_loss.value)
            jitter_score = self._calculate_jitter_score(jitter.value)
            
            # Availability score (simulated)
            availability_score = 99.5 + np.random.normal(0, 0.5)
            availability_score = max(95.0, min(100.0, availability_score))
            
            # Create quality score
            quality_score = NetworkQualityScore(
                resource_id=path_id,
                resource_type="network",
                overall_score=0,  # Will be calculated
                status=QualityStatus.GOOD,
                component_scores={
                    "latency": latency_score,
                    "bandwidth": bandwidth_score,
                    "packet_loss": packet_loss_score,
                    "jitter": jitter_score,
                    "availability": availability_score
                },
                measurement_count=4,
                last_updated=datetime.utcnow(),
                confidence_interval=(85.0, 95.0),
                trend="stable",
                latency_score=latency_score,
                bandwidth_score=bandwidth_score,
                packet_loss_score=packet_loss_score,
                jitter_score=jitter_score,
                availability_score=availability_score
            )
            
            # Calculate overall score
            quality_score.overall_score = quality_score.calculate_overall_score()
            
            # Determine status
            if quality_score.overall_score >= 90:
                quality_score.status = QualityStatus.EXCELLENT
            elif quality_score.overall_score >= 75:
                quality_score.status = QualityStatus.GOOD
            elif quality_score.overall_score >= 50:
                quality_score.status = QualityStatus.FAIR
            else:
                quality_score.status = QualityStatus.POOR
            
            # Store quality score
            self.quality_cache.put(
                f"{path_id}_quality",
                quality_score.dict(),
                ttl=settings.QUALITY_UPDATE_INTERVAL
            )
            
            return quality_score
            
        except Exception as e:
            logger.error(f"Failed to calculate quality score: {e}")
            raise
    
    def _calculate_latency_score(self, latency_ms: float) -> float:
        """Calculate latency component score"""
        # Scoring: lower is better
        # < 10ms: 100 points
        # 10-20ms: 90-100 points
        # 20-50ms: 70-90 points
        # 50-100ms: 50-70 points
        # > 100ms: < 50 points
        
        if latency_ms < 10:
            return 100.0
        elif latency_ms < 20:
            return 100 - (latency_ms - 10)
        elif latency_ms < 50:
            return 90 - (latency_ms - 20) * 0.67
        elif latency_ms < 100:
            return 70 - (latency_ms - 50) * 0.4
        else:
            return max(0, 50 - (latency_ms - 100) * 0.1)
    
    def _calculate_bandwidth_score(self, bandwidth_mbps: float) -> float:
        """Calculate bandwidth component score"""
        # Scoring based on available bandwidth
        # > 5000 Mbps: 100 points
        # 1000-5000: 80-100 points
        # 100-1000: 60-80 points
        # < 100: < 60 points
        
        if bandwidth_mbps >= 5000:
            return 100.0
        elif bandwidth_mbps >= 1000:
            return 80 + (bandwidth_mbps - 1000) / 200
        elif bandwidth_mbps >= 100:
            return 60 + (bandwidth_mbps - 100) / 45
        else:
            return bandwidth_mbps * 0.6
    
    def _calculate_packet_loss_score(self, loss_rate: float) -> float:
        """Calculate packet loss component score"""
        # Scoring: lower is better
        # 0%: 100 points
        # < 0.1%: 95-100 points
        # < 1%: 70-95 points
        # < 5%: 30-70 points
        # >= 5%: < 30 points
        
        if loss_rate == 0:
            return 100.0
        elif loss_rate < 0.001:  # < 0.1%
            return 100 - loss_rate * 5000
        elif loss_rate < 0.01:  # < 1%
            return 95 - (loss_rate - 0.001) * 2777
        elif loss_rate < 0.05:  # < 5%
            return 70 - (loss_rate - 0.01) * 1000
        else:
            return max(0, 30 - (loss_rate - 0.05) * 200)
    
    def _calculate_jitter_score(self, jitter_ms: float) -> float:
        """Calculate jitter component score"""
        # Scoring: lower is better
        # < 1ms: 100 points
        # 1-5ms: 80-100 points
        # 5-20ms: 50-80 points
        # > 20ms: < 50 points
        
        if jitter_ms < 1:
            return 100.0
        elif jitter_ms < 5:
            return 100 - (jitter_ms - 1) * 5
        elif jitter_ms < 20:
            return 80 - (jitter_ms - 5) * 2
        else:
            return max(0, 50 - (jitter_ms - 20) * 0.5)
    
    async def verify_sla_compliance(
        self,
        path_id: str,
        sla_parameters: Dict[str, float]
    ) -> Tuple[bool, Dict[str, any]]:
        """Verify network SLA compliance"""
        try:
            # Get current measurements
            source = "node_a"  # Would get from path info
            destination = "node_b"
            
            latency = await self.measure_latency(path_id, source, destination)
            bandwidth = await self.measure_bandwidth(path_id, source, destination)
            packet_loss = await self.measure_packet_loss(path_id, source, destination)
            jitter = await self.measure_jitter(path_id, source, destination)
            
            # Check SLA parameters
            violations = []
            
            if "max_latency_ms" in sla_parameters:
                if latency.value > sla_parameters["max_latency_ms"]:
                    violations.append({
                        "parameter": "latency",
                        "measured": latency.value,
                        "sla_limit": sla_parameters["max_latency_ms"],
                        "violation_percent": (latency.value / sla_parameters["max_latency_ms"] - 1) * 100
                    })
            
            if "min_bandwidth_mbps" in sla_parameters:
                if bandwidth.value < sla_parameters["min_bandwidth_mbps"]:
                    violations.append({
                        "parameter": "bandwidth",
                        "measured": bandwidth.value,
                        "sla_limit": sla_parameters["min_bandwidth_mbps"],
                        "violation_percent": (1 - bandwidth.value / sla_parameters["min_bandwidth_mbps"]) * 100
                    })
            
            if "max_packet_loss" in sla_parameters:
                if packet_loss.value > sla_parameters["max_packet_loss"]:
                    violations.append({
                        "parameter": "packet_loss",
                        "measured": packet_loss.value,
                        "sla_limit": sla_parameters["max_packet_loss"],
                        "violation_percent": (packet_loss.value / sla_parameters["max_packet_loss"] - 1) * 100
                    })
            
            if "max_jitter_ms" in sla_parameters:
                if jitter.value > sla_parameters["max_jitter_ms"]:
                    violations.append({
                        "parameter": "jitter",
                        "measured": jitter.value,
                        "sla_limit": sla_parameters["max_jitter_ms"],
                        "violation_percent": (jitter.value / sla_parameters["max_jitter_ms"] - 1) * 100
                    })
            
            compliant = len(violations) == 0
            
            result = {
                "compliant": compliant,
                "violations": violations,
                "measurements": {
                    "latency_ms": latency.value,
                    "bandwidth_mbps": bandwidth.value,
                    "packet_loss_rate": packet_loss.value,
                    "jitter_ms": jitter.value
                },
                "verification_time": datetime.utcnow().isoformat()
            }
            
            return compliant, result
            
        except Exception as e:
            logger.error(f"Failed to verify SLA compliance: {e}")
            return False, {"error": str(e)}
    
    async def monitor_path_health(
        self,
        path_id: str,
        source_node: str,
        destination_node: str
    ) -> Dict[str, any]:
        """Monitor real-time network path health"""
        try:
            health_metrics = {
                "path_id": path_id,
                "timestamp": datetime.utcnow(),
                "status": "healthy",
                "alerts": []
            }
            
            # Quick latency check
            latency = await self.measure_latency(
                path_id, source_node, destination_node,
                packet_count=5
            )
            if latency.value > 50:  # High latency threshold
                health_metrics["alerts"].append({
                    "type": "high_latency",
                    "value": latency.value,
                    "threshold": 50
                })
                health_metrics["status"] = "degraded"
            
            # Packet loss check
            packet_loss = await self.measure_packet_loss(
                path_id, source_node, destination_node,
                packet_count=100
            )
            if packet_loss.value > settings.PACKET_LOSS_THRESHOLD:
                health_metrics["alerts"].append({
                    "type": "packet_loss",
                    "value": packet_loss.value,
                    "threshold": settings.PACKET_LOSS_THRESHOLD
                })
                health_metrics["status"] = "degraded"
            
            # Jitter check
            jitter = await self.measure_jitter(
                path_id, source_node, destination_node,
                sample_count=20
            )
            if jitter.value > 10:  # High jitter threshold
                health_metrics["alerts"].append({
                    "type": "high_jitter",
                    "value": jitter.value,
                    "threshold": 10
                })
                if health_metrics["status"] == "healthy":
                    health_metrics["status"] = "warning"
            
            health_metrics["measurements"] = {
                "latency_ms": latency.value,
                "packet_loss_rate": packet_loss.value,
                "jitter_ms": jitter.value
            }
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"Failed to monitor path health: {e}")
            return {
                "path_id": path_id,
                "timestamp": datetime.utcnow(),
                "status": "error",
                "error": str(e)
            } 