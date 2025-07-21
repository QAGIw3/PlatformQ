"""
Quantum Resource Oracle Implementation
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import uuid
from pyignite import Client

from ..models.measurements import (
    QuantumMeasurement, QuantumQualityScore, MeasurementType,
    OracleSource, QualityStatus
)
from ..config import settings
from ..utils.aggregation import aggregate_measurements, detect_outliers


logger = logging.getLogger(__name__)


class QuantumOracle:
    """Oracle for quantum resource measurements and quality scoring"""
    
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
                f"{settings.IGNITE_CACHE_MEASUREMENTS}_quantum"
            )
            self.quality_cache = self.ignite_client.get_or_create_cache(
                f"{settings.IGNITE_CACHE_QUALITY_SCORES}_quantum"
            )
            
            logger.info("Quantum Oracle initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Quantum Oracle: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def measure_fidelity(
        self,
        qpu_id: str,
        gate_type: str = "single_qubit",
        qubit_count: int = 1,
        samples: int = None
    ) -> QuantumMeasurement:
        """Measure quantum gate fidelity"""
        samples = samples or settings.COHERENCE_MEASUREMENT_SAMPLES
        
        try:
            # Simulate fidelity measurement
            # In production, this would interface with QPU calibration system
            if gate_type == "single_qubit":
                base_fidelity = 0.999
                noise = np.random.normal(0, 0.0005, samples)
            elif gate_type == "two_qubit":
                base_fidelity = 0.995
                noise = np.random.normal(0, 0.001, samples)
            else:
                base_fidelity = 0.99
                noise = np.random.normal(0, 0.002, samples)
            
            measurements = base_fidelity + noise
            measurements = np.clip(measurements, 0, 1)
            
            # Aggregate measurements
            fidelity = float(np.median(measurements))
            confidence = 1.0 - float(np.std(measurements))
            
            measurement = QuantumMeasurement(
                measurement_id=f"qm_{uuid.uuid4().hex[:8]}",
                resource_id=qpu_id,
                measurement_type=MeasurementType.QUANTUM_FIDELITY,
                value=fidelity,
                unit="fraction",
                timestamp=datetime.utcnow(),
                source=OracleSource.HARDWARE,
                confidence=confidence,
                qubit_count=qubit_count,
                gate_type=gate_type,
                metadata={
                    "samples": samples,
                    "std_dev": float(np.std(measurements)),
                    "min": float(np.min(measurements)),
                    "max": float(np.max(measurements))
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure fidelity for {qpu_id}: {e}")
            raise
    
    async def measure_coherence_time(
        self,
        qpu_id: str,
        qubit_indices: List[int],
        coherence_type: str = "T1"  # T1 or T2
    ) -> QuantumMeasurement:
        """Measure qubit coherence time"""
        try:
            # Simulate coherence measurement
            # In production, this would perform actual T1/T2 measurements
            if coherence_type == "T1":
                base_time = 100  # microseconds
                variance = 20
            else:  # T2
                base_time = 80  # microseconds
                variance = 15
            
            # Measure each qubit
            coherence_times = []
            for qubit in qubit_indices:
                # Add qubit-specific variation
                qubit_factor = 1.0 - (qubit * 0.02)  # Slight degradation with qubit index
                time = base_time * qubit_factor + np.random.normal(0, variance)
                coherence_times.append(max(time, 10))  # Minimum 10 microseconds
            
            # Take worst case (weakest link)
            coherence_time = float(np.min(coherence_times))
            confidence = 0.95 if len(qubit_indices) == 1 else 0.9
            
            measurement = QuantumMeasurement(
                measurement_id=f"qm_{uuid.uuid4().hex[:8]}",
                resource_id=qpu_id,
                measurement_type=MeasurementType.QUANTUM_COHERENCE,
                value=coherence_time,
                unit="microseconds",
                timestamp=datetime.utcnow(),
                source=OracleSource.HARDWARE,
                confidence=confidence,
                qubit_count=len(qubit_indices),
                metadata={
                    "coherence_type": coherence_type,
                    "qubit_indices": qubit_indices,
                    "individual_times": coherence_times
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure coherence for {qpu_id}: {e}")
            raise
    
    async def measure_error_rate(
        self,
        qpu_id: str,
        circuit_depth: int,
        gate_count: int
    ) -> QuantumMeasurement:
        """Measure quantum circuit error rate"""
        try:
            # Simulate error rate measurement
            # Error rate increases with circuit depth and gate count
            base_error_rate = 0.001  # 0.1% base error
            depth_factor = 1 + (circuit_depth * 0.0001)
            gate_factor = 1 + (gate_count * 0.00005)
            
            error_rate = base_error_rate * depth_factor * gate_factor
            
            # Add measurement noise
            error_rate += np.random.normal(0, error_rate * 0.1)
            error_rate = max(0, min(error_rate, 0.1))  # Cap at 10%
            
            measurement = QuantumMeasurement(
                measurement_id=f"qm_{uuid.uuid4().hex[:8]}",
                resource_id=qpu_id,
                measurement_type=MeasurementType.QUANTUM_ERROR_RATE,
                value=float(error_rate),
                unit="fraction",
                timestamp=datetime.utcnow(),
                source=OracleSource.BENCHMARK_SUITE,
                confidence=0.92,
                circuit_depth=circuit_depth,
                metadata={
                    "gate_count": gate_count,
                    "error_type": "total",
                    "mitigation_available": True
                }
            )
            
            # Store measurement
            self.measurement_cache.put(
                measurement.measurement_id,
                measurement.dict()
            )
            
            return measurement
            
        except Exception as e:
            logger.error(f"Failed to measure error rate for {qpu_id}: {e}")
            raise
    
    async def calculate_quality_score(
        self,
        qpu_id: str,
        time_window_hours: int = 24
    ) -> QuantumQualityScore:
        """Calculate comprehensive quality score for a QPU"""
        try:
            # Get recent measurements
            cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
            
            fidelity_measurements = []
            coherence_measurements = []
            error_measurements = []
            
            # In production, query measurements from cache/database
            # For now, simulate with recent data
            
            # Calculate component scores
            fidelity_score = await self._calculate_fidelity_score(
                qpu_id, fidelity_measurements
            )
            coherence_score = await self._calculate_coherence_score(
                qpu_id, coherence_measurements
            )
            error_rate_score = await self._calculate_error_rate_score(
                qpu_id, error_measurements
            )
            
            # Gate quality scores (simulate different gate types)
            gate_quality_scores = {
                "single_qubit": 95.0,
                "two_qubit": 92.0,
                "three_qubit": 88.0
            }
            
            # Create quality score
            quality_score = QuantumQualityScore(
                resource_id=qpu_id,
                resource_type="quantum",
                overall_score=0,  # Will be calculated
                status=QualityStatus.GOOD,
                component_scores={
                    "fidelity": fidelity_score,
                    "coherence": coherence_score,
                    "error_rate": error_rate_score,
                    "readout": 94.0,  # Simulated
                    "crosstalk": 91.0  # Simulated
                },
                measurement_count=len(fidelity_measurements) + 
                                len(coherence_measurements) + 
                                len(error_measurements),
                last_updated=datetime.utcnow(),
                confidence_interval=(85.0, 95.0),
                trend="stable",
                fidelity_score=fidelity_score,
                coherence_score=coherence_score,
                error_rate_score=error_rate_score,
                gate_quality_scores=gate_quality_scores,
                readout_fidelity=94.0,
                crosstalk_score=91.0
            )
            
            # Calculate overall score
            quality_score.overall_score = quality_score.calculate_overall_score()
            
            # Determine status based on overall score
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
                f"{qpu_id}_quality",
                quality_score.dict(),
                ttl=settings.QUALITY_UPDATE_INTERVAL
            )
            
            return quality_score
            
        except Exception as e:
            logger.error(f"Failed to calculate quality score for {qpu_id}: {e}")
            raise
    
    async def _calculate_fidelity_score(
        self,
        qpu_id: str,
        measurements: List[QuantumMeasurement]
    ) -> float:
        """Calculate fidelity component score"""
        if not measurements:
            # Use default or last known value
            return 95.0
        
        # Extract fidelity values
        fidelities = [m.value for m in measurements if m.value > 0]
        
        if not fidelities:
            return 0.0
        
        # Remove outliers
        cleaned_fidelities = detect_outliers(
            fidelities,
            settings.OUTLIER_ZSCORE_THRESHOLD
        )
        
        # Calculate score (fidelity * 100)
        avg_fidelity = aggregate_measurements(
            cleaned_fidelities,
            settings.AGGREGATION_METHOD
        )
        
        return min(avg_fidelity * 100, 100.0)
    
    async def _calculate_coherence_score(
        self,
        qpu_id: str,
        measurements: List[QuantumMeasurement]
    ) -> float:
        """Calculate coherence component score"""
        if not measurements:
            # Use default or last known value
            return 90.0
        
        # Extract coherence times
        coherence_times = [m.value for m in measurements]
        
        if not coherence_times:
            return 0.0
        
        # Remove outliers
        cleaned_times = detect_outliers(
            coherence_times,
            settings.OUTLIER_ZSCORE_THRESHOLD
        )
        
        # Calculate score based on coherence time
        # Score = 100 * (actual_time / target_time)
        target_coherence = 100.0  # microseconds
        avg_coherence = aggregate_measurements(
            cleaned_times,
            settings.AGGREGATION_METHOD
        )
        
        score = min((avg_coherence / target_coherence) * 100, 100.0)
        return score
    
    async def _calculate_error_rate_score(
        self,
        qpu_id: str,
        measurements: List[QuantumMeasurement]
    ) -> float:
        """Calculate error rate component score"""
        if not measurements:
            # Use default or last known value
            return 92.0
        
        # Extract error rates
        error_rates = [m.value for m in measurements]
        
        if not error_rates:
            return 100.0
        
        # Remove outliers
        cleaned_rates = detect_outliers(
            error_rates,
            settings.OUTLIER_ZSCORE_THRESHOLD
        )
        
        # Calculate score (inverse of error rate)
        # Score = 100 * (1 - error_rate)
        avg_error_rate = aggregate_measurements(
            cleaned_rates,
            settings.AGGREGATION_METHOD
        )
        
        score = max(0, (1 - avg_error_rate) * 100)
        return score
    
    async def verify_quantum_computation(
        self,
        qpu_id: str,
        algorithm_id: str,
        expected_result: Dict,
        actual_result: Dict
    ) -> Tuple[bool, float]:
        """Verify quantum computation result"""
        try:
            # Compare quantum computation results
            # In production, this would use quantum verification protocols
            
            # For now, simulate verification
            if expected_result.get("type") == "probability_distribution":
                # Statistical verification for quantum results
                expected_probs = expected_result.get("probabilities", {})
                actual_probs = actual_result.get("probabilities", {})
                
                # Calculate fidelity between distributions
                fidelity = self._calculate_distribution_fidelity(
                    expected_probs,
                    actual_probs
                )
                
                # Verification passes if fidelity > threshold
                verified = fidelity > settings.QUANTUM_FIDELITY_THRESHOLD
                
                return verified, fidelity
            else:
                # Exact match verification
                verified = expected_result == actual_result
                confidence = 1.0 if verified else 0.0
                
                return verified, confidence
                
        except Exception as e:
            logger.error(f"Failed to verify quantum computation: {e}")
            return False, 0.0
    
    def _calculate_distribution_fidelity(
        self,
        expected: Dict[str, float],
        actual: Dict[str, float]
    ) -> float:
        """Calculate fidelity between probability distributions"""
        # Get all possible outcomes
        outcomes = set(expected.keys()) | set(actual.keys())
        
        # Calculate state fidelity
        fidelity = 0.0
        for outcome in outcomes:
            p_expected = expected.get(outcome, 0.0)
            p_actual = actual.get(outcome, 0.0)
            fidelity += np.sqrt(p_expected * p_actual)
        
        return fidelity
    
    async def monitor_qpu_health(
        self,
        qpu_id: str
    ) -> Dict[str, any]:
        """Monitor real-time QPU health metrics"""
        try:
            # Perform quick health checks
            health_metrics = {
                "qpu_id": qpu_id,
                "timestamp": datetime.utcnow(),
                "status": "healthy",
                "alerts": []
            }
            
            # Check recent fidelity
            fidelity = await self.measure_fidelity(qpu_id, samples=10)
            if fidelity.value < settings.QUANTUM_FIDELITY_THRESHOLD:
                health_metrics["alerts"].append({
                    "type": "low_fidelity",
                    "value": fidelity.value,
                    "threshold": settings.QUANTUM_FIDELITY_THRESHOLD
                })
                health_metrics["status"] = "degraded"
            
            # Check coherence
            coherence = await self.measure_coherence_time(
                qpu_id,
                qubit_indices=[0]  # Sample qubit
            )
            if coherence.value < 50:  # Less than 50 microseconds
                health_metrics["alerts"].append({
                    "type": "low_coherence",
                    "value": coherence.value,
                    "threshold": 50
                })
                health_metrics["status"] = "degraded"
            
            # Check error rate
            error_rate = await self.measure_error_rate(
                qpu_id,
                circuit_depth=10,
                gate_count=20
            )
            if error_rate.value > settings.QUANTUM_ERROR_THRESHOLD:
                health_metrics["alerts"].append({
                    "type": "high_error_rate",
                    "value": error_rate.value,
                    "threshold": settings.QUANTUM_ERROR_THRESHOLD
                })
                health_metrics["status"] = "degraded"
            
            health_metrics["measurements"] = {
                "fidelity": fidelity.value,
                "coherence_us": coherence.value,
                "error_rate": error_rate.value
            }
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"Failed to monitor QPU health: {e}")
            return {
                "qpu_id": qpu_id,
                "timestamp": datetime.utcnow(),
                "status": "error",
                "error": str(e)
            } 