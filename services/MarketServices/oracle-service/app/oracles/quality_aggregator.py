"""
Quality Aggregator Oracle

Aggregates quality scores from multiple sources for DeFi protocols.
Provides real-time and historical quality data with confidence levels.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
import statistics
import numpy as np
from collections import defaultdict
import hashlib
import json

from web3 import Web3
from fastapi import HTTPException
from prometheus_client import Counter, Gauge, Histogram

from ..core.blockchain import BlockchainClient
from ..models.quality import QualityScore, QualityComponent, ConfidenceLevel
from ..utils.signing import sign_oracle_data
from .quantum_oracle import QuantumOracle
from .ai_oracle import AIOracle
from .network_oracle import NetworkOracle

logger = logging.getLogger(__name__)

# Metrics
QUALITY_UPDATES = Counter(
    'oracle_quality_updates_total',
    'Total quality score updates',
    ['resource_type', 'resource_id']
)
QUALITY_SCORE_GAUGE = Gauge(
    'oracle_quality_score',
    'Current quality score',
    ['resource_type', 'resource_id']
)
AGGREGATION_TIME = Histogram(
    'oracle_quality_aggregation_seconds',
    'Time to aggregate quality scores'
)


class QualityAggregator:
    """Aggregates quality scores for DeFi protocols"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        quantum_oracle: QuantumOracle,
        ai_oracle: AIOracle,
        network_oracle: NetworkOracle,
        oracle_contract_address: str,
        signing_key: str
    ):
        self.blockchain = blockchain_client
        self.quantum_oracle = quantum_oracle
        self.ai_oracle = ai_oracle
        self.network_oracle = network_oracle
        self.oracle_contract_address = oracle_contract_address
        self.signing_key = signing_key
        
        # Quality score cache
        self._quality_cache = {}  # resource_id -> quality_data
        self._history_cache = defaultdict(list)  # resource_id -> [historical_scores]
        
        # Aggregation parameters
        self.min_data_points = 3  # Minimum data points for confidence
        self.cache_ttl = 300  # 5 minutes
        self.history_window = 86400  # 24 hours
        
        # Weights for quality components
        self.component_weights = {
            'quantum': {
                'fidelity': 0.4,
                'coherence': 0.3,
                'connectivity': 0.2,
                'availability': 0.1
            },
            'ai': {
                'performance': 0.35,
                'accuracy': 0.25,
                'efficiency': 0.25,
                'availability': 0.15
            },
            'network': {
                'latency': 0.3,
                'bandwidth': 0.3,
                'reliability': 0.25,
                'jitter': 0.15
            }
        }
        
        # Quality thresholds
        self.quality_thresholds = {
            'excellent': 90,
            'good': 80,
            'fair': 70,
            'poor': 60
        }
        
    async def get_quality_score(
        self,
        resource_id: int,
        resource_type: str,
        include_components: bool = True
    ) -> Dict[str, Any]:
        """
        Get aggregated quality score for a resource
        
        Args:
            resource_id: Resource identifier
            resource_type: Type of resource (quantum/ai/network)
            include_components: Include component breakdown
            
        Returns:
            Quality score with confidence level
        """
        try:
            # Check cache first
            cache_key = f"{resource_type}:{resource_id}"
            cached_data = self._quality_cache.get(cache_key)
            
            if cached_data and (datetime.utcnow() - cached_data['timestamp']).seconds < self.cache_ttl:
                return cached_data['score']
            
            # Aggregate fresh data
            with AGGREGATION_TIME.time():
                quality_data = await self._aggregate_quality(
                    resource_id,
                    resource_type,
                    include_components
                )
            
            # Cache the result
            self._quality_cache[cache_key] = {
                'score': quality_data,
                'timestamp': datetime.utcnow()
            }
            
            # Update metrics
            QUALITY_UPDATES.labels(
                resource_type=resource_type,
                resource_id=resource_id
            ).inc()
            
            QUALITY_SCORE_GAUGE.labels(
                resource_type=resource_type,
                resource_id=resource_id
            ).set(quality_data['overall_score'])
            
            # Store in history
            self._update_history(resource_id, quality_data)
            
            return quality_data
            
        except Exception as e:
            logger.error(f"Failed to get quality score: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_quality_history(
        self,
        resource_id: int,
        hours: int = 24,
        interval: str = "hourly"
    ) -> Dict[str, Any]:
        """
        Get historical quality scores
        
        Args:
            resource_id: Resource identifier
            hours: Number of hours of history
            interval: Aggregation interval (hourly/daily)
            
        Returns:
            Historical quality data
        """
        try:
            history = self._history_cache.get(resource_id, [])
            
            # Filter by time window
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
            
            # Aggregate by interval
            if interval == "hourly":
                aggregated = self._aggregate_by_hour(filtered_history)
            else:
                aggregated = self._aggregate_by_day(filtered_history)
            
            # Calculate statistics
            scores = [h['overall_score'] for h in filtered_history]
            
            statistics = {
                'mean': statistics.mean(scores),
                'median': statistics.median(scores),
                'std_dev': statistics.stdev(scores) if len(scores) > 1 else 0,
                'min': min(scores),
                'max': max(scores),
                'volatility': self._calculate_volatility(scores)
            }
            
            return {
                'resource_id': resource_id,
                'history': aggregated,
                'statistics': statistics,
                'data_points': len(filtered_history)
            }
            
        except Exception as e:
            logger.error(f"Failed to get quality history: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def sign_quality_data(
        self,
        resource_id: int,
        quality_score: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sign quality data for on-chain verification
        
        Args:
            resource_id: Resource identifier
            quality_score: Quality score data
            
        Returns:
            Signed quality data
        """
        try:
            # Prepare data for signing
            oracle_data = {
                'resource_id': resource_id,
                'overall_score': int(quality_score['overall_score'] * 100),  # Scale to integer
                'confidence': int(quality_score['confidence'] * 100),
                'timestamp': int(datetime.utcnow().timestamp()),
                'components_hash': self._hash_components(quality_score.get('components', {}))
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
                'signer': signed_data['signer']
            }
            
        except Exception as e:
            logger.error(f"Failed to sign quality data: {e}")
            raise
    
    async def submit_to_chain(
        self,
        resource_id: int,
        signed_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit signed quality data to blockchain
        
        Args:
            resource_id: Resource identifier
            signed_data: Signed oracle data
            
        Returns:
            Transaction result
        """
        try:
            oracle_contract = await self.blockchain.get_contract(
                self.oracle_contract_address,
                "QualityOracle"
            )
            
            tx = await oracle_contract.functions.updateQualityScore(
                resource_id,
                signed_data['oracle_data']['overall_score'],
                signed_data['oracle_data']['confidence'],
                signed_data['oracle_data']['timestamp'],
                signed_data['oracle_data']['components_hash'],
                signed_data['signature']
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            return {
                'tx_hash': tx,
                'block_number': receipt['blockNumber'],
                'gas_used': receipt['gasUsed']
            }
            
        except Exception as e:
            logger.error(f"Failed to submit to chain: {e}")
            raise
    
    # Private methods
    
    async def _aggregate_quality(
        self,
        resource_id: int,
        resource_type: str,
        include_components: bool
    ) -> Dict[str, Any]:
        """Aggregate quality from multiple sources"""
        
        components = {}
        raw_scores = []
        
        if resource_type.lower() == 'quantum':
            # Get quantum-specific metrics
            quantum_data = await self.quantum_oracle.measure_quality(resource_id)
            
            components = {
                'fidelity': quantum_data.get('gate_fidelity', 0),
                'coherence': quantum_data.get('coherence_time', 0) / 1000,  # Convert to score
                'connectivity': quantum_data.get('connectivity', 0),
                'availability': quantum_data.get('availability', 0)
            }
            
        elif resource_type.lower() == 'ai':
            # Get AI-specific metrics
            ai_data = await self.ai_oracle.benchmark_accelerator(resource_id)
            
            components = {
                'performance': ai_data.get('tflops', 0) / 10,  # Normalize
                'accuracy': ai_data.get('accuracy', 0),
                'efficiency': ai_data.get('power_efficiency', 0),
                'availability': ai_data.get('availability', 0)
            }
            
        elif resource_type.lower() == 'network':
            # Get network-specific metrics
            network_data = await self.network_oracle.measure_path_quality(resource_id)
            
            components = {
                'latency': 100 - min(network_data.get('latency', 100), 100),  # Lower is better
                'bandwidth': min(network_data.get('bandwidth', 0) / 10, 100),  # Normalize to 100
                'reliability': network_data.get('packet_success_rate', 0) * 100,
                'jitter': 100 - min(network_data.get('jitter', 100), 100)  # Lower is better
            }
        
        # Calculate weighted overall score
        weights = self.component_weights.get(resource_type.lower(), {})
        overall_score = sum(
            components.get(comp, 0) * weight
            for comp, weight in weights.items()
        )
        
        # Calculate confidence based on data availability
        available_components = sum(1 for v in components.values() if v > 0)
        confidence = available_components / len(weights) if weights else 0
        
        # Determine quality tier
        quality_tier = self._get_quality_tier(overall_score)
        
        result = {
            'resource_id': resource_id,
            'resource_type': resource_type,
            'overall_score': overall_score,
            'quality_tier': quality_tier,
            'confidence': confidence,
            'timestamp': datetime.utcnow()
        }
        
        if include_components:
            result['components'] = components
            result['weights'] = weights
        
        return result
    
    def _calculate_volatility(self, scores: List[float]) -> float:
        """Calculate quality score volatility"""
        if len(scores) < 2:
            return 0.0
        
        returns = []
        for i in range(1, len(scores)):
            if scores[i-1] > 0:
                returns.append((scores[i] - scores[i-1]) / scores[i-1])
        
        if not returns:
            return 0.0
        
        return float(np.std(returns) * np.sqrt(252))  # Annualized volatility
    
    def _get_quality_tier(self, score: float) -> str:
        """Determine quality tier from score"""
        for tier, threshold in self.quality_thresholds.items():
            if score >= threshold:
                return tier
        return 'poor'
    
    def _hash_components(self, components: Dict[str, Any]) -> str:
        """Create hash of component data"""
        component_str = json.dumps(components, sort_keys=True)
        return hashlib.sha256(component_str.encode()).hexdigest()
    
    def _update_history(self, resource_id: int, quality_data: Dict[str, Any]):
        """Update historical cache"""
        history = self._history_cache[resource_id]
        
        # Add new data point
        history.append({
            'overall_score': quality_data['overall_score'],
            'confidence': quality_data['confidence'],
            'timestamp': quality_data['timestamp']
        })
        
        # Remove old data
        cutoff_time = datetime.utcnow() - timedelta(seconds=self.history_window)
        self._history_cache[resource_id] = [
            h for h in history
            if h['timestamp'] > cutoff_time
        ]
    
    def _aggregate_by_hour(self, history: List[Dict]) -> List[Dict]:
        """Aggregate history by hour"""
        hourly_data = defaultdict(list)
        
        for point in history:
            hour_key = point['timestamp'].replace(minute=0, second=0, microsecond=0)
            hourly_data[hour_key].append(point['overall_score'])
        
        return [
            {
                'timestamp': hour,
                'avg_score': statistics.mean(scores),
                'min_score': min(scores),
                'max_score': max(scores),
                'data_points': len(scores)
            }
            for hour, scores in sorted(hourly_data.items())
        ]
    
    def _aggregate_by_day(self, history: List[Dict]) -> List[Dict]:
        """Aggregate history by day"""
        daily_data = defaultdict(list)
        
        for point in history:
            day_key = point['timestamp'].replace(hour=0, minute=0, second=0, microsecond=0)
            daily_data[day_key].append(point['overall_score'])
        
        return [
            {
                'timestamp': day,
                'avg_score': statistics.mean(scores),
                'min_score': min(scores),
                'max_score': max(scores),
                'data_points': len(scores)
            }
            for day, scores in sorted(daily_data.items())
        ]
    
    async def start_periodic_updates(self, interval: int = 60):
        """Start periodic quality updates"""
        while True:
            try:
                # Update quality scores for active resources
                # This would be integrated with resource registry
                logger.info("Running periodic quality updates")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in periodic updates: {e}")
                await asyncio.sleep(interval) 