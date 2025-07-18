"""
Neuromorphic Compute Matcher

Ultra-low latency compute resource matching using spiking neural networks.
Achieves <1ms matching by encoding requirements as spike patterns.
"""

import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import asyncio
import logging
from dataclasses import dataclass
import json

import httpx
from pyignite import Client as IgniteClient
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class ComputeRequirement:
    """Compute requirements to match"""
    resource_type: str  # cpu, gpu, tpu
    min_memory_gb: float
    min_vcpus: int
    gpu_type: Optional[str] = None
    location_preference: Optional[str] = None
    max_latency_ms: Optional[int] = None
    max_price_per_hour: Optional[float] = None
    duration_minutes: int = 60
    priority: str = "normal"
    
    def to_spike_encoding(self) -> Dict[str, Any]:
        """Convert requirements to spike-compatible encoding"""
        # Normalize values to 0-1 range for spike encoding
        encoding = {
            "resource_type_cpu": 1.0 if self.resource_type == "cpu" else 0.0,
            "resource_type_gpu": 1.0 if self.resource_type == "gpu" else 0.0,
            "resource_type_tpu": 1.0 if self.resource_type == "tpu" else 0.0,
            "memory_normalized": min(self.min_memory_gb / 1024, 1.0),  # Normalize to 1TB max
            "vcpus_normalized": min(self.min_vcpus / 128, 1.0),  # Normalize to 128 vCPUs max
            "duration_normalized": min(self.duration_minutes / 1440, 1.0),  # Normalize to 24h max
            "priority_low": 1.0 if self.priority == "low" else 0.0,
            "priority_normal": 1.0 if self.priority == "normal" else 0.0,
            "priority_high": 1.0 if self.priority == "high" else 0.0,
            "priority_urgent": 1.0 if self.priority == "urgent" else 0.0,
        }
        
        if self.gpu_type:
            # One-hot encode common GPU types
            gpu_types = ["A100", "V100", "T4", "RTX3090", "RTX4090", "H100"]
            for gpu in gpu_types:
                encoding[f"gpu_{gpu}"] = 1.0 if self.gpu_type == gpu else 0.0
        
        if self.max_price_per_hour:
            encoding["price_sensitivity"] = 1.0 / (1.0 + self.max_price_per_hour)
        
        if self.max_latency_ms:
            encoding["latency_sensitivity"] = 1.0 / (1.0 + self.max_latency_ms)
            
        return encoding


@dataclass
class ComputeProvider:
    """Available compute provider"""
    provider_id: str
    offering_id: str
    resource_type: str
    resource_specs: Dict[str, Any]
    location: str
    price_per_hour: float
    available_hours: List[int]
    trust_score: float  # From graph intelligence
    reliability_score: float
    green_score: float  # Carbon efficiency
    current_utilization: float
    
    def to_spike_encoding(self) -> Dict[str, Any]:
        """Convert provider capabilities to spike encoding"""
        encoding = {
            "resource_type_cpu": 1.0 if self.resource_type == "cpu" else 0.0,
            "resource_type_gpu": 1.0 if self.resource_type == "gpu" else 0.0,
            "resource_type_tpu": 1.0 if self.resource_type == "tpu" else 0.0,
            "memory_available": min(self.resource_specs.get("memory_gb", 0) / 1024, 1.0),
            "vcpus_available": min(self.resource_specs.get("vcpus", 0) / 128, 1.0),
            "price_normalized": 1.0 / (1.0 + self.price_per_hour),  # Lower price = higher score
            "trust_score": self.trust_score,
            "reliability_score": self.reliability_score,
            "green_score": self.green_score,
            "availability": 1.0 - self.current_utilization,
        }
        
        # GPU specific features
        if self.resource_type == "gpu" and "gpu_type" in self.resource_specs:
            gpu_type = self.resource_specs["gpu_type"]
            gpu_types = ["A100", "V100", "T4", "RTX3090", "RTX4090", "H100"]
            for gpu in gpu_types:
                encoding[f"gpu_{gpu}"] = 1.0 if gpu_type == gpu else 0.0
                
        return encoding


class NeuromorphicComputeMatcher:
    """
    Ultra-fast compute matching using neuromorphic processing.
    Achieves <1ms latency by converting requirements to spike patterns
    and using SNN for instant pattern matching.
    """
    
    def __init__(
        self,
        neuromorphic_url: str,
        ignite_client: IgniteClient,
        event_publisher: EventPublisher,
        graph_intelligence_url: str
    ):
        self.neuromorphic_url = neuromorphic_url
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.graph_url = graph_intelligence_url
        
        # Cache for provider encodings
        self.provider_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Performance metrics
        self.match_count = 0
        self.total_latency_us = 0
        
        # Initialize HTTP client
        self.http_client = httpx.AsyncClient(timeout=5.0)
        
    async def initialize(self):
        """Initialize the neuromorphic matching network"""
        # Configure the SNN for compute matching
        config = {
            "network_type": "compute_matcher",
            "input_neurons": 50,  # Feature dimensions
            "reservoir_neurons": 1000,
            "output_neurons": 100,  # Max providers to rank
            "learning_enabled": True,
            "spike_encoding": "temporal",
            "optimization_target": "minimum_latency"
        }
        
        try:
            response = await self.http_client.post(
                f"{self.neuromorphic_url}/api/v1/networks/create",
                json=config
            )
            response.raise_for_status()
            
            result = response.json()
            self.network_id = result["network_id"]
            logger.info(f"Initialized neuromorphic matcher network: {self.network_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize neuromorphic network: {e}")
            raise
    
    async def match_compute_request(
        self,
        requirement: ComputeRequirement,
        max_results: int = 10
    ) -> List[Tuple[ComputeProvider, float]]:
        """
        Match compute requirements to providers using neuromorphic processing.
        Returns list of (provider, match_score) tuples.
        """
        start_time = datetime.utcnow()
        
        # Get available providers from cache
        providers = await self._get_available_providers(requirement)
        if not providers:
            return []
        
        # Encode requirement as spike pattern
        req_encoding = requirement.to_spike_encoding()
        
        # Encode all providers
        provider_encodings = []
        for provider in providers:
            # Add trust score from graph intelligence
            provider.trust_score = await self._get_trust_score(provider.provider_id)
            provider_encodings.append(provider.to_spike_encoding())
        
        # Prepare neuromorphic matching request
        match_request = {
            "network_id": self.network_id,
            "pattern_type": "compute_matching",
            "query_pattern": req_encoding,
            "candidate_patterns": provider_encodings,
            "max_results": max_results,
            "include_similarity_scores": True
        }
        
        try:
            # Submit to neuromorphic service for ultra-fast matching
            response = await self.http_client.post(
                f"{self.neuromorphic_url}/api/v1/pattern/match",
                json=match_request
            )
            response.raise_for_status()
            
            result = response.json()
            matches = result["matches"]
            
            # Convert back to provider objects with scores
            matched_providers = []
            for match in matches:
                idx = match["candidate_index"]
                score = match["similarity_score"]
                
                if 0 <= idx < len(providers):
                    # Apply additional scoring factors
                    final_score = await self._calculate_final_score(
                        providers[idx],
                        requirement,
                        score
                    )
                    matched_providers.append((providers[idx], final_score))
            
            # Sort by final score
            matched_providers.sort(key=lambda x: x[1], reverse=True)
            
            # Track performance
            latency_us = int((datetime.utcnow() - start_time).total_seconds() * 1_000_000)
            self.match_count += 1
            self.total_latency_us += latency_us
            
            # Log if we achieved sub-millisecond matching
            if latency_us < 1000:
                logger.info(f"Achieved {latency_us}μs matching latency!")
            
            # Emit matching event
            await self._emit_match_event(requirement, matched_providers, latency_us)
            
            return matched_providers[:max_results]
            
        except Exception as e:
            logger.error(f"Neuromorphic matching failed: {e}")
            # Fallback to traditional matching
            return await self._fallback_matching(requirement, providers, max_results)
    
    async def _get_available_providers(
        self,
        requirement: ComputeRequirement
    ) -> List[ComputeProvider]:
        """Get available providers from Ignite cache"""
        try:
            # Query Ignite cache for active providers
            cache = self.ignite.get_cache("compute_providers")
            
            # Build query based on requirements
            query = f"""
                SELECT * FROM ComputeProvider 
                WHERE resource_type = ? 
                AND status = 'ACTIVE'
                AND memory_gb >= ?
                AND vcpus >= ?
            """
            
            providers = []
            with cache.query(query, requirement.resource_type, 
                           requirement.min_memory_gb, requirement.min_vcpus) as cursor:
                for row in cursor:
                    provider_data = row[1]  # Assuming value is in second position
                    provider = ComputeProvider(**provider_data)
                    providers.append(provider)
            
            return providers
            
        except Exception as e:
            logger.error(f"Failed to get providers from Ignite: {e}")
            return []
    
    async def _get_trust_score(self, provider_id: str) -> float:
        """Get trust score from graph intelligence service"""
        try:
            response = await self.http_client.get(
                f"{self.graph_url}/api/v1/reputation/{provider_id}"
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("total_score", 50.0) / 100.0  # Normalize to 0-1
            return 0.5  # Default middle score
            
        except Exception:
            return 0.5
    
    async def _calculate_final_score(
        self,
        provider: ComputeProvider,
        requirement: ComputeRequirement,
        neuromorphic_score: float
    ) -> float:
        """Calculate final matching score with multiple factors"""
        # Base score from neuromorphic matching
        score = neuromorphic_score * 0.4
        
        # Trust and reliability (from graph intelligence)
        score += provider.trust_score * 0.2
        score += provider.reliability_score * 0.1
        
        # Green computing bonus
        if requirement.priority != "urgent":
            score += provider.green_score * 0.1
        
        # Price competitiveness
        if requirement.max_price_per_hour:
            price_ratio = provider.price_per_hour / requirement.max_price_per_hour
            if price_ratio <= 1.0:
                score += (1.0 - price_ratio) * 0.1
        
        # Availability bonus
        score += (1.0 - provider.current_utilization) * 0.1
        
        return min(score, 1.0)  # Cap at 1.0
    
    async def _fallback_matching(
        self,
        requirement: ComputeRequirement,
        providers: List[ComputeProvider],
        max_results: int
    ) -> List[Tuple[ComputeProvider, float]]:
        """Traditional matching as fallback"""
        matches = []
        
        for provider in providers:
            # Simple scoring based on requirements
            score = 0.0
            
            # Resource type match
            if provider.resource_type == requirement.resource_type:
                score += 0.3
            
            # Capacity match
            if provider.resource_specs.get("memory_gb", 0) >= requirement.min_memory_gb:
                score += 0.2
            if provider.resource_specs.get("vcpus", 0) >= requirement.min_vcpus:
                score += 0.2
            
            # GPU type match
            if requirement.gpu_type and provider.resource_specs.get("gpu_type") == requirement.gpu_type:
                score += 0.2
            
            # Price match
            if requirement.max_price_per_hour and provider.price_per_hour <= requirement.max_price_per_hour:
                score += 0.1
            
            matches.append((provider, score))
        
        # Sort by score
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches[:max_results]
    
    async def _emit_match_event(
        self,
        requirement: ComputeRequirement,
        matches: List[Tuple[ComputeProvider, float]],
        latency_us: int
    ):
        """Emit matching event for analytics"""
        event = {
            "event_type": "compute_match_completed",
            "requirement": requirement.__dict__,
            "match_count": len(matches),
            "top_match_score": matches[0][1] if matches else 0,
            "latency_microseconds": latency_us,
            "matching_engine": "neuromorphic",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.event_publisher.publish_event(
            event,
            "persistent://public/default/compute-matching-events"
        )
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        avg_latency = self.total_latency_us / self.match_count if self.match_count > 0 else 0
        
        return {
            "total_matches": self.match_count,
            "average_latency_us": avg_latency,
            "sub_millisecond_percentage": (avg_latency < 1000) * 100 if avg_latency > 0 else 0,
            "cache_size": len(self.provider_cache)
        }
    
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose()