"""
Trust-Based Pricing Engine

Uses graph intelligence and reputation scores to provide dynamic, trust-based pricing.
Trusted providers get better rates, trusted buyers get discounts.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import json
from dataclasses import dataclass
from collections import defaultdict

import httpx
import numpy as np
from pyignite import Client as IgniteClient

from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class TrustRelationship:
    """Trust relationship between entities"""
    from_entity: str
    to_entity: str
    trust_score: float  # 0-1
    relationship_type: str  # direct, transitive, inferred
    path_length: int  # Number of hops
    confidence: float  # Confidence in the trust score
    last_updated: datetime


@dataclass
class ReputationScore:
    """Multi-dimensional reputation from graph intelligence"""
    entity_id: str
    technical_prowess: float  # Technical capability
    collaboration_rating: float  # Teamwork and communication
    governance_influence: float  # DAO participation
    creativity_index: float  # Innovation
    reliability_score: float  # Timeliness and consistency
    total_score: float  # Weighted average
    compute_specific_score: float  # Compute marketplace specific


class TrustPricingEngine:
    """
    Dynamic pricing engine based on trust relationships and reputation scores.
    Provides incentives for building trust and maintaining good reputation.
    """
    
    def __init__(
        self,
        graph_intelligence_url: str,
        ignite_client: IgniteClient,
        event_publisher: EventPublisher,
        quantum_optimization_url: Optional[str] = None
    ):
        self.graph_url = graph_intelligence_url
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.quantum_url = quantum_optimization_url
        
        # Cache for trust relationships and reputation
        self.trust_cache = {}
        self.reputation_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Pricing configuration
        self.trust_discount_max = 0.20  # Max 20% discount for high trust
        self.reputation_bonus_max = 0.15  # Max 15% bonus for reputation
        self.new_user_penalty = 0.10  # 10% penalty for new users
        self.community_bonus = 0.05  # 5% bonus for community members
        
        # HTTP client
        self.http_client = httpx.AsyncClient(timeout=10.0)
        
        # Metrics
        self.pricing_calculations = 0
        self.trust_discounts_applied = 0
        
    async def calculate_trust_adjusted_price(
        self,
        base_price: float,
        provider_id: str,
        buyer_id: str,
        resource_type: str,
        duration_minutes: int
    ) -> Dict[str, Any]:
        """
        Calculate price with trust-based adjustments.
        
        Returns:
            Dictionary with final price and breakdown
        """
        start_time = datetime.utcnow()
        
        # Get trust relationship
        trust_relationship = await self._get_trust_relationship(buyer_id, provider_id)
        
        # Get reputation scores
        provider_reputation = await self._get_reputation_score(provider_id)
        buyer_reputation = await self._get_reputation_score(buyer_id)
        
        # Get community relationships
        community_score = await self._get_community_score(buyer_id, provider_id)
        
        # Calculate adjustments
        adjustments = {
            "base_price": base_price,
            "trust_discount": 0.0,
            "reputation_adjustment": 0.0,
            "community_bonus": 0.0,
            "new_user_adjustment": 0.0,
            "volume_discount": 0.0,
            "green_computing_bonus": 0.0
        }
        
        # 1. Trust-based discount
        if trust_relationship and trust_relationship.trust_score > 0.5:
            # Higher trust = bigger discount
            trust_discount_rate = min(
                (trust_relationship.trust_score - 0.5) * 0.4,  # 0-20% based on 0.5-1.0 trust
                self.trust_discount_max
            )
            
            # Adjust for relationship type
            if trust_relationship.relationship_type == "direct":
                trust_discount_rate *= 1.0  # Full discount for direct trust
            elif trust_relationship.relationship_type == "transitive":
                trust_discount_rate *= 0.7  # 70% for transitive trust
            else:
                trust_discount_rate *= 0.5  # 50% for inferred trust
                
            adjustments["trust_discount"] = base_price * trust_discount_rate
        
        # 2. Reputation-based adjustment
        if provider_reputation and buyer_reputation:
            # Provider reputation affects base quality
            provider_multiplier = self._calculate_reputation_multiplier(
                provider_reputation,
                "provider"
            )
            
            # Buyer reputation affects discount
            buyer_multiplier = self._calculate_reputation_multiplier(
                buyer_reputation,
                "buyer"
            )
            
            reputation_adjustment = base_price * (provider_multiplier - 1.0)
            reputation_adjustment += base_price * (1.0 - buyer_multiplier)
            
            adjustments["reputation_adjustment"] = reputation_adjustment
        
        # 3. Community bonus
        if community_score > 0.7:
            adjustments["community_bonus"] = base_price * self.community_bonus
        
        # 4. New user adjustment
        if not buyer_reputation or buyer_reputation.total_score < 10:
            adjustments["new_user_adjustment"] = -base_price * self.new_user_penalty
        elif provider_reputation and provider_reputation.compute_specific_score < 10:
            adjustments["new_user_adjustment"] = base_price * 0.05  # Discount for new provider
        
        # 5. Volume discount (for longer durations)
        if duration_minutes >= 1440:  # 24 hours or more
            volume_discount_rate = min(duration_minutes / 10080, 0.10)  # Max 10% for week+
            adjustments["volume_discount"] = base_price * volume_discount_rate
        
        # 6. Green computing bonus (if applicable)
        if await self._check_green_computing(provider_id):
            adjustments["green_computing_bonus"] = base_price * 0.03  # 3% bonus
        
        # Calculate final price
        total_adjustment = sum([
            -adjustments["trust_discount"],
            adjustments["reputation_adjustment"],
            -adjustments["community_bonus"],
            -adjustments["new_user_adjustment"],
            -adjustments["volume_discount"],
            -adjustments["green_computing_bonus"]
        ])
        
        final_price = max(base_price + total_adjustment, base_price * 0.5)  # Min 50% of base
        
        # Use quantum optimization for complex multi-party deals
        if self.quantum_url and await self._should_use_quantum_optimization(
            provider_id, buyer_id, duration_minutes
        ):
            quantum_price = await self._quantum_optimize_price(
                base_price,
                adjustments,
                trust_relationship,
                provider_reputation,
                buyer_reputation
            )
            if quantum_price:
                final_price = quantum_price
        
        # Track metrics
        self.pricing_calculations += 1
        if adjustments["trust_discount"] > 0:
            self.trust_discounts_applied += 1
        
        # Calculate processing time
        processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Emit pricing event
        await self._emit_pricing_event(
            provider_id,
            buyer_id,
            base_price,
            final_price,
            adjustments,
            processing_time_ms
        )
        
        return {
            "final_price": final_price,
            "base_price": base_price,
            "total_adjustment": final_price - base_price,
            "discount_percentage": ((base_price - final_price) / base_price) * 100,
            "adjustments": adjustments,
            "trust_relationship": {
                "exists": trust_relationship is not None,
                "score": trust_relationship.trust_score if trust_relationship else 0,
                "type": trust_relationship.relationship_type if trust_relationship else None
            },
            "reputation_scores": {
                "provider": provider_reputation.total_score if provider_reputation else 0,
                "buyer": buyer_reputation.total_score if buyer_reputation else 0
            },
            "processing_time_ms": processing_time_ms
        }
    
    async def _get_trust_relationship(
        self,
        from_entity: str,
        to_entity: str
    ) -> Optional[TrustRelationship]:
        """Get trust relationship between two entities"""
        cache_key = f"{from_entity}:{to_entity}"
        
        # Check cache
        if cache_key in self.trust_cache:
            cached = self.trust_cache[cache_key]
            if (datetime.utcnow() - cached["timestamp"]).seconds < self.cache_ttl:
                return cached["data"]
        
        try:
            # Query graph intelligence for trust path
            response = await self.http_client.get(
                f"{self.graph_url}/api/v1/trust/path",
                params={
                    "from_id": from_entity,
                    "to_id": to_entity,
                    "max_hops": 3
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("trust_path"):
                    trust_rel = TrustRelationship(
                        from_entity=from_entity,
                        to_entity=to_entity,
                        trust_score=data["trust_score"],
                        relationship_type=data["relationship_type"],
                        path_length=len(data["trust_path"]) - 1,
                        confidence=data.get("confidence", 0.8),
                        last_updated=datetime.utcnow()
                    )
                    
                    # Cache result
                    self.trust_cache[cache_key] = {
                        "data": trust_rel,
                        "timestamp": datetime.utcnow()
                    }
                    
                    return trust_rel
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get trust relationship: {e}")
            return None
    
    async def _get_reputation_score(self, entity_id: str) -> Optional[ReputationScore]:
        """Get reputation score from graph intelligence"""
        # Check cache
        if entity_id in self.reputation_cache:
            cached = self.reputation_cache[entity_id]
            if (datetime.utcnow() - cached["timestamp"]).seconds < self.cache_ttl:
                return cached["data"]
        
        try:
            response = await self.http_client.get(
                f"{self.graph_url}/api/v1/reputation/multi-dimensional",
                params={"user_id": entity_id}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Get compute-specific reputation
                compute_score = await self._get_compute_specific_reputation(entity_id)
                
                reputation = ReputationScore(
                    entity_id=entity_id,
                    technical_prowess=data.get("technical_prowess", 50.0),
                    collaboration_rating=data.get("collaboration_rating", 50.0),
                    governance_influence=data.get("governance_influence", 50.0),
                    creativity_index=data.get("creativity_index", 50.0),
                    reliability_score=data.get("reliability_score", 50.0),
                    total_score=data.get("total_score", 50.0),
                    compute_specific_score=compute_score
                )
                
                # Cache result
                self.reputation_cache[entity_id] = {
                    "data": reputation,
                    "timestamp": datetime.utcnow()
                }
                
                return reputation
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get reputation score: {e}")
            return None
    
    async def _get_compute_specific_reputation(self, entity_id: str) -> float:
        """Get compute marketplace specific reputation"""
        try:
            # Query Ignite for compute-specific metrics
            cache = self.ignite.get_cache("compute_reputation")
            
            metrics = cache.get(entity_id)
            if metrics:
                # Calculate score based on compute-specific factors
                score = 50.0  # Base score
                
                # Successful transactions
                if metrics.get("successful_transactions", 0) > 0:
                    success_rate = metrics["successful_transactions"] / metrics.get("total_transactions", 1)
                    score += success_rate * 20
                
                # Average rating
                if metrics.get("average_rating"):
                    score += (metrics["average_rating"] / 5.0) * 15
                
                # Uptime for providers
                if metrics.get("average_uptime"):
                    score += (metrics["average_uptime"] / 100.0) * 15
                
                return min(score, 100.0)
            
            return 50.0  # Default score for new users
            
        except Exception as e:
            logger.error(f"Failed to get compute reputation: {e}")
            return 50.0
    
    async def _get_community_score(self, buyer_id: str, provider_id: str) -> float:
        """Calculate community relationship score"""
        try:
            # Check if both are in same communities
            response = await self.http_client.post(
                f"{self.graph_url}/api/v1/analytics/communities",
                json={
                    "entity_ids": [buyer_id, provider_id],
                    "algorithm": "label_propagation"
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Check overlap in communities
                buyer_communities = set(data.get(buyer_id, {}).get("communities", []))
                provider_communities = set(data.get(provider_id, {}).get("communities", []))
                
                overlap = buyer_communities.intersection(provider_communities)
                
                if overlap:
                    # More overlap = higher score
                    return min(len(overlap) / max(len(buyer_communities), len(provider_communities)), 1.0)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Failed to get community score: {e}")
            return 0.0
    
    def _calculate_reputation_multiplier(
        self,
        reputation: ReputationScore,
        role: str  # "provider" or "buyer"
    ) -> float:
        """Calculate price multiplier based on reputation"""
        if role == "provider":
            # Higher reputation providers can charge more
            if reputation.total_score >= 90:
                return 1.10  # 10% premium
            elif reputation.total_score >= 80:
                return 1.05  # 5% premium
            elif reputation.total_score >= 70:
                return 1.02  # 2% premium
            elif reputation.total_score < 30:
                return 0.90  # 10% discount for low reputation
            else:
                return 1.0
        else:  # buyer
            # Higher reputation buyers get discounts
            if reputation.total_score >= 90:
                return 0.95  # 5% discount
            elif reputation.total_score >= 80:
                return 0.97  # 3% discount
            elif reputation.total_score >= 70:
                return 0.99  # 1% discount
            else:
                return 1.0
    
    async def _check_green_computing(self, provider_id: str) -> bool:
        """Check if provider uses green/renewable energy"""
        try:
            # Check provider's green credentials
            cache = self.ignite.get_cache("provider_credentials")
            credentials = cache.get(provider_id)
            
            if credentials and credentials.get("green_certified"):
                return True
            
            # Check with graph intelligence for green reputation
            reputation = await self._get_reputation_score(provider_id)
            if reputation and hasattr(reputation, "green_score"):
                return reputation.green_score > 80
            
            return False
            
        except Exception:
            return False
    
    async def _should_use_quantum_optimization(
        self,
        provider_id: str,
        buyer_id: str,
        duration_minutes: int
    ) -> bool:
        """Determine if quantum optimization should be used"""
        # Use quantum for complex multi-party or long-term deals
        return (
            self.quantum_url is not None and
            duration_minutes > 10080  # More than a week
        )
    
    async def _quantum_optimize_price(
        self,
        base_price: float,
        adjustments: Dict[str, float],
        trust_relationship: Optional[TrustRelationship],
        provider_reputation: Optional[ReputationScore],
        buyer_reputation: Optional[ReputationScore]
    ) -> Optional[float]:
        """Use quantum optimization for complex pricing"""
        try:
            # Prepare optimization problem
            problem = {
                "problem_type": "pricing_optimization",
                "objective": "maximize_social_welfare",  # Balance provider profit and buyer value
                "variables": {
                    "base_price": base_price,
                    "adjustments": adjustments,
                    "trust_score": trust_relationship.trust_score if trust_relationship else 0.5,
                    "provider_reputation": provider_reputation.total_score if provider_reputation else 50,
                    "buyer_reputation": buyer_reputation.total_score if buyer_reputation else 50
                },
                "constraints": {
                    "min_price_ratio": 0.5,  # At least 50% of base price
                    "max_price_ratio": 1.5,  # At most 150% of base price
                    "fairness_constraint": 0.8  # Ensure fair pricing
                }
            }
            
            response = await self.http_client.post(
                f"{self.quantum_url}/api/v1/optimize",
                json=problem,
                timeout=5.0  # Quick timeout for pricing
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("optimal_price")
            
            return None
            
        except Exception as e:
            logger.error(f"Quantum optimization failed: {e}")
            return None
    
    async def _emit_pricing_event(
        self,
        provider_id: str,
        buyer_id: str,
        base_price: float,
        final_price: float,
        adjustments: Dict[str, float],
        processing_time_ms: int
    ):
        """Emit pricing calculation event"""
        event = {
            "event_type": "trust_pricing_calculated",
            "provider_id": provider_id,
            "buyer_id": buyer_id,
            "base_price": base_price,
            "final_price": final_price,
            "discount_amount": base_price - final_price,
            "adjustments": adjustments,
            "processing_time_ms": processing_time_ms,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.event_publisher.publish_event(
            event,
            "persistent://public/default/compute-pricing-events"
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get pricing engine metrics"""
        trust_discount_rate = (
            self.trust_discounts_applied / self.pricing_calculations 
            if self.pricing_calculations > 0 else 0
        )
        
        return {
            "total_calculations": self.pricing_calculations,
            "trust_discounts_applied": self.trust_discounts_applied,
            "trust_discount_rate": trust_discount_rate,
            "cache_size": {
                "trust": len(self.trust_cache),
                "reputation": len(self.reputation_cache)
            }
        }
    
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose()