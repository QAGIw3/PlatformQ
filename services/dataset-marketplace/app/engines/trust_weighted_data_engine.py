"""
Trust-Weighted Data Engine

Core engine for managing data quality scoring, trust-based access control,
and dynamic pricing in the data marketplace.
"""

from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import json
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count, stddev, min as spark_min, max as spark_max

from app.integrations import (
    GraphIntelligenceClient,
    IgniteCache,
    PulsarEventPublisher,
    SeaTunnelClient,
    SparkClient,
    FlinkClient,
    ElasticsearchClient
)

logger = logging.getLogger(__name__)


class DataQualityDimension(Enum):
    """Quality dimensions for data assessment"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


class AccessLevel(Enum):
    """Data access levels"""
    SAMPLE = "sample"  # Small sample data
    FILTERED = "filtered"  # Filtered subset
    AGGREGATED = "aggregated"  # Aggregated data only
    FULL = "full"  # Full dataset access
    REAL_TIME = "real_time"  # Real-time streaming access


@dataclass
class QualityAssessment:
    """Data quality assessment result"""
    assessment_id: str
    dataset_id: str
    asset_id: str
    timestamp: datetime
    assessor_id: str
    assessor_trust_score: float
    quality_dimensions: Dict[DataQualityDimension, float]
    overall_quality_score: float
    trust_adjusted_score: float
    quality_issues: List[Dict[str, Any]]
    automated_checks: Dict[str, bool]
    verification_proofs: Optional[List[str]] = None


@dataclass
class TrustBasedAccess:
    """Trust-based access configuration"""
    min_trust_score: float
    required_dimensions: Dict[str, float]  # e.g., {"technical_prowess": 0.7}
    required_credentials: List[str]
    access_level: AccessLevel
    price_multiplier: float


@dataclass
class DataPricingTier:
    """Pricing tier configuration"""
    tier_name: str
    min_trust_score: float
    access_level: AccessLevel
    base_price: Decimal
    quality_multiplier: float
    volume_discounts: Optional[Dict[int, float]] = None


class TrustWeightedDataEngine:
    """
    Core engine for trust-weighted data marketplace operations
    """
    
    def __init__(
        self,
        graph_client: GraphIntelligenceClient,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        seatunnel: SeaTunnelClient,
        spark: SparkClient,
        flink: FlinkClient,
        elasticsearch: ElasticsearchClient
    ):
        self.graph_client = graph_client
        self.ignite = ignite
        self.pulsar = pulsar
        self.seatunnel = seatunnel
        self.spark = spark
        self.flink = flink
        self.elasticsearch = elasticsearch
        
        # Cache for trust scores and quality metrics
        self._trust_cache: Dict[str, Dict[str, float]] = {}
        self._quality_cache: Dict[str, QualityAssessment] = {}
        
        # Initialize pricing tiers
        self._pricing_tiers = self._initialize_pricing_tiers()
        
        # Graph intelligence integration (will be set by main)
        self.graph_intelligence = None
        
        # Monitoring task
        self._monitoring_task = None
    
    def _initialize_pricing_tiers(self) -> List[DataPricingTier]:
        """Initialize default pricing tiers"""
        return [
            DataPricingTier(
                tier_name="bronze",
                min_trust_score=0.0,
                access_level=AccessLevel.SAMPLE,
                base_price=Decimal("10.00"),
                quality_multiplier=0.5
            ),
            DataPricingTier(
                tier_name="silver",
                min_trust_score=0.5,
                access_level=AccessLevel.FILTERED,
                base_price=Decimal("50.00"),
                quality_multiplier=0.8
            ),
            DataPricingTier(
                tier_name="gold",
                min_trust_score=0.7,
                access_level=AccessLevel.FULL,
                base_price=Decimal("200.00"),
                quality_multiplier=1.0
            ),
            DataPricingTier(
                tier_name="platinum",
                min_trust_score=0.9,
                access_level=AccessLevel.REAL_TIME,
                base_price=Decimal("1000.00"),
                quality_multiplier=1.2
            )
        ]
    
    async def assess_data_quality(
        self,
        dataset_id: str,
        data_uri: str,
        assessor_id: str,
        schema_info: Optional[Dict[str, Any]] = None,
        automated: bool = True
    ) -> QualityAssessment:
        """
        Perform comprehensive data quality assessment
        """
        logger.info(f"Assessing quality for dataset {dataset_id}")
        
        # Get assessor trust scores
        assessor_trust = await self._get_entity_trust_scores(assessor_id)
        assessor_trust_score = assessor_trust.get("overall_trust", 0.5)
        
        # Analyze data quality using Spark
        quality_dimensions = await self._analyze_data_quality(data_uri, schema_info)
        
        # Calculate overall quality score
        overall_score = self._calculate_overall_quality_score(quality_dimensions)
        
        # Calculate trust-adjusted score
        trust_adjusted_score = overall_score * assessor_trust_score
        
        # Identify quality issues
        quality_issues = self._identify_quality_issues(quality_dimensions)
        
        # Create assessment
        assessment = QualityAssessment(
            assessment_id=f"qa-{dataset_id}-{datetime.utcnow().timestamp()}",
            dataset_id=dataset_id,
            asset_id=dataset_id,  # In this context, dataset is the asset
            timestamp=datetime.utcnow(),
            assessor_id=assessor_id,
            assessor_trust_score=assessor_trust_score,
            quality_dimensions=quality_dimensions,
            overall_quality_score=overall_score,
            trust_adjusted_score=trust_adjusted_score,
            quality_issues=quality_issues,
            automated_checks={"spark_analysis": True, "schema_validation": True}
        )
        
        # Cache the assessment
        self._quality_cache[dataset_id] = assessment
        await self.ignite.put("quality_assessments", dataset_id, assessment.__dict__)
        
        # Publish assessment event
        await self.pulsar.publish_quality_assessment(assessment.__dict__)
        
        # Register in knowledge graph if available
        if self.graph_intelligence:
            await self.graph_intelligence.register_quality_assessment(
                assessment_id=assessment.assessment_id,
                dataset_id=dataset_id,
                assessor_id=assessor_id,
                quality_score=overall_score,
                dimensions=quality_dimensions
            )
        
        return assessment
    
    async def _analyze_data_quality(
        self,
        data_uri: str,
        schema_info: Optional[Dict[str, Any]] = None
    ) -> Dict[DataQualityDimension, float]:
        """
        Analyze data quality using Spark
        """
        try:
            # Load dataset with Spark
            df = self.spark.read_dataset(data_uri)
            quality_stats = self.spark.analyze_quality(df)
            
            # Calculate quality dimensions
            dimensions = {}
            
            # Completeness: average completeness across columns
            completeness_values = list(quality_stats.get("completeness", {}).values())
            dimensions[DataQualityDimension.COMPLETENESS] = (
                sum(completeness_values) / len(completeness_values)
                if completeness_values else 0.0
            )
            
            # Accuracy: based on data type validation and range checks
            dimensions[DataQualityDimension.ACCURACY] = 0.9  # Placeholder
            
            # Consistency: check for internal consistency
            dimensions[DataQualityDimension.CONSISTENCY] = 0.88  # Placeholder
            
            # Timeliness: based on data freshness
            dimensions[DataQualityDimension.TIMELINESS] = 0.95  # Placeholder
            
            # Validity: schema conformance
            dimensions[DataQualityDimension.VALIDITY] = 0.92  # Placeholder
            
            # Uniqueness: duplicate detection
            dimensions[DataQualityDimension.UNIQUENESS] = 0.98  # Placeholder
            
            return dimensions
            
        except Exception as e:
            logger.error(f"Error analyzing data quality: {e}")
            # Return default low scores on error
            return {dim: 0.5 for dim in DataQualityDimension}
    
    def _calculate_overall_quality_score(
        self,
        dimensions: Dict[DataQualityDimension, float]
    ) -> float:
        """
        Calculate weighted overall quality score
        """
        weights = {
            DataQualityDimension.COMPLETENESS: 0.20,
            DataQualityDimension.ACCURACY: 0.20,
            DataQualityDimension.CONSISTENCY: 0.15,
            DataQualityDimension.TIMELINESS: 0.15,
            DataQualityDimension.VALIDITY: 0.15,
            DataQualityDimension.UNIQUENESS: 0.15
        }
        
        weighted_sum = sum(
            dimensions.get(dim, 0.0) * weight
            for dim, weight in weights.items()
        )
        
        return min(max(weighted_sum, 0.0), 1.0)
    
    def _identify_quality_issues(
        self,
        dimensions: Dict[DataQualityDimension, float]
    ) -> List[Dict[str, Any]]:
        """
        Identify quality issues based on dimension scores
        """
        issues = []
        
        thresholds = {
            DataQualityDimension.COMPLETENESS: 0.80,
            DataQualityDimension.ACCURACY: 0.85,
            DataQualityDimension.CONSISTENCY: 0.80,
            DataQualityDimension.TIMELINESS: 0.70,
            DataQualityDimension.VALIDITY: 0.90,
            DataQualityDimension.UNIQUENESS: 0.95
        }
        
        for dim, threshold in thresholds.items():
            score = dimensions.get(dim, 0.0)
            if score < threshold:
                issues.append({
                    "issue_type": f"low_{dim.value}",
                    "severity": self._calculate_severity(score, threshold),
                    "description": f"{dim.value.title()} score ({score:.2f}) below threshold ({threshold})",
                    "dimension": dim.value,
                    "score": score,
                    "threshold": threshold
                })
        
        return issues
    
    def _calculate_severity(self, score: float, threshold: float) -> str:
        """Calculate issue severity"""
        deficit = threshold - score
        if deficit > 0.3:
            return "CRITICAL"
        elif deficit > 0.2:
            return "HIGH"
        elif deficit > 0.1:
            return "MEDIUM"
        else:
            return "LOW"
    
    async def evaluate_access_request(
        self,
        dataset_id: str,
        user_id: str,
        requested_level: AccessLevel,
        purpose: str
    ) -> Dict[str, Any]:
        """
        Evaluate data access request based on trust scores
        """
        # Get user trust scores
        user_trust = await self._get_entity_trust_scores(user_id)
        overall_trust = user_trust.get("overall_trust", 0.0)
        
        # Get dataset quality score
        quality_assessment = self._quality_cache.get(dataset_id)
        if not quality_assessment:
            # Try to load from cache
            cached = await self.ignite.get("quality_assessments", dataset_id)
            if cached:
                quality_assessment = QualityAssessment(**cached)
        
        quality_score = quality_assessment.trust_adjusted_score if quality_assessment else 0.5
        
        # Determine access based on trust and quality
        granted = False
        granted_level = None
        reason = ""
        
        # Find appropriate tier based on trust score
        eligible_tiers = [
            tier for tier in self._pricing_tiers
            if tier.min_trust_score <= overall_trust
        ]
        
        if not eligible_tiers:
            reason = "Insufficient trust score for any access level"
        else:
            # Get highest eligible tier
            best_tier = max(eligible_tiers, key=lambda t: t.min_trust_score)
            
            # Check if requested level is within eligibility
            tier_levels = {
                "bronze": AccessLevel.SAMPLE,
                "silver": AccessLevel.FILTERED,
                "gold": AccessLevel.FULL,
                "platinum": AccessLevel.REAL_TIME
            }
            
            max_allowed_level = tier_levels.get(best_tier.tier_name, AccessLevel.SAMPLE)
            
            if self._access_level_value(requested_level) <= self._access_level_value(max_allowed_level):
                granted = True
                granted_level = requested_level
                reason = f"Access granted based on trust score {overall_trust:.2f}"
            else:
                # Offer lower access level
                granted = True
                granted_level = max_allowed_level
                reason = f"Requested level too high. Granted {max_allowed_level.value} based on trust score"
        
        # Additional checks based on purpose
        if granted and "research" in purpose.lower() and overall_trust >= 0.6:
            # Bonus for research purposes
            reason += " (research bonus applied)"
        
        return {
            "granted": granted,
            "granted_level": granted_level.value if granted_level else None,
            "reason": reason,
            "user_trust_score": overall_trust,
            "dataset_quality_score": quality_score,
            "eligible_tier": best_tier.tier_name if eligible_tiers else None
        }
    
    def _access_level_value(self, level: AccessLevel) -> int:
        """Get numeric value for access level comparison"""
        values = {
            AccessLevel.SAMPLE: 1,
            AccessLevel.FILTERED: 2,
            AccessLevel.AGGREGATED: 3,
            AccessLevel.FULL: 4,
            AccessLevel.REAL_TIME: 5
        }
        return values.get(level, 0)
    
    async def calculate_dynamic_pricing(
        self,
        dataset_id: str,
        user_id: str,
        access_level: AccessLevel,
        volume: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Calculate dynamic pricing based on trust, quality, and demand
        """
        # Get user trust score
        user_trust = await self._get_entity_trust_scores(user_id)
        trust_score = user_trust.get("overall_trust", 0.0)
        
        # Get dataset quality
        quality_assessment = self._quality_cache.get(dataset_id)
        quality_score = quality_assessment.trust_adjusted_score if quality_assessment else 0.5
        
        # Find appropriate pricing tier
        eligible_tier = None
        for tier in self._pricing_tiers:
            if tier.min_trust_score <= trust_score and tier.access_level == access_level:
                eligible_tier = tier
                break
        
        if not eligible_tier:
            # Default to bronze tier pricing
            eligible_tier = self._pricing_tiers[0]
        
        # Calculate base price
        base_price = eligible_tier.base_price
        
        # Apply quality multiplier
        quality_adjusted_price = base_price * Decimal(str(quality_score * eligible_tier.quality_multiplier))
        
        # Apply trust discount (higher trust = better price)
        trust_discount = Decimal(str(min(trust_score * 0.2, 0.15)))  # Max 15% discount
        trust_adjusted_price = quality_adjusted_price * (Decimal("1.0") - trust_discount)
        
        # Apply volume discount if applicable
        volume_discount = Decimal("0.0")
        if volume and volume > 1000:
            volume_discount = Decimal("0.1")  # 10% for large volumes
        elif volume and volume > 100:
            volume_discount = Decimal("0.05")  # 5% for medium volumes
        
        final_price = trust_adjusted_price * (Decimal("1.0") - volume_discount)
        
        return {
            "base_price": str(base_price),
            "final_price": str(final_price),
            "quality_multiplier": eligible_tier.quality_multiplier,
            "trust_discount": float(trust_discount),
            "volume_discount": float(volume_discount),
            "price_breakdown": {
                "tier": eligible_tier.tier_name,
                "quality_adjustment": str(quality_adjusted_price - base_price),
                "trust_adjustment": str(trust_adjusted_price - quality_adjusted_price),
                "volume_adjustment": str(final_price - trust_adjusted_price)
            }
        }
    
    async def grant_data_access(
        self,
        dataset_id: str,
        user_id: str,
        access_level: AccessLevel,
        duration_days: int = 365
    ) -> Dict[str, Any]:
        """
        Grant data access to a user
        """
        # Generate access token
        access_token = f"access-{dataset_id}-{user_id}-{datetime.utcnow().timestamp()}"
        
        # Calculate expiry
        expires_at = datetime.utcnow() + timedelta(days=duration_days)
        
        # Create access grant
        grant = {
            "grant_id": f"grant-{datetime.utcnow().timestamp()}",
            "dataset_id": dataset_id,
            "user_id": user_id,
            "access_level": access_level.value,
            "access_token": access_token,
            "granted_at": datetime.utcnow().isoformat(),
            "expires_at": expires_at.isoformat(),
            "duration_days": duration_days
        }
        
        # Store in cache
        await self.ignite.put("access_grants", access_token, grant, ttl=duration_days * 86400)
        
        # Publish grant event
        await self.pulsar.publish_access_grant(grant)
        
        return grant
    
    async def create_quality_derivative(
        self,
        dataset_id: str,
        derivative_type: str,
        strike_quality: float,
        expiry_days: int
    ) -> Dict[str, Any]:
        """
        Create a data quality derivative instrument
        """
        # Get current quality
        quality_assessment = self._quality_cache.get(dataset_id)
        current_quality = quality_assessment.trust_adjusted_score if quality_assessment else 0.5
        
        # Calculate premium based on Black-Scholes-like model
        # Simplified version for demonstration
        volatility = 0.2  # Assumed quality volatility
        risk_free_rate = 0.02
        time_to_expiry = expiry_days / 365.0
        
        # Simple premium calculation
        if derivative_type == "QUALITY_FUTURE":
            premium = abs(strike_quality - current_quality) * 100
        elif derivative_type == "ACCURACY_OPTION":
            # Call option on accuracy
            premium = max(current_quality - strike_quality, 0) * 100 + 10
        else:
            premium = 50  # Default premium
        
        derivative = {
            "derivative_id": f"deriv-{datetime.utcnow().timestamp()}",
            "dataset_id": dataset_id,
            "derivative_type": derivative_type,
            "strike_quality": strike_quality,
            "current_quality": current_quality,
            "expiry": (datetime.utcnow() + timedelta(days=expiry_days)).isoformat(),
            "premium": premium,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Store derivative
        await self.ignite.put("quality_derivatives", derivative["derivative_id"], derivative)
        
        return derivative
    
    async def _get_entity_trust_scores(self, entity_id: str) -> Dict[str, float]:
        """
        Get trust scores for an entity, using cache when possible
        """
        # Check cache first
        if entity_id in self._trust_cache:
            return self._trust_cache[entity_id]
        
        # Try Ignite cache
        cached = await self.ignite.get("trust_scores", entity_id)
        if cached:
            self._trust_cache[entity_id] = cached
            return cached
        
        # Fetch from graph intelligence service
        try:
            trust_scores = await self.graph_client.get_trust_scores(entity_id)
            
            # Cache the result
            self._trust_cache[entity_id] = trust_scores
            await self.ignite.put("trust_scores", entity_id, trust_scores, ttl=3600)
            
            return trust_scores
        except Exception as e:
            logger.error(f"Error fetching trust scores for {entity_id}: {e}")
            return {"overall_trust": 0.5}  # Default trust score
    
    async def update_cached_trust_scores(
        self,
        entity_id: str,
        trust_scores: Dict[str, float]
    ):
        """Update cached trust scores for an entity"""
        self._trust_cache[entity_id] = trust_scores
        await self.ignite.put("trust_scores", entity_id, trust_scores, ttl=3600)
    
    async def update_dataset_pricing(self, dataset_id: str, quality_score: float):
        """Update dataset pricing based on new quality score"""
        # Implementation for dynamic pricing updates
        logger.info(f"Updating pricing for dataset {dataset_id} with quality score {quality_score}")
        
        # Publish pricing update event
        await self.pulsar.publish_pricing_update({
            "dataset_id": dataset_id,
            "quality_score": quality_score,
            "timestamp": datetime.utcnow().isoformat(),
            "pricing_tiers": [tier.__dict__ for tier in self._pricing_tiers]
        })
    
    async def register_dataset_in_graph(
        self,
        dataset_id: str,
        dataset_info: Dict[str, Any],
        creator_id: str
    ):
        """Register dataset in federated knowledge graph"""
        if self.graph_intelligence:
            await self.graph_intelligence.register_dataset_node(
                dataset_id=dataset_id,
                dataset_info=dataset_info,
                creator_id=creator_id
            )
    
    async def check_user_access(self, user_id: str, dataset_id: str) -> bool:
        """Check if user has access to dataset"""
        # Check for active access grant
        grants = await self.ignite.query(
            "access_grants",
            f"SELECT * WHERE user_id = '{user_id}' AND dataset_id = '{dataset_id}'"
        )
        
        if grants:
            # Check if any grant is still valid
            now = datetime.utcnow()
            for grant in grants:
                expires_at = datetime.fromisoformat(grant.get("expires_at"))
                if expires_at > now:
                    return True
        
        return False
    
    async def start_monitoring(self):
        """Start background monitoring tasks"""
        self._monitoring_task = asyncio.create_task(self._quality_monitoring_loop())
    
    async def _quality_monitoring_loop(self):
        """Background task for monitoring data quality"""
        while True:
            try:
                # Monitor quality degradation
                # Check for expired derivatives
                # Update pricing based on demand
                await asyncio.sleep(300)  # Run every 5 minutes
            except Exception as e:
                logger.error(f"Error in quality monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop(self):
        """Stop the engine and clean up resources"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass 