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
        spark_session: SparkSession,
        elasticsearch: ElasticsearchClient
    ):
        self.graph_client = graph_client
        self.ignite = ignite
        self.pulsar = pulsar
        self.seatunnel = seatunnel
        self.spark = spark_session
        self.elasticsearch = elasticsearch
        
        # Quality assessment cache
        self.quality_cache: Dict[str, QualityAssessment] = {}
        
        # Access control cache
        self.access_configs: Dict[str, List[TrustBasedAccess]] = {}
        
        # Pricing tiers cache
        self.pricing_tiers: Dict[str, List[DataPricingTier]] = {}
        
        # Quality weights for overall score
        self.quality_weights = {
            DataQualityDimension.COMPLETENESS: 0.25,
            DataQualityDimension.ACCURACY: 0.30,
            DataQualityDimension.CONSISTENCY: 0.15,
            DataQualityDimension.TIMELINESS: 0.15,
            DataQualityDimension.VALIDITY: 0.10,
            DataQualityDimension.UNIQUENESS: 0.05
        }
        
    async def assess_data_quality(
        self,
        dataset_id: str,
        asset_id: str,
        assessor_id: str,
        data_uri: str,
        schema_info: Optional[Dict[str, Any]] = None
    ) -> QualityAssessment:
        """
        Perform comprehensive data quality assessment
        """
        try:
            # Get assessor trust score
            assessor_trust = await self._get_user_trust_score(assessor_id)
            
            # Load data using Spark
            df = await self._load_dataset(data_uri, schema_info)
            
            # Perform quality assessments
            quality_dimensions = await self._assess_quality_dimensions(df, schema_info)
            
            # Detect quality issues
            quality_issues = await self._detect_quality_issues(df, quality_dimensions)
            
            # Run automated checks
            automated_checks = await self._run_automated_checks(df, schema_info)
            
            # Calculate overall quality score
            overall_score = self._calculate_overall_score(quality_dimensions)
            
            # Adjust score based on assessor trust
            trust_adjusted_score = self._adjust_score_by_trust(
                overall_score,
                assessor_trust
            )
            
            # Create assessment
            assessment = QualityAssessment(
                assessment_id=f"qa_{dataset_id}_{datetime.utcnow().timestamp()}",
                dataset_id=dataset_id,
                asset_id=asset_id,
                timestamp=datetime.utcnow(),
                assessor_id=assessor_id,
                assessor_trust_score=assessor_trust,
                quality_dimensions=quality_dimensions,
                overall_quality_score=overall_score,
                trust_adjusted_score=trust_adjusted_score,
                quality_issues=quality_issues,
                automated_checks=automated_checks
            )
            
            # Cache assessment
            await self._cache_assessment(assessment)
            
            # Publish assessment event
            await self._publish_quality_assessment(assessment)
            
            # Update dataset metadata with quality scores
            await self._update_dataset_quality_metadata(dataset_id, assessment)
            
            return assessment
            
        except Exception as e:
            logger.error(f"Error assessing data quality: {e}")
            raise
            
    async def check_access_permission(
        self,
        dataset_id: str,
        user_id: str,
        requested_access: AccessLevel,
        purpose: str
    ) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """
        Check if user has permission to access dataset based on trust scores
        
        Returns: (has_access, reason, access_details)
        """
        try:
            # Get user trust scores
            user_trust = await self._get_user_trust_profile(user_id)
            
            # Get dataset access requirements
            access_configs = await self._get_dataset_access_configs(dataset_id)
            
            # Get dataset quality assessment
            quality = await self._get_latest_quality_assessment(dataset_id)
            
            # Check each access configuration
            for config in access_configs:
                if config.access_level.value >= requested_access.value:
                    # Check trust score requirement
                    if user_trust['overall_score'] < config.min_trust_score:
                        continue
                        
                    # Check dimension requirements
                    dimension_met = True
                    for dim, min_score in config.required_dimensions.items():
                        if user_trust.get(dim, 0) < min_score:
                            dimension_met = False
                            break
                            
                    if not dimension_met:
                        continue
                        
                    # Check credential requirements
                    credentials_met = await self._check_user_credentials(
                        user_id,
                        config.required_credentials
                    )
                    
                    if not credentials_met:
                        continue
                        
                    # Access granted
                    access_details = {
                        'access_level': config.access_level.value,
                        'trust_tier': self._get_trust_tier(user_trust['overall_score']),
                        'quality_tier': self._get_quality_tier(quality.trust_adjusted_score),
                        'price_multiplier': config.price_multiplier,
                        'expires_in_hours': self._calculate_access_duration(
                            user_trust['overall_score'],
                            config.access_level
                        )
                    }
                    
                    # Log access grant
                    await self._log_access_grant(
                        dataset_id,
                        user_id,
                        config.access_level,
                        purpose,
                        access_details
                    )
                    
                    return True, "Access granted based on trust profile", access_details
                    
            # Access denied
            reason = self._generate_access_denial_reason(
                user_trust,
                access_configs,
                requested_access
            )
            
            return False, reason, None
            
        except Exception as e:
            logger.error(f"Error checking access permission: {e}")
            return False, "Error checking permissions", None
            
    async def calculate_dynamic_price(
        self,
        dataset_id: str,
        user_id: str,
        access_level: AccessLevel,
        volume: Optional[int] = None
    ) -> Decimal:
        """
        Calculate dynamic price based on data quality, user trust, and market conditions
        """
        try:
            # Get base pricing tiers
            pricing_tiers = await self._get_pricing_tiers(dataset_id)
            
            # Get user trust score
            user_trust = await self._get_user_trust_score(user_id)
            
            # Get data quality assessment
            quality = await self._get_latest_quality_assessment(dataset_id)
            
            # Get market conditions
            market_conditions = await self._get_market_conditions(dataset_id)
            
            # Find applicable pricing tier
            base_price = Decimal('0')
            quality_multiplier = 1.0
            
            for tier in pricing_tiers:
                if (tier.min_trust_score <= user_trust and
                    tier.access_level == access_level):
                    base_price = tier.base_price
                    quality_multiplier = tier.quality_multiplier
                    
                    # Apply volume discount if applicable
                    if volume and tier.volume_discounts:
                        for vol_threshold, discount in sorted(
                            tier.volume_discounts.items(),
                            reverse=True
                        ):
                            if volume >= vol_threshold:
                                base_price *= Decimal(1 - discount)
                                break
                    break
                    
            # Apply quality multiplier
            quality_factor = quality.trust_adjusted_score * quality_multiplier
            price = base_price * Decimal(quality_factor)
            
            # Apply trust-based discount
            trust_discount = self._calculate_trust_discount(user_trust)
            price *= Decimal(1 - trust_discount)
            
            # Apply market dynamics
            market_multiplier = market_conditions.get('demand_multiplier', 1.0)
            price *= Decimal(market_multiplier)
            
            # Apply data freshness premium/discount
            freshness_factor = self._calculate_freshness_factor(quality)
            price *= Decimal(freshness_factor)
            
            # Round to reasonable precision
            price = price.quantize(Decimal('0.01'))
            
            # Log pricing calculation
            await self._log_pricing_calculation(
                dataset_id,
                user_id,
                access_level,
                base_price,
                price,
                {
                    'quality_factor': quality_factor,
                    'trust_discount': trust_discount,
                    'market_multiplier': market_multiplier,
                    'freshness_factor': freshness_factor
                }
            )
            
            return price
            
        except Exception as e:
            logger.error(f"Error calculating dynamic price: {e}")
            raise
            
    async def create_quality_derivative(
        self,
        dataset_id: str,
        derivative_type: str,
        strike_quality: float,
        expiry: datetime,
        creator_id: str
    ) -> Dict[str, Any]:
        """
        Create a data quality derivative instrument
        """
        try:
            # Validate creator has sufficient trust
            creator_trust = await self._get_user_trust_score(creator_id)
            if creator_trust < 0.7:
                raise ValueError("Insufficient trust score to create derivatives")
                
            # Get current quality assessment
            current_quality = await self._get_latest_quality_assessment(dataset_id)
            
            # Calculate derivative pricing
            premium = await self._calculate_derivative_premium(
                dataset_id,
                derivative_type,
                strike_quality,
                current_quality.trust_adjusted_score,
                expiry
            )
            
            # Calculate required collateral
            collateral = await self._calculate_derivative_collateral(
                dataset_id,
                derivative_type,
                strike_quality,
                premium
            )
            
            # Create derivative
            derivative = {
                'derivative_id': f"dqd_{dataset_id}_{datetime.utcnow().timestamp()}",
                'dataset_id': dataset_id,
                'derivative_type': derivative_type,
                'strike_quality': strike_quality,
                'current_quality': current_quality.trust_adjusted_score,
                'expiry': expiry,
                'premium': str(premium),
                'collateral_required': str(collateral),
                'creator_id': creator_id,
                'created_at': datetime.utcnow(),
                'trust_adjusted_pricing': True
            }
            
            # Store derivative
            await self._store_derivative(derivative)
            
            # Publish derivative creation event
            await self._publish_derivative_creation(derivative)
            
            return derivative
            
        except Exception as e:
            logger.error(f"Error creating quality derivative: {e}")
            raise
            
    async def _assess_quality_dimensions(
        self,
        df: DataFrame,
        schema_info: Optional[Dict[str, Any]]
    ) -> Dict[DataQualityDimension, float]:
        """
        Assess individual quality dimensions using Spark
        """
        dimensions = {}
        
        # Completeness - check for null/missing values
        total_cells = df.count() * len(df.columns)
        null_count = sum(df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0])
        dimensions[DataQualityDimension.COMPLETENESS] = 1.0 - (null_count / total_cells)
        
        # Accuracy - requires external validation or rules
        if schema_info and 'validation_rules' in schema_info:
            accuracy_score = await self._validate_accuracy(df, schema_info['validation_rules'])
            dimensions[DataQualityDimension.ACCURACY] = accuracy_score
        else:
            dimensions[DataQualityDimension.ACCURACY] = 0.8  # Default assumption
            
        # Consistency - check for internal consistency
        consistency_score = await self._check_consistency(df, schema_info)
        dimensions[DataQualityDimension.CONSISTENCY] = consistency_score
        
        # Timeliness - check data freshness
        timeliness_score = await self._assess_timeliness(df, schema_info)
        dimensions[DataQualityDimension.TIMELINESS] = timeliness_score
        
        # Validity - check data format and constraints
        validity_score = await self._check_validity(df, schema_info)
        dimensions[DataQualityDimension.VALIDITY] = validity_score
        
        # Uniqueness - check for duplicates
        unique_count = df.distinct().count()
        total_count = df.count()
        dimensions[DataQualityDimension.UNIQUENESS] = unique_count / total_count if total_count > 0 else 1.0
        
        return dimensions
        
    async def _get_user_trust_profile(self, user_id: str) -> Dict[str, float]:
        """
        Get comprehensive trust profile from Graph Intelligence
        """
        # Get multi-dimensional trust scores
        trust_response = await self.graph_client.get_user_reputation(user_id)
        
        return {
            'overall_score': trust_response.get('overall_score', 0.0),
            'technical_prowess': trust_response.get('technical_prowess', 0.0),
            'collaboration_rating': trust_response.get('collaboration_rating', 0.0),
            'governance_influence': trust_response.get('governance_influence', 0.0),
            'creativity_index': trust_response.get('creativity_index', 0.0),
            'reliability_score': trust_response.get('reliability_score', 0.0),
            'data_quality_history': trust_response.get('data_quality_history', 0.0)
        }
        
    def _calculate_overall_score(
        self,
        dimensions: Dict[DataQualityDimension, float]
    ) -> float:
        """
        Calculate weighted overall quality score
        """
        score = 0.0
        for dim, value in dimensions.items():
            weight = self.quality_weights.get(dim, 0.0)
            score += value * weight
        return min(max(score, 0.0), 1.0)
        
    def _adjust_score_by_trust(
        self,
        quality_score: float,
        assessor_trust: float
    ) -> float:
        """
        Adjust quality score based on assessor's trust score
        """
        # Use weighted average with higher weight on quality
        # but bounded by assessor trust
        trust_weight = 0.3
        quality_weight = 0.7
        
        adjusted = (quality_score * quality_weight + assessor_trust * trust_weight)
        
        # Can't exceed assessor's trust by more than 10%
        max_score = min(assessor_trust * 1.1, 1.0)
        
        return min(adjusted, max_score) 