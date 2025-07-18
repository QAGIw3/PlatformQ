"""
Asset-Compute-Model Nexus Integration

Creates deep integration between digital assets (NFTs, models), compute resources,
and ML models to enable new financial instruments and collateral types.
"""

import httpx
import logging
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio

from app.integrations import IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)


class AssetType(Enum):
    """Types of digital assets"""
    MODEL_NFT = "model_nft"  # Trained ML model as NFT
    DATASET_NFT = "dataset_nft"  # Valuable dataset as NFT
    COMPUTE_VOUCHER = "compute_voucher"  # Pre-paid compute time
    ALGORITHM_NFT = "algorithm_nft"  # Proprietary algorithm
    SIMULATION_RESULT = "simulation_result"  # Valuable simulation output
    SYNTHETIC_DATA = "synthetic_data"  # Generated synthetic datasets


@dataclass
class DigitalAssetValuation:
    """Valuation of a digital asset for collateral purposes"""
    asset_id: str
    asset_type: AssetType
    owner_id: str
    intrinsic_value: Decimal  # Base value of the asset
    liquidity_discount: Decimal  # Discount for illiquidity (0-1)
    volatility_adjustment: Decimal  # Adjustment for price volatility
    collateral_value: Decimal  # Final collateral value
    confidence_score: float  # Confidence in valuation (0-1)
    valuation_date: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelPerformanceMetrics:
    """Performance metrics for ML models"""
    model_id: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    inference_time_ms: float
    resource_efficiency: float  # Performance per compute unit
    last_updated: datetime
    usage_count: int = 0
    revenue_generated: Decimal = Decimal("0")


@dataclass
class ComputeModelBundle:
    """Bundle of compute resources with ML model"""
    bundle_id: str
    model_id: str
    compute_hours: Decimal
    resource_type: str  # GPU, TPU, CPU
    guaranteed_performance: float
    price_per_inference: Decimal
    expiry_date: datetime
    transferable: bool = True


@dataclass
class AssetBackedComputeFuture:
    """Compute future backed by digital assets"""
    future_id: str
    underlying_asset_id: str
    asset_type: AssetType
    compute_equivalent: Decimal  # Compute hours equivalent
    strike_price: Decimal
    expiry_date: datetime
    is_american: bool = False  # American vs European style


class AssetComputeNexus:
    """Integration between digital assets, compute, and models"""
    
    def __init__(
        self,
        digital_asset_url: str = "http://digital-asset-service:8000",
        mlops_url: str = "http://mlops-service:8000",
        ignite_cache: Optional[IgniteCache] = None,
        pulsar_publisher: Optional[PulsarEventPublisher] = None
    ):
        self.digital_asset_url = digital_asset_url
        self.mlops_url = mlops_url
        self.ignite_cache = ignite_cache
        self.pulsar_publisher = pulsar_publisher
        
        # HTTP clients
        self.asset_client = httpx.AsyncClient(base_url=digital_asset_url, timeout=30.0)
        self.mlops_client = httpx.AsyncClient(base_url=mlops_url, timeout=30.0)
        
        # Valuation parameters
        self.liquidity_discounts = {
            AssetType.MODEL_NFT: Decimal("0.3"),  # 30% discount
            AssetType.DATASET_NFT: Decimal("0.4"),  # 40% discount
            AssetType.COMPUTE_VOUCHER: Decimal("0.1"),  # 10% discount
            AssetType.ALGORITHM_NFT: Decimal("0.5"),  # 50% discount
            AssetType.SIMULATION_RESULT: Decimal("0.6"),  # 60% discount
            AssetType.SYNTHETIC_DATA: Decimal("0.35")  # 35% discount
        }
        
    async def value_digital_asset_as_collateral(
        self,
        asset_id: str,
        asset_type: AssetType,
        owner_id: str
    ) -> DigitalAssetValuation:
        """Value a digital asset for use as collateral"""
        try:
            # Get asset details from digital asset service
            asset_details = await self._get_asset_details(asset_id)
            
            # Calculate intrinsic value based on asset type
            intrinsic_value = await self._calculate_intrinsic_value(
                asset_id,
                asset_type,
                asset_details
            )
            
            # Get market data for liquidity assessment
            market_data = await self._get_asset_market_data(asset_id)
            liquidity_discount = self._calculate_liquidity_discount(
                asset_type,
                market_data
            )
            
            # Calculate volatility adjustment
            volatility = await self._calculate_asset_volatility(asset_id, market_data)
            volatility_adjustment = self._calculate_volatility_adjustment(volatility)
            
            # Final collateral value
            collateral_value = (
                intrinsic_value * 
                (Decimal("1") - liquidity_discount) * 
                (Decimal("1") - volatility_adjustment)
            )
            
            # Confidence score based on data quality
            confidence = self._calculate_confidence_score(
                asset_details,
                market_data,
                asset_type
            )
            
            valuation = DigitalAssetValuation(
                asset_id=asset_id,
                asset_type=asset_type,
                owner_id=owner_id,
                intrinsic_value=intrinsic_value,
                liquidity_discount=liquidity_discount,
                volatility_adjustment=volatility_adjustment,
                collateral_value=collateral_value,
                confidence_score=confidence,
                valuation_date=datetime.utcnow(),
                metadata={
                    "asset_details": asset_details,
                    "market_data": market_data,
                    "volatility_30d": float(volatility)
                }
            )
            
            # Cache valuation
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"asset_valuation:{asset_id}",
                    valuation.__dict__,
                    ttl=3600  # 1 hour cache
                )
            
            # Publish valuation event
            if self.pulsar_publisher:
                await self.pulsar_publisher.publish(
                    "persistent://platformq/assets/valuations",
                    {
                        "asset_id": asset_id,
                        "collateral_value": str(collateral_value),
                        "confidence": confidence,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            
            return valuation
            
        except Exception as e:
            logger.error(f"Error valuing digital asset: {e}")
            # Return conservative valuation on error
            return DigitalAssetValuation(
                asset_id=asset_id,
                asset_type=asset_type,
                owner_id=owner_id,
                intrinsic_value=Decimal("0"),
                liquidity_discount=Decimal("1"),  # 100% discount
                volatility_adjustment=Decimal("0"),
                collateral_value=Decimal("0"),
                confidence_score=0.0,
                valuation_date=datetime.utcnow()
            )
    
    async def create_model_compute_bundle(
        self,
        model_id: str,
        compute_hours: Decimal,
        resource_type: str,
        guaranteed_performance: float
    ) -> ComputeModelBundle:
        """Create a bundle of ML model + compute resources"""
        try:
            # Get model performance metrics
            metrics = await self.get_model_performance_metrics(model_id)
            
            # Calculate optimal pricing
            price_per_inference = self._calculate_inference_price(
                metrics,
                resource_type,
                compute_hours
            )
            
            # Create bundle
            bundle = ComputeModelBundle(
                bundle_id=f"bundle_{model_id}_{datetime.utcnow().timestamp()}",
                model_id=model_id,
                compute_hours=compute_hours,
                resource_type=resource_type,
                guaranteed_performance=guaranteed_performance,
                price_per_inference=price_per_inference,
                expiry_date=datetime.utcnow() + timedelta(days=30)
            )
            
            # Register bundle as digital asset
            asset_id = await self._register_bundle_as_asset(bundle)
            
            # Store bundle details
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"compute_bundle:{bundle.bundle_id}",
                    bundle.__dict__,
                    ttl=86400 * 30  # 30 days
                )
            
            return bundle
            
        except Exception as e:
            logger.error(f"Error creating model-compute bundle: {e}")
            raise
    
    async def create_asset_backed_compute_future(
        self,
        asset_id: str,
        asset_type: AssetType,
        compute_hours: Decimal,
        strike_price: Decimal,
        expiry_days: int = 30
    ) -> AssetBackedComputeFuture:
        """Create a compute future backed by a digital asset"""
        try:
            # Value the underlying asset
            valuation = await self.value_digital_asset_as_collateral(
                asset_id,
                asset_type,
                "system"  # System valuation
            )
            
            # Calculate compute equivalent based on asset value
            compute_equivalent = self._calculate_compute_equivalent(
                valuation.collateral_value,
                strike_price
            )
            
            # Create future contract
            future = AssetBackedComputeFuture(
                future_id=f"acf_{asset_id}_{datetime.utcnow().timestamp()}",
                underlying_asset_id=asset_id,
                asset_type=asset_type,
                compute_equivalent=compute_equivalent,
                strike_price=strike_price,
                expiry_date=datetime.utcnow() + timedelta(days=expiry_days)
            )
            
            # Register future
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"asset_compute_future:{future.future_id}",
                    future.__dict__,
                    ttl=86400 * expiry_days
                )
            
            # Publish creation event
            if self.pulsar_publisher:
                await self.pulsar_publisher.publish(
                    "persistent://platformq/derivatives/asset-backed-futures",
                    {
                        "future_id": future.future_id,
                        "asset_id": asset_id,
                        "compute_equivalent": str(compute_equivalent),
                        "strike_price": str(strike_price),
                        "expiry": future.expiry_date.isoformat()
                    }
                )
            
            return future
            
        except Exception as e:
            logger.error(f"Error creating asset-backed compute future: {e}")
            raise
    
    async def get_model_performance_metrics(
        self,
        model_id: str
    ) -> ModelPerformanceMetrics:
        """Get performance metrics for an ML model"""
        try:
            # Get metrics from MLOps service
            response = await self.mlops_client.get(
                f"/api/v1/models/{model_id}/metrics"
            )
            
            if response.status_code == 200:
                data = response.json()
                
                return ModelPerformanceMetrics(
                    model_id=model_id,
                    accuracy=data.get("accuracy", 0.0),
                    precision=data.get("precision", 0.0),
                    recall=data.get("recall", 0.0),
                    f1_score=data.get("f1_score", 0.0),
                    inference_time_ms=data.get("inference_time_ms", 100),
                    resource_efficiency=data.get("resource_efficiency", 0.5),
                    last_updated=datetime.fromisoformat(data.get("last_updated", datetime.utcnow().isoformat())),
                    usage_count=data.get("usage_count", 0),
                    revenue_generated=Decimal(str(data.get("revenue_generated", "0")))
                )
            else:
                # Return default metrics
                return ModelPerformanceMetrics(
                    model_id=model_id,
                    accuracy=0.7,
                    precision=0.7,
                    recall=0.7,
                    f1_score=0.7,
                    inference_time_ms=100,
                    resource_efficiency=0.5,
                    last_updated=datetime.utcnow()
                )
                
        except Exception as e:
            logger.error(f"Error getting model metrics: {e}")
            return ModelPerformanceMetrics(
                model_id=model_id,
                accuracy=0.5,
                precision=0.5,
                recall=0.5,
                f1_score=0.5,
                inference_time_ms=200,
                resource_efficiency=0.3,
                last_updated=datetime.utcnow()
            )
    
    async def evaluate_model_collateral_value(
        self,
        model_id: str,
        owner_id: str
    ) -> Decimal:
        """Evaluate an ML model's value as collateral"""
        try:
            # Get model performance
            metrics = await self.get_model_performance_metrics(model_id)
            
            # Base value from revenue generation
            base_value = metrics.revenue_generated * Decimal("10")  # 10x revenue multiple
            
            # Adjust for performance
            performance_multiplier = Decimal(str(
                (metrics.accuracy + metrics.precision + metrics.recall) / 3
            ))
            
            # Adjust for efficiency
            efficiency_multiplier = Decimal(str(metrics.resource_efficiency))
            
            # Adjust for usage/popularity
            usage_multiplier = min(Decimal(str(metrics.usage_count / 1000)), Decimal("2"))
            
            # Calculate collateral value
            collateral_value = (
                base_value * 
                performance_multiplier * 
                efficiency_multiplier * 
                (Decimal("1") + usage_multiplier)
            )
            
            # Apply model-specific discount
            model_discount = Decimal("0.4")  # 40% haircut for models
            final_value = collateral_value * (Decimal("1") - model_discount)
            
            return max(final_value, Decimal("100"))  # Minimum $100 value
            
        except Exception as e:
            logger.error(f"Error evaluating model collateral: {e}")
            return Decimal("100")  # Minimum value
    
    async def create_synthetic_data_derivative(
        self,
        dataset_specs: Dict[str, Any],
        generation_model_id: str,
        compute_budget: Decimal
    ) -> Dict[str, Any]:
        """Create a derivative for synthetic data generation"""
        try:
            # Estimate generation cost
            generation_cost = await self._estimate_synthetic_data_cost(
                dataset_specs,
                generation_model_id
            )
            
            # Create synthetic data future
            future = {
                "id": f"sdf_{datetime.utcnow().timestamp()}",
                "dataset_specs": dataset_specs,
                "generation_model_id": generation_model_id,
                "compute_budget": str(compute_budget),
                "estimated_cost": str(generation_cost),
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store future
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"synthetic_data_future:{future['id']}",
                    future,
                    ttl=86400 * 7  # 7 days
                )
            
            return future
            
        except Exception as e:
            logger.error(f"Error creating synthetic data derivative: {e}")
            raise
    
    async def optimize_asset_compute_portfolio(
        self,
        user_id: str,
        assets: List[str],
        compute_needs: Dict[str, Decimal],
        risk_tolerance: float = 0.5
    ) -> Dict[str, Any]:
        """Optimize portfolio of digital assets and compute resources"""
        try:
            # Get asset valuations
            valuations = []
            for asset_id in assets:
                val = await self.value_digital_asset_as_collateral(
                    asset_id,
                    AssetType.MODEL_NFT,  # Default type
                    user_id
                )
                valuations.append(val)
            
            # Calculate total collateral value
            total_collateral = sum(v.collateral_value for v in valuations)
            
            # Optimize compute allocation
            allocation = {}
            remaining_value = total_collateral
            
            for resource_type, needed_hours in compute_needs.items():
                # Get current market price
                market_price = await self._get_compute_market_price(resource_type)
                
                # Calculate affordable hours
                affordable_hours = remaining_value / market_price
                allocated_hours = min(needed_hours, affordable_hours)
                
                allocation[resource_type] = {
                    "requested": str(needed_hours),
                    "allocated": str(allocated_hours),
                    "market_price": str(market_price),
                    "total_cost": str(allocated_hours * market_price)
                }
                
                remaining_value -= allocated_hours * market_price
            
            # Calculate portfolio metrics
            portfolio_metrics = {
                "total_asset_value": str(total_collateral),
                "compute_purchasing_power": str(total_collateral * Decimal("0.8")),  # 80% utilization
                "allocation": allocation,
                "remaining_value": str(max(remaining_value, Decimal("0"))),
                "risk_score": self._calculate_portfolio_risk(valuations, risk_tolerance),
                "recommendations": self._generate_portfolio_recommendations(
                    valuations,
                    allocation,
                    risk_tolerance
                )
            }
            
            return portfolio_metrics
            
        except Exception as e:
            logger.error(f"Error optimizing asset-compute portfolio: {e}")
            return {
                "error": str(e),
                "total_asset_value": "0",
                "compute_purchasing_power": "0",
                "allocation": {},
                "recommendations": ["Unable to optimize - please try again"]
            }
    
    # Private helper methods
    async def _get_asset_details(self, asset_id: str) -> Dict[str, Any]:
        """Get details of a digital asset"""
        try:
            response = await self.asset_client.get(f"/api/v1/assets/{asset_id}")
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except:
            return {}
    
    async def _calculate_intrinsic_value(
        self,
        asset_id: str,
        asset_type: AssetType,
        asset_details: Dict[str, Any]
    ) -> Decimal:
        """Calculate intrinsic value of an asset"""
        if asset_type == AssetType.MODEL_NFT:
            # Value based on model performance and revenue
            return await self.evaluate_model_collateral_value(
                asset_details.get("model_id", asset_id),
                asset_details.get("owner_id", "unknown")
            )
        
        elif asset_type == AssetType.COMPUTE_VOUCHER:
            # Value based on compute hours and market price
            hours = Decimal(str(asset_details.get("compute_hours", "0")))
            market_price = await self._get_compute_market_price(
                asset_details.get("resource_type", "gpu")
            )
            return hours * market_price * Decimal("0.9")  # 10% discount
        
        elif asset_type == AssetType.DATASET_NFT:
            # Value based on data quality and uniqueness
            quality_score = asset_details.get("quality_score", 0.5)
            unique_records = asset_details.get("unique_records", 1000)
            base_value = Decimal(str(unique_records)) * Decimal("0.01")  # $0.01 per record
            return base_value * Decimal(str(quality_score))
        
        else:
            # Default valuation
            return Decimal(str(asset_details.get("listed_price", "1000")))
    
    async def _get_asset_market_data(self, asset_id: str) -> Dict[str, Any]:
        """Get market data for an asset"""
        try:
            response = await self.asset_client.get(
                f"/api/v1/assets/{asset_id}/market-data"
            )
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "last_price": "0",
                    "volume_24h": "0",
                    "trades_24h": 0
                }
        except:
            return {
                "last_price": "0",
                "volume_24h": "0",
                "trades_24h": 0
            }
    
    def _calculate_liquidity_discount(
        self,
        asset_type: AssetType,
        market_data: Dict[str, Any]
    ) -> Decimal:
        """Calculate liquidity discount based on trading activity"""
        base_discount = self.liquidity_discounts.get(
            asset_type,
            Decimal("0.5")
        )
        
        # Adjust based on trading volume
        volume_24h = Decimal(str(market_data.get("volume_24h", "0")))
        trades_24h = market_data.get("trades_24h", 0)
        
        if volume_24h > Decimal("10000") and trades_24h > 10:
            # High liquidity - reduce discount
            return base_discount * Decimal("0.5")
        elif volume_24h > Decimal("1000") and trades_24h > 5:
            # Medium liquidity
            return base_discount * Decimal("0.75")
        else:
            # Low liquidity - full discount
            return base_discount
    
    async def _calculate_asset_volatility(
        self,
        asset_id: str,
        market_data: Dict[str, Any]
    ) -> Decimal:
        """Calculate asset price volatility"""
        # Simplified volatility calculation
        # In production, would use historical price data
        return Decimal("0.3")  # 30% volatility
    
    def _calculate_volatility_adjustment(self, volatility: Decimal) -> Decimal:
        """Calculate adjustment factor based on volatility"""
        # Higher volatility = higher adjustment
        if volatility > Decimal("0.5"):
            return Decimal("0.5")  # 50% adjustment
        elif volatility > Decimal("0.3"):
            return Decimal("0.3")  # 30% adjustment
        elif volatility > Decimal("0.1"):
            return Decimal("0.15")  # 15% adjustment
        else:
            return Decimal("0.05")  # 5% adjustment
    
    def _calculate_confidence_score(
        self,
        asset_details: Dict[str, Any],
        market_data: Dict[str, Any],
        asset_type: AssetType
    ) -> float:
        """Calculate confidence in valuation"""
        confidence = 0.5  # Base confidence
        
        # Adjust based on data completeness
        if asset_details:
            confidence += 0.2
        if market_data.get("volume_24h") and Decimal(market_data["volume_24h"]) > 0:
            confidence += 0.2
        if asset_type in [AssetType.MODEL_NFT, AssetType.COMPUTE_VOUCHER]:
            confidence += 0.1  # More confidence in these asset types
        
        return min(confidence, 1.0)
    
    def _calculate_inference_price(
        self,
        metrics: ModelPerformanceMetrics,
        resource_type: str,
        compute_hours: Decimal
    ) -> Decimal:
        """Calculate price per inference for a model"""
        # Base compute cost
        compute_costs = {
            "gpu": Decimal("2.5"),  # $2.5/hour
            "tpu": Decimal("4.0"),  # $4/hour
            "cpu": Decimal("0.1")   # $0.1/hour
        }
        
        hourly_cost = compute_costs.get(resource_type, Decimal("1.0"))
        
        # Inferences per hour based on inference time
        inferences_per_hour = Decimal("3600000") / Decimal(str(metrics.inference_time_ms))
        
        # Cost per inference
        base_cost = hourly_cost / inferences_per_hour
        
        # Add margin based on model quality
        quality_multiplier = Decimal(str(1 + metrics.f1_score))
        
        return base_cost * quality_multiplier
    
    async def _register_bundle_as_asset(self, bundle: ComputeModelBundle) -> str:
        """Register a compute bundle as a digital asset"""
        try:
            response = await self.asset_client.post(
                "/api/v1/assets/create",
                json={
                    "asset_type": "compute_bundle",
                    "name": f"Model Compute Bundle - {bundle.model_id}",
                    "description": f"{bundle.compute_hours} hours of {bundle.resource_type} with model {bundle.model_id}",
                    "metadata": {
                        "bundle_id": bundle.bundle_id,
                        "model_id": bundle.model_id,
                        "compute_hours": str(bundle.compute_hours),
                        "resource_type": bundle.resource_type,
                        "guaranteed_performance": bundle.guaranteed_performance,
                        "price_per_inference": str(bundle.price_per_inference),
                        "expiry_date": bundle.expiry_date.isoformat()
                    }
                }
            )
            
            if response.status_code == 200:
                return response.json()["asset_id"]
            else:
                raise Exception(f"Failed to register bundle: {response.text}")
                
        except Exception as e:
            logger.error(f"Error registering bundle as asset: {e}")
            raise
    
    def _calculate_compute_equivalent(
        self,
        asset_value: Decimal,
        compute_price: Decimal
    ) -> Decimal:
        """Calculate compute hours equivalent to asset value"""
        if compute_price > 0:
            return asset_value / compute_price
        else:
            return Decimal("0")
    
    async def _estimate_synthetic_data_cost(
        self,
        dataset_specs: Dict[str, Any],
        generation_model_id: str
    ) -> Decimal:
        """Estimate cost to generate synthetic data"""
        # Get model metrics
        metrics = await self.get_model_performance_metrics(generation_model_id)
        
        # Estimate based on dataset size
        num_records = dataset_specs.get("num_records", 1000)
        num_features = dataset_specs.get("num_features", 10)
        
        # Rough estimate: 1ms per record per feature
        total_inference_time = num_records * num_features * 1
        total_hours = Decimal(str(total_inference_time)) / Decimal("3600000")
        
        # GPU cost
        gpu_cost = total_hours * Decimal("2.5")
        
        return gpu_cost * Decimal("1.5")  # 50% margin
    
    async def _get_compute_market_price(self, resource_type: str) -> Decimal:
        """Get current market price for compute resources"""
        # In production, would query live market data
        prices = {
            "gpu": Decimal("2.5"),
            "tpu": Decimal("4.0"),
            "cpu": Decimal("0.1"),
            "fpga": Decimal("1.5")
        }
        return prices.get(resource_type, Decimal("1.0"))
    
    def _calculate_portfolio_risk(
        self,
        valuations: List[DigitalAssetValuation],
        risk_tolerance: float
    ) -> float:
        """Calculate portfolio risk score"""
        if not valuations:
            return 1.0  # Maximum risk
        
        # Average confidence weighted by value
        total_value = sum(v.collateral_value for v in valuations)
        if total_value == 0:
            return 1.0
        
        weighted_confidence = sum(
            v.confidence_score * float(v.collateral_value / total_value)
            for v in valuations
        )
        
        # Risk is inverse of confidence
        base_risk = 1.0 - weighted_confidence
        
        # Adjust for concentration
        concentration_risk = 1.0 / len(valuations) if valuations else 1.0
        
        # Combined risk
        portfolio_risk = (base_risk * 0.7 + concentration_risk * 0.3)
        
        # Adjust for risk tolerance
        if risk_tolerance < 0.3:  # Conservative
            portfolio_risk *= 1.2
        elif risk_tolerance > 0.7:  # Aggressive
            portfolio_risk *= 0.8
        
        return min(portfolio_risk, 1.0)
    
    def _generate_portfolio_recommendations(
        self,
        valuations: List[DigitalAssetValuation],
        allocation: Dict[str, Any],
        risk_tolerance: float
    ) -> List[str]:
        """Generate portfolio optimization recommendations"""
        recommendations = []
        
        # Asset diversity
        if len(valuations) < 3:
            recommendations.append("Diversify portfolio with more digital assets")
        
        # Asset quality
        low_confidence_assets = [v for v in valuations if v.confidence_score < 0.5]
        if low_confidence_assets:
            recommendations.append(f"Consider replacing {len(low_confidence_assets)} low-confidence assets")
        
        # Compute allocation efficiency
        for resource_type, alloc in allocation.items():
            requested = Decimal(alloc["requested"])
            allocated = Decimal(alloc["allocated"])
            if allocated < requested * Decimal("0.8"):
                recommendations.append(f"Insufficient collateral for {resource_type} needs - add more assets")
        
        # Risk management
        avg_liquidity_discount = sum(v.liquidity_discount for v in valuations) / len(valuations) if valuations else 0
        if avg_liquidity_discount > Decimal("0.4"):
            recommendations.append("Portfolio has high illiquidity risk - consider more liquid assets")
        
        return recommendations
    
    async def close(self):
        """Close HTTP clients"""
        await self.asset_client.aclose()
        await self.mlops_client.aclose() 