import asyncio
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import httpx
from functools import lru_cache

from app.models.integration import AssetDerivativeLink, CollateralAsset, MarketCreationRequest
from app.integrations.digital_asset_service import DigitalAssetServiceClient
from app.integrations.graph_intelligence import GraphIntelligenceClient
from app.integrations.verifiable_credentials import VerifiableCredentialClient
from app.integrations.pulsar_events import PulsarEventPublisher

logger = logging.getLogger(__name__)

class MarketplaceIntegration:
    """
    Integration layer between derivatives platform and digital asset marketplace
    Enables creation of derivatives on NFTs, digital assets, and compute resources
    """
    
    def __init__(
        self,
        asset_service: DigitalAssetServiceClient,
        graph_service: GraphIntelligenceClient,
        vc_service: VerifiableCredentialClient,
        pulsar_client: PulsarEventPublisher
    ):
        self.asset_service = asset_service
        self.graph_service = graph_service
        self.vc_service = vc_service
        self.pulsar = pulsar_client
        
        # Cache for asset data
        self._asset_cache = {}
        self._price_cache = {}
        
    async def create_asset_derivative_market(
        self,
        asset_id: str,
        derivative_type: str = "perpetual",
        oracle_sources: Optional[List[str]] = None
    ) -> Dict:
        """
        Create a derivative market for a digital asset
        """
        # Fetch asset details
        asset = await self.asset_service.get_asset(asset_id)
        if not asset:
            raise ValueError(f"Asset {asset_id} not found")
            
        # Verify asset is eligible for derivatives
        eligibility = await self._check_derivative_eligibility(asset)
        if not eligibility["eligible"]:
            raise ValueError(f"Asset not eligible: {eligibility['reason']}")
            
        # Generate oracle configuration
        oracle_config = await self._generate_oracle_config(asset, oracle_sources)
        
        # Calculate risk parameters based on asset characteristics
        risk_params = await self._calculate_risk_parameters(asset)
        
        # Create market creation request
        market_request = MarketCreationRequest(
            underlying_asset_id=asset_id,
            asset_type=asset["type"],
            derivative_type=derivative_type,
            oracle_config=oracle_config,
            risk_parameters=risk_params,
            metadata={
                "asset_name": asset["name"],
                "asset_collection": asset.get("collection"),
                "asset_creator": asset["creator"],
                "integration_source": "marketplace"
            }
        )
        
        # Submit to DAO for approval
        proposal_id = await self._submit_market_proposal(market_request)
        
        # Emit integration event
        await self.pulsar.publish(
            "derivative-asset-integration",
            {
                "event_type": "market_proposed",
                "asset_id": asset_id,
                "proposal_id": proposal_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {
            "proposal_id": proposal_id,
            "asset_id": asset_id,
            "estimated_approval_time": "24h",
            "oracle_config": oracle_config,
            "risk_parameters": risk_params
        }
    
    async def enable_asset_as_collateral(
        self,
        asset_id: str,
        weight: Decimal = Decimal("0.5"),
        cap_usd: Decimal = Decimal("1000000")
    ) -> Dict:
        """
        Enable a digital asset to be used as collateral in derivatives
        """
        # Fetch asset details
        asset = await self.asset_service.get_asset(asset_id)
        
        # Evaluate asset for collateral eligibility
        evaluation = await self._evaluate_collateral_eligibility(asset)
        if not evaluation["eligible"]:
            raise ValueError(f"Asset not eligible as collateral: {evaluation['reason']}")
            
        # Calculate dynamic weight based on asset properties
        dynamic_weight = await self._calculate_collateral_weight(asset)
        final_weight = min(weight, dynamic_weight)
        
        # Register as collateral
        collateral_asset = CollateralAsset(
            asset_id=asset_id,
            asset_address=asset["contract_address"],
            weight=final_weight,
            cap_usd=cap_usd,
            metadata={
                "asset_type": asset["type"],
                "liquidity_score": evaluation["liquidity_score"],
                "volatility_score": evaluation["volatility_score"],
                "trust_score": evaluation["trust_score"]
            }
        )
        
        # Submit for approval
        result = await self._register_collateral_asset(collateral_asset)
        
        return {
            "asset_id": asset_id,
            "approved_weight": final_weight,
            "cap_usd": cap_usd,
            "status": result["status"],
            "evaluation_scores": evaluation
        }
    
    async def create_compute_futures(
        self,
        compute_spec: Dict,
        duration_hours: int,
        quantity: int
    ) -> Dict:
        """
        Create futures contracts for compute resources
        """
        # Verify compute resource availability
        availability = await self._check_compute_availability(compute_spec)
        if not availability["available"]:
            raise ValueError("Compute resources not available")
            
        # Create standardized compute contract
        contract_spec = {
            "resource_type": "compute",
            "specifications": {
                "gpu_type": compute_spec.get("gpu_type"),
                "gpu_count": compute_spec.get("gpu_count", 1),
                "cpu_cores": compute_spec.get("cpu_cores", 4),
                "memory_gb": compute_spec.get("memory_gb", 16),
                "storage_gb": compute_spec.get("storage_gb", 100)
            },
            "duration_hours": duration_hours,
            "quantity": quantity,
            "delivery_date": (datetime.utcnow() + timedelta(hours=duration_hours)).isoformat(),
            "settlement_type": "physical"  # Actual compute allocation
        }
        
        # Calculate pricing based on spot and historical data
        pricing = await self._calculate_compute_futures_price(compute_spec, duration_hours)
        
        # Create futures market
        market_id = await self._create_futures_market(
            "compute",
            contract_spec,
            pricing["initial_price"],
            pricing["margin_requirements"]
        )
        
        return {
            "market_id": market_id,
            "contract_spec": contract_spec,
            "initial_price": pricing["initial_price"],
            "margin_requirements": pricing["margin_requirements"],
            "estimated_spot_at_delivery": pricing["estimated_spot"]
        }
    
    async def create_model_derivatives(
        self,
        model_id: str,
        metric_type: str = "accuracy",
        strike_value: float = 0.95,
        expiry_days: int = 30
    ) -> Dict:
        """
        Create derivatives on AI model performance metrics
        """
        # Fetch model details
        model = await self.asset_service.get_model(model_id)
        
        # Get historical performance metrics
        performance = await self._get_model_performance_history(model_id, metric_type)
        
        # Calculate option parameters
        option_params = await self._calculate_model_option_params(
            performance,
            strike_value,
            expiry_days
        )
        
        # Create binary option market
        market_spec = {
            "underlying": f"model_{model_id}_{metric_type}",
            "option_type": "binary",
            "strike": strike_value,
            "expiry": (datetime.utcnow() + timedelta(days=expiry_days)).isoformat(),
            "settlement_source": "mlflow",  # Use MLflow for performance verification
            "payout_structure": {
                "if_true": 1.0,  # Payout if metric >= strike
                "if_false": 0.0
            }
        }
        
        # Create market
        result = await self._create_option_market(market_spec, option_params)
        
        return {
            "market_id": result["market_id"],
            "model_id": model_id,
            "metric_type": metric_type,
            "strike_value": strike_value,
            "expiry": market_spec["expiry"],
            "implied_probability": option_params["implied_probability"],
            "initial_premium": option_params["premium"]
        }
    
    async def create_dataset_derivatives(
        self,
        dataset_id: str,
        metric: str = "usage_volume",
        derivative_type: str = "swap"
    ) -> Dict:
        """
        Create derivatives on dataset usage metrics
        """
        # Fetch dataset information
        dataset = await self.asset_service.get_dataset(dataset_id)
        
        # Analyze dataset usage patterns
        usage_analysis = await self._analyze_dataset_usage(dataset_id)
        
        if derivative_type == "swap":
            # Create usage volume swap
            swap_spec = {
                "underlying": f"dataset_{dataset_id}_usage",
                "notional_type": "variable",  # Notional based on actual usage
                "payment_frequency": "monthly",
                "floating_rate": "actual_usage_revenue",
                "fixed_rate": usage_analysis["average_monthly_revenue"],
                "margin_requirements": {
                    "initial": 0.1,  # 10%
                    "maintenance": 0.05  # 5%
                }
            }
            
            result = await self._create_swap_market(swap_spec)
            
        elif derivative_type == "future":
            # Create dataset access futures
            future_spec = {
                "underlying": f"dataset_{dataset_id}_access",
                "contract_size": 1000,  # 1000 API calls
                "delivery_months": ["spot", "next_month", "quarter"],
                "settlement": "cash",
                "tick_size": 0.001  # $0.001 per API call
            }
            
            result = await self._create_futures_market("dataset", future_spec)
            
        return {
            "market_id": result["market_id"],
            "dataset_id": dataset_id,
            "derivative_type": derivative_type,
            "usage_analysis": usage_analysis,
            "pricing_model": result.get("pricing_model")
        }
    
    async def get_cross_asset_correlations(
        self,
        asset_ids: List[str],
        window_days: int = 30
    ) -> Dict:
        """
        Calculate correlations between different asset types for risk management
        """
        # Fetch price histories
        price_histories = {}
        for asset_id in asset_ids:
            history = await self._get_asset_price_history(asset_id, window_days)
            price_histories[asset_id] = history
            
        # Calculate correlation matrix
        correlations = await self._calculate_correlation_matrix(price_histories)
        
        # Identify hedging opportunities
        hedging_pairs = await self._identify_hedging_opportunities(correlations)
        
        return {
            "correlation_matrix": correlations,
            "hedging_opportunities": hedging_pairs,
            "calculation_window": window_days,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def create_index_derivative(
        self,
        index_name: str,
        components: List[Dict[str, float]],  # [(asset_id, weight)]
        derivative_type: str = "perpetual"
    ) -> Dict:
        """
        Create derivatives on baskets/indices of digital assets
        """
        # Validate components
        total_weight = sum(c["weight"] for c in components)
        if abs(total_weight - 1.0) > 0.001:
            raise ValueError("Component weights must sum to 1.0")
            
        # Fetch component details
        component_assets = []
        for component in components:
            asset = await self.asset_service.get_asset(component["asset_id"])
            component_assets.append({
                "asset": asset,
                "weight": component["weight"]
            })
            
        # Calculate index parameters
        index_params = await self._calculate_index_parameters(component_assets)
        
        # Create index definition
        index_def = {
            "name": index_name,
            "components": components,
            "rebalance_frequency": "monthly",
            "calculation_method": "market_cap_weighted",
            "base_value": 1000,
            "inception_date": datetime.utcnow().isoformat()
        }
        
        # Register index
        index_id = await self._register_index(index_def)
        
        # Create derivative market on index
        if derivative_type == "perpetual":
            market_spec = {
                "underlying": f"index_{index_id}",
                "contract_multiplier": 0.01,  # $0.01 per index point
                "funding_interval": "8h",
                "max_leverage": 20,
                "margin_requirements": index_params["margin_requirements"]
            }
        elif derivative_type == "option":
            market_spec = {
                "underlying": f"index_{index_id}",
                "option_style": "european",
                "contract_multiplier": 0.01,
                "available_strikes": index_params["suggested_strikes"],
                "available_expiries": ["weekly", "monthly", "quarterly"]
            }
            
        result = await self._create_derivative_market(derivative_type, market_spec)
        
        return {
            "index_id": index_id,
            "market_id": result["market_id"],
            "index_definition": index_def,
            "current_value": index_params["current_value"],
            "component_valuations": index_params["component_valuations"]
        }
    
    # Helper methods
    
    async def _check_derivative_eligibility(self, asset: Dict) -> Dict:
        """Check if an asset is eligible for derivative creation"""
        eligibility_criteria = {
            "min_liquidity": 10000,  # $10k daily volume
            "min_holders": 10,
            "min_age_days": 7,
            "verified_creator": True
        }
        
        # Check liquidity
        liquidity = await self._get_asset_liquidity(asset["id"])
        if liquidity < eligibility_criteria["min_liquidity"]:
            return {
                "eligible": False,
                "reason": f"Insufficient liquidity: ${liquidity:,.2f}"
            }
            
        # Check holder distribution
        holders = await self.graph_service.get_asset_holders(asset["id"])
        if len(holders) < eligibility_criteria["min_holders"]:
            return {
                "eligible": False,
                "reason": f"Insufficient holders: {len(holders)}"
            }
            
        # Check age
        age_days = (datetime.utcnow() - datetime.fromisoformat(asset["created_at"])).days
        if age_days < eligibility_criteria["min_age_days"]:
            return {
                "eligible": False,
                "reason": f"Asset too new: {age_days} days"
            }
            
        # Verify creator
        if eligibility_criteria["verified_creator"]:
            creator_verified = await self.vc_service.is_verified(asset["creator"])
            if not creator_verified:
                return {
                    "eligible": False,
                    "reason": "Creator not verified"
                }
                
        return {
            "eligible": True,
            "liquidity": liquidity,
            "holders": len(holders),
            "age_days": age_days
        }
    
    async def _generate_oracle_config(
        self,
        asset: Dict,
        custom_sources: Optional[List[str]] = None
    ) -> Dict:
        """Generate oracle configuration for an asset"""
        oracle_config = {
            "price_sources": [],
            "aggregation_method": "weighted_median",
            "update_frequency": "1m",
            "staleness_threshold": "5m"
        }
        
        # Add marketplace internal price
        oracle_config["price_sources"].append({
            "type": "internal",
            "source": "marketplace_trades",
            "weight": 0.4,
            "min_trades": 5
        })
        
        # Add collection floor price if applicable
        if asset.get("collection"):
            oracle_config["price_sources"].append({
                "type": "collection_floor",
                "collection_id": asset["collection"],
                "weight": 0.3,
                "adjustment_factor": await self._get_rarity_factor(asset)
            })
            
        # Add ML price prediction
        oracle_config["price_sources"].append({
            "type": "ml_prediction",
            "model": "asset_price_predictor",
            "weight": 0.2,
            "features": ["volume", "holder_count", "social_sentiment"]
        })
        
        # Add custom sources if provided
        if custom_sources:
            for source in custom_sources:
                oracle_config["price_sources"].append({
                    "type": "external",
                    "source": source,
                    "weight": 0.1
                })
                
        return oracle_config
    
    async def _calculate_risk_parameters(self, asset: Dict) -> Dict:
        """Calculate risk parameters for an asset derivative"""
        # Get historical volatility
        volatility = await self._calculate_asset_volatility(asset["id"])
        
        # Base parameters
        risk_params = {
            "max_leverage": 10,
            "initial_margin": 0.1,  # 10%
            "maintenance_margin": 0.05,  # 5%
            "liquidation_fee": 0.01,  # 1%
            "max_position_size": 100000,  # $100k
            "funding_rate_limit": 0.003  # 0.3% per hour max
        }
        
        # Adjust based on volatility
        if volatility > 1.0:  # High volatility
            risk_params["max_leverage"] = 5
            risk_params["initial_margin"] = 0.2
            risk_params["maintenance_margin"] = 0.1
        elif volatility > 0.5:  # Medium volatility
            risk_params["max_leverage"] = 10
            risk_params["initial_margin"] = 0.15
            risk_params["maintenance_margin"] = 0.075
            
        # Adjust based on asset type
        if asset["type"] == "compute":
            risk_params["settlement_type"] = "physical"
            risk_params["delivery_notice_period"] = "24h"
        elif asset["type"] == "model":
            risk_params["settlement_type"] = "cash"
            risk_params["performance_verification"] = True
            
        return risk_params
    
    @lru_cache(maxsize=128)
    async def _get_asset_liquidity(self, asset_id: str) -> float:
        """Get 24h liquidity for an asset"""
        trades = await self.asset_service.get_recent_trades(asset_id, hours=24)
        return sum(float(t["price"]) * float(t["amount"]) for t in trades)
    
    async def _calculate_asset_volatility(self, asset_id: str, days: int = 30) -> float:
        """Calculate historical volatility for an asset"""
        prices = await self._get_asset_price_history(asset_id, days)
        if len(prices) < 2:
            return 0.5  # Default medium volatility
            
        returns = []
        for i in range(1, len(prices)):
            ret = (prices[i] - prices[i-1]) / prices[i-1]
            returns.append(ret)
            
        if not returns:
            return 0.5
            
        # Calculate annualized volatility
        import numpy as np
        daily_vol = np.std(returns)
        annual_vol = daily_vol * np.sqrt(365)
        
        return annual_vol
    
    async def _get_asset_price_history(
        self,
        asset_id: str,
        days: int
    ) -> List[float]:
        """Get historical prices for an asset"""
        # Check cache first
        cache_key = f"{asset_id}:{days}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
            
        # Fetch from service
        history = await self.asset_service.get_price_history(
            asset_id,
            start_date=(datetime.utcnow() - timedelta(days=days)).isoformat(),
            end_date=datetime.utcnow().isoformat()
        )
        
        prices = [float(h["price"]) for h in history]
        self._price_cache[cache_key] = prices
        
        return prices 