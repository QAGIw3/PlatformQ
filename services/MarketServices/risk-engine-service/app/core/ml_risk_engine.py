"""ML-based risk engine for advanced risk assessment"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import joblib
import asyncio
import logging

from ..models.risk import RiskMetrics, MarketRisk, PositionRisk
from ..config import Settings

logger = logging.getLogger(__name__)


class MLRiskEngine:
    """
    Advanced ML-based risk management system
    Provides adaptive risk parameters based on real-time market conditions
    """
    
    def __init__(self, settings: Settings, ignite_client=None):
        self.settings = settings
        self.ignite = ignite_client
        
        # Risk models
        self.models = {
            "volatility_predictor": None,
            "liquidation_predictor": None,
            "correlation_analyzer": None,
            "anomaly_detector": None,
            "var_calculator": None  # Value at Risk
        }
        
        # Risk parameters (dynamic)
        self.risk_params = {
            "base_initial_margin": Decimal("0.1"),    # 10%
            "base_maintenance_margin": Decimal("0.05"), # 5%
            "max_leverage": 100,
            "position_limit_multiplier": Decimal("10"), # 10x average volume
            "circuit_breaker_threshold": Decimal("0.2"), # 20% move
            "correlation_threshold": Decimal("0.7"),    # High correlation warning
        }
        
        # Feature scalers
        self.scalers = {
            "volatility": StandardScaler(),
            "liquidation": StandardScaler()
        }
        
        # Load or train models
        asyncio.create_task(self._initialize_models())
        
    async def assess_market_risk(self, market_id: str, market_data: Dict) -> MarketRisk:
        """
        Comprehensive market risk assessment using ML
        """
        # Feature engineering
        features = self._engineer_risk_features(market_data)
        
        # Predict future volatility
        predicted_volatility = await self._predict_volatility(features)
        
        # Detect anomalies
        anomaly_score = await self._detect_anomalies(features)
        
        # Calculate Value at Risk (VaR)
        var_95, var_99 = await self._calculate_var(market_data)
        
        # Assess liquidity risk
        liquidity_risk = await self._assess_liquidity_risk(market_data)
        
        # Correlation analysis
        correlation_risk = await self._analyze_correlations(market_id, market_data)
        
        # Calculate dynamic risk parameters
        risk_params = self._calculate_dynamic_risk_params(
            predicted_volatility,
            liquidity_risk,
            anomaly_score
        )
        
        return MarketRisk(
            market_id=market_id,
            timestamp=datetime.utcnow(),
            current_volatility=Decimal(str(market_data.get("volatility_24h", 0))),
            predicted_volatility=predicted_volatility,
            anomaly_score=anomaly_score,
            var_95=var_95,
            var_99=var_99,
            liquidity_score=Decimal(str(liquidity_risk.get("score", 1))),
            correlation_risk=correlation_risk,
            recommended_params=risk_params,
            risk_level=self._classify_risk_level(predicted_volatility, anomaly_score),
            warnings=self._generate_risk_warnings(market_data, predicted_volatility, anomaly_score)
        )
    
    async def assess_position_risk(
        self,
        position: Dict,
        market_risk: MarketRisk,
        user_profile: Optional[Dict] = None
    ) -> PositionRisk:
        """
        Individual position risk assessment
        """
        if user_profile is None:
            user_profile = {"historical_liquidation_rate": 0}
        
        # Calculate position-specific metrics
        liquidation_probability = await self._predict_liquidation_probability(
            position,
            market_risk,
            user_profile
        )
        
        # Expected shortfall
        expected_shortfall = await self._calculate_expected_shortfall(
            position,
            market_risk
        )
        
        # Margin utilization
        margin_utilization = Decimal(str(position.get("margin_used", 0))) / max(Decimal(str(position.get("collateral_value", 1))), Decimal("1"))
        
        # Stress test results
        stress_scenarios = await self._run_position_stress_tests(position)
        
        return PositionRisk(
            position_id=position.get("position_id", "unknown"),
            market_risk=market_risk,
            liquidation_probability=liquidation_probability,
            expected_shortfall=expected_shortfall,
            margin_utilization=margin_utilization,
            health_factor=Decimal(str(position.get("health_factor", 1))),
            stress_test_results=stress_scenarios,
            recommendations=self._generate_position_recommendations(
                position,
                liquidation_probability,
                margin_utilization
            )
        )
    
    def _engineer_risk_features(self, market_data: Dict) -> np.ndarray:
        """
        Engineer features for ML models from market data
        """
        features = []
        
        # Basic market metrics
        features.append(float(market_data.get("volatility_24h", 0)))
        features.append(float(market_data.get("volume_24h", 0)))
        features.append(float(market_data.get("price_change_24h", 0)))
        
        # Technical indicators
        features.append(float(market_data.get("rsi", 50)))
        features.append(float(market_data.get("bollinger_position", 0.5)))
        
        # Market microstructure
        features.append(float(market_data.get("bid_ask_spread", 0)))
        features.append(float(market_data.get("order_book_imbalance", 0)))
        
        # Sentiment indicators
        features.append(float(market_data.get("funding_rate", 0)))
        features.append(float(market_data.get("open_interest_change", 0)))
        
        return np.array(features)
    
    async def _predict_volatility(self, features: np.ndarray) -> Decimal:
        """
        Predict future volatility using ML model
        """
        if self.models["volatility_predictor"] is None:
            # Fallback to statistical method
            return Decimal(str(features[0]))  # Current volatility
        
        try:
            # Scale features
            scaled_features = self.scalers["volatility"].transform(features.reshape(1, -1))
            
            # Predict next period volatility
            prediction = self.models["volatility_predictor"].predict(scaled_features)[0]
            
            # Ensure reasonable bounds
            prediction = max(0.01, min(2.0, prediction))  # 1% to 200% volatility
            
            return Decimal(str(prediction))
            
        except Exception as e:
            logger.error(f"Error predicting volatility: {e}")
            return Decimal(str(features[0]))
    
    async def _detect_anomalies(self, features: np.ndarray) -> float:
        """
        Detect market anomalies using Isolation Forest
        """
        if self.models["anomaly_detector"] is None:
            return 0.0
        
        try:
            # Get anomaly score (-1 for anomaly, 1 for normal)
            anomaly_score = self.models["anomaly_detector"].score_samples(features.reshape(1, -1))[0]
            
            # Normalize to 0-1 range (1 being highly anomalous)
            normalized_score = max(0, min(1, -anomaly_score))
            
            return normalized_score
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")
            return 0.0
    
    async def _calculate_var(self, market_data: Dict) -> Tuple[Decimal, Decimal]:
        """
        Calculate Value at Risk using historical simulation
        """
        returns = market_data.get("historical_returns", [])
        
        if not returns:
            return Decimal("0"), Decimal("0")
        
        # Convert to numpy array
        returns_array = np.array(returns)
        
        # Calculate VaR at 95% and 99% confidence levels
        var_95 = np.percentile(returns_array, 5)
        var_99 = np.percentile(returns_array, 1)
        
        # Adjust for current market conditions
        avg_volatility = market_data.get("avg_volatility", 1)
        if avg_volatility > 0:
            volatility_adjustment = float(market_data.get("volatility_24h", 1)) / float(avg_volatility)
        else:
            volatility_adjustment = 1.0
        
        var_95_adjusted = abs(var_95) * volatility_adjustment
        var_99_adjusted = abs(var_99) * volatility_adjustment
        
        return Decimal(str(var_95_adjusted)), Decimal(str(var_99_adjusted))
    
    async def _assess_liquidity_risk(self, market_data: Dict) -> Dict:
        """
        Assess market liquidity risk
        """
        volume_24h = float(market_data.get("volume_24h", 0))
        avg_volume = float(market_data.get("avg_volume", 1))
        bid_ask_spread = float(market_data.get("bid_ask_spread", 0))
        order_book_depth = float(market_data.get("order_book_depth", 0))
        
        # Calculate liquidity score (0-1, 1 being most liquid)
        volume_ratio = min(1, volume_24h / avg_volume) if avg_volume > 0 else 0
        spread_score = max(0, 1 - bid_ask_spread * 100)  # Assuming spread in decimal
        depth_score = min(1, order_book_depth / 1000000)  # Normalize by $1M
        
        liquidity_score = (volume_ratio + spread_score + depth_score) / 3
        
        return {
            "score": liquidity_score,
            "volume_ratio": volume_ratio,
            "spread_score": spread_score,
            "depth_score": depth_score,
            "risk_level": "low" if liquidity_score > 0.7 else "medium" if liquidity_score > 0.3 else "high"
        }
    
    async def _analyze_correlations(self, market_id: str, market_data: Dict) -> Decimal:
        """
        Analyze correlation risk with other markets
        """
        # This would typically fetch correlation data from a correlation matrix
        # For now, return a placeholder
        return Decimal("0.5")
    
    def _calculate_dynamic_risk_params(
        self,
        predicted_volatility: Decimal,
        liquidity_risk: Dict,
        anomaly_score: float
    ) -> Dict[str, Decimal]:
        """
        Calculate dynamic risk parameters based on market conditions
        """
        # Base parameters
        params = self.risk_params.copy()
        
        # Volatility adjustment
        volatility_multiplier = min(
            Decimal("3"),
            Decimal("1") + predicted_volatility * Decimal("10")
        )
        
        # Liquidity adjustment
        liquidity_multiplier = Decimal("2") - Decimal(str(liquidity_risk["score"]))
        
        # Anomaly adjustment
        anomaly_multiplier = Decimal("1") + Decimal(str(anomaly_score))
        
        # Apply adjustments
        params["initial_margin"] = params["base_initial_margin"] * volatility_multiplier * liquidity_multiplier * anomaly_multiplier
        params["maintenance_margin"] = params["base_maintenance_margin"] * volatility_multiplier * liquidity_multiplier
        
        # Reduce max leverage in risky conditions
        if predicted_volatility > Decimal("0.1") or anomaly_score > 0.7:
            params["max_leverage"] = max(5, int(100 / float(volatility_multiplier)))
        
        # Tighten position limits
        if liquidity_risk["score"] < 0.5:
            params["position_limit_multiplier"] *= Decimal("0.5")
        
        return params
    
    async def _predict_liquidation_probability(
        self,
        position: Dict,
        market_risk: MarketRisk,
        user_profile: Dict
    ) -> Decimal:
        """
        Predict probability of liquidation using ML
        """
        health_factor = float(position.get("health_factor", 1))
        
        if self.models["liquidation_predictor"] is None:
            # Fallback to simple calculation
            if health_factor > 2:
                return Decimal("0.01")  # 1%
            elif health_factor > 1.5:
                return Decimal("0.05")  # 5%
            elif health_factor > 1.2:
                return Decimal("0.20")  # 20%
            else:
                return Decimal("0.50")  # 50%
        
        try:
            # Feature vector for ML model
            features = np.array([
                health_factor,
                float(position.get("leverage", 1)),
                float(market_risk.predicted_volatility),
                float(market_risk.liquidity_score),
                float(user_profile.get("historical_liquidation_rate", 0)),
                float(position.get("time_held_hours", 0)),
                float(position.get("unrealized_pnl_percent", 0))
            ])
            
            # Scale features
            scaled_features = self.scalers["liquidation"].transform(features.reshape(1, -1))
            
            # Get prediction probabilities
            probability = self.models["liquidation_predictor"].predict_proba(
                scaled_features
            )[0][1]  # Probability of liquidation
            
            return Decimal(str(probability))
            
        except Exception as e:
            logger.error(f"Error predicting liquidation probability: {e}")
            # Fallback to health factor based calculation
            if health_factor > 2:
                return Decimal("0.01")
            elif health_factor > 1.5:
                return Decimal("0.05")
            elif health_factor > 1.2:
                return Decimal("0.20")
            else:
                return Decimal("0.50")
    
    async def _calculate_expected_shortfall(
        self,
        position: Dict,
        market_risk: MarketRisk
    ) -> Decimal:
        """
        Calculate expected shortfall (conditional VaR)
        """
        position_value = Decimal(str(position.get("notional_value", 0)))
        var_99 = market_risk.var_99
        
        # Expected shortfall is typically 1.2-1.5x VaR
        expected_shortfall = position_value * var_99 * Decimal("1.3")
        
        return expected_shortfall
    
    async def _run_position_stress_tests(self, position: Dict) -> List[Dict]:
        """
        Run various stress test scenarios
        """
        scenarios = []
        mark_price = Decimal(str(position.get("mark_price", 1)))
        
        # Define stress scenarios
        stress_tests = [
            {"name": "flash_crash", "price_change": -0.3, "volatility_spike": 3},
            {"name": "black_swan", "price_change": -0.5, "volatility_spike": 5},
            {"name": "liquidity_crisis", "price_change": -0.2, "liquidity_drop": 0.9},
            {"name": "correlation_breakdown", "price_change": -0.15, "correlation_flip": True},
        ]
        
        for test in stress_tests:
            # Simulate scenario
            stressed_price = mark_price * (1 + Decimal(str(test.get("price_change", 0))))
            
            # Calculate stressed health factor
            stressed_health = self._calculate_stressed_health_factor(
                position,
                stressed_price,
                test.get("volatility_spike", 1)
            )
            
            scenarios.append({
                "scenario": test["name"],
                "survival": stressed_health > 1.0,
                "stressed_health_factor": float(stressed_health),
                "potential_loss": float(self._calculate_scenario_loss(position, stressed_price)),
                "recommended_action": self._get_scenario_recommendation(stressed_health)
            })
        
        return scenarios
    
    def _calculate_stressed_health_factor(
        self,
        position: Dict,
        stressed_price: Decimal,
        volatility_spike: float
    ) -> Decimal:
        """
        Calculate health factor under stressed conditions
        """
        # Get position details
        size = Decimal(str(position.get("size", 0)))
        entry_price = Decimal(str(position.get("entry_price", 1)))
        leverage = Decimal(str(position.get("leverage", 1)))
        side = position.get("side", "long")
        
        # Calculate P&L under stressed price
        if side == "long":
            pnl = size * (stressed_price - entry_price)
        else:
            pnl = size * (entry_price - stressed_price)
        
        # Calculate stressed equity
        initial_margin = size * entry_price / leverage
        stressed_equity = initial_margin + pnl
        
        # Calculate stressed margin requirement (increased due to volatility)
        stressed_margin_req = initial_margin * Decimal(str(volatility_spike))
        
        # Health factor = equity / margin requirement
        if stressed_margin_req > 0:
            health_factor = stressed_equity / stressed_margin_req
        else:
            health_factor = Decimal("999")
        
        return max(Decimal("0"), health_factor)
    
    def _calculate_scenario_loss(self, position: Dict, stressed_price: Decimal) -> Decimal:
        """
        Calculate potential loss under scenario
        """
        size = Decimal(str(position.get("size", 0)))
        entry_price = Decimal(str(position.get("entry_price", 1)))
        side = position.get("side", "long")
        
        if side == "long":
            loss = size * (entry_price - stressed_price)
        else:
            loss = size * (stressed_price - entry_price)
        
        return max(Decimal("0"), loss)
    
    def _get_scenario_recommendation(self, stressed_health: Decimal) -> str:
        """
        Get recommendation based on stressed health factor
        """
        if stressed_health > 2:
            return "Position would survive - no action needed"
        elif stressed_health > 1.5:
            return "Consider reducing position size by 25%"
        elif stressed_health > 1:
            return "Reduce position size by 50% or add collateral"
        else:
            return "Position would be liquidated - immediate action required"
    
    def _classify_risk_level(self, predicted_volatility: Decimal, anomaly_score: float) -> str:
        """
        Classify overall risk level
        """
        if predicted_volatility > Decimal("0.2") or anomaly_score > 0.8:
            return "critical"
        elif predicted_volatility > Decimal("0.1") or anomaly_score > 0.6:
            return "high"
        elif predicted_volatility > Decimal("0.05") or anomaly_score > 0.3:
            return "medium"
        else:
            return "low"
    
    def _generate_risk_warnings(
        self,
        market_data: Dict,
        predicted_volatility: Decimal,
        anomaly_score: float
    ) -> List[str]:
        """
        Generate human-readable risk warnings
        """
        warnings = []
        
        if predicted_volatility > Decimal("0.15"):
            warnings.append(f"High volatility predicted: {predicted_volatility*100:.1f}% daily")
        
        if anomaly_score > 0.8:
            warnings.append("Unusual market behavior detected - exercise caution")
        
        if market_data.get("volume_24h", 0) < market_data.get("avg_volume", 1) * 0.5:
            warnings.append("Low trading volume - potential liquidity issues")
        
        if market_data.get("funding_rate", 0) > 0.001:
            warnings.append("High funding rate - consider market dynamics")
        
        return warnings
    
    def _generate_position_recommendations(
        self,
        position: Dict,
        liquidation_probability: Decimal,
        margin_utilization: Decimal
    ) -> List[str]:
        """
        Generate position-specific recommendations
        """
        recommendations = []
        
        if liquidation_probability > Decimal("0.3"):
            recommendations.append("High liquidation risk - consider reducing leverage")
        
        if margin_utilization > Decimal("0.8"):
            recommendations.append("High margin utilization - add collateral or reduce position")
        
        leverage = Decimal(str(position.get("leverage", 1)))
        if leverage > 20:
            recommendations.append("Extremely high leverage - consider reducing to safer levels")
        
        return recommendations
    
    async def _initialize_models(self):
        """
        Initialize or load ML models
        """
        try:
            # Try to load pre-trained models from Ignite cache
            if self.ignite:
                models_cache = self.ignite.get_or_create_cache("ml_models")
                self.models["volatility_predictor"] = models_cache.get("volatility_predictor")
                self.models["liquidation_predictor"] = models_cache.get("liquidation_predictor")
                self.models["anomaly_detector"] = models_cache.get("anomaly_detector")
                
            if any(m is None for m in self.models.values()):
                logger.info("Some models not found in cache, training new models")
                await self._train_models()
            else:
                logger.info("Loaded ML models from cache")
                
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            await self._train_models()
    
    async def _train_models(self):
        """
        Train ML models on historical data
        """
        # Initialize default models
        self.models["volatility_predictor"] = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        self.models["liquidation_predictor"] = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        self.models["anomaly_detector"] = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        
        # In production, this would load and train on actual historical data
        # For now, we'll fit with dummy data to initialize the models
        
        # Dummy training data for volatility predictor
        n_samples = 1000
        n_features = 9
        X_volatility = np.random.randn(n_samples, n_features)
        y_volatility = np.abs(np.random.randn(n_samples) * 0.1)  # Volatility values
        
        self.scalers["volatility"].fit(X_volatility)
        X_volatility_scaled = self.scalers["volatility"].transform(X_volatility)
        self.models["volatility_predictor"].fit(X_volatility_scaled, y_volatility)
        
        # Dummy training data for liquidation predictor
        n_liquidation_features = 7
        X_liquidation = np.random.randn(n_samples, n_liquidation_features)
        y_liquidation = np.random.randint(0, 2, n_samples)  # Binary: liquidated or not
        
        self.scalers["liquidation"] = StandardScaler()
        self.scalers["liquidation"].fit(X_liquidation)
        X_liquidation_scaled = self.scalers["liquidation"].transform(X_liquidation)
        self.models["liquidation_predictor"].fit(X_liquidation_scaled, y_liquidation)
        
        # Dummy training data for anomaly detector
        X_anomaly = np.random.randn(n_samples, n_features)
        self.models["anomaly_detector"].fit(X_anomaly)
        
        # Save models to Ignite cache if available
        if self.ignite:
            try:
                models_cache = self.ignite.get_or_create_cache("ml_models")
                models_cache.put("volatility_predictor", self.models["volatility_predictor"])
                models_cache.put("liquidation_predictor", self.models["liquidation_predictor"])
                models_cache.put("anomaly_detector", self.models["anomaly_detector"])
                logger.info("Saved ML models to cache")
            except Exception as e:
                logger.error(f"Error saving models to cache: {e}")
        
        logger.info("Initialized ML models with default parameters") 