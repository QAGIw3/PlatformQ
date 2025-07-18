from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import asyncio

from app.models.risk import RiskMetrics, MarketRisk, PositionRisk
from app.integrations.oracle_aggregator import OracleAggregatorClient
from app.integrations.neuromorphic import NeuromorphicClient

class MLRiskEngine:
    """
    Advanced ML-based risk management system
    Most advantageous: Adaptive risk parameters based on real-time conditions
    """
    
    def __init__(
        self,
        oracle_client: OracleAggregatorClient,
        neuromorphic_client: NeuromorphicClient,
        ignite_client
    ):
        self.oracle = oracle_client
        self.neuromorphic = neuromorphic_client
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
        
        # Load or train models
        asyncio.create_task(self._initialize_models())
        
    async def assess_market_risk(self, market_id: str) -> MarketRisk:
        """
        Comprehensive market risk assessment using ML
        """
        # Gather market data
        market_data = await self._gather_market_data(market_id)
        
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
        correlation_risk = await self._analyze_correlations(market_id)
        
        # Calculate dynamic risk parameters
        risk_params = self._calculate_dynamic_risk_params(
            predicted_volatility,
            liquidity_risk,
            anomaly_score
        )
        
        return MarketRisk(
            market_id=market_id,
            timestamp=datetime.utcnow(),
            current_volatility=market_data["volatility_24h"],
            predicted_volatility=predicted_volatility,
            anomaly_score=anomaly_score,
            var_95=var_95,
            var_99=var_99,
            liquidity_score=liquidity_risk["score"],
            correlation_risk=correlation_risk,
            recommended_params=risk_params,
            risk_level=self._classify_risk_level(predicted_volatility, anomaly_score),
            warnings=self._generate_risk_warnings(market_data, predicted_volatility, anomaly_score)
        )
    
    async def assess_position_risk(
        self,
        position_id: str,
        user_id: str
    ) -> PositionRisk:
        """
        Individual position risk assessment
        """
        position = await self._get_position(position_id)
        market_risk = await self.assess_market_risk(position.market_id)
        
        # User-specific risk factors
        user_risk_profile = await self._get_user_risk_profile(user_id)
        
        # Calculate position-specific metrics
        liquidation_probability = await self._predict_liquidation_probability(
            position,
            market_risk,
            user_risk_profile
        )
        
        # Expected shortfall
        expected_shortfall = await self._calculate_expected_shortfall(
            position,
            market_risk
        )
        
        # Margin utilization
        margin_utilization = position.borrowed_amount / position.collateral_value
        
        # Stress test results
        stress_scenarios = await self._run_position_stress_tests(position)
        
        return PositionRisk(
            position_id=position_id,
            market_risk=market_risk,
            liquidation_probability=liquidation_probability,
            expected_shortfall=expected_shortfall,
            margin_utilization=margin_utilization,
            health_factor=position.health_factor,
            stress_test_results=stress_scenarios,
            recommendations=self._generate_position_recommendations(
                position,
                liquidation_probability,
                margin_utilization
            )
        )
    
    async def _predict_volatility(self, features: np.ndarray) -> Decimal:
        """
        Predict future volatility using ML model
        """
        if self.models["volatility_predictor"] is None:
            # Fallback to statistical method
            return Decimal(str(features[0]))  # Current volatility
        
        # Predict next period volatility
        prediction = self.models["volatility_predictor"].predict(features.reshape(1, -1))[0]
        
        # Use neuromorphic processor for real-time adjustment
        neuromorphic_adjustment = await self.neuromorphic.process(
            "volatility_spike_detection",
            features
        )
        
        adjusted_prediction = prediction * (1 + neuromorphic_adjustment)
        
        return Decimal(str(adjusted_prediction))
    
    async def _detect_anomalies(self, features: np.ndarray) -> float:
        """
        Detect market anomalies using Isolation Forest
        """
        if self.models["anomaly_detector"] is None:
            return 0.0
        
        # Get anomaly score (-1 for anomaly, 1 for normal)
        anomaly_prediction = self.models["anomaly_detector"].predict(features.reshape(1, -1))[0]
        anomaly_score = self.models["anomaly_detector"].score_samples(features.reshape(1, -1))[0]
        
        # Normalize to 0-1 range (1 being highly anomalous)
        normalized_score = max(0, min(1, -anomaly_score))
        
        return normalized_score
    
    async def _calculate_var(
        self,
        market_data: Dict
    ) -> Tuple[Decimal, Decimal]:
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
        volatility_adjustment = market_data["volatility_24h"] / market_data.get("avg_volatility", 1)
        
        var_95_adjusted = abs(var_95) * float(volatility_adjustment)
        var_99_adjusted = abs(var_99) * float(volatility_adjustment)
        
        return Decimal(str(var_95_adjusted)), Decimal(str(var_99_adjusted))
    
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
        if self.models["liquidation_predictor"] is None:
            # Fallback to simple calculation
            health_factor = position.health_factor
            if health_factor > 2:
                return Decimal("0.01")  # 1%
            elif health_factor > 1.5:
                return Decimal("0.05")  # 5%
            elif health_factor > 1.2:
                return Decimal("0.20")  # 20%
            else:
                return Decimal("0.50")  # 50%
        
        # Feature vector for ML model
        features = np.array([
            float(position.health_factor),
            float(position.leverage),
            float(market_risk.predicted_volatility),
            float(market_risk.liquidity_score),
            float(user_profile.get("historical_liquidation_rate", 0)),
            float(position.time_held_hours),
            float(position.unrealized_pnl_percent)
        ])
        
        probability = self.models["liquidation_predictor"].predict_proba(
            features.reshape(1, -1)
        )[0][1]  # Probability of liquidation
        
        return Decimal(str(probability))
    
    async def _run_position_stress_tests(self, position: Dict) -> List[Dict]:
        """
        Run various stress test scenarios
        """
        scenarios = []
        
        # Define stress scenarios
        stress_tests = [
            {"name": "flash_crash", "price_change": -0.3, "volatility_spike": 3},
            {"name": "black_swan", "price_change": -0.5, "volatility_spike": 5},
            {"name": "liquidity_crisis", "price_change": -0.2, "liquidity_drop": 0.9},
            {"name": "correlation_breakdown", "price_change": -0.15, "correlation_flip": True},
        ]
        
        for test in stress_tests:
            # Simulate scenario
            stressed_price = position.mark_price * (1 + test.get("price_change", 0))
            
            # Calculate stressed health factor
            stressed_health = self._calculate_stressed_health_factor(
                position,
                stressed_price,
                test.get("volatility_spike", 1)
            )
            
            scenarios.append({
                "scenario": test["name"],
                "survival": stressed_health > 1.0,
                "stressed_health_factor": stressed_health,
                "potential_loss": self._calculate_scenario_loss(position, stressed_price),
                "recommended_action": self._get_scenario_recommendation(stressed_health)
            })
        
        return scenarios
    
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
    
    async def _initialize_models(self):
        """
        Initialize or load ML models
        """
        try:
            # Try to load pre-trained models
            self.models["volatility_predictor"] = joblib.load("models/volatility_predictor.pkl")
            self.models["liquidation_predictor"] = joblib.load("models/liquidation_predictor.pkl")
            self.models["anomaly_detector"] = joblib.load("models/anomaly_detector.pkl")
        except:
            # Train new models if not found
            await self._train_models()
    
    async def _train_models(self):
        """
        Train ML models on historical data
        """
        # This would be a comprehensive training pipeline
        # For now, initialize with default models
        
        # Volatility predictor
        self.models["volatility_predictor"] = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        # Anomaly detector
        self.models["anomaly_detector"] = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        
        # Train on historical data (placeholder)
        # In production, this would load and train on actual historical data 