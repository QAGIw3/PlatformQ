"""
Predictive Liquidation Model

ML model that predicts liquidations before they happen, allowing
for proactive risk management and trader notification.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import precision_recall_curve, roc_auc_score
import lightgbm as lgb
import shap
from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


@dataclass 
class LiquidationPrediction:
    """Liquidation prediction result."""
    trader_id: str
    position_id: Optional[str]
    probability: float
    time_horizon: str  # '1h', '4h', '24h', '48h'
    risk_factors: Dict[str, float]
    recommended_actions: List[str]
    confidence_interval: Tuple[float, float]
    expected_time_to_liquidation: Optional[float]  # hours
    

class PredictiveLiquidationModel:
    """
    Advanced ML model for predicting liquidations before they occur.
    
    Features:
    - Multi-horizon predictions (1h, 4h, 24h, 48h)
    - Real-time feature engineering
    - Explainable predictions with SHAP
    - Confidence intervals
    - Proactive recommendations
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite = ignite_client
        
        # Models for different time horizons
        self.models = {
            '1h': None,
            '4h': None,
            '24h': None,
            '48h': None
        }
        
        # Feature scalers
        self.scalers = {
            '1h': StandardScaler(),
            '4h': StandardScaler(),
            '24h': StandardScaler(),
            '48h': StandardScaler()
        }
        
        # Feature configuration
        self.feature_config = self._get_feature_config()
        
        # SHAP explainers
        self.explainers = {}
        
        # Model performance tracking
        self.performance_metrics = {}
        
    def _get_feature_config(self) -> Dict[str, List[str]]:
        """Define features for liquidation prediction."""
        return {
            'position_features': [
                'margin_ratio',
                'leverage',
                'position_size_relative',  # Relative to account size
                'unrealized_pnl_pct',
                'time_held_hours',
                'distance_to_liquidation_price',
                'funding_rate_exposure'
            ],
            'market_features': [
                'price_volatility_1h',
                'price_volatility_24h',
                'volume_ratio',  # Current vs average
                'spread_ratio',
                'order_book_imbalance',
                'large_trade_ratio',
                'momentum_1h',
                'momentum_24h'
            ],
            'trader_features': [
                'account_age_days',
                'total_positions',
                'position_concentration',
                'historical_win_rate',
                'avg_hold_duration',
                'liquidation_history_count',
                'recent_pnl_volatility',
                'trading_frequency'
            ],
            'risk_features': [
                'portfolio_var',
                'correlation_exposure',
                'max_drawdown_7d',
                'sharpe_ratio_30d',
                'tail_risk_measure',
                'stress_test_score'
            ],
            'behavioral_features': [
                'panic_trading_score',  # Rapid position changes
                'revenge_trading_score',  # Trading after losses
                'overtrading_score',
                'position_sizing_consistency',
                'stop_loss_usage_rate',
                'avg_leverage_trend'
            ]
        }
        
    async def train_models(self, training_data: pd.DataFrame):
        """Train liquidation prediction models for all time horizons."""
        logger.info("Training predictive liquidation models")
        
        for horizon in self.models.keys():
            logger.info(f"Training model for {horizon} horizon")
            
            # Prepare features and target
            X, y = self._prepare_training_data(training_data, horizon)
            
            # Time series split for validation
            tscv = TimeSeriesSplit(n_splits=5)
            
            # Train model
            model = self._create_model(horizon)
            
            # Track performance
            scores = []
            for train_idx, val_idx in tscv.split(X):
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
                
                # Scale features
                X_train_scaled = self.scalers[horizon].fit_transform(X_train)
                X_val_scaled = self.scalers[horizon].transform(X_val)
                
                # Train
                model.fit(X_train_scaled, y_train)
                
                # Evaluate
                y_pred_proba = model.predict_proba(X_val_scaled)[:, 1]
                score = roc_auc_score(y_val, y_pred_proba)
                scores.append(score)
                
            avg_score = np.mean(scores)
            logger.info(f"Model {horizon} - Average AUC: {avg_score:.4f}")
            
            self.models[horizon] = model
            self.performance_metrics[horizon] = {
                'auc': avg_score,
                'scores': scores
            }
            
            # Create SHAP explainer
            self.explainers[horizon] = shap.TreeExplainer(model)
            
    async def predict_liquidation(self, 
                                trader_data: Dict[str, Any],
                                market_data: Dict[str, Any]) -> Dict[str, LiquidationPrediction]:
        """
        Predict liquidation probability for a trader across time horizons.
        
        Args:
            trader_data: Trader and position information
            market_data: Current market conditions
            
        Returns:
            Dictionary of predictions for each time horizon
        """
        predictions = {}
        
        # Engineer features
        features = await self._engineer_features(trader_data, market_data)
        
        for horizon, model in self.models.items():
            if model is None:
                continue
                
            # Prepare feature vector
            X = self._prepare_feature_vector(features, horizon)
            
            # Scale
            X_scaled = self.scalers[horizon].transform(X)
            
            # Predict
            prob = model.predict_proba(X_scaled)[0, 1]
            
            # Get feature importance
            shap_values = self.explainers[horizon].shap_values(X_scaled)
            if isinstance(shap_values, list):
                shap_values = shap_values[1]  # For binary classification
                
            # Get top risk factors
            feature_names = X.columns.tolist()
            risk_factors = {}
            for i, (feature, value) in enumerate(zip(feature_names, shap_values[0])):
                if abs(value) > 0.01:  # Threshold for significance
                    risk_factors[feature] = float(value)
                    
            # Sort by impact
            risk_factors = dict(sorted(risk_factors.items(), 
                                     key=lambda x: abs(x[1]), 
                                     reverse=True)[:10])
            
            # Calculate confidence interval
            confidence_interval = self._calculate_confidence_interval(prob, horizon)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                prob, risk_factors, trader_data, market_data
            )
            
            # Estimate time to liquidation
            time_to_liquidation = self._estimate_time_to_liquidation(
                prob, horizon, trader_data
            )
            
            predictions[horizon] = LiquidationPrediction(
                trader_id=trader_data['trader_id'],
                position_id=trader_data.get('position_id'),
                probability=float(prob),
                time_horizon=horizon,
                risk_factors=risk_factors,
                recommended_actions=recommendations,
                confidence_interval=confidence_interval,
                expected_time_to_liquidation=time_to_liquidation
            )
            
        return predictions
        
    async def _engineer_features(self, 
                               trader_data: Dict[str, Any],
                               market_data: Dict[str, Any]) -> Dict[str, float]:
        """Engineer features for prediction."""
        features = {}
        
        # Position features
        margin_ratio = trader_data.get('margin_ratio', 0)
        features['margin_ratio'] = margin_ratio
        features['leverage'] = trader_data.get('leverage', 1)
        features['position_size_relative'] = (
            trader_data.get('position_value', 0) / 
            trader_data.get('account_balance', 1)
        )
        features['unrealized_pnl_pct'] = (
            trader_data.get('unrealized_pnl', 0) / 
            trader_data.get('position_value', 1) 
            if trader_data.get('position_value', 0) > 0 else 0
        )
        
        # Distance to liquidation
        liq_price = trader_data.get('liquidation_price', 0)
        current_price = market_data.get('price', 0)
        if liq_price > 0 and current_price > 0:
            features['distance_to_liquidation_price'] = abs(
                (current_price - liq_price) / current_price
            )
        else:
            features['distance_to_liquidation_price'] = 1.0
            
        # Market features
        features['price_volatility_1h'] = market_data.get('volatility_1h', 0)
        features['price_volatility_24h'] = market_data.get('volatility_24h', 0)
        features['volume_ratio'] = market_data.get('volume_ratio', 1)
        features['spread_ratio'] = market_data.get('spread_ratio', 0)
        features['order_book_imbalance'] = market_data.get('order_imbalance', 0)
        
        # Trader behavioral features
        features['account_age_days'] = trader_data.get('account_age_days', 0)
        features['liquidation_history_count'] = trader_data.get('past_liquidations', 0)
        features['historical_win_rate'] = trader_data.get('win_rate', 0.5)
        
        # Panic trading detection
        recent_trades = trader_data.get('recent_trades', [])
        if recent_trades:
            # High frequency of trades in short time = panic
            trades_last_hour = sum(1 for t in recent_trades 
                                 if t['timestamp'] > datetime.utcnow() - timedelta(hours=1))
            features['panic_trading_score'] = min(trades_last_hour / 10, 1.0)
        else:
            features['panic_trading_score'] = 0
            
        # Risk features
        features['portfolio_var'] = trader_data.get('portfolio_var', 0)
        features['max_drawdown_7d'] = trader_data.get('max_drawdown_7d', 0)
        features['correlation_exposure'] = trader_data.get('correlation_exposure', 0)
        
        return features
        
    def _prepare_feature_vector(self, 
                              features: Dict[str, float],
                              horizon: str) -> pd.DataFrame:
        """Prepare feature vector for model input."""
        # Get all feature names
        all_features = []
        for feature_list in self.feature_config.values():
            all_features.extend(feature_list)
            
        # Create dataframe with all features
        feature_dict = {f: [features.get(f, 0)] for f in all_features}
        
        return pd.DataFrame(feature_dict)
        
    def _create_model(self, horizon: str):
        """Create model for specific time horizon."""
        # Use LightGBM for better performance
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'random_state': 42
        }
        
        # Adjust parameters based on horizon
        if horizon in ['1h', '4h']:
            # More aggressive for short-term predictions
            params['num_leaves'] = 63
            params['learning_rate'] = 0.1
            params['n_estimators'] = 200
        else:
            # More conservative for long-term
            params['num_leaves'] = 31
            params['learning_rate'] = 0.05
            params['n_estimators'] = 300
            
        return lgb.LGBMClassifier(**params)
        
    def _calculate_confidence_interval(self, 
                                     probability: float,
                                     horizon: str) -> Tuple[float, float]:
        """Calculate confidence interval for prediction."""
        # Base uncertainty increases with time horizon
        base_uncertainty = {
            '1h': 0.05,
            '4h': 0.08,
            '24h': 0.12,
            '48h': 0.15
        }
        
        uncertainty = base_uncertainty.get(horizon, 0.1)
        
        # Adjust based on probability (more uncertain at extremes)
        if probability < 0.2 or probability > 0.8:
            uncertainty *= 0.7  # More confident at extremes
        else:
            uncertainty *= 1.2  # Less confident in middle
            
        lower = max(0, probability - uncertainty)
        upper = min(1, probability + uncertainty)
        
        return (lower, upper)
        
    def _generate_recommendations(self,
                                probability: float,
                                risk_factors: Dict[str, float],
                                trader_data: Dict[str, Any],
                                market_data: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on prediction."""
        recommendations = []
        
        # High risk recommendations
        if probability > 0.7:
            recommendations.append("⚠️ URGENT: High liquidation risk detected")
            
            # Check top risk factors
            if 'leverage' in risk_factors and risk_factors['leverage'] > 0.2:
                recommendations.append("Reduce leverage immediately (current: {:.1f}x)".format(
                    trader_data.get('leverage', 0)
                ))
                
            if 'margin_ratio' in risk_factors and trader_data.get('margin_ratio', 0) < 150:
                recommendations.append("Add margin to improve ratio (current: {:.0f}%)".format(
                    trader_data.get('margin_ratio', 0)
                ))
                
            if 'position_size_relative' in risk_factors:
                recommendations.append("Reduce position size by 30-50%")
                
            recommendations.append("Consider setting stop-loss at {:.2f}".format(
                market_data.get('price', 0) * 0.95
            ))
            
        # Medium risk recommendations
        elif probability > 0.4:
            recommendations.append("⚡ WARNING: Elevated liquidation risk")
            
            if trader_data.get('leverage', 0) > 10:
                recommendations.append("Consider reducing leverage below 10x")
                
            if 'price_volatility_24h' in risk_factors and market_data.get('volatility_24h', 0) > 0.05:
                recommendations.append("High market volatility - consider hedging")
                
            recommendations.append("Monitor position closely over next 24 hours")
            
        # Low risk recommendations
        else:
            recommendations.append("✅ Liquidation risk is currently low")
            
            if trader_data.get('margin_ratio', 0) > 200:
                recommendations.append("Healthy margin levels maintained")
                
            recommendations.append("Continue monitoring market conditions")
            
        return recommendations
        
    def _estimate_time_to_liquidation(self,
                                    probability: float,
                                    horizon: str,
                                    trader_data: Dict[str, Any]) -> Optional[float]:
        """Estimate expected time to liquidation in hours."""
        if probability < 0.3:
            return None  # Too uncertain
            
        # Base estimates for each horizon
        base_times = {
            '1h': 0.5,
            '4h': 2.0,
            '24h': 12.0,
            '48h': 36.0
        }
        
        base_time = base_times.get(horizon, 24.0)
        
        # Adjust based on probability
        if probability > 0.8:
            time_factor = 0.5  # Very likely, sooner
        elif probability > 0.6:
            time_factor = 0.75
        else:
            time_factor = 1.0
            
        # Adjust based on margin level
        margin_ratio = trader_data.get('margin_ratio', 150)
        if margin_ratio < 110:
            time_factor *= 0.5  # Very close to liquidation
        elif margin_ratio < 130:
            time_factor *= 0.75
            
        estimated_time = base_time * time_factor
        
        # Add some uncertainty
        uncertainty = estimated_time * 0.2
        
        return estimated_time
        
    def _prepare_training_data(self, 
                             data: pd.DataFrame,
                             horizon: str) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare training data for specific horizon."""
        # Create target based on horizon
        horizon_hours = {
            '1h': 1,
            '4h': 4,
            '24h': 24,
            '48h': 48
        }
        
        hours = horizon_hours[horizon]
        
        # Target: liquidated within horizon
        data[f'liquidated_within_{horizon}'] = (
            data['time_to_liquidation'] <= hours
        ).astype(int)
        
        # Select features
        all_features = []
        for feature_list in self.feature_config.values():
            all_features.extend(feature_list)
            
        X = data[all_features]
        y = data[f'liquidated_within_{horizon}']
        
        return X, y
        
    async def monitor_high_risk_traders(self) -> List[Dict[str, Any]]:
        """Monitor and return list of high-risk traders."""
        high_risk_traders = []
        
        # Get all monitored traders from cache
        cache = self.ignite.get_cache("monitored_traders")
        
        # Check each trader
        for trader_id in cache.keys():
            trader_data = cache.get(trader_id)
            
            # Get current market data
            market_data = await self._get_market_data(trader_data['market_id'])
            
            # Predict liquidation risk
            predictions = await self.predict_liquidation(trader_data, market_data)
            
            # Check short-term risk
            if predictions.get('1h') and predictions['1h'].probability > 0.7:
                high_risk_traders.append({
                    'trader_id': trader_id,
                    'probability_1h': predictions['1h'].probability,
                    'expected_time': predictions['1h'].expected_time_to_liquidation,
                    'top_risk_factor': list(predictions['1h'].risk_factors.keys())[0],
                    'recommendations': predictions['1h'].recommended_actions[:2]
                })
                
        # Sort by risk
        high_risk_traders.sort(key=lambda x: x['probability_1h'], reverse=True)
        
        return high_risk_traders
        
    async def _get_market_data(self, market_id: str) -> Dict[str, Any]:
        """Get current market data."""
        # Would fetch from market data service
        # For now, return mock data
        return {
            'price': 50000,
            'volatility_1h': 0.02,
            'volatility_24h': 0.03,
            'volume_ratio': 1.2,
            'spread_ratio': 0.001,
            'order_imbalance': 0.1
        } 