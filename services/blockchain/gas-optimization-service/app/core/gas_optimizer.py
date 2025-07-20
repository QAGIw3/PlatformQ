"""
Gas Optimizer - Core optimization logic
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import numpy as np
from sklearn.ensemble import RandomForestRegressor

from pyignite import AsyncClient as IgniteClient
import httpx
from prometheus_client import Counter, Histogram, Gauge

from ..config import Settings
from ..models.optimization import (
    OptimizationRequest, GasRecommendation, OptimizationStrategy,
    BatchOptimization, L2Suggestion, MetaTransactionOption,
    GasPricePrediction, GasPriceLevel
)
from ..strategies.batch_strategy import BatchStrategy
from ..strategies.meta_tx_strategy import MetaTransactionStrategy
from ..strategies.time_based_strategy import TimeBasedStrategy
from ..strategies.l2_strategy import L2Strategy

logger = logging.getLogger(__name__)

# Metrics
optimization_requests = Counter(
    'gas_optimization_requests_total',
    'Total optimization requests',
    ['chain', 'strategy']
)

optimization_savings = Counter(
    'gas_optimization_savings_wei',
    'Total gas saved in wei',
    ['chain', 'strategy']
)

optimization_duration = Histogram(
    'gas_optimization_duration_seconds',
    'Optimization calculation duration',
    ['chain']
)

current_gas_price = Gauge(
    'current_gas_price_wei',
    'Current gas price',
    ['chain', 'level']
)


class GasOptimizer:
    """Core gas optimization engine"""
    
    def __init__(
        self,
        settings: Settings,
        ignite_client: IgniteClient,
        blockchain_connector_url: str
    ):
        self.settings = settings
        self.ignite = ignite_client
        self.blockchain_connector_url = blockchain_connector_url
        
        # HTTP client
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Optimization strategies
        self.batch_strategy = BatchStrategy(settings, ignite_client)
        self.meta_tx_strategy = MetaTransactionStrategy(settings)
        self.time_based_strategy = TimeBasedStrategy(settings, ignite_client)
        self.l2_strategy = L2Strategy(settings)
        
        # Price tracking
        self._gas_prices: Dict[str, Dict[str, Any]] = {}
        self._price_history: Dict[str, List[Tuple[datetime, Dict[str, float]]]] = {}
        
        # ML model for predictions
        self._prediction_models: Dict[str, RandomForestRegressor] = {}
        
        # Background tasks
        self._running = False
        self._update_task = None
        self._training_task = None
        
    async def start(self):
        """Start gas optimizer"""
        logger.info("Starting Gas Optimizer")
        
        # Initialize strategies
        await self.batch_strategy.initialize()
        await self.meta_tx_strategy.initialize()
        await self.time_based_strategy.initialize()
        await self.l2_strategy.initialize()
        
        # Start background tasks
        self._running = True
        self._update_task = asyncio.create_task(self._update_gas_prices())
        self._training_task = asyncio.create_task(self._train_prediction_models())
        
        logger.info("Gas Optimizer started")
        
    async def stop(self):
        """Stop gas optimizer"""
        logger.info("Stopping Gas Optimizer")
        self._running = False
        
        # Cancel background tasks
        if self._update_task:
            self._update_task.cancel()
        if self._training_task:
            self._training_task.cancel()
            
        # Stop strategies
        await self.batch_strategy.shutdown()
        await self.meta_tx_strategy.shutdown()
        await self.time_based_strategy.shutdown()
        await self.l2_strategy.shutdown()
        
        await self.http_client.aclose()
        
        logger.info("Gas Optimizer stopped")
        
    async def optimize(
        self,
        request: OptimizationRequest
    ) -> GasRecommendation:
        """Optimize gas for a transaction"""
        with optimization_duration.labels(chain=request.chain).time():
            try:
                # Get current gas prices
                gas_prices = await self._get_current_gas_prices(request.chain)
                
                # Evaluate all strategies
                strategies = await self._evaluate_strategies(request, gas_prices)
                
                # Select best strategy
                best_strategy = self._select_best_strategy(strategies, request)
                
                # Update metrics
                optimization_requests.labels(
                    chain=request.chain,
                    strategy=best_strategy.strategy.value
                ).inc()
                
                if float(best_strategy.estimated_savings) > 0:
                    optimization_savings.labels(
                        chain=request.chain,
                        strategy=best_strategy.strategy.value
                    ).inc(float(best_strategy.estimated_savings))
                    
                return best_strategy
                
            except Exception as e:
                logger.error(f"Error optimizing gas: {e}")
                # Return standard recommendation as fallback
                return await self._get_standard_recommendation(request, gas_prices)
                
    async def _evaluate_strategies(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> List[GasRecommendation]:
        """Evaluate all applicable strategies"""
        strategies = []
        
        # Standard strategy (baseline)
        standard = await self._get_standard_recommendation(request, gas_prices)
        strategies.append(standard)
        
        # Batch optimization
        if request.batch_eligible and self.settings.ENABLE_BATCH_OPTIMIZATION:
            batch_rec = await self.batch_strategy.evaluate(request, gas_prices)
            if batch_rec:
                strategies.append(batch_rec)
                
        # Meta-transactions
        if request.meta_tx_eligible and self.settings.ENABLE_META_TRANSACTIONS:
            meta_rec = await self.meta_tx_strategy.evaluate(request, gas_prices)
            if meta_rec:
                strategies.append(meta_rec)
                
        # Time-based optimization
        if request.max_wait_time and self.settings.ENABLE_TIME_BASED_OPTIMIZATION:
            time_rec = await self.time_based_strategy.evaluate(request, gas_prices)
            if time_rec:
                strategies.append(time_rec)
                
        # L2 suggestions
        if self.settings.ENABLE_L2_SUGGESTIONS:
            l2_rec = await self.l2_strategy.evaluate(request, gas_prices)
            if l2_rec:
                strategies.append(l2_rec)
                
        return strategies
        
    async def _get_standard_recommendation(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> GasRecommendation:
        """Get standard gas recommendation"""
        # Select gas price based on urgency
        price_key = request.urgency.value
        gas_price = gas_prices.get(price_key, gas_prices.get('standard'))
        
        # Estimate gas if not provided
        if not request.estimated_gas:
            gas_estimate = await self._estimate_gas(request)
        else:
            gas_estimate = request.estimated_gas
            
        # Calculate cost
        estimated_cost = int(gas_price) * gas_estimate
        
        # Expected confirmation time
        confirmation_times = {
            GasPriceLevel.SLOW: 600,     # 10 minutes
            GasPriceLevel.STANDARD: 180,  # 3 minutes
            GasPriceLevel.FAST: 60,       # 1 minute
            GasPriceLevel.INSTANT: 15     # 15 seconds
        }
        
        return GasRecommendation(
            strategy=OptimizationStrategy.STANDARD,
            gas_price=str(gas_price),
            max_fee_per_gas=gas_prices.get('maxFeePerGas'),
            max_priority_fee_per_gas=gas_prices.get('maxPriorityFeePerGas'),
            estimated_cost=str(estimated_cost),
            expected_confirmation_time=confirmation_times.get(request.urgency, 180),
            confidence_score=0.9,
            reasoning=f"Standard {request.urgency.value} gas price for immediate execution"
        )
        
    def _select_best_strategy(
        self,
        strategies: List[GasRecommendation],
        request: OptimizationRequest
    ) -> GasRecommendation:
        """Select the best strategy from available options"""
        # Sort by savings percentage and confidence
        sorted_strategies = sorted(
            strategies,
            key=lambda s: (s.savings_percentage * s.confidence_score),
            reverse=True
        )
        
        # Apply business rules
        for strategy in sorted_strategies:
            # Check if strategy meets minimum savings threshold
            if strategy.savings_percentage < 0.05 and strategy.strategy != OptimizationStrategy.STANDARD:
                continue
                
            # Check timing constraints
            if request.deadline:
                if strategy.recommended_time and strategy.recommended_time > request.deadline:
                    continue
                    
            # Check confidence threshold
            if strategy.confidence_score < 0.7:
                continue
                
            # Add alternatives to the recommendation
            strategy.alternatives = [
                {
                    "strategy": s.strategy.value,
                    "savings": s.estimated_savings,
                    "confidence": s.confidence_score
                }
                for s in strategies if s != strategy
            ][:3]  # Top 3 alternatives
            
            return strategy
            
        # Default to standard if no better option
        return strategies[0]
        
    async def _estimate_gas(self, request: OptimizationRequest) -> int:
        """Estimate gas for a transaction"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/gas/estimate",
                json={
                    "chain": request.chain,
                    "from_address": request.from_address,
                    "to_address": request.to_address,
                    "value": request.value,
                    "data": request.data
                }
            )
            response.raise_for_status()
            return response.json()['gasLimit']
            
        except Exception as e:
            logger.error(f"Error estimating gas: {e}")
            # Return default based on transaction type
            return 100000  # Default gas limit
            
    async def _get_current_gas_prices(self, chain: str) -> Dict[str, Any]:
        """Get current gas prices"""
        try:
            response = await self.http_client.get(
                f"{self.blockchain_connector_url}/api/v1/gas/price/{chain}"
            )
            response.raise_for_status()
            prices = response.json()
            
            # Update metrics
            for level in ['slow', 'standard', 'fast', 'instant']:
                if level in prices:
                    current_gas_price.labels(chain=chain, level=level).set(
                        float(prices[level])
                    )
                    
            return prices
            
        except Exception as e:
            logger.error(f"Error getting gas prices: {e}")
            # Return cached prices or defaults
            return self._gas_prices.get(chain, {
                "standard": "20000000000",
                "slow": "15000000000",
                "fast": "30000000000",
                "instant": "50000000000"
            })
            
    async def _update_gas_prices(self):
        """Background task to update gas prices"""
        while self._running:
            try:
                # Get list of chains
                response = await self.http_client.get(
                    f"{self.blockchain_connector_url}/api/v1/chains"
                )
                chains = [c['type'] for c in response.json()['chains']]
                
                # Update prices for each chain
                for chain in chains:
                    prices = await self._get_current_gas_prices(chain)
                    self._gas_prices[chain] = prices
                    
                    # Store in history
                    if chain not in self._price_history:
                        self._price_history[chain] = []
                        
                    self._price_history[chain].append((
                        datetime.utcnow(),
                        {
                            'slow': float(prices.get('slow', 0)),
                            'standard': float(prices.get('standard', 0)),
                            'fast': float(prices.get('fast', 0)),
                            'instant': float(prices.get('instant', 0))
                        }
                    ))
                    
                    # Trim history
                    cutoff = datetime.utcnow() - timedelta(
                        seconds=self.settings.PRICE_HISTORY_WINDOW
                    )
                    self._price_history[chain] = [
                        (ts, p) for ts, p in self._price_history[chain]
                        if ts > cutoff
                    ]
                    
                await asyncio.sleep(self.settings.GAS_PRICE_UPDATE_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error updating gas prices: {e}")
                await asyncio.sleep(5)
                
    async def _train_prediction_models(self):
        """Background task to train prediction models"""
        while self._running:
            try:
                await asyncio.sleep(self.settings.MODEL_UPDATE_INTERVAL)
                
                for chain, history in self._price_history.items():
                    if len(history) < self.settings.MIN_TRAINING_SAMPLES:
                        continue
                        
                    # Prepare training data
                    X, y = self._prepare_training_data(history)
                    
                    if len(X) < 100:
                        continue
                        
                    # Train model
                    model = RandomForestRegressor(
                        n_estimators=100,
                        max_depth=10,
                        random_state=42
                    )
                    model.fit(X, y)
                    
                    self._prediction_models[chain] = model
                    logger.info(f"Updated prediction model for {chain}")
                    
            except Exception as e:
                logger.error(f"Error training models: {e}")
                
    def _prepare_training_data(
        self,
        history: List[Tuple[datetime, Dict[str, float]]]
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare data for model training"""
        # Extract features and targets
        features = []
        targets = []
        
        for i in range(len(history) - 1):
            timestamp, prices = history[i]
            next_prices = history[i + 1][1]
            
            # Features: time of day, day of week, recent price trends
            hour = timestamp.hour
            day_of_week = timestamp.weekday()
            
            # Price features
            feature_vector = [
                hour,
                day_of_week,
                prices['slow'],
                prices['standard'],
                prices['fast'],
                prices['instant']
            ]
            
            # Add moving averages
            for window in self.settings.FEATURE_WINDOW_SIZES:
                if i >= window:
                    window_prices = [h[1]['standard'] for h in history[i-window:i]]
                    feature_vector.append(np.mean(window_prices))
                else:
                    feature_vector.append(prices['standard'])
                    
            features.append(feature_vector)
            targets.append(next_prices['standard'])
            
        return np.array(features), np.array(targets)
        
    async def predict_gas_prices(
        self,
        chain: str,
        horizon_minutes: int = 60
    ) -> GasPricePrediction:
        """Predict future gas prices"""
        model = self._prediction_models.get(chain)
        if not model:
            raise ValueError(f"No prediction model for {chain}")
            
        # Prepare features for prediction
        current_prices = self._gas_prices.get(chain, {})
        history = self._price_history.get(chain, [])
        
        predictions = {}
        now = datetime.utcnow()
        
        for minute_offset in range(0, horizon_minutes, 5):
            future_time = now + timedelta(minutes=minute_offset)
            
            # Create feature vector
            features = [
                future_time.hour,
                future_time.weekday(),
                float(current_prices.get('slow', 0)),
                float(current_prices.get('standard', 0)),
                float(current_prices.get('fast', 0)),
                float(current_prices.get('instant', 0))
            ]
            
            # Add historical features
            for window in self.settings.FEATURE_WINDOW_SIZES:
                if len(history) >= window:
                    window_prices = [h[1]['standard'] for h in history[-window:]]
                    features.append(np.mean(window_prices))
                else:
                    features.append(float(current_prices.get('standard', 0)))
                    
            # Predict
            predicted_price = model.predict([features])[0]
            
            predictions[minute_offset] = {
                'standard': str(int(predicted_price)),
                'slow': str(int(predicted_price * 0.75)),
                'fast': str(int(predicted_price * 1.5)),
                'instant': str(int(predicted_price * 2.5))
            }
            
        # Find best time window
        min_price = min(predictions.values(), key=lambda p: float(p['standard']))
        best_offset = [k for k, v in predictions.items() if v == min_price][0]
        
        return GasPricePrediction(
            timestamp=now,
            chain=chain,
            predictions=predictions,
            model_confidence=0.8,  # TODO: Calculate actual confidence
            features_used=[
                'hour', 'day_of_week', 'current_prices',
                'moving_averages'
            ],
            best_time_window={
                'offset_minutes': best_offset,
                'predicted_price': min_price['standard']
            },
            potential_savings=str(
                int(float(current_prices.get('standard', 0)) - float(min_price['standard']))
            )
        ) 