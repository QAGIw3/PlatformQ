#!/usr/bin/env python3
"""
Trade Enricher Pulsar Function

Enriches trading events with additional context:
- Market data (price, volume, volatility)
- Risk metrics (position size, exposure)
- Trader information (tier, reputation)
- Technical indicators
"""

from pulsar import Function
from typing import Dict, Any, Optional, List
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal

logger = logging.getLogger(__name__)


class TradeEnricherFunction(Function):
    """Enriches trading events with market and risk data"""
    
    def __init__(self):
        super().__init__()
        self.market_data_cache = {}
        self.trader_info_cache = {}
        self.risk_metrics_cache = {}
        
    def process(self, input_data: bytes, context: Any) -> bytes:
        """Process and enrich trading event"""
        try:
            # Parse input event
            event = json.loads(input_data.decode('utf-8'))
            event_type = event.get('event_type', '')
            
            # Route to appropriate enrichment method
            if event_type == 'trade.executed':
                enriched = self._enrich_trade_execution(event)
            elif event_type == 'position.updated':
                enriched = self._enrich_position_update(event)
            elif event_type == 'risk.alert':
                enriched = self._enrich_risk_alert(event)
            elif event_type == 'strategy.signal':
                enriched = self._enrich_strategy_signal(event)
            else:
                # Pass through with basic enrichment
                enriched = self._basic_enrichment(event)
            
            # Add processing metadata
            enriched['enrichment'] = {
                'enriched_at': datetime.utcnow().isoformat(),
                'function_id': context.get_function_id(),
                'version': '1.0.0'
            }
            
            return json.dumps(enriched).encode('utf-8')
            
        except Exception as e:
            logger.error(f"Error enriching event: {e}")
            # Return original event on error
            return input_data
    
    def _enrich_trade_execution(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich trade execution event"""
        trader_id = event.get('trader_id')
        market_id = event.get('market_id')
        trade_data = event.get('data', {})
        
        # Get market data
        market_data = self._get_market_data(market_id)
        
        # Get trader information
        trader_info = self._get_trader_info(trader_id)
        
        # Calculate trade metrics
        trade_metrics = self._calculate_trade_metrics(trade_data, market_data)
        
        # Calculate risk impact
        risk_impact = self._calculate_risk_impact(trader_id, trade_data)
        
        # Enrich event
        event['enriched_data'] = {
            'market': {
                'last_price': market_data.get('last_price'),
                'volume_24h': market_data.get('volume_24h'),
                'volatility': market_data.get('volatility'),
                'bid_ask_spread': market_data.get('spread'),
                'liquidity_depth': market_data.get('liquidity_depth')
            },
            'trader': {
                'tier': trader_info.get('tier', 'standard'),
                'reputation_score': trader_info.get('reputation_score', 0.5),
                'win_rate': trader_info.get('win_rate'),
                'total_volume': trader_info.get('total_volume'),
                'active_positions': trader_info.get('active_positions', 0)
            },
            'trade_analysis': {
                'size_category': trade_metrics.get('size_category'),
                'price_impact': trade_metrics.get('price_impact'),
                'slippage_estimate': trade_metrics.get('slippage_estimate'),
                'execution_quality': trade_metrics.get('execution_quality')
            },
            'risk_metrics': {
                'position_risk_score': risk_impact.get('position_risk'),
                'portfolio_var': risk_impact.get('portfolio_var'),
                'margin_utilization': risk_impact.get('margin_utilization'),
                'concentration_risk': risk_impact.get('concentration_risk')
            }
        }
        
        return event
    
    def _enrich_position_update(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich position update event"""
        trader_id = event.get('trader_id')
        position_data = event.get('data', {})
        
        # Calculate position metrics
        position_metrics = {
            'position_age': self._calculate_position_age(position_data),
            'unrealized_pnl_percent': self._calculate_pnl_percent(position_data),
            'risk_reward_ratio': self._calculate_risk_reward(position_data),
            'position_health': self._assess_position_health(position_data)
        }
        
        # Get correlated positions
        correlations = self._find_correlated_positions(trader_id, position_data)
        
        event['enriched_data'] = {
            'position_metrics': position_metrics,
            'correlations': correlations,
            'suggested_actions': self._suggest_position_actions(position_data, position_metrics)
        }
        
        return event
    
    def _enrich_risk_alert(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich risk alert event"""
        alert_data = event.get('data', {})
        trader_id = event.get('trader_id')
        
        # Analyze risk context
        risk_context = {
            'market_conditions': self._assess_market_conditions(),
            'trader_risk_profile': self._get_trader_risk_profile(trader_id),
            'historical_alerts': self._get_historical_alerts(trader_id),
            'peer_comparison': self._compare_to_peers(trader_id, alert_data)
        }
        
        # Determine severity and urgency
        severity_analysis = self._analyze_alert_severity(alert_data, risk_context)
        
        event['enriched_data'] = {
            'risk_context': risk_context,
            'severity_analysis': severity_analysis,
            'recommended_actions': self._recommend_risk_actions(alert_data, severity_analysis),
            'escalation_required': severity_analysis.get('score', 0) > 0.8
        }
        
        return event
    
    def _enrich_strategy_signal(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich strategy signal event"""
        signal_data = event.get('data', {})
        market_id = event.get('market_id')
        
        # Get market context
        market_context = self._get_market_context(market_id)
        
        # Analyze signal strength
        signal_analysis = {
            'signal_strength': self._calculate_signal_strength(signal_data, market_context),
            'confidence_score': self._calculate_signal_confidence(signal_data),
            'market_alignment': self._check_market_alignment(signal_data, market_context),
            'timing_quality': self._assess_signal_timing(signal_data, market_context)
        }
        
        # Historical performance of similar signals
        historical_performance = self._get_signal_historical_performance(signal_data)
        
        event['enriched_data'] = {
            'market_context': market_context,
            'signal_analysis': signal_analysis,
            'historical_performance': historical_performance,
            'execution_recommendations': self._recommend_execution(signal_analysis)
        }
        
        return event
    
    def _basic_enrichment(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Basic enrichment for all events"""
        event['enriched_data'] = {
            'timestamp': datetime.utcnow().isoformat(),
            'market_phase': self._get_market_phase(),
            'system_load': self._get_system_load()
        }
        return event
    
    # Helper methods
    def _get_market_data(self, market_id: str) -> Dict[str, Any]:
        """Get current market data (cached)"""
        # In production, this would fetch from Ignite cache or market data service
        return self.market_data_cache.get(market_id, {
            'last_price': 100.0,
            'volume_24h': 1000000,
            'volatility': 0.25,
            'spread': 0.001,
            'liquidity_depth': 50000
        })
    
    def _get_trader_info(self, trader_id: str) -> Dict[str, Any]:
        """Get trader information (cached)"""
        # In production, this would fetch from graph intelligence service
        return self.trader_info_cache.get(trader_id, {
            'tier': 'standard',
            'reputation_score': 0.5,
            'win_rate': 0.55,
            'total_volume': 100000,
            'active_positions': 3
        })
    
    def _calculate_trade_metrics(self, trade_data: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate trade-specific metrics"""
        quantity = Decimal(str(trade_data.get('quantity', 0)))
        price = Decimal(str(trade_data.get('price', 0)))
        market_price = Decimal(str(market_data.get('last_price', price)))
        
        # Size category
        if quantity > 10000:
            size_category = 'whale'
        elif quantity > 1000:
            size_category = 'large'
        elif quantity > 100:
            size_category = 'medium'
        else:
            size_category = 'small'
        
        # Price impact estimate
        liquidity = Decimal(str(market_data.get('liquidity_depth', 50000)))
        price_impact = float((quantity / liquidity) * Decimal('0.1'))  # Simplified model
        
        # Slippage
        slippage = abs(float((price - market_price) / market_price))
        
        # Execution quality (0-1 score)
        execution_quality = max(0, 1 - slippage - price_impact)
        
        return {
            'size_category': size_category,
            'price_impact': price_impact,
            'slippage_estimate': slippage,
            'execution_quality': execution_quality
        }
    
    def _calculate_risk_impact(self, trader_id: str, trade_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate risk impact of trade"""
        # Simplified risk calculations
        return {
            'position_risk': 0.3,
            'portfolio_var': 0.05,
            'margin_utilization': 0.6,
            'concentration_risk': 0.2
        }
    
    def _calculate_position_age(self, position_data: Dict[str, Any]) -> float:
        """Calculate position age in hours"""
        entry_time = position_data.get('entry_timestamp')
        if entry_time:
            entry_dt = datetime.fromisoformat(entry_time)
            age = (datetime.utcnow() - entry_dt).total_seconds() / 3600
            return age
        return 0
    
    def _calculate_pnl_percent(self, position_data: Dict[str, Any]) -> float:
        """Calculate PnL percentage"""
        pnl = Decimal(str(position_data.get('pnl', 0)))
        entry_value = Decimal(str(position_data.get('entry_value', 1)))
        if entry_value != 0:
            return float((pnl / entry_value) * 100)
        return 0
    
    def _calculate_risk_reward(self, position_data: Dict[str, Any]) -> float:
        """Calculate risk/reward ratio"""
        potential_profit = position_data.get('target_profit', 0)
        potential_loss = position_data.get('stop_loss_amount', 1)
        if potential_loss != 0:
            return abs(potential_profit / potential_loss)
        return 0
    
    def _assess_position_health(self, position_data: Dict[str, Any]) -> str:
        """Assess overall position health"""
        pnl_percent = self._calculate_pnl_percent(position_data)
        if pnl_percent > 5:
            return 'excellent'
        elif pnl_percent > 0:
            return 'good'
        elif pnl_percent > -5:
            return 'warning'
        else:
            return 'critical'
    
    def _find_correlated_positions(self, trader_id: str, position_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find correlated positions"""
        # Simplified - would query position database
        return []
    
    def _suggest_position_actions(self, position_data: Dict[str, Any], metrics: Dict[str, Any]) -> List[str]:
        """Suggest actions based on position state"""
        suggestions = []
        
        health = metrics.get('position_health')
        if health == 'critical':
            suggestions.append('Consider reducing position size or closing')
        elif health == 'warning':
            suggestions.append('Monitor closely, consider partial profit taking')
        elif health == 'excellent':
            suggestions.append('Consider trailing stop to protect profits')
        
        return suggestions
    
    def _assess_market_conditions(self) -> str:
        """Assess current market conditions"""
        # Simplified - would analyze real market data
        return 'volatile'
    
    def _get_trader_risk_profile(self, trader_id: str) -> Dict[str, Any]:
        """Get trader's risk profile"""
        return {
            'risk_tolerance': 'moderate',
            'max_drawdown': 0.15,
            'preferred_leverage': 2.0
        }
    
    def _get_historical_alerts(self, trader_id: str) -> List[Dict[str, Any]]:
        """Get trader's historical alerts"""
        # Would query alert history
        return []
    
    def _compare_to_peers(self, trader_id: str, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Compare trader metrics to peers"""
        return {
            'risk_percentile': 75,
            'performance_percentile': 60
        }
    
    def _analyze_alert_severity(self, alert_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze alert severity"""
        risk_level = alert_data.get('risk_level', 'medium')
        severity_scores = {
            'low': 0.2,
            'medium': 0.5,
            'high': 0.8,
            'critical': 1.0
        }
        
        return {
            'score': severity_scores.get(risk_level, 0.5),
            'factors': ['market_volatility', 'position_size'],
            'trend': 'increasing'
        }
    
    def _recommend_risk_actions(self, alert_data: Dict[str, Any], severity: Dict[str, Any]) -> List[str]:
        """Recommend risk mitigation actions"""
        score = severity.get('score', 0)
        actions = []
        
        if score > 0.8:
            actions.extend([
                'Immediate position reduction required',
                'Increase monitoring frequency',
                'Review stop loss levels'
            ])
        elif score > 0.5:
            actions.extend([
                'Consider hedging strategies',
                'Review position sizing',
                'Set tighter risk limits'
            ])
        else:
            actions.append('Continue monitoring')
        
        return actions
    
    def _get_market_context(self, market_id: str) -> Dict[str, Any]:
        """Get market context for signal"""
        return {
            'trend': 'bullish',
            'momentum': 'strong',
            'volume_trend': 'increasing',
            'volatility_regime': 'normal'
        }
    
    def _calculate_signal_strength(self, signal_data: Dict[str, Any], market_context: Dict[str, Any]) -> float:
        """Calculate signal strength (0-1)"""
        base_strength = signal_data.get('strength', 0.5)
        
        # Adjust for market alignment
        if market_context.get('trend') == signal_data.get('direction'):
            return min(1.0, base_strength * 1.2)
        else:
            return base_strength * 0.8
    
    def _calculate_signal_confidence(self, signal_data: Dict[str, Any]) -> float:
        """Calculate confidence in signal"""
        indicators_count = len(signal_data.get('indicators', []))
        confirmations = signal_data.get('confirmations', 0)
        
        return min(1.0, (indicators_count * 0.2) + (confirmations * 0.1))
    
    def _check_market_alignment(self, signal_data: Dict[str, Any], market_context: Dict[str, Any]) -> bool:
        """Check if signal aligns with market"""
        signal_direction = signal_data.get('direction')
        market_trend = market_context.get('trend')
        
        return (signal_direction == 'buy' and market_trend == 'bullish') or \
               (signal_direction == 'sell' and market_trend == 'bearish')
    
    def _assess_signal_timing(self, signal_data: Dict[str, Any], market_context: Dict[str, Any]) -> str:
        """Assess signal timing quality"""
        volatility = market_context.get('volatility_regime', 'normal')
        
        if volatility == 'high':
            return 'risky'
        elif market_context.get('volume_trend') == 'increasing':
            return 'good'
        else:
            return 'neutral'
    
    def _get_signal_historical_performance(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get historical performance of similar signals"""
        return {
            'win_rate': 0.62,
            'average_return': 0.023,
            'max_drawdown': 0.08,
            'sample_size': 150
        }
    
    def _recommend_execution(self, signal_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Recommend execution parameters"""
        strength = signal_analysis.get('signal_strength', 0.5)
        
        if strength > 0.8:
            return {
                'action': 'execute_full',
                'urgency': 'high',
                'position_size': 1.0
            }
        elif strength > 0.5:
            return {
                'action': 'execute_partial',
                'urgency': 'medium',
                'position_size': 0.5
            }
        else:
            return {
                'action': 'monitor',
                'urgency': 'low',
                'position_size': 0
            }
    
    def _get_market_phase(self) -> str:
        """Get current market phase"""
        hour = datetime.utcnow().hour
        if 14 <= hour <= 20:  # US market hours (UTC)
            return 'active'
        elif 1 <= hour <= 9:  # Asian market hours
            return 'asian_session'
        else:
            return 'low_activity'
    
    def _get_system_load(self) -> str:
        """Get system load status"""
        # Would check actual system metrics
        return 'normal' 