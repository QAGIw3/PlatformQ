"""
Market Intelligence API

Provides unified access to integrated market intelligence capabilities
combining data platform, graph intelligence, and market services.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
from fastapi import APIRouter, Request, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from ..pipelines.trading_realtime_pipeline import TradingRealtimePipeline
from ..integrations.service_orchestrator import ServiceOrchestrator

router = APIRouter(prefix="/market-intelligence", tags=["Market Intelligence"])


class MarketInsightRequest(BaseModel):
    """Request for market insights"""
    market_id: str = Field(..., description="Market identifier")
    trader_id: Optional[str] = Field(None, description="Trader ID for personalized insights")
    time_range: str = Field("24h", description="Time range for analysis")
    include_graph_insights: bool = Field(True, description="Include graph-based insights")
    include_ml_predictions: bool = Field(True, description="Include ML predictions")


class TradingSignalRequest(BaseModel):
    """Request for trading signals"""
    markets: List[str] = Field(..., description="List of market IDs")
    signal_types: List[str] = Field(
        ["momentum", "mean_reversion", "breakout"],
        description="Types of signals to generate"
    )
    risk_tolerance: float = Field(0.5, ge=0, le=1, description="Risk tolerance level")


class SystemicRiskRequest(BaseModel):
    """Request for systemic risk analysis"""
    markets: List[str] = Field(..., description="Markets to analyze")
    shock_scenarios: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Custom shock scenarios to simulate"
    )
    include_contagion_paths: bool = Field(True)


class MLModelUpdateRequest(BaseModel):
    """Request to update ML models"""
    model_type: str = Field(..., description="Type of model to update")
    retrain: bool = Field(False, description="Trigger retraining")
    deploy_to_production: bool = Field(False, description="Auto-deploy if better")


@router.get("/insights/{market_id}")
async def get_market_insights(
    market_id: str,
    request: Request,
    time_range: str = Query("24h", description="Time range"),
    include_graph: bool = Query(True, description="Include graph insights"),
    include_ml: bool = Query(True, description="Include ML predictions")
) -> Dict[str, Any]:
    """Get comprehensive market insights combining all data sources"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        # Base market data from data platform
        market_data = await orchestrator.analytics.execute_unified_query(
            query=f"""
            SELECT 
                market_id,
                AVG(price) as avg_price,
                STDDEV(price) as price_volatility,
                SUM(volume) as total_volume,
                AVG(spread) as avg_spread,
                MAX(price) as high,
                MIN(price) as low
            FROM market_data_1min
            WHERE market_id = '{market_id}'
            AND timestamp > NOW() - INTERVAL '{time_range}'
            GROUP BY market_id
            """,
            cache_results=True
        )
        
        insights = {
            "market_id": market_id,
            "timestamp": datetime.utcnow().isoformat(),
            "market_data": market_data,
            "time_range": time_range
        }
        
        # Add graph-based insights
        if include_graph:
            try:
                graph_insights = await orchestrator.get_graph_insights(market_id)
                insights["graph_analysis"] = {
                    "trader_network": graph_insights.get("trader_network"),
                    "manipulation_risk": graph_insights.get("manipulation_patterns"),
                    "systemic_importance": graph_insights.get("centrality_score"),
                    "correlation_network": graph_insights.get("correlated_markets")
                }
            except Exception as e:
                insights["graph_analysis"] = {"error": str(e)}
        
        # Add ML predictions
        if include_ml:
            try:
                ml_predictions = await orchestrator.get_ml_predictions(
                    market_id=market_id,
                    horizons=["1h", "4h", "24h"]
                )
                insights["predictions"] = {
                    "price_forecast": ml_predictions.get("price_predictions"),
                    "volatility_forecast": ml_predictions.get("volatility_predictions"),
                    "volume_forecast": ml_predictions.get("volume_predictions"),
                    "confidence_intervals": ml_predictions.get("confidence_intervals"),
                    "model_version": ml_predictions.get("model_version")
                }
            except Exception as e:
                insights["predictions"] = {"error": str(e)}
        
        # Add risk metrics
        risk_metrics = await orchestrator.analytics.execute_unified_query(
            query=f"""
            SELECT 
                market_id,
                var_95,
                max_drawdown,
                sharpe_ratio,
                sortino_ratio
            FROM risk_metrics_realtime
            WHERE market_id = '{market_id}'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        
        if risk_metrics:
            insights["risk_metrics"] = risk_metrics[0]
        
        return insights
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trading-signals")
async def generate_trading_signals(
    request: Request,
    signal_request: TradingSignalRequest
) -> Dict[str, Any]:
    """Generate trading signals based on integrated analysis"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        signals = []
        
        for market_id in signal_request.markets:
            # Get market insights
            insights = await get_market_insights(
                market_id=market_id,
                request=request,
                time_range="24h",
                include_graph=True,
                include_ml=True
            )
            
            market_signals = []
            
            # Momentum signals
            if "momentum" in signal_request.signal_types:
                momentum = await _calculate_momentum_signal(insights)
                if momentum["strength"] > signal_request.risk_tolerance:
                    market_signals.append(momentum)
            
            # Mean reversion signals
            if "mean_reversion" in signal_request.signal_types:
                mean_rev = await _calculate_mean_reversion_signal(insights)
                if mean_rev["confidence"] > signal_request.risk_tolerance:
                    market_signals.append(mean_rev)
            
            # Breakout signals
            if "breakout" in signal_request.signal_types:
                breakout = await _calculate_breakout_signal(insights)
                if breakout["probability"] > signal_request.risk_tolerance:
                    market_signals.append(breakout)
            
            # Add graph-enhanced signals
            if insights.get("graph_analysis", {}).get("manipulation_risk", 0) < 0.3:
                for signal in market_signals:
                    signal["graph_confidence_boost"] = 0.1
            
            signals.extend(market_signals)
        
        return {
            "signals": signals,
            "generated_at": datetime.utcnow().isoformat(),
            "total_signals": len(signals),
            "risk_tolerance": signal_request.risk_tolerance
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/systemic-risk")
async def analyze_systemic_risk(
    request: Request,
    risk_request: SystemicRiskRequest
) -> Dict[str, Any]:
    """Analyze systemic risk across markets"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        # Get cross-market correlations
        correlation_matrix = await orchestrator.analytics.execute_unified_query(
            query=f"""
            SELECT 
                market1,
                market2,
                correlation,
                rolling_correlation_30d
            FROM market_correlations
            WHERE market1 IN ({','.join([f"'{m}'" for m in risk_request.markets])})
            AND market2 IN ({','.join([f"'{m}'" for m in risk_request.markets])})
            AND correlation > 0.3
            """
        )
        
        # Get systemic risk from graph analysis
        graph_risk = await orchestrator.get_systemic_risk_analysis(
            markets=risk_request.markets,
            include_contagion=risk_request.include_contagion_paths
        )
        
        # Run shock simulations
        shock_results = []
        if risk_request.shock_scenarios:
            for scenario in risk_request.shock_scenarios:
                result = await orchestrator.simulate_market_shock(
                    shock_market=scenario["market"],
                    shock_size=scenario["size"],
                    propagation_model="network"
                )
                shock_results.append(result)
        
        # Calculate aggregate risk metrics
        risk_score = _calculate_systemic_risk_score(
            correlations=correlation_matrix,
            graph_metrics=graph_risk,
            shock_simulations=shock_results
        )
        
        return {
            "systemic_risk_score": risk_score,
            "risk_level": _get_risk_level(risk_score),
            "correlation_analysis": {
                "high_correlations": len([c for c in correlation_matrix if c["correlation"] > 0.7]),
                "correlation_matrix": correlation_matrix
            },
            "graph_analysis": graph_risk,
            "shock_simulations": shock_results,
            "recommendations": _generate_risk_recommendations(risk_score, graph_risk),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pipeline-status")
async def get_pipeline_status(request: Request) -> Dict[str, Any]:
    """Get status of all trading data pipelines"""
    try:
        pipeline_manager = request.app.state.trading_pipeline
        
        metrics = await pipeline_manager.get_pipeline_metrics()
        
        return {
            "pipelines": metrics,
            "overall_health": _calculate_pipeline_health(metrics),
            "data_freshness": await _check_data_freshness(request),
            "error_summary": _summarize_errors(metrics)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml-models/update")
async def update_ml_models(
    request: Request,
    update_request: MLModelUpdateRequest
) -> Dict[str, Any]:
    """Update or retrain ML models"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        if update_request.retrain:
            # Trigger retraining DAG
            training_id = await orchestrator.trigger_ml_training(
                model_type=update_request.model_type,
                auto_deploy=update_request.deploy_to_production
            )
            
            return {
                "status": "training_initiated",
                "training_id": training_id,
                "estimated_completion": (datetime.utcnow() + timedelta(hours=2)).isoformat(),
                "auto_deploy": update_request.deploy_to_production
            }
        else:
            # Just update model configuration
            result = await orchestrator.update_model_config(
                model_type=update_request.model_type,
                deploy_to_production=update_request.deploy_to_production
            )
            
            return {
                "status": "configuration_updated",
                "model_version": result["version"],
                "deployed": update_request.deploy_to_production
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trader/{trader_id}/insights")
async def get_trader_insights(
    trader_id: str,
    request: Request,
    include_network: bool = Query(True, description="Include network analysis")
) -> Dict[str, Any]:
    """Get insights for a specific trader"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        # Get trader metrics
        trader_metrics = await orchestrator.analytics.execute_unified_query(
            query=f"""
            SELECT 
                trader_id,
                COUNT(DISTINCT order_id) as total_orders,
                AVG(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as win_rate,
                SUM(pnl) as total_pnl,
                AVG(holding_period) as avg_holding_period,
                COUNT(DISTINCT market_id) as markets_traded
            FROM trader_activity
            WHERE trader_id = '{trader_id}'
            AND timestamp > NOW() - INTERVAL '30d'
            GROUP BY trader_id
            """
        )
        
        insights = {
            "trader_id": trader_id,
            "metrics": trader_metrics[0] if trader_metrics else {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add network insights
        if include_network:
            network = await orchestrator.get_trader_network_insights(trader_id)
            insights["network_analysis"] = {
                "influence_score": network.get("influence_score"),
                "network_size": network.get("network_size"),
                "copy_traders": network.get("copy_traders", []),
                "trading_cliques": network.get("cliques", []),
                "risk_metrics": {
                    "copy_cascade_risk": network.get("copy_risk"),
                    "manipulation_risk": network.get("manipulation_risk"),
                    "network_sentiment": network.get("network_sentiment")
                }
            }
        
        # Add behavioral analysis
        behavior = await _analyze_trader_behavior(trader_id, orchestrator)
        insights["behavioral_analysis"] = behavior
        
        return insights
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions

async def _calculate_momentum_signal(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate momentum trading signal"""
    market_data = insights.get("market_data", {})
    predictions = insights.get("predictions", {})
    
    # Simple momentum calculation
    if market_data and predictions:
        current_price = market_data.get("avg_price", 0)
        predicted_price = predictions.get("price_forecast", {}).get("1h", current_price)
        
        momentum = (predicted_price - current_price) / current_price
        
        return {
            "type": "momentum",
            "direction": "buy" if momentum > 0 else "sell",
            "strength": abs(momentum),
            "confidence": predictions.get("confidence_intervals", {}).get("1h", 0.5),
            "target_price": predicted_price,
            "stop_loss": current_price * (0.98 if momentum > 0 else 1.02)
        }
    
    return {"type": "momentum", "strength": 0, "confidence": 0}


async def _calculate_mean_reversion_signal(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate mean reversion signal"""
    market_data = insights.get("market_data", {})
    
    if market_data:
        current_price = market_data.get("avg_price", 0)
        high = market_data.get("high", current_price)
        low = market_data.get("low", current_price)
        
        # Calculate distance from mean
        mean = (high + low) / 2
        deviation = (current_price - mean) / mean
        
        if abs(deviation) > 0.02:  # 2% threshold
            return {
                "type": "mean_reversion",
                "direction": "sell" if deviation > 0 else "buy",
                "confidence": min(abs(deviation) * 10, 1.0),
                "target_price": mean,
                "entry_price": current_price,
                "deviation_percent": deviation * 100
            }
    
    return {"type": "mean_reversion", "confidence": 0}


async def _calculate_breakout_signal(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate breakout signal"""
    market_data = insights.get("market_data", {})
    
    if market_data:
        volatility = market_data.get("price_volatility", 0)
        volume = market_data.get("total_volume", 0)
        
        # Simple breakout detection
        breakout_probability = min(volatility * 2 + (volume / 1000000), 1.0)
        
        return {
            "type": "breakout",
            "probability": breakout_probability,
            "volatility": volatility,
            "volume_spike": volume > 1000000,
            "recommended_action": "watch" if breakout_probability > 0.7 else "ignore"
        }
    
    return {"type": "breakout", "probability": 0}


def _calculate_systemic_risk_score(correlations, graph_metrics, shock_simulations) -> float:
    """Calculate aggregate systemic risk score"""
    # Weighted combination of risk factors
    correlation_risk = len([c for c in correlations if c["correlation"] > 0.8]) * 0.1
    graph_risk = graph_metrics.get("systemic_risk_score", 0) / 100
    shock_risk = max([s.get("max_impact", 0) for s in shock_simulations]) if shock_simulations else 0
    
    return min(correlation_risk * 0.3 + graph_risk * 0.4 + shock_risk * 0.3, 1.0)


def _get_risk_level(risk_score: float) -> str:
    """Convert risk score to risk level"""
    if risk_score < 0.3:
        return "low"
    elif risk_score < 0.6:
        return "moderate"
    elif risk_score < 0.8:
        return "high"
    else:
        return "critical"


def _generate_risk_recommendations(risk_score: float, graph_risk: Dict) -> List[str]:
    """Generate risk mitigation recommendations"""
    recommendations = []
    
    if risk_score > 0.7:
        recommendations.append("Consider reducing position sizes across correlated markets")
        recommendations.append("Implement stop-loss orders on high-risk positions")
        
    if graph_risk.get("contagion_paths"):
        recommendations.append("Monitor markets in identified contagion paths closely")
        
    if graph_risk.get("central_markets"):
        recommendations.append(f"Pay special attention to central markets: {graph_risk['central_markets'][:3]}")
    
    return recommendations


def _calculate_pipeline_health(metrics: Dict[str, Any]) -> str:
    """Calculate overall pipeline health"""
    error_rates = [m.get("error_rate", 0) for m in metrics.values()]
    avg_error_rate = sum(error_rates) / len(error_rates) if error_rates else 0
    
    if avg_error_rate < 0.01:
        return "healthy"
    elif avg_error_rate < 0.05:
        return "degraded"
    else:
        return "unhealthy"


async def _check_data_freshness(request: Request) -> Dict[str, Any]:
    """Check data freshness across sources"""
    orchestrator = request.app.state.service_orchestrator
    
    freshness = await orchestrator.analytics.execute_unified_query(
        query="""
        SELECT 
            'market_data' as source,
            MAX(timestamp) as latest_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) as lag_seconds
        FROM market_data_1min
        UNION ALL
        SELECT 
            'risk_metrics' as source,
            MAX(timestamp) as latest_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) as lag_seconds
        FROM risk_metrics_realtime
        """
    )
    
    return {row["source"]: row["lag_seconds"] for row in freshness}


def _summarize_errors(metrics: Dict[str, Any]) -> Dict[str, int]:
    """Summarize errors across pipelines"""
    return {
        name: int(data.get("error_rate", 0) * data.get("processed_records", 0))
        for name, data in metrics.items()
    }


async def _analyze_trader_behavior(trader_id: str, orchestrator) -> Dict[str, Any]:
    """Analyze trader behavioral patterns"""
    # This would include more sophisticated behavioral analysis
    return {
        "trading_style": "momentum",  # or "scalping", "swing", etc.
        "risk_profile": "moderate",
        "preferred_markets": ["BTC-USD", "ETH-USD"],
        "peak_trading_hours": [9, 10, 14, 15],
        "avg_position_size": 10000
    } 