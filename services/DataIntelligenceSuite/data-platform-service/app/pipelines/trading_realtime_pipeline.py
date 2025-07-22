"""
Trading Real-time Data Pipeline

Ingests trading data from MarketServices into Data Platform using SeaTunnel
with real-time quality scoring and enrichment.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from app.pipelines.seatunnel_manager import (
    SeaTunnelPipelineManager,
    PipelineConfig,
    ConnectorType,
    TransformType
)
from app.lake.trading_medallion import TradingMedallionArchitecture

logger = logging.getLogger(__name__)


class TradingRealtimePipeline:
    """Real-time trading data pipeline with market intelligence enrichment"""
    
    def __init__(self,
                 pipeline_manager: SeaTunnelPipelineManager,
                 trading_medallion: TradingMedallionArchitecture):
        self.pipeline_manager = pipeline_manager
        self.trading_medallion = trading_medallion
        
        # Pipeline configurations
        self.pipelines = {}
        
    async def initialize(self):
        """Initialize all trading pipelines"""
        await self._create_order_flow_pipeline()
        await self._create_market_data_pipeline()
        await self._create_risk_metrics_pipeline()
        await self._create_ml_features_pipeline()
        
    async def _create_order_flow_pipeline(self):
        """Create pipeline for real-time order flow data"""
        config = PipelineConfig(
            name="trading_order_flow",
            description="Real-time order flow from trading-core-service",
            source={
                "type": ConnectorType.PULSAR.value,
                "config": {
                    "service_url": "pulsar://pulsar:6650",
                    "topics": [
                        "trading.orders.submitted",
                        "trading.orders.executed",
                        "trading.orders.cancelled"
                    ],
                    "subscription_name": "data-platform-order-flow"
                }
            },
            transforms=[
                {
                    "type": TransformType.SQL.value,
                    "config": {
                        "query": """
                        SELECT 
                            order_id,
                            market_id,
                            trader_id,
                            order_type,
                            side,
                            price,
                            quantity,
                            timestamp,
                            CASE 
                                WHEN price > lag_price * 1.01 THEN 'aggressive_buy'
                                WHEN price < lag_price * 0.99 THEN 'aggressive_sell'
                                ELSE 'passive'
                            END as order_aggressiveness,
                            ROW_NUMBER() OVER (PARTITION BY trader_id ORDER BY timestamp) as trader_order_seq
                        FROM source
                        """
                    }
                },
                {
                    "type": "enrichment",
                    "config": {
                        "source": "market_intelligence",
                        "fields": ["market_sentiment", "volatility_regime", "liquidity_score"]
                    }
                }
            ],
            sink={
                "type": ConnectorType.IGNITE.value,
                "config": {
                    "cache_name": "order_flow_realtime",
                    "write_mode": "streaming",
                    "ttl_seconds": 3600
                }
            }
        )
        
        pipeline_id = await self.pipeline_manager.create_pipeline(
            config=config,
            tenant_id="platform"
        )
        
        self.pipelines["order_flow"] = pipeline_id
        logger.info(f"Created order flow pipeline: {pipeline_id}")
        
    async def _create_market_data_pipeline(self):
        """Create pipeline for market data aggregation"""
        config = PipelineConfig(
            name="trading_market_data",
            description="Real-time market data with technical indicators",
            source={
                "type": ConnectorType.PULSAR.value,
                "config": {
                    "topics": ["trading.market.trades", "trading.market.orderbook"],
                    "subscription_name": "data-platform-market-data"
                }
            },
            transforms=[
                {
                    "type": TransformType.WATERMARK.value,
                    "config": {
                        "column": "timestamp",
                        "delay": "5 seconds"
                    }
                },
                {
                    "type": TransformType.AGGREGATE.value,
                    "config": {
                        "window": "1 minute",
                        "group_by": ["market_id"],
                        "aggregations": {
                            "vwap": "SUM(price * volume) / SUM(volume)",
                            "high": "MAX(price)",
                            "low": "MIN(price)",
                            "volume": "SUM(volume)",
                            "trade_count": "COUNT(*)",
                            "spread": "AVG(ask_price - bid_price)"
                        }
                    }
                },
                {
                    "type": "technical_indicators",
                    "config": {
                        "indicators": ["RSI", "MACD", "BollingerBands"],
                        "window_sizes": [14, 26, 20]
                    }
                }
            ],
            sink={
                "type": "multi",
                "sinks": [
                    {
                        "type": ConnectorType.DRUID.value,
                        "config": {
                            "datasource": "market_data_1min",
                            "timestamp_column": "window_start"
                        }
                    },
                    {
                        "type": ConnectorType.ELASTICSEARCH.value,
                        "config": {
                            "index": "market-data-realtime",
                            "id_field": "market_id"
                        }
                    }
                ]
            }
        )
        
        pipeline_id = await self.pipeline_manager.create_pipeline(
            config=config,
            tenant_id="platform"
        )
        
        self.pipelines["market_data"] = pipeline_id
        
    async def _create_risk_metrics_pipeline(self):
        """Create pipeline for real-time risk metrics"""
        config = PipelineConfig(
            name="trading_risk_metrics",
            description="Real-time risk calculations and monitoring",
            source={
                "type": ConnectorType.IGNITE.value,
                "config": {
                    "cache_name": "positions_realtime",
                    "query": "SELECT * FROM positions WHERE last_update > ?"
                }
            },
            transforms=[
                {
                    "type": "join",
                    "config": {
                        "join_type": "left",
                        "right_source": "market_data_1min",
                        "on": "market_id",
                        "fields": ["current_price", "volatility", "liquidity_score"]
                    }
                },
                {
                    "type": TransformType.SQL.value,
                    "config": {
                        "query": """
                        SELECT 
                            trader_id,
                            SUM(position_value) as total_exposure,
                            SUM(unrealized_pnl) as total_pnl,
                            MAX(position_value / account_equity) as max_leverage,
                            STDDEV(returns) * SQRT(252) as portfolio_volatility,
                            -- VaR calculation
                            PERCENTILE(returns, 0.05) * total_exposure as var_95,
                            -- Concentration risk
                            MAX(position_value) / SUM(position_value) as max_concentration
                        FROM source
                        GROUP BY trader_id
                        """
                    }
                },
                {
                    "type": "risk_scoring",
                    "config": {
                        "model": "ml_risk_model_v2",
                        "features": ["leverage", "volatility", "concentration", "trading_frequency"]
                    }
                }
            ],
            sink={
                "type": ConnectorType.IGNITE.value,
                "config": {
                    "cache_name": "risk_metrics_realtime",
                    "alert_on": {
                        "max_leverage": "> 10",
                        "var_95": "< -100000",
                        "risk_score": "> 0.8"
                    }
                }
            }
        )
        
        pipeline_id = await self.pipeline_manager.create_pipeline(
            config=config,
            tenant_id="platform"
        )
        
        self.pipelines["risk_metrics"] = pipeline_id
        
    async def _create_ml_features_pipeline(self):
        """Create pipeline for ML feature generation"""
        config = PipelineConfig(
            name="trading_ml_features",
            description="Generate ML features for market prediction",
            source={
                "type": "union",
                "sources": [
                    {"cache": "order_flow_realtime"},
                    {"datasource": "market_data_1min"},
                    {"cache": "risk_metrics_realtime"}
                ]
            },
            transforms=[
                {
                    "type": "feature_engineering",
                    "config": {
                        "features": {
                            "order_imbalance": "buy_volume / (buy_volume + sell_volume)",
                            "price_momentum": "(price - lag(price, 5)) / lag(price, 5)",
                            "volume_ratio": "volume / avg(volume) OVER (ORDER BY timestamp ROWS 20 PRECEDING)",
                            "spread_zscore": "(spread - avg(spread)) / stddev(spread)",
                            "trader_skill": "win_rate * sharpe_ratio",
                            "market_regime": "CASE WHEN volatility > 0.3 THEN 'high_vol' ELSE 'normal' END"
                        }
                    }
                },
                {
                    "type": "time_series_features",
                    "config": {
                        "windows": [5, 15, 60],
                        "features": ["return", "volatility", "autocorrelation"]
                    }
                }
            ],
            sink={
                "type": ConnectorType.ICEBERG.value,
                "config": {
                    "table": "ml_features.trading_features",
                    "partition_by": ["date", "market_id"],
                    "write_mode": "append"
                }
            }
        )
        
        pipeline_id = await self.pipeline_manager.create_pipeline(
            config=config,
            tenant_id="platform"
        )
        
        self.pipelines["ml_features"] = pipeline_id
        
    async def start_all_pipelines(self):
        """Start all trading pipelines"""
        for name, pipeline_id in self.pipelines.items():
            try:
                await self.pipeline_manager.run_pipeline(pipeline_id)
                logger.info(f"Started pipeline {name}: {pipeline_id}")
            except Exception as e:
                logger.error(f"Failed to start pipeline {name}: {e}")
                
    async def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get metrics for all pipelines"""
        metrics = {}
        
        for name, pipeline_id in self.pipelines.items():
            try:
                status = await self.pipeline_manager.get_pipeline_status(pipeline_id)
                metrics[name] = {
                    "status": status["status"],
                    "processed_records": status.get("metrics", {}).get("records_processed", 0),
                    "error_rate": status.get("metrics", {}).get("error_rate", 0),
                    "latency_ms": status.get("metrics", {}).get("processing_latency_ms", 0)
                }
            except Exception as e:
                logger.error(f"Failed to get metrics for {name}: {e}")
                
        return metrics 