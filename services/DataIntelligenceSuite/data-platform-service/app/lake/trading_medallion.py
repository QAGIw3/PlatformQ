"""
Trading Data Medallion Architecture

Implements Bronze, Silver, and Gold layers for trading data:
- Bronze: Raw trading events and market data
- Silver: Cleaned and validated trading data
- Gold: Analytics-ready features and aggregations
"""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from decimal import Decimal
import json
import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum
import asyncio

from ..analytics.druid_analytics import DruidAnalyticsEngine
from ..core.cache_manager import CacheManager
from ..feature_store.feature_store_manager import FeatureStoreManager
from ..lake.medallion_architecture import MedallionArchitecture
from .transformation_engine import TransformationEngine

logger = logging.getLogger(__name__)


class TradingDataType(Enum):
    """Types of trading data"""
    ORDER_BOOK = "order_book"
    TRADES = "trades"
    MARKET_DATA = "market_data"
    POSITIONS = "positions"
    RISK_METRICS = "risk_metrics"
    TRADER_ACTIVITY = "trader_activity"
    STRATEGY_SIGNALS = "strategy_signals"


@dataclass
class TradingDataQuality:
    """Trading data quality metrics"""
    completeness: float  # 0-1
    accuracy: float  # 0-1
    timeliness: float  # 0-1
    consistency: float  # 0-1
    issues: List[str]
    timestamp: datetime


class TradingMedallionArchitecture:
    """Specialized medallion architecture for trading data"""
    
    def __init__(self,
                 medallion_arch: MedallionArchitecture,
                 druid_engine: DruidAnalyticsEngine,
                 feature_store: FeatureStoreManager,
                 cache_manager: CacheManager,
                 transformation_engine: TransformationEngine):
        self.medallion = medallion_arch
        self.druid = druid_engine
        self.feature_store = feature_store
        self.cache = cache_manager
        self.transformation = transformation_engine
        
        # Trading-specific configurations
        self.bronze_retention_days = 90
        self.silver_retention_days = 365
        self.gold_retention_days = 730
        
        # Quality thresholds
        self.quality_thresholds = {
            'completeness': 0.95,
            'accuracy': 0.99,
            'timeliness': 0.999,
            'consistency': 0.98
        }
        
    async def ingest_trading_events(self, 
                                  events: List[Dict[str, Any]],
                                  event_type: TradingDataType) -> Dict[str, Any]:
        """Ingest raw trading events into Bronze layer"""
        try:
            # Add metadata
            enriched_events = []
            for event in events:
                enriched_event = {
                    **event,
                    '_ingestion_timestamp': datetime.utcnow().isoformat(),
                    '_event_type': event_type.value,
                    '_raw_size': len(json.dumps(event)),
                    '_schema_version': '1.0'
                }
                enriched_events.append(enriched_event)
            
            # Write to Bronze layer
            bronze_path = f"trading/bronze/{event_type.value}/{datetime.utcnow().strftime('%Y/%m/%d')}"
            result = await self.medallion.write_to_bronze(
                data=enriched_events,
                dataset_name=bronze_path,
                partition_by=['market_id', 'trader_id']
            )
            
            # Stream to Druid for real-time analytics
            if event_type in [TradingDataType.TRADES, TradingDataType.MARKET_DATA]:
                await self._stream_to_druid(enriched_events, event_type)
            
            # Update metrics
            await self._update_ingestion_metrics(event_type, len(events))
            
            return {
                'status': 'success',
                'events_ingested': len(events),
                'bronze_path': bronze_path,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error ingesting trading events: {e}")
            raise
    
    async def process_to_silver(self, 
                              event_type: TradingDataType,
                              processing_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Process Bronze data to Silver layer with validation and cleaning"""
        try:
            if not processing_date:
                processing_date = datetime.utcnow() - timedelta(hours=1)
            
            # Read from Bronze
            bronze_path = f"trading/bronze/{event_type.value}/{processing_date.strftime('%Y/%m/%d')}"
            bronze_data = await self.medallion.read_from_bronze(bronze_path)
            
            # Apply transformations based on event type
            if event_type == TradingDataType.TRADES:
                silver_data = await self._process_trades_to_silver(bronze_data)
            elif event_type == TradingDataType.ORDER_BOOK:
                silver_data = await self._process_orderbook_to_silver(bronze_data)
            elif event_type == TradingDataType.POSITIONS:
                silver_data = await self._process_positions_to_silver(bronze_data)
            else:
                silver_data = await self._generic_silver_processing(bronze_data)
            
            # Validate data quality
            quality_report = await self._validate_data_quality(silver_data, event_type)
            
            # Only write to Silver if quality meets thresholds
            if self._meets_quality_thresholds(quality_report):
                silver_path = f"trading/silver/{event_type.value}/{processing_date.strftime('%Y/%m/%d')}"
                result = await self.medallion.write_to_silver(
                    data=silver_data,
                    dataset_name=silver_path,
                    quality_metrics=quality_report
                )
                
                return {
                    'status': 'success',
                    'records_processed': len(silver_data),
                    'silver_path': silver_path,
                    'quality_report': quality_report,
                    'timestamp': datetime.utcnow().isoformat()
                }
            else:
                # Send to remediation queue
                await self._send_to_remediation(bronze_data, quality_report)
                return {
                    'status': 'quality_failed',
                    'quality_report': quality_report,
                    'sent_to_remediation': True
                }
                
        except Exception as e:
            logger.error(f"Error processing to Silver: {e}")
            raise
    
    async def generate_gold_features(self, 
                                   feature_sets: List[str],
                                   time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Generate Gold layer features for ML and analytics"""
        try:
            generated_features = {}
            
            for feature_set in feature_sets:
                if feature_set == 'market_microstructure':
                    features = await self._generate_market_microstructure_features(time_range)
                elif feature_set == 'trader_behavior':
                    features = await self._generate_trader_behavior_features(time_range)
                elif feature_set == 'risk_indicators':
                    features = await self._generate_risk_indicator_features(time_range)
                elif feature_set == 'technical_indicators':
                    features = await self._generate_technical_indicators(time_range)
                else:
                    logger.warning(f"Unknown feature set: {feature_set}")
                    continue
                
                generated_features[feature_set] = features
                
                # Write to Gold layer
                gold_path = f"trading/gold/{feature_set}/{datetime.utcnow().strftime('%Y/%m/%d')}"
                await self.medallion.write_to_gold(
                    data=features,
                    dataset_name=gold_path,
                    feature_metadata={
                        'feature_set': feature_set,
                        'generation_time': datetime.utcnow().isoformat(),
                        'time_range': {
                            'start': time_range['start'].isoformat(),
                            'end': time_range['end'].isoformat()
                        }
                    }
                )
                
                # Update feature store
                await self._update_feature_store(feature_set, features)
            
            return {
                'status': 'success',
                'feature_sets_generated': list(generated_features.keys()),
                'total_features': sum(len(f) for f in generated_features.values()),
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating Gold features: {e}")
            raise
    
    # Processing methods for different data types
    async def _process_trades_to_silver(self, bronze_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process raw trades to Silver layer"""
        silver_trades = []
        
        for trade in bronze_data:
            try:
                # Clean and validate
                cleaned_trade = {
                    'trade_id': trade.get('trade_id', trade.get('id')),
                    'market_id': trade['market_id'],
                    'trader_id': trade['trader_id'],
                    'side': trade['side'].upper(),
                    'price': float(trade['price']),
                    'quantity': float(trade['quantity']),
                    'value': float(trade['price']) * float(trade['quantity']),
                    'timestamp': self._standardize_timestamp(trade['timestamp']),
                    'order_type': trade.get('order_type', 'MARKET'),
                    'fees': float(trade.get('fees', 0)),
                    'slippage': self._calculate_slippage(trade),
                    'market_impact': self._estimate_market_impact(trade)
                }
                
                # Add derived fields
                cleaned_trade['hour_of_day'] = cleaned_trade['timestamp'].hour
                cleaned_trade['day_of_week'] = cleaned_trade['timestamp'].weekday()
                cleaned_trade['is_large_trade'] = cleaned_trade['value'] > 10000
                
                silver_trades.append(cleaned_trade)
                
            except Exception as e:
                logger.warning(f"Error processing trade: {e}")
                continue
        
        return silver_trades
    
    async def _process_orderbook_to_silver(self, bronze_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process raw orderbook snapshots to Silver layer"""
        silver_orderbooks = []
        
        for snapshot in bronze_data:
            try:
                # Calculate orderbook metrics
                bids = snapshot.get('bids', [])
                asks = snapshot.get('asks', [])
                
                cleaned_snapshot = {
                    'market_id': snapshot['market_id'],
                    'timestamp': self._standardize_timestamp(snapshot['timestamp']),
                    'best_bid': float(bids[0][0]) if bids else None,
                    'best_ask': float(asks[0][0]) if asks else None,
                    'bid_ask_spread': self._calculate_spread(bids, asks),
                    'mid_price': self._calculate_mid_price(bids, asks),
                    'bid_depth': self._calculate_depth(bids, levels=5),
                    'ask_depth': self._calculate_depth(asks, levels=5),
                    'order_imbalance': self._calculate_imbalance(bids, asks),
                    'price_pressure': self._calculate_price_pressure(bids, asks),
                    'liquidity_score': self._calculate_liquidity_score(bids, asks)
                }
                
                silver_orderbooks.append(cleaned_snapshot)
                
            except Exception as e:
                logger.warning(f"Error processing orderbook: {e}")
                continue
        
        return silver_orderbooks
    
    async def _process_positions_to_silver(self, bronze_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process raw positions to Silver layer"""
        silver_positions = []
        
        for position in bronze_data:
            try:
                # Calculate position metrics
                cleaned_position = {
                    'position_id': position['position_id'],
                    'trader_id': position['trader_id'],
                    'market_id': position['market_id'],
                    'side': position['side'].upper(),
                    'quantity': float(position['quantity']),
                    'entry_price': float(position['entry_price']),
                    'current_price': float(position.get('current_price', position['entry_price'])),
                    'timestamp': self._standardize_timestamp(position['timestamp']),
                    'unrealized_pnl': self._calculate_unrealized_pnl(position),
                    'realized_pnl': float(position.get('realized_pnl', 0)),
                    'position_age_hours': self._calculate_position_age(position),
                    'return_percent': self._calculate_return_percent(position),
                    'risk_score': self._calculate_position_risk(position)
                }
                
                silver_positions.append(cleaned_position)
                
            except Exception as e:
                logger.warning(f"Error processing position: {e}")
                continue
        
        return silver_positions
    
    # Feature generation methods
    async def _generate_market_microstructure_features(self, time_range: Dict[str, datetime]) -> List[Dict[str, Any]]:
        """Generate market microstructure features"""
        # Read Silver orderbook data
        orderbook_data = await self._read_silver_data(
            TradingDataType.ORDER_BOOK,
            time_range
        )
        
        # Read Silver trades data
        trades_data = await self._read_silver_data(
            TradingDataType.TRADES,
            time_range
        )
        
        # Convert to DataFrames for easier manipulation
        orderbook_df = pd.DataFrame(orderbook_data)
        trades_df = pd.DataFrame(trades_data)
        
        # Generate features
        features = []
        
        # Group by market and time window
        for market_id in orderbook_df['market_id'].unique():
            market_orderbook = orderbook_df[orderbook_df['market_id'] == market_id]
            market_trades = trades_df[trades_df['market_id'] == market_id]
            
            # Calculate rolling features
            feature_record = {
                'market_id': market_id,
                'timestamp': datetime.utcnow(),
                'avg_spread': market_orderbook['bid_ask_spread'].mean(),
                'spread_volatility': market_orderbook['bid_ask_spread'].std(),
                'avg_depth': (market_orderbook['bid_depth'] + market_orderbook['ask_depth']).mean(),
                'order_imbalance_mean': market_orderbook['order_imbalance'].mean(),
                'price_volatility': market_trades['price'].std() if len(market_trades) > 0 else 0,
                'trade_frequency': len(market_trades) / ((time_range['end'] - time_range['start']).total_seconds() / 3600),
                'volume_profile': self._calculate_volume_profile(market_trades),
                'kyle_lambda': self._calculate_kyle_lambda(market_trades, market_orderbook),
                'amihud_illiquidity': self._calculate_amihud_illiquidity(market_trades)
            }
            
            features.append(feature_record)
        
        return features
    
    async def _generate_trader_behavior_features(self, time_range: Dict[str, datetime]) -> List[Dict[str, Any]]:
        """Generate trader behavior features"""
        # Read Silver trades and positions data
        trades_data = await self._read_silver_data(
            TradingDataType.TRADES,
            time_range
        )
        
        positions_data = await self._read_silver_data(
            TradingDataType.POSITIONS,
            time_range
        )
        
        trades_df = pd.DataFrame(trades_data)
        positions_df = pd.DataFrame(positions_data)
        
        features = []
        
        # Group by trader
        for trader_id in trades_df['trader_id'].unique():
            trader_trades = trades_df[trades_df['trader_id'] == trader_id]
            trader_positions = positions_df[positions_df['trader_id'] == trader_id]
            
            feature_record = {
                'trader_id': trader_id,
                'timestamp': datetime.utcnow(),
                'trade_frequency': len(trader_trades) / ((time_range['end'] - time_range['start']).days + 1),
                'avg_trade_size': trader_trades['value'].mean(),
                'trade_size_volatility': trader_trades['value'].std(),
                'win_rate': self._calculate_win_rate(trader_positions),
                'avg_holding_period': self._calculate_avg_holding_period(trader_positions),
                'profit_factor': self._calculate_profit_factor(trader_positions),
                'max_drawdown': self._calculate_max_drawdown(trader_positions),
                'sharpe_ratio': self._calculate_sharpe_ratio(trader_positions),
                'market_timing_score': self._calculate_market_timing_score(trader_trades),
                'strategy_consistency': self._calculate_strategy_consistency(trader_trades)
            }
            
            features.append(feature_record)
        
        return features
    
    async def _generate_risk_indicator_features(self, time_range: Dict[str, datetime]) -> List[Dict[str, Any]]:
        """Generate risk indicator features"""
        # This would integrate with the risk management system
        positions_data = await self._read_silver_data(
            TradingDataType.POSITIONS,
            time_range
        )
        
        risk_metrics_data = await self._read_silver_data(
            TradingDataType.RISK_METRICS,
            time_range
        )
        
        features = []
        
        # Calculate aggregate risk indicators
        positions_df = pd.DataFrame(positions_data)
        
        for market_id in positions_df['market_id'].unique():
            market_positions = positions_df[positions_df['market_id'] == market_id]
            
            feature_record = {
                'market_id': market_id,
                'timestamp': datetime.utcnow(),
                'concentration_risk': self._calculate_concentration_risk(market_positions),
                'directional_risk': self._calculate_directional_risk(market_positions),
                'leverage_ratio': self._calculate_market_leverage(market_positions),
                'liquidation_risk': self._calculate_liquidation_risk(market_positions),
                'correlation_risk': self._calculate_correlation_risk(market_positions),
                'var_95': self._calculate_var(market_positions, 0.95),
                'cvar_95': self._calculate_cvar(market_positions, 0.95),
                'stress_test_score': self._run_stress_test(market_positions)
            }
            
            features.append(feature_record)
        
        return features
    
    # Helper methods
    def _standardize_timestamp(self, timestamp: Union[str, int, float]) -> datetime:
        """Standardize timestamp to datetime object"""
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp)
        return timestamp
    
    def _calculate_slippage(self, trade: Dict[str, Any]) -> float:
        """Calculate trade slippage"""
        expected_price = trade.get('expected_price', trade['price'])
        actual_price = trade['price']
        return abs(actual_price - expected_price) / expected_price
    
    def _estimate_market_impact(self, trade: Dict[str, Any]) -> float:
        """Estimate market impact of trade"""
        # Simplified model - would use more sophisticated models in production
        trade_size = float(trade['quantity'])
        market_volume = float(trade.get('market_volume', 100000))
        return min(trade_size / market_volume * 0.1, 0.01)
    
    def _calculate_spread(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """Calculate bid-ask spread"""
        if not bids or not asks:
            return 0
        return asks[0][0] - bids[0][0]
    
    def _calculate_mid_price(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """Calculate mid price"""
        if not bids or not asks:
            return 0
        return (bids[0][0] + asks[0][0]) / 2
    
    def _calculate_depth(self, orders: List[List[float]], levels: int = 5) -> float:
        """Calculate order book depth"""
        depth = 0
        for i, (price, quantity) in enumerate(orders[:levels]):
            depth += float(price) * float(quantity)
        return depth
    
    def _calculate_imbalance(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """Calculate order imbalance"""
        bid_volume = sum(float(q) for _, q in bids[:5])
        ask_volume = sum(float(q) for _, q in asks[:5])
        total_volume = bid_volume + ask_volume
        if total_volume == 0:
            return 0
        return (bid_volume - ask_volume) / total_volume
    
    def _calculate_price_pressure(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """Calculate price pressure indicator"""
        if not bids or not asks:
            return 0
        
        # Weighted average price for top 5 levels
        bid_wavg = sum(float(p) * float(q) for p, q in bids[:5]) / sum(float(q) for _, q in bids[:5]) if bids else 0
        ask_wavg = sum(float(p) * float(q) for p, q in asks[:5]) / sum(float(q) for _, q in asks[:5]) if asks else 0
        
        mid_price = self._calculate_mid_price(bids, asks)
        if mid_price == 0:
            return 0
        
        return (bid_wavg - ask_wavg) / mid_price
    
    def _calculate_liquidity_score(self, bids: List[List[float]], asks: List[List[float]]) -> float:
        """Calculate liquidity score"""
        spread = self._calculate_spread(bids, asks)
        depth = self._calculate_depth(bids) + self._calculate_depth(asks)
        
        if spread == 0:
            return 100
        
        return min(depth / spread, 100)
    
    def _calculate_unrealized_pnl(self, position: Dict[str, Any]) -> float:
        """Calculate unrealized PnL"""
        entry_price = float(position['entry_price'])
        current_price = float(position.get('current_price', entry_price))
        quantity = float(position['quantity'])
        side = position['side'].upper()
        
        if side == 'LONG':
            return (current_price - entry_price) * quantity
        else:
            return (entry_price - current_price) * quantity
    
    def _calculate_position_age(self, position: Dict[str, Any]) -> float:
        """Calculate position age in hours"""
        entry_time = self._standardize_timestamp(position.get('entry_timestamp', position['timestamp']))
        current_time = datetime.utcnow()
        return (current_time - entry_time).total_seconds() / 3600
    
    def _calculate_return_percent(self, position: Dict[str, Any]) -> float:
        """Calculate return percentage"""
        entry_price = float(position['entry_price'])
        current_price = float(position.get('current_price', entry_price))
        
        if entry_price == 0:
            return 0
        
        return ((current_price - entry_price) / entry_price) * 100
    
    def _calculate_position_risk(self, position: Dict[str, Any]) -> float:
        """Calculate position risk score"""
        # Simplified risk calculation
        leverage = float(position.get('leverage', 1))
        position_size = float(position['quantity']) * float(position.get('current_price', position['entry_price']))
        volatility = float(position.get('market_volatility', 0.02))
        
        risk_score = min(leverage * volatility * (position_size / 100000), 1.0)
        return risk_score
    
    async def _stream_to_druid(self, events: List[Dict[str, Any]], event_type: TradingDataType):
        """Stream events to Druid for real-time analytics"""
        try:
            datasource = f"trading_{event_type.value}"
            await self.druid.ingest_batch(
                datasource=datasource,
                data=events,
                timestamp_column='timestamp'
            )
        except Exception as e:
            logger.warning(f"Failed to stream to Druid: {e}")
    
    async def _update_ingestion_metrics(self, event_type: TradingDataType, count: int):
        """Update ingestion metrics"""
        metrics_key = f"trading_ingestion_{event_type.value}"
        await self.cache.increment(metrics_key, count)
    
    async def _validate_data_quality(self, data: List[Dict[str, Any]], event_type: TradingDataType) -> TradingDataQuality:
        """Validate data quality"""
        if not data:
            return TradingDataQuality(
                completeness=0,
                accuracy=0,
                timeliness=0,
                consistency=0,
                issues=["No data"],
                timestamp=datetime.utcnow()
            )
        
        issues = []
        
        # Completeness check
        required_fields = self._get_required_fields(event_type)
        total_fields = len(required_fields) * len(data)
        missing_fields = 0
        
        for record in data:
            for field in required_fields:
                if field not in record or record[field] is None:
                    missing_fields += 1
        
        completeness = 1 - (missing_fields / total_fields) if total_fields > 0 else 0
        
        # Accuracy check (simplified)
        accuracy = 0.99  # Would implement actual validation logic
        
        # Timeliness check
        current_time = datetime.utcnow()
        delays = []
        for record in data:
            if 'timestamp' in record:
                record_time = self._standardize_timestamp(record['timestamp'])
                delay = (current_time - record_time).total_seconds()
                delays.append(delay)
        
        avg_delay = sum(delays) / len(delays) if delays else 0
        timeliness = max(0, 1 - (avg_delay / 3600))  # Penalize delays over 1 hour
        
        # Consistency check
        consistency = self._check_data_consistency(data, event_type)
        
        return TradingDataQuality(
            completeness=completeness,
            accuracy=accuracy,
            timeliness=timeliness,
            consistency=consistency,
            issues=issues,
            timestamp=datetime.utcnow()
        )
    
    def _get_required_fields(self, event_type: TradingDataType) -> List[str]:
        """Get required fields for event type"""
        required_fields = {
            TradingDataType.TRADES: ['trade_id', 'market_id', 'trader_id', 'price', 'quantity', 'timestamp'],
            TradingDataType.ORDER_BOOK: ['market_id', 'timestamp', 'bids', 'asks'],
            TradingDataType.POSITIONS: ['position_id', 'trader_id', 'market_id', 'quantity', 'entry_price'],
            TradingDataType.RISK_METRICS: ['trader_id', 'timestamp', 'risk_score']
        }
        return required_fields.get(event_type, [])
    
    def _check_data_consistency(self, data: List[Dict[str, Any]], event_type: TradingDataType) -> float:
        """Check data consistency"""
        # Simplified consistency check
        return 0.98
    
    def _meets_quality_thresholds(self, quality: TradingDataQuality) -> bool:
        """Check if quality meets thresholds"""
        return (
            quality.completeness >= self.quality_thresholds['completeness'] and
            quality.accuracy >= self.quality_thresholds['accuracy'] and
            quality.timeliness >= self.quality_thresholds['timeliness'] and
            quality.consistency >= self.quality_thresholds['consistency']
        )
    
    async def _send_to_remediation(self, data: List[Dict[str, Any]], quality: TradingDataQuality):
        """Send data to remediation queue"""
        logger.warning(f"Data quality failed, sending to remediation: {quality}")
        # Would implement actual remediation logic
    
    async def _read_silver_data(self, data_type: TradingDataType, time_range: Dict[str, datetime]) -> List[Dict[str, Any]]:
        """Read data from Silver layer"""
        # Would implement actual data reading logic
        return []
    
    async def _update_feature_store(self, feature_set: str, features: List[Dict[str, Any]]):
        """Update feature store with new features"""
        # Would implement actual feature store update
        pass
    
    # Additional calculation methods for features
    def _calculate_volume_profile(self, trades_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate volume profile"""
        if trades_df.empty:
            return {}
        
        # Volume by price level
        price_bins = pd.qcut(trades_df['price'], q=10, duplicates='drop')
        volume_profile = trades_df.groupby(price_bins)['quantity'].sum().to_dict()
        return {str(k): float(v) for k, v in volume_profile.items()}
    
    def _calculate_kyle_lambda(self, trades_df: pd.DataFrame, orderbook_df: pd.DataFrame) -> float:
        """Calculate Kyle's lambda (price impact coefficient)"""
        if trades_df.empty or orderbook_df.empty:
            return 0
        
        # Simplified Kyle's lambda calculation
        price_changes = trades_df['price'].diff().abs()
        volume_changes = trades_df['quantity'].diff().abs()
        
        valid_indices = ~(price_changes.isna() | volume_changes.isna() | (volume_changes == 0))
        if not valid_indices.any():
            return 0
        
        return (price_changes[valid_indices] / volume_changes[valid_indices]).mean()
    
    def _calculate_amihud_illiquidity(self, trades_df: pd.DataFrame) -> float:
        """Calculate Amihud illiquidity measure"""
        if trades_df.empty:
            return 0
        
        # Daily returns and volume
        daily_returns = trades_df.groupby(trades_df['timestamp'].dt.date)['price'].apply(
            lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0] if len(x) > 0 and x.iloc[0] != 0 else 0
        )
        daily_volume = trades_df.groupby(trades_df['timestamp'].dt.date)['value'].sum()
        
        # Amihud measure
        illiquidity = (abs(daily_returns) / daily_volume).mean()
        return float(illiquidity) if not pd.isna(illiquidity) else 0 