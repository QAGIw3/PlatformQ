"""
Real-time Anomaly Detection using Apache Flink

Processes streaming market data to detect anomalies in real-time.
Uses statistical methods and ML models for anomaly detection.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction, ProcessFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.table import StreamTableEnvironment
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration, Time
import numpy as np
from typing import Iterable, Tuple
import json
import logging
from datetime import datetime
from decimal import Decimal


class MarketDataAnomalyDetector(ProcessFunction):
    """
    Detects anomalies in market data using multiple techniques:
    - Statistical outlier detection (Z-score, IQR)
    - Sudden spike/drop detection
    - Volume anomalies
    - Microstructure anomalies
    """
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.price_history = None
        self.volume_history = None
        self.spread_history = None
        
    def open(self, runtime_context):
        """Initialize state."""
        # State for historical data
        self.price_history = runtime_context.get_list_state(
            ListStateDescriptor("price_history", Types.DOUBLE())
        )
        self.volume_history = runtime_context.get_list_state(
            ListStateDescriptor("volume_history", Types.DOUBLE())
        )
        self.spread_history = runtime_context.get_list_state(
            ListStateDescriptor("spread_history", Types.DOUBLE())
        )
        
    def process_element(self, value, ctx, out):
        """Process each market data point for anomalies."""
        market_id = value['market_id']
        price = float(value['price'])
        volume = float(value['volume'])
        spread = float(value['spread'])
        timestamp = value['timestamp']
        
        # Get historical data
        price_hist = list(self.price_history.get() or [])
        volume_hist = list(self.volume_history.get() or [])
        spread_hist = list(self.spread_history.get() or [])
        
        # Detect anomalies if we have enough history
        anomalies = []
        
        if len(price_hist) >= 30:  # Need minimum history
            # Price anomaly detection
            price_anomaly = self._detect_price_anomaly(price, price_hist)
            if price_anomaly:
                anomalies.append(price_anomaly)
                
            # Volume anomaly detection
            volume_anomaly = self._detect_volume_anomaly(volume, volume_hist)
            if volume_anomaly:
                anomalies.append(volume_anomaly)
                
            # Spread anomaly detection
            spread_anomaly = self._detect_spread_anomaly(spread, spread_hist, price)
            if spread_anomaly:
                anomalies.append(spread_anomaly)
                
            # Microstructure anomaly
            micro_anomaly = self._detect_microstructure_anomaly(
                price, volume, spread, price_hist, volume_hist
            )
            if micro_anomaly:
                anomalies.append(micro_anomaly)
                
        # Update history
        price_hist.append(price)
        volume_hist.append(volume)
        spread_hist.append(spread)
        
        # Keep only recent history
        if len(price_hist) > self.window_size:
            price_hist = price_hist[-self.window_size:]
            volume_hist = volume_hist[-self.window_size:]
            spread_hist = spread_hist[-self.window_size:]
            
        # Update state
        self.price_history.clear()
        self.price_history.add_all(price_hist)
        self.volume_history.clear()
        self.volume_history.add_all(volume_hist)
        self.spread_history.clear()
        self.spread_history.add_all(spread_hist)
        
        # Output anomalies
        if anomalies:
            anomaly_event = {
                'market_id': market_id,
                'timestamp': timestamp,
                'price': price,
                'volume': volume,
                'spread': spread,
                'anomalies': anomalies,
                'severity': max(a['severity'] for a in anomalies),
                'detection_time': datetime.utcnow().isoformat()
            }
            out.collect(anomaly_event)
            
    def _detect_price_anomaly(self, price: float, history: list) -> dict:
        """Detect price anomalies using statistical methods."""
        prices = np.array(history)
        
        # Z-score method
        mean = np.mean(prices)
        std = np.std(prices)
        if std > 0:
            z_score = abs((price - mean) / std)
            
            if z_score > 3:  # 3 standard deviations
                return {
                    'type': 'price_spike',
                    'severity': min(z_score / 3, 2.0),  # Cap at 2.0
                    'z_score': z_score,
                    'expected_range': [mean - 3*std, mean + 3*std],
                    'actual_value': price
                }
                
        # Sudden change detection
        if len(history) >= 5:
            recent_mean = np.mean(prices[-5:])
            pct_change = abs((price - recent_mean) / recent_mean)
            
            if pct_change > 0.1:  # 10% sudden change
                return {
                    'type': 'sudden_price_change',
                    'severity': min(pct_change * 10, 2.0),
                    'percent_change': pct_change,
                    'recent_mean': recent_mean,
                    'actual_value': price
                }
                
        return None
        
    def _detect_volume_anomaly(self, volume: float, history: list) -> dict:
        """Detect volume anomalies."""
        volumes = np.array(history)
        
        # IQR method for volume
        q1 = np.percentile(volumes, 25)
        q3 = np.percentile(volumes, 75)
        iqr = q3 - q1
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        if volume < lower_bound or volume > upper_bound:
            # Calculate severity based on distance from bounds
            if volume < lower_bound:
                severity = min((lower_bound - volume) / (lower_bound + 1), 2.0)
                anomaly_type = 'unusually_low_volume'
            else:
                severity = min((volume - upper_bound) / (upper_bound + 1), 2.0)
                anomaly_type = 'unusually_high_volume'
                
            return {
                'type': anomaly_type,
                'severity': severity,
                'expected_range': [lower_bound, upper_bound],
                'actual_value': volume,
                'median_volume': np.median(volumes)
            }
            
        return None
        
    def _detect_spread_anomaly(self, spread: float, history: list, price: float) -> dict:
        """Detect spread anomalies relative to price."""
        spreads = np.array(history)
        
        # Spread as percentage of price
        spread_pct = spread / price if price > 0 else 0
        historical_pct = [s / price for s in spreads if price > 0]
        
        if historical_pct:
            mean_pct = np.mean(historical_pct)
            std_pct = np.std(historical_pct)
            
            if std_pct > 0:
                z_score = abs((spread_pct - mean_pct) / std_pct)
                
                if z_score > 2.5:  # More sensitive for spreads
                    return {
                        'type': 'abnormal_spread',
                        'severity': min(z_score / 2.5, 2.0),
                        'spread_percentage': spread_pct,
                        'normal_range_pct': [mean_pct - 2.5*std_pct, mean_pct + 2.5*std_pct],
                        'z_score': z_score
                    }
                    
        return None
        
    def _detect_microstructure_anomaly(self, price: float, volume: float, 
                                      spread: float, price_hist: list, 
                                      volume_hist: list) -> dict:
        """Detect anomalies in market microstructure."""
        if len(price_hist) < 10:
            return None
            
        # Calculate recent volatility
        recent_returns = np.diff(price_hist[-10:]) / price_hist[-10:-1]
        recent_vol = np.std(recent_returns)
        
        # Calculate volume-weighted average price movement
        recent_volumes = volume_hist[-10:]
        vwap_movement = np.average(np.diff(price_hist[-10:]), weights=recent_volumes[:-1])
        
        # Detect unusual price movement relative to volume
        if volume > np.mean(recent_volumes) * 2:  # High volume
            if abs(price - price_hist[-1]) < recent_vol * 0.1:  # But low price movement
                return {
                    'type': 'volume_price_divergence',
                    'severity': 1.5,
                    'description': 'High volume with minimal price movement',
                    'volume_ratio': volume / np.mean(recent_volumes),
                    'price_movement': abs(price - price_hist[-1]),
                    'expected_movement': recent_vol
                }
                
        # Detect potential spoofing patterns
        if spread > np.mean([abs(price_hist[i] - price_hist[i-1]) for i in range(-5, 0)]):
            if volume < np.mean(recent_volumes) * 0.5:  # Wide spread, low volume
                return {
                    'type': 'potential_spoofing',
                    'severity': 1.0,
                    'description': 'Wide spread with unusually low volume',
                    'spread_to_volatility_ratio': spread / (recent_vol + 0.0001),
                    'volume_percentile': np.percentile(recent_volumes, 
                        np.searchsorted(np.sort(recent_volumes), volume))
                }
                
        return None


class CrossMarketAnomalyDetector(ProcessWindowFunction):
    """
    Detects anomalies across multiple markets by analyzing correlations
    and divergences from normal patterns.
    """
    
    def process(self, key, context, elements):
        """Process window of market data across multiple markets."""
        market_data = list(elements)
        
        if len(market_data) < 2:
            return
            
        # Group by timestamp
        time_groups = {}
        for data in market_data:
            ts = data['timestamp']
            if ts not in time_groups:
                time_groups[ts] = []
            time_groups[ts].append(data)
            
        # Analyze each time point
        for ts, markets in time_groups.items():
            if len(markets) < 2:
                continue
                
            anomalies = self._detect_cross_market_anomalies(markets)
            
            if anomalies:
                yield {
                    'type': 'cross_market_anomaly',
                    'timestamp': ts,
                    'affected_markets': [m['market_id'] for m in markets],
                    'anomalies': anomalies,
                    'severity': max(a['severity'] for a in anomalies)
                }
                
    def _detect_cross_market_anomalies(self, markets: list) -> list:
        """Detect anomalies in cross-market relationships."""
        anomalies = []
        
        # Extract price returns
        returns = {}
        for market in markets:
            if 'previous_price' in market and market['previous_price'] > 0:
                returns[market['market_id']] = (
                    (market['price'] - market['previous_price']) / 
                    market['previous_price']
                )
                
        if len(returns) < 2:
            return anomalies
            
        # Check for correlation breaks
        market_ids = list(returns.keys())
        for i in range(len(market_ids)):
            for j in range(i + 1, len(market_ids)):
                m1, m2 = market_ids[i], market_ids[j]
                r1, r2 = returns[m1], returns[m2]
                
                # Simple correlation break detection
                # In practice, would use historical correlation
                if abs(r1) > 0.05 and abs(r2) > 0.05:  # Both moved significantly
                    if r1 * r2 < 0:  # But in opposite directions
                        anomalies.append({
                            'type': 'correlation_break',
                            'markets': [m1, m2],
                            'returns': {m1: r1, m2: r2},
                            'severity': min(abs(r1 - r2) * 10, 2.0)
                        })
                        
        # Check for contagion patterns
        high_movers = [m for m, r in returns.items() if abs(r) > 0.03]
        if len(high_movers) > len(returns) * 0.7:  # 70% of markets moving significantly
            anomalies.append({
                'type': 'potential_contagion',
                'affected_markets': high_movers,
                'severity': 1.8,
                'market_returns': returns
            })
            
        return anomalies


def create_anomaly_detection_job():
    """Create and configure the Flink anomaly detection job."""
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    # Configure checkpointing
    env.enable_checkpointing(60000)  # 1 minute
    
    # Create table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Define source - Pulsar market data stream
    market_data_source = FlinkKafkaConsumer(
        topics='market-data-stream',
        deserialization_schema=JsonRowDeserializationSchema.builder()
            .type_info(Types.ROW([
                Types.STRING(),  # market_id
                Types.DOUBLE(),  # price
                Types.DOUBLE(),  # volume
                Types.DOUBLE(),  # spread
                Types.STRING(),  # timestamp
            ])).build(),
        properties={
            'bootstrap.servers': 'pulsar://localhost:6650',
            'group.id': 'anomaly-detection-group'
        }
    )
    
    # Create data stream
    market_stream = env.add_source(market_data_source) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
                .with_timestamp_assigner(lambda x, t: int(x['timestamp']))
        )
    
    # Single market anomaly detection
    single_market_anomalies = market_stream \
        .key_by(lambda x: x['market_id']) \
        .process(MarketDataAnomalyDetector(window_size=100))
    
    # Cross-market anomaly detection
    cross_market_anomalies = market_stream \
        .key_by(lambda x: 1) \
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .process(CrossMarketAnomalyDetector())
    
    # Define sink - send anomalies to risk services
    anomaly_sink = FlinkKafkaProducer(
        topic='risk-anomalies',
        serialization_schema=JsonRowSerializationSchema.builder().build(),
        producer_config={
            'bootstrap.servers': 'pulsar://localhost:6650'
        }
    )
    
    # Output anomalies
    single_market_anomalies.add_sink(anomaly_sink)
    cross_market_anomalies.add_sink(anomaly_sink)
    
    # Execute job
    env.execute("Real-time Market Anomaly Detection")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_anomaly_detection_job() 