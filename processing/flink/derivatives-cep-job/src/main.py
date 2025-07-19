"""
Derivatives Trading CEP Job

Complex Event Processing for derivatives trading platform with:
- Fraud detection (wash trading, spoofing, layering)
- Anomaly detection (price/volume anomalies, liquidation cascades)
- Real-time compliance monitoring
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.cep import CEP, Pattern, PatternSelectFunction, PatternTimeoutFunction
from pyflink.datastream.functions import RuntimeContext
from pyflink.common.typeinfo import Types
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict
import hashlib
import numpy as np
from decimal import Decimal
from pyignite import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DerivativesFraudPatterns:
    """Fraud detection patterns for derivatives trading"""
    
    @staticmethod
    def wash_trading_pattern() -> Pattern:
        """
        Detect wash trading: Same user/entity buying and selling to themselves
        Pattern: Buy -> Sell within short time with minimal price change
        """
        return Pattern.begin("buy_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and x.get('side') == 'buy'
        ).followed_by("sell_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and x.get('side') == 'sell'
        ).within(Time.minutes(5))
    
    @staticmethod
    def spoofing_pattern() -> Pattern:
        """
        Detect spoofing: Large orders placed and quickly cancelled to manipulate price
        Pattern: Large order -> Multiple cancellations -> Small opposite order
        """
        return Pattern.begin("large_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and 
                     float(x.get('size', 0)) > float(x.get('avg_order_size', 0)) * 10
        ).followed_by("cancellations").where(
            lambda x: x.get('event_type') == 'ORDER_CANCELLED'
        ).times(3, 10).within(Time.seconds(30)).followed_by("opposite_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and 
                     x.get('side') != x.get('initial_side')
        ).within(Time.minutes(1))
    
    @staticmethod
    def layering_pattern() -> Pattern:
        """
        Detect layering: Multiple orders at different price levels to create false depth
        Pattern: Multiple orders at increasing prices within short time
        """
        return Pattern.begin("first_layer").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED'
        ).followed_by("subsequent_layers").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and 
                     x.get('user_id') == x.get('first_user_id')
        ).times(4, 20).within(Time.seconds(10))
    
    @staticmethod
    def front_running_pattern() -> Pattern:
        """
        Detect potential front-running based on order patterns
        Pattern: Small order -> Large order (different user) -> Small opposite order
        """
        return Pattern.begin("scout_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and
                     float(x.get('size', 0)) < float(x.get('avg_order_size', 0)) * 0.1
        ).followed_by("victim_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and
                     float(x.get('size', 0)) > float(x.get('avg_order_size', 0)) * 5 and
                     x.get('user_id') != x.get('scout_user_id')
        ).within(Time.seconds(5)).followed_by("exit_order").where(
            lambda x: x.get('event_type') == 'ORDER_PLACED' and
                     x.get('side') != x.get('scout_side') and
                     x.get('user_id') == x.get('scout_user_id')
        ).within(Time.seconds(30))


class DerivativesAnomalyPatterns:
    """Anomaly detection patterns for derivatives trading"""
    
    @staticmethod
    def price_spike_pattern() -> Pattern:
        """
        Detect unusual price spikes
        Pattern: Normal price -> Sudden spike (>5% in 1 minute)
        """
        return Pattern.begin("normal_price").where(
            lambda x: x.get('event_type') == 'PRICE_UPDATE'
        ).followed_by("spike").where(
            lambda x: x.get('event_type') == 'PRICE_UPDATE' and
                     abs(float(x.get('price', 0)) - float(x.get('prev_price', 0))) / 
                     float(x.get('prev_price', 1)) > 0.05
        ).within(Time.minutes(1))
    
    @staticmethod
    def volume_anomaly_pattern() -> Pattern:
        """
        Detect unusual volume spikes
        Pattern: Volume > 10x average in single period
        """
        return Pattern.begin("volume_spike").where(
            lambda x: x.get('event_type') == 'VOLUME_UPDATE' and
                     float(x.get('volume', 0)) > float(x.get('avg_volume', 1)) * 10
        )
    
    @staticmethod
    def liquidation_cascade_pattern() -> Pattern:
        """
        Detect liquidation cascades
        Pattern: Multiple liquidations in short time
        """
        return Pattern.begin("first_liquidation").where(
            lambda x: x.get('event_type') == 'POSITION_LIQUIDATED'
        ).followed_by("cascade").where(
            lambda x: x.get('event_type') == 'POSITION_LIQUIDATED'
        ).times(3, 100).within(Time.minutes(5))
    
    @staticmethod
    def funding_rate_anomaly_pattern() -> Pattern:
        """
        Detect unusual funding rate changes
        Pattern: Funding rate changes by more than 0.1% in single update
        """
        return Pattern.begin("funding_spike").where(
            lambda x: x.get('event_type') == 'FUNDING_RATE_UPDATE' and
                     abs(float(x.get('new_rate', 0)) - float(x.get('old_rate', 0))) > 0.001
        )


class DerivativesCompliancePatterns:
    """Compliance monitoring patterns"""
    
    @staticmethod
    def position_limit_pattern() -> Pattern:
        """
        Detect position limit violations
        Pattern: Position size exceeds configured limit
        """
        return Pattern.begin("limit_breach").where(
            lambda x: x.get('event_type') == 'POSITION_UPDATE' and
                     float(x.get('position_size', 0)) > float(x.get('position_limit', float('inf')))
        )
    
    @staticmethod
    def concentration_risk_pattern() -> Pattern:
        """
        Detect concentration risk
        Pattern: Single entity controls >25% of market
        """
        return Pattern.begin("concentration").where(
            lambda x: x.get('event_type') == 'MARKET_SHARE_UPDATE' and
                     float(x.get('market_share', 0)) > 0.25
        )
    
    @staticmethod
    def rapid_position_change_pattern() -> Pattern:
        """
        Detect rapid position changes that might indicate risk
        Pattern: Position changes by >50% within 1 hour
        """
        return Pattern.begin("position_start").where(
            lambda x: x.get('event_type') == 'POSITION_UPDATE'
        ).followed_by("rapid_change").where(
            lambda x: x.get('event_type') == 'POSITION_UPDATE' and
                     abs(float(x.get('new_size', 0)) - float(x.get('old_size', 1))) / 
                     float(x.get('old_size', 1)) > 0.5
        ).within(Time.hours(1))


class PatternMatchProcessor(PatternSelectFunction):
    """Process pattern matches and generate alerts"""
    
    def __init__(self, pattern_name: str, severity: str):
        self.pattern_name = pattern_name
        self.severity = severity
        
    def select(self, pattern: Dict[str, List[Any]]) -> Dict[str, Any]:
        # Extract relevant events
        events = []
        for key, event_list in pattern.items():
            events.extend(event_list)
        
        # Calculate risk score based on pattern
        risk_score = self._calculate_risk_score(events)
        
        # Generate alert
        alert = {
            'alert_id': f"alert_{datetime.utcnow().timestamp()}",
            'pattern_name': self.pattern_name,
            'severity': self.severity,
            'risk_score': risk_score,
            'timestamp': datetime.utcnow().isoformat(),
            'events': [self._sanitize_event(e) for e in events[:10]],  # Include first 10 events
            'event_count': len(events),
            'recommended_actions': self._get_recommended_actions()
        }
        
        # Add pattern-specific details
        if self.pattern_name == 'wash_trading':
            alert['user_id'] = events[0].get('user_id')
            alert['market_id'] = events[0].get('market_id')
            alert['total_volume'] = sum(float(e.get('size', 0)) for e in events)
            
        elif self.pattern_name == 'liquidation_cascade':
            alert['liquidation_count'] = len(events)
            alert['total_liquidated_value'] = sum(float(e.get('liquidation_value', 0)) for e in events)
            alert['affected_markets'] = list(set(e.get('market_id') for e in events))
            
        return alert
    
    def _calculate_risk_score(self, events: List[Dict]) -> float:
        """Calculate risk score based on pattern and events"""
        base_scores = {
            'wash_trading': 0.8,
            'spoofing': 0.9,
            'layering': 0.85,
            'front_running': 0.95,
            'price_spike': 0.7,
            'volume_anomaly': 0.6,
            'liquidation_cascade': 0.9,
            'position_limit': 0.8,
            'concentration_risk': 0.85
        }
        
        base_score = base_scores.get(self.pattern_name, 0.5)
        
        # Adjust based on event count
        if len(events) > 10:
            base_score = min(1.0, base_score + 0.1)
            
        return round(base_score, 2)
    
    def _get_recommended_actions(self) -> List[str]:
        """Get recommended actions for pattern"""
        actions = {
            'wash_trading': ['freeze_account', 'manual_review', 'report_to_compliance'],
            'spoofing': ['cancel_orders', 'temporary_trading_ban', 'increase_monitoring'],
            'layering': ['cancel_orders', 'warning_notice', 'increase_fees'],
            'liquidation_cascade': ['halt_trading', 'increase_margin_requirements', 'activate_circuit_breakers'],
            'position_limit': ['prevent_new_orders', 'force_position_reduction', 'notify_risk_team'],
            'price_spike': ['temporary_halt', 'widen_price_bands', 'alert_market_makers']
        }
        
        return actions.get(self.pattern_name, ['manual_review'])
    
    def _sanitize_event(self, event: Dict) -> Dict:
        """Remove sensitive data from event"""
        sanitized = event.copy()
        # Remove sensitive fields
        for field in ['password', 'api_key', 'secret']:
            sanitized.pop(field, None)
        return sanitized


class RiskAggregationProcessor(ProcessFunction):
    """Aggregate risk metrics across patterns"""
    
    def __init__(self, ignite_nodes: List[Tuple[str, int]]):
        self.ignite_nodes = ignite_nodes
        self.risk_scores = defaultdict(lambda: {'total': 0, 'count': 0})
        
    def process_element(self, alert: Dict, ctx: ProcessFunction.Context):
        # Update risk scores
        entity_id = alert.get('user_id') or alert.get('market_id', 'unknown')
        self.risk_scores[entity_id]['total'] += alert['risk_score']
        self.risk_scores[entity_id]['count'] += 1
        
        # Calculate average risk
        avg_risk = self.risk_scores[entity_id]['total'] / self.risk_scores[entity_id]['count']
        
        # Emit aggregated risk event if threshold exceeded
        if avg_risk > 0.7:
            risk_event = {
                'entity_id': entity_id,
                'risk_level': 'HIGH' if avg_risk > 0.8 else 'MEDIUM',
                'average_risk_score': avg_risk,
                'alert_count': self.risk_scores[entity_id]['count'],
                'timestamp': datetime.utcnow().isoformat(),
                'recent_patterns': []  # Would include recent pattern matches
            }
            
            yield risk_event


def create_derivatives_cep_job():
    """Create and configure the Derivatives CEP job"""
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Enable checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    
    # Configuration
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    IGNITE_NODES = [
        ('ignite-0.ignite', 10800),
        ('ignite-1.ignite', 10800),
        ('ignite-2.ignite', 10800)
    ]
    
    # Configure Pulsar source for trading events
    event_source = FlinkKafkaConsumer(
        topics=[
            'trading-events',
            'order-events',
            'position-events',
            'market-data-events',
            'liquidation-events',
            'compliance-events'
        ],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': PULSAR_SERVICE_URL,
            'group.id': 'derivatives-cep-processor'
        }
    )
    
    # Create main event stream
    event_stream = env.add_source(event_source) \
        .map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(lambda x: int(datetime.fromisoformat(x['timestamp']).timestamp() * 1000))
        )
    
    # Apply fraud detection patterns
    fraud_patterns = [
        ('wash_trading', DerivativesFraudPatterns.wash_trading_pattern(), 'HIGH'),
        ('spoofing', DerivativesFraudPatterns.spoofing_pattern(), 'CRITICAL'),
        ('layering', DerivativesFraudPatterns.layering_pattern(), 'HIGH'),
        ('front_running', DerivativesFraudPatterns.front_running_pattern(), 'CRITICAL')
    ]
    
    fraud_alerts = []
    for pattern_name, pattern, severity in fraud_patterns:
        # Key by user for fraud patterns
        keyed_stream = event_stream.key_by(lambda x: x.get('user_id', 'unknown'))
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        fraud_alerts.append(pattern_stream)
    
    # Apply anomaly detection patterns
    anomaly_patterns = [
        ('price_spike', DerivativesAnomalyPatterns.price_spike_pattern(), 'MEDIUM'),
        ('volume_anomaly', DerivativesAnomalyPatterns.volume_anomaly_pattern(), 'MEDIUM'),
        ('liquidation_cascade', DerivativesAnomalyPatterns.liquidation_cascade_pattern(), 'CRITICAL'),
        ('funding_rate_anomaly', DerivativesAnomalyPatterns.funding_rate_anomaly_pattern(), 'LOW')
    ]
    
    anomaly_alerts = []
    for pattern_name, pattern, severity in anomaly_patterns:
        # Key by market for anomaly patterns
        keyed_stream = event_stream.key_by(lambda x: x.get('market_id', 'unknown'))
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        anomaly_alerts.append(pattern_stream)
    
    # Apply compliance patterns
    compliance_patterns = [
        ('position_limit', DerivativesCompliancePatterns.position_limit_pattern(), 'HIGH'),
        ('concentration_risk', DerivativesCompliancePatterns.concentration_risk_pattern(), 'HIGH'),
        ('rapid_position_change', DerivativesCompliancePatterns.rapid_position_change_pattern(), 'MEDIUM')
    ]
    
    compliance_alerts = []
    for pattern_name, pattern, severity in compliance_patterns:
        # Key by user for compliance patterns
        keyed_stream = event_stream.key_by(lambda x: x.get('user_id', 'unknown'))
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        compliance_alerts.append(pattern_stream)
    
    # Union all alert streams
    all_alerts = fraud_alerts[0]
    for alert_stream in fraud_alerts[1:] + anomaly_alerts + compliance_alerts:
        all_alerts = all_alerts.union(alert_stream)
    
    # Aggregate risk metrics
    risk_aggregation = all_alerts.process(RiskAggregationProcessor(IGNITE_NODES))
    
    # Configure sinks
    alert_sink = FlinkKafkaProducer(
        topic='derivatives-cep-alerts',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    critical_alert_sink = FlinkKafkaProducer(
        topic='critical-derivatives-alerts',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    risk_sink = FlinkKafkaProducer(
        topic='aggregated-risk-scores',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    # Write to sinks
    all_alerts.map(lambda x: json.dumps(x)).add_sink(alert_sink)
    
    # Filter and send critical alerts
    all_alerts.filter(lambda x: x['severity'] == 'CRITICAL') \
        .map(lambda x: json.dumps(x)) \
        .add_sink(critical_alert_sink)
    
    # Send aggregated risk scores
    risk_aggregation.map(lambda x: json.dumps(x)).add_sink(risk_sink)
    
    # Execute job
    env.execute("Derivatives Trading CEP - Pattern Detection")


if __name__ == "__main__":
    create_derivatives_cep_job() 