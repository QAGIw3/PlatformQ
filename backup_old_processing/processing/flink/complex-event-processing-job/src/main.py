"""
Flink Complex Event Processing (CEP) Job

Real-time pattern detection across multiple event streams:
- Fraud detection patterns
- Security threat detection
- System anomaly detection
- Business rule violations
- Behavioral pattern analysis
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
from pyignite import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventClassifier(ProcessFunction):
    """Classifies incoming events for CEP processing"""
    
    def process_element(self, event: Dict[str, Any], ctx: ProcessFunction.Context):
        """Classify and enrich events with metadata"""
        # Add processing timestamp
        event['_processing_timestamp'] = ctx.timestamp()
        
        # Extract entity information
        event['_entity_id'] = event.get('entity_id') or event.get('user_id') or event.get('asset_id', 'unknown')
        event['_entity_type'] = event.get('entity_type', 'unknown')
        
        # Add risk indicators
        event['_risk_indicators'] = self._extract_risk_indicators(event)
        
        yield event
    
    def _extract_risk_indicators(self, event: Dict[str, Any]) -> List[str]:
        """Extract risk indicators from event"""
        indicators = []
        
        # Check for high-value transactions
        if event.get('event_type') == 'TRANSACTION':
            amount = event.get('amount', 0)
            if amount > 10000:
                indicators.append('high_value')
            if amount > 100000:
                indicators.append('very_high_value')
        
        # Check for new accounts
        if event.get('account_age_days', float('inf')) < 7:
            indicators.append('new_account')
        
        # Check for rapid actions
        if event.get('action_rate_per_minute', 0) > 10:
            indicators.append('rapid_actions')
        
        return indicators


class FraudPatternLibrary:
    """Library of fraud detection patterns"""
    
    @staticmethod
    def velocity_check_pattern() -> Pattern:
        """Detect unusual velocity of transactions"""
        return Pattern.begin("first_transaction").where(
            lambda x: x.get('event_type') == 'TRANSACTION'
        ).times(10).within(Time.minutes(5))
    
    @staticmethod
    def account_takeover_pattern() -> Pattern:
        """Detect potential account takeover"""
        return Pattern.begin("login_failure").where(
            lambda x: x.get('event_type') == 'LOGIN_FAILED'
        ).times(3).consecutive().next("login_success").where(
            lambda x: x.get('event_type') == 'LOGIN_SUCCESS'
        ).next("unusual_action").where(
            lambda x: x.get('event_type') in ['PASSWORD_CHANGE', 'EMAIL_CHANGE', 'LARGE_WITHDRAWAL']
        ).within(Time.minutes(30))
    
    @staticmethod
    def money_laundering_pattern() -> Pattern:
        """Detect potential money laundering"""
        return Pattern.begin("deposit").where(
            lambda x: x.get('event_type') == 'DEPOSIT' and x.get('amount', 0) > 5000
        ).followed_by("split").where(
            lambda x: x.get('event_type') == 'TRANSFER' and x.get('amount', 0) < 1000
        ).times(5, 20).within(Time.hours(24))
    
    @staticmethod
    def pump_and_dump_pattern() -> Pattern:
        """Detect pump and dump schemes"""
        return Pattern.begin("promotion").where(
            lambda x: x.get('event_type') == 'ASSET_PROMOTED'
        ).followed_by("price_spike").where(
            lambda x: x.get('event_type') == 'PRICE_UPDATE' and x.get('price_change_percent', 0) > 50
        ).followed_by("large_sell").where(
            lambda x: x.get('event_type') == 'SELL_ORDER' and x.get('amount', 0) > 10000
        ).within(Time.hours(48))
    
    @staticmethod
    def sybil_attack_pattern() -> Pattern:
        """Detect Sybil attack patterns"""
        return Pattern.begin("account_creation").where(
            lambda x: x.get('event_type') == 'ACCOUNT_CREATED'
        ).times(5).followed_by("coordinated_action").where(
            lambda x: x.get('event_type') in ['VOTE', 'REVIEW', 'LIKE']
        ).times(5).within(Time.hours(1))


class SecurityPatternLibrary:
    """Library of security threat patterns"""
    
    @staticmethod
    def brute_force_pattern() -> Pattern:
        """Detect brute force attacks"""
        return Pattern.begin("failed_auth").where(
            lambda x: x.get('event_type') in ['LOGIN_FAILED', 'API_AUTH_FAILED']
        ).times(10).within(Time.minutes(5))
    
    @staticmethod
    def privilege_escalation_pattern() -> Pattern:
        """Detect privilege escalation attempts"""
        return Pattern.begin("normal_access").where(
            lambda x: x.get('event_type') == 'RESOURCE_ACCESS' and x.get('privilege_level') == 'user'
        ).followed_by("admin_attempt").where(
            lambda x: x.get('event_type') == 'RESOURCE_ACCESS' and x.get('privilege_level') == 'admin'
        ).followed_by("unauthorized").where(
            lambda x: x.get('event_type') == 'ACCESS_DENIED'
        ).times(3).within(Time.minutes(10))
    
    @staticmethod
    def data_exfiltration_pattern() -> Pattern:
        """Detect potential data exfiltration"""
        return Pattern.begin("bulk_access").where(
            lambda x: x.get('event_type') == 'DATA_ACCESS' and x.get('record_count', 0) > 1000
        ).times(5).followed_by("external_transfer").where(
            lambda x: x.get('event_type') == 'EXTERNAL_API_CALL'
        ).within(Time.minutes(30))


class SystemAnomalyPatternLibrary:
    """Library of system anomaly patterns"""
    
    @staticmethod
    def cascading_failure_pattern() -> Pattern:
        """Detect cascading system failures"""
        return Pattern.begin("first_error").where(
            lambda x: x.get('event_type') == 'SYSTEM_ERROR'
        ).followed_by("propagation").where(
            lambda x: x.get('event_type') == 'SYSTEM_ERROR' and x.get('related_service') is not None
        ).times(3, 10).within(Time.minutes(5))
    
    @staticmethod
    def resource_exhaustion_pattern() -> Pattern:
        """Detect resource exhaustion"""
        return Pattern.begin("high_usage").where(
            lambda x: x.get('event_type') == 'RESOURCE_METRIC' and x.get('usage_percent', 0) > 80
        ).times(5).consecutive().followed_by("critical").where(
            lambda x: x.get('event_type') == 'RESOURCE_METRIC' and x.get('usage_percent', 0) > 95
        ).within(Time.minutes(10))
    
    @staticmethod
    def latency_spike_pattern() -> Pattern:
        """Detect latency spikes"""
        return Pattern.begin("normal_latency").where(
            lambda x: x.get('event_type') == 'API_RESPONSE' and x.get('latency_ms', 0) < 100
        ).followed_by("spike").where(
            lambda x: x.get('event_type') == 'API_RESPONSE' and x.get('latency_ms', 0) > 1000
        ).times(10).within(Time.minutes(5))


class BusinessRulePatternLibrary:
    """Library of business rule violation patterns"""
    
    @staticmethod
    def compliance_violation_pattern() -> Pattern:
        """Detect compliance violations"""
        return Pattern.begin("high_risk_action").where(
            lambda x: x.get('event_type') in ['LARGE_TRANSACTION', 'CROSS_BORDER_TRANSFER']
        ).not_followed_by("compliance_check").where(
            lambda x: x.get('event_type') == 'COMPLIANCE_CHECK_COMPLETED'
        ).within(Time.minutes(30))
    
    @staticmethod
    def trading_manipulation_pattern() -> Pattern:
        """Detect trading manipulation"""
        return Pattern.begin("large_buy").where(
            lambda x: x.get('event_type') == 'BUY_ORDER' and x.get('amount', 0) > 50000
        ).followed_by("price_increase").where(
            lambda x: x.get('event_type') == 'PRICE_UPDATE' and x.get('price_change_percent', 0) > 10
        ).followed_by("large_sell").where(
            lambda x: x.get('event_type') == 'SELL_ORDER' and x.get('amount', 0) > 50000
        ).within(Time.hours(4))
    
    @staticmethod
    def duplicate_submission_pattern() -> Pattern:
        """Detect duplicate submissions"""
        return Pattern.begin("submission").where(
            lambda x: x.get('event_type') == 'FORM_SUBMITTED'
        ).followed_by("duplicate").where(
            lambda x: x.get('event_type') == 'FORM_SUBMITTED'
        ).times(2, 5).within(Time.minutes(1))


class PatternMatchProcessor(PatternSelectFunction):
    """Processes pattern matches and generates alerts"""
    
    def __init__(self, pattern_name: str, severity: str):
        self.pattern_name = pattern_name
        self.severity = severity
    
    def select(self, pattern_match: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Generate alert from pattern match"""
        # Get all events in the pattern
        all_events = []
        for event_list in pattern_match.values():
            all_events.extend(event_list)
        
        # Sort by timestamp
        all_events.sort(key=lambda x: x.get('timestamp', ''))
        
        # Extract relevant information
        entity_ids = list(set(e.get('_entity_id', 'unknown') for e in all_events))
        event_types = [e.get('event_type') for e in all_events]
        
        alert = {
            'alert_id': hashlib.md5(f"{self.pattern_name}_{all_events[0]['timestamp']}".encode()).hexdigest(),
            'pattern_name': self.pattern_name,
            'severity': self.severity,
            'timestamp': datetime.utcnow().isoformat(),
            'entity_ids': entity_ids,
            'event_count': len(all_events),
            'event_types': event_types,
            'first_event_time': all_events[0].get('timestamp'),
            'last_event_time': all_events[-1].get('timestamp'),
            'pattern_duration_seconds': self._calculate_duration(all_events),
            'risk_score': self._calculate_risk_score(all_events),
            'recommended_actions': self._get_recommended_actions(),
            'evidence': self._extract_evidence(all_events)
        }
        
        return alert
    
    def _calculate_duration(self, events: List[Dict[str, Any]]) -> float:
        """Calculate pattern duration in seconds"""
        if len(events) < 2:
            return 0
        
        first_time = datetime.fromisoformat(events[0]['timestamp'])
        last_time = datetime.fromisoformat(events[-1]['timestamp'])
        return (last_time - first_time).total_seconds()
    
    def _calculate_risk_score(self, events: List[Dict[str, Any]]) -> float:
        """Calculate risk score based on events"""
        base_score = {
            'CRITICAL': 1.0,
            'HIGH': 0.75,
            'MEDIUM': 0.5,
            'LOW': 0.25
        }.get(self.severity, 0.5)
        
        # Adjust based on event characteristics
        modifiers = []
        
        # Check for high-value transactions
        amounts = [e.get('amount', 0) for e in events if 'amount' in e]
        if amounts and max(amounts) > 100000:
            modifiers.append(0.2)
        
        # Check for new accounts
        if any('new_account' in e.get('_risk_indicators', []) for e in events):
            modifiers.append(0.15)
        
        # Check for rapid actions
        if len(events) > 10:
            modifiers.append(0.1)
        
        final_score = min(1.0, base_score + sum(modifiers))
        return round(final_score, 2)
    
    def _get_recommended_actions(self) -> List[str]:
        """Get recommended actions based on pattern"""
        actions_map = {
            'velocity_check': ['freeze_account', 'manual_review', 'notify_user'],
            'account_takeover': ['force_logout', 'reset_password', 'notify_user', 'block_ip'],
            'money_laundering': ['freeze_account', 'file_sar', 'compliance_review'],
            'pump_and_dump': ['halt_trading', 'investigate_promoters', 'notify_exchange'],
            'sybil_attack': ['block_accounts', 'invalidate_actions', 'ip_analysis'],
            'brute_force': ['block_ip', 'rate_limit', 'captcha_enable'],
            'data_exfiltration': ['revoke_access', 'audit_logs', 'notify_security']
        }
        
        return actions_map.get(self.pattern_name, ['manual_review'])
    
    def _extract_evidence(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract key evidence from events"""
        evidence = []
        
        for event in events[:5]:  # Limit to first 5 events
            evidence.append({
                'timestamp': event.get('timestamp'),
                'event_type': event.get('event_type'),
                'entity_id': event.get('_entity_id'),
                'key_fields': {
                    k: v for k, v in event.items() 
                    if k in ['amount', 'ip_address', 'user_agent', 'location']
                }
            })
        
        return evidence


class AlertEnrichmentProcessor(KeyedProcessFunction):
    """Enriches alerts with historical context and entity information"""
    
    def __init__(self, ignite_nodes: List[Tuple[str, int]]):
        self.ignite_nodes = ignite_nodes
        self.ignite_client = None
        self.alert_history = None
        self.entity_cache = None
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize state and connections"""
        # Initialize Ignite client
        self.ignite_client = Client()
        self.ignite_client.connect(self.ignite_nodes)
        
        # Get caches
        self.alert_history = self.ignite_client.get_or_create_cache('alert_history')
        self.entity_cache = self.ignite_client.get_or_create_cache('entity_profiles')
        
        # Initialize state
        self.alert_count_state = runtime_context.get_state(
            ValueStateDescriptor('alert_count', Types.INT())
        )
    
    def process_element(self, alert: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Enrich alert with context"""
        entity_id = alert['entity_ids'][0] if alert['entity_ids'] else 'unknown'
        
        # Get historical alert count
        current_count = self.alert_count_state.value() or 0
        current_count += 1
        self.alert_count_state.update(current_count)
        
        # Enrich with entity profile
        entity_profile = self.entity_cache.get(entity_id) or {}
        
        # Enrich with historical alerts
        recent_alerts = self._get_recent_alerts(entity_id)
        
        enriched_alert = {
            **alert,
            'entity_profile': {
                'trust_score': entity_profile.get('trust_score', 0.5),
                'account_age_days': entity_profile.get('account_age_days', 0),
                'total_transactions': entity_profile.get('total_transactions', 0),
                'risk_level': entity_profile.get('risk_level', 'MEDIUM')
            },
            'historical_context': {
                'total_alerts': current_count,
                'recent_alerts_24h': len(recent_alerts),
                'alert_trend': self._calculate_alert_trend(recent_alerts),
                'repeat_pattern': self._is_repeat_pattern(alert, recent_alerts)
            },
            'enrichment_timestamp': datetime.utcnow().isoformat()
        }
        
        # Store alert in history
        self.alert_history.put(
            f"{entity_id}_{alert['alert_id']}", 
            enriched_alert,
            ttl=86400 * 7  # 7 days TTL
        )
        
        yield enriched_alert
    
    def _get_recent_alerts(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get recent alerts for entity"""
        # In production, use SQL query on Ignite
        # For now, simplified implementation
        recent = []
        for i in range(10):  # Check last 10 alerts
            key = f"{entity_id}_recent_{i}"
            alert = self.alert_history.get(key)
            if alert:
                recent.append(alert)
        return recent
    
    def _calculate_alert_trend(self, recent_alerts: List[Dict[str, Any]]) -> str:
        """Calculate alert trend"""
        if len(recent_alerts) < 2:
            return 'STABLE'
        
        # Count alerts by hour
        hourly_counts = defaultdict(int)
        for alert in recent_alerts:
            hour = datetime.fromisoformat(alert['timestamp']).replace(minute=0, second=0)
            hourly_counts[hour] += 1
        
        # Calculate trend
        counts = list(hourly_counts.values())
        if len(counts) >= 3 and counts[-1] > counts[-2] > counts[-3]:
            return 'INCREASING'
        elif len(counts) >= 3 and counts[-1] < counts[-2] < counts[-3]:
            return 'DECREASING'
        else:
            return 'STABLE'
    
    def _is_repeat_pattern(self, current_alert: Dict[str, Any], recent_alerts: List[Dict[str, Any]]) -> bool:
        """Check if this is a repeat pattern"""
        pattern_name = current_alert['pattern_name']
        
        for alert in recent_alerts:
            if alert.get('pattern_name') == pattern_name:
                # Check if within 1 hour
                alert_time = datetime.fromisoformat(alert['timestamp'])
                current_time = datetime.fromisoformat(current_alert['timestamp'])
                if (current_time - alert_time).total_seconds() < 3600:
                    return True
        
        return False


class RealTimeAlertAggregator(ProcessFunction):
    """Aggregates alerts for dashboard and reporting"""
    
    def __init__(self):
        self.alert_buffer = defaultdict(list)
        self.stats = defaultdict(lambda: defaultdict(int))
    
    def process_element(self, alert: Dict[str, Any], ctx: ProcessFunction.Context):
        """Aggregate alerts for real-time metrics"""
        # Buffer alerts by severity
        severity = alert['severity']
        self.alert_buffer[severity].append(alert)
        
        # Update statistics
        self.stats['by_pattern'][alert['pattern_name']] += 1
        self.stats['by_severity'][severity] += 1
        self.stats['by_entity'][alert['entity_ids'][0]] += 1
        
        # Emit aggregated metrics every minute
        ctx.timer_service().register_processing_time_timer(
            ctx.timer_service().current_processing_time() + 60000
        )
    
    def on_timer(self, timestamp: int, ctx: ProcessFunction.OnTimerContext):
        """Emit aggregated metrics"""
        metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'window_end': datetime.fromtimestamp(timestamp / 1000).isoformat(),
            'alert_counts': {
                severity: len(alerts) 
                for severity, alerts in self.alert_buffer.items()
            },
            'pattern_distribution': dict(self.stats['by_pattern']),
            'top_entities': sorted(
                self.stats['by_entity'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10],
            'high_risk_alerts': [
                alert for alert in self.alert_buffer.get('CRITICAL', [])
                if alert['risk_score'] > 0.8
            ][:5]
        }
        
        yield metrics
        
        # Clear buffers
        self.alert_buffer.clear()
        self.stats.clear()


def create_cep_job():
    """Create and configure the Complex Event Processing job"""
    
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
    
    # Configure Pulsar source for multiple event streams
    event_source = FlinkKafkaConsumer(
        topics=[
            'user-activity-events',
            'transaction-events',
            'system-events',
            'security-events',
            'business-events',
            'api-events'
        ],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': PULSAR_SERVICE_URL,
            'group.id': 'cep-processor-group'
        }
    )
    
    # Create main event stream
    event_stream = env.add_source(event_source) \
        .map(lambda x: json.loads(x)) \
        .process(EventClassifier()) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(lambda x: int(datetime.fromisoformat(x['timestamp']).timestamp() * 1000))
        )
    
    # Key stream by entity ID
    keyed_stream = event_stream.key_by(lambda x: x['_entity_id'])
    
    # Apply fraud detection patterns
    fraud_patterns = [
        ('velocity_check', FraudPatternLibrary.velocity_check_pattern(), 'HIGH'),
        ('account_takeover', FraudPatternLibrary.account_takeover_pattern(), 'CRITICAL'),
        ('money_laundering', FraudPatternLibrary.money_laundering_pattern(), 'CRITICAL'),
        ('pump_and_dump', FraudPatternLibrary.pump_and_dump_pattern(), 'HIGH'),
        ('sybil_attack', FraudPatternLibrary.sybil_attack_pattern(), 'HIGH')
    ]
    
    fraud_alerts = []
    for pattern_name, pattern, severity in fraud_patterns:
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        fraud_alerts.append(pattern_stream)
    
    # Apply security patterns
    security_patterns = [
        ('brute_force', SecurityPatternLibrary.brute_force_pattern(), 'HIGH'),
        ('privilege_escalation', SecurityPatternLibrary.privilege_escalation_pattern(), 'CRITICAL'),
        ('data_exfiltration', SecurityPatternLibrary.data_exfiltration_pattern(), 'CRITICAL')
    ]
    
    security_alerts = []
    for pattern_name, pattern, severity in security_patterns:
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        security_alerts.append(pattern_stream)
    
    # Apply system anomaly patterns
    anomaly_patterns = [
        ('cascading_failure', SystemAnomalyPatternLibrary.cascading_failure_pattern(), 'HIGH'),
        ('resource_exhaustion', SystemAnomalyPatternLibrary.resource_exhaustion_pattern(), 'MEDIUM'),
        ('latency_spike', SystemAnomalyPatternLibrary.latency_spike_pattern(), 'MEDIUM')
    ]
    
    anomaly_alerts = []
    for pattern_name, pattern, severity in anomaly_patterns:
        pattern_stream = CEP.pattern(event_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        anomaly_alerts.append(pattern_stream)
    
    # Apply business rule patterns
    business_patterns = [
        ('compliance_violation', BusinessRulePatternLibrary.compliance_violation_pattern(), 'HIGH'),
        ('trading_manipulation', BusinessRulePatternLibrary.trading_manipulation_pattern(), 'CRITICAL'),
        ('duplicate_submission', BusinessRulePatternLibrary.duplicate_submission_pattern(), 'LOW')
    ]
    
    business_alerts = []
    for pattern_name, pattern, severity in business_patterns:
        pattern_stream = CEP.pattern(keyed_stream, pattern).select(
            PatternMatchProcessor(pattern_name, severity)
        )
        business_alerts.append(pattern_stream)
    
    # Union all alert streams
    all_alerts = fraud_alerts[0]
    for alert_stream in fraud_alerts[1:] + security_alerts + anomaly_alerts + business_alerts:
        all_alerts = all_alerts.union(alert_stream)
    
    # Enrich alerts
    enriched_alerts = all_alerts \
        .key_by(lambda x: x['entity_ids'][0] if x['entity_ids'] else 'unknown') \
        .process(AlertEnrichmentProcessor(IGNITE_NODES))
    
    # Real-time aggregation
    alert_metrics = enriched_alerts.process(RealTimeAlertAggregator())
    
    # Configure sinks
    alert_sink = FlinkKafkaProducer(
        topic='cep-alerts',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    critical_alert_sink = FlinkKafkaProducer(
        topic='critical-alerts',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    metrics_sink = FlinkKafkaProducer(
        topic='cep-metrics',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    # Write to sinks
    enriched_alerts.map(lambda x: json.dumps(x)).add_sink(alert_sink)
    
    # Critical alerts to separate topic
    enriched_alerts \
        .filter(lambda x: x['severity'] == 'CRITICAL') \
        .map(lambda x: json.dumps(x)) \
        .add_sink(critical_alert_sink)
    
    # Metrics to separate topic
    alert_metrics.map(lambda x: json.dumps(x)).add_sink(metrics_sink)
    
    # Execute job
    env.execute("Complex Event Processing - Real-time Pattern Detection")


if __name__ == "__main__":
    create_cep_job() 