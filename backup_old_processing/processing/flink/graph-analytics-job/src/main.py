"""
Real-time Graph Analytics Flink Job

This job processes event streams to perform real-time graph analytics including:
- Trust score updates
- Fraud detection
- Influence propagation
- Community evolution tracking
- Real-time recommendations
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.cep import CEP, Pattern, PatternSelectFunction
from pyflink.datastream.functions import RuntimeContext
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict, deque
import hashlib
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P
from pyignite import Client
from pyignite.datatypes import String, DoubleObject, IntObject, BoolObject

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GraphUpdateEvent:
    """Container for graph update events"""
    def __init__(self, event_data: Dict[str, Any]):
        self.event_type = event_data.get('event_type')
        self.entity_id = event_data.get('entity_id')
        self.entity_type = event_data.get('entity_type')
        self.tenant_id = event_data.get('tenant_id')
        self.timestamp = event_data.get('timestamp')
        self.properties = event_data.get('properties', {})
        self.relationships = event_data.get('relationships', [])


class TrustScoreProcessor(KeyedProcessFunction):
    """Process trust-affecting events and update trust scores in real-time"""
    
    def __init__(self, janusgraph_url: str, ignite_nodes: List[Tuple[str, int]]):
        self.janusgraph_url = janusgraph_url
        self.ignite_nodes = ignite_nodes
        self.gremlin_client = None
        self.ignite_client = None
        self.trust_state = None
        self.activity_window = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize connections and state"""
        # Connect to JanusGraph
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        # Connect to Ignite
        self.ignite_client = Client()
        self.ignite_client.connect(self.ignite_nodes)
        
        # Initialize state descriptors
        self.trust_state = runtime_context.get_state(
            ValueStateDescriptor(
                "trust_scores",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT())
            )
        )
        
        self.activity_window = runtime_context.get_list_state(
            ListStateDescriptor(
                "activity_window",
                type_info=DataTypes.ROW([
                    DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
                    DataTypes.FIELD("event_type", DataTypes.STRING()),
                    DataTypes.FIELD("trust_impact", DataTypes.FLOAT())
                ])
            )
        )
    
    def close(self):
        """Clean up connections"""
        if self.gremlin_client:
            self.gremlin_client.close()
        if self.ignite_client:
            self.ignite_client.close()
    
    def process_element(self, event: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Process trust-affecting events"""
        try:
            entity_id = event['entity_id']
            event_type = event['event_type']
            
            # Calculate trust impact based on event type
            trust_impact = self._calculate_trust_impact(event)
            
            # Update local trust state
            current_scores = self.trust_state.value() or {}
            old_score = current_scores.get(entity_id, 0.5)  # Default trust: 0.5
            
            # Apply trust update with decay
            new_score = self._update_trust_score(old_score, trust_impact, event)
            current_scores[entity_id] = new_score
            self.trust_state.update(current_scores)
            
            # Update activity window
            activity_entry = {
                "timestamp": ctx.timestamp(),
                "event_type": event_type,
                "trust_impact": trust_impact
            }
            self.activity_window.add(activity_entry)
            
            # Update graph database
            self._update_graph_trust_score(entity_id, new_score, event)
            
            # Cache in Ignite for fast access
            cache_key = f"trust_score:{entity_id}"
            self.ignite_client.put_all({
                cache_key: {
                    "score": new_score,
                    "last_updated": datetime.utcnow().isoformat(),
                    "trend": "increasing" if new_score > old_score else "decreasing"
                }
            })
            
            # Emit trust update event
            output_event = {
                "entity_id": entity_id,
                "entity_type": event.get('entity_type'),
                "old_trust_score": old_score,
                "new_trust_score": new_score,
                "trust_delta": new_score - old_score,
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            yield output_event
            
            # Set timer for trust decay
            ctx.timer_service().register_processing_time_timer(
                ctx.timestamp() + 86400000  # 24 hours
            )
            
        except Exception as e:
            logger.error(f"Error processing trust update: {e}")
    
    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """Apply trust decay over time"""
        current_scores = self.trust_state.value() or {}
        decay_factor = 0.99  # 1% decay per day
        
        for entity_id, score in current_scores.items():
            # Move score towards neutral (0.5)
            decayed_score = 0.5 + (score - 0.5) * decay_factor
            current_scores[entity_id] = decayed_score
            
            # Update in graph
            self._update_graph_trust_score(entity_id, decayed_score, {"reason": "time_decay"})
        
        self.trust_state.update(current_scores)
    
    def _calculate_trust_impact(self, event: Dict[str, Any]) -> float:
        """Calculate trust impact based on event type and properties"""
        event_type = event['event_type']
        
        # Define trust impacts for different events
        trust_impacts = {
            'ASSET_CREATED': 0.05,
            'ASSET_VERIFIED': 0.1,
            'POSITIVE_REVIEW': 0.08,
            'NEGATIVE_REVIEW': -0.15,
            'COLLABORATION_COMPLETED': 0.06,
            'DISPUTE_RAISED': -0.1,
            'DISPUTE_RESOLVED': 0.03,
            'FRAUD_DETECTED': -0.5,
            'VERIFICATION_FAILED': -0.2
        }
        
        base_impact = trust_impacts.get(event_type, 0.0)
        
        # Adjust based on event properties
        if 'severity' in event:
            base_impact *= (1 + event['severity'] * 0.5)
        
        if 'confidence' in event:
            base_impact *= event['confidence']
        
        return max(-1.0, min(1.0, base_impact))  # Clamp to [-1, 1]
    
    def _update_trust_score(self, current_score: float, impact: float, 
                            event: Dict[str, Any]) -> float:
        """Update trust score with bounded growth"""
        # Use sigmoid-like function to prevent extreme values
        alpha = 0.1  # Learning rate
        
        # Calculate new score
        if impact > 0:
            # Positive impact: diminishing returns as score approaches 1
            new_score = current_score + alpha * impact * (1 - current_score)
        else:
            # Negative impact: accelerating decline as score approaches 0
            new_score = current_score + alpha * impact * current_score
        
        # Apply bounds
        return max(0.0, min(1.0, new_score))
    
    def _update_graph_trust_score(self, entity_id: str, trust_score: float, 
                                  metadata: Dict[str, Any]):
        """Update trust score in JanusGraph"""
        try:
            query = f"""
                g.V().has('entity_id', '{entity_id}')
                .property('trust_score', {trust_score})
                .property('trust_updated_at', '{datetime.utcnow().isoformat()}')
                .property('trust_update_reason', '{metadata.get('reason', 'event')}')
            """
            self.gremlin_client.submit(query).all().result()
        except Exception as e:
            logger.error(f"Failed to update graph trust score: {e}")


class FraudDetectionProcessor(ProcessFunction):
    """Real-time fraud detection using graph patterns"""
    
    def __init__(self, janusgraph_url: str):
        self.janusgraph_url = janusgraph_url
        self.gremlin_client = None
        self.pattern_buffer = defaultdict(list)
        self.suspicious_entities = set()
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize connections"""
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
    
    def close(self):
        """Clean up connections"""
        if self.gremlin_client:
            self.gremlin_client.close()
    
    def process_element(self, event: Dict[str, Any], ctx: ProcessFunction.Context):
        """Process events for fraud patterns"""
        entity_id = event['entity_id']
        event_type = event['event_type']
        
        # Buffer events by entity
        self.pattern_buffer[entity_id].append({
            'timestamp': ctx.timestamp(),
            'event': event
        })
        
        # Keep only recent events (last hour)
        cutoff_time = ctx.timestamp() - 3600000
        self.pattern_buffer[entity_id] = [
            e for e in self.pattern_buffer[entity_id] 
            if e['timestamp'] > cutoff_time
        ]
        
        # Check for suspicious patterns
        fraud_indicators = self._check_fraud_patterns(entity_id)
        
        if fraud_indicators:
            # Query graph for additional context
            graph_context = self._get_graph_context(entity_id)
            
            # Calculate fraud score
            fraud_score = self._calculate_fraud_score(
                fraud_indicators, 
                graph_context
            )
            
            if fraud_score > 0.7:  # High risk threshold
                self.suspicious_entities.add(entity_id)
                
                # Emit fraud alert
                fraud_alert = {
                    'alert_type': 'FRAUD_DETECTION',
                    'entity_id': entity_id,
                    'fraud_score': fraud_score,
                    'indicators': fraud_indicators,
                    'graph_context': graph_context,
                    'timestamp': datetime.utcnow().isoformat(),
                    'severity': 'HIGH' if fraud_score > 0.9 else 'MEDIUM'
                }
                
                yield fraud_alert
                
                # Update entity in graph
                self._mark_entity_suspicious(entity_id, fraud_score)
    
    def _check_fraud_patterns(self, entity_id: str) -> List[str]:
        """Check for known fraud patterns"""
        indicators = []
        events = self.pattern_buffer[entity_id]
        
        if not events:
            return indicators
        
        # Pattern 1: Rapid activity burst
        if len(events) > 20:  # More than 20 events in an hour
            indicators.append('rapid_activity_burst')
        
        # Pattern 2: Repetitive actions
        event_types = [e['event']['event_type'] for e in events]
        type_counts = defaultdict(int)
        for et in event_types:
            type_counts[et] += 1
        
        for event_type, count in type_counts.items():
            if count > 10:  # Same action more than 10 times
                indicators.append(f'repetitive_{event_type.lower()}')
        
        # Pattern 3: Suspicious timing
        timestamps = [e['timestamp'] for e in events]
        if len(timestamps) > 5:
            intervals = [timestamps[i+1] - timestamps[i] 
                         for i in range(len(timestamps)-1)]
            avg_interval = np.mean(intervals)
            std_interval = np.std(intervals)
            
            if std_interval < 1000:  # Very regular intervals (bot-like)
                indicators.append('automated_behavior')
        
        return indicators
    
    def _get_graph_context(self, entity_id: str) -> Dict[str, Any]:
        """Get graph context for fraud analysis"""
        try:
            # Get entity properties and relationships
            query = f"""
                g.V().has('entity_id', '{entity_id}').as('entity')
                .project('properties', 'inDegree', 'outDegree', 'created_at')
                .by(valueMap())
                .by(inE().count())
                .by(outE().count())
                .by(values('created_at'))
            """
            result = self.gremlin_client.submit(query).all().result()
            
            if result:
                context = result[0]
                
                # Check account age
                if 'created_at' in context:
                    created_at = datetime.fromisoformat(context['created_at'])
                    account_age_days = (datetime.utcnow() - created_at).days
                    context['account_age_days'] = account_age_days
                    context['is_new_account'] = account_age_days < 7
                
                return context
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get graph context: {e}")
            return {}
    
    def _calculate_fraud_score(self, indicators: List[str], 
                               context: Dict[str, Any]) -> float:
        """Calculate comprehensive fraud score"""
        score = 0.0
        
        # Score based on indicators
        indicator_weights = {
            'rapid_activity_burst': 0.3,
            'automated_behavior': 0.4,
            'repetitive_': 0.2  # Prefix match
        }
        
        for indicator in indicators:
            for pattern, weight in indicator_weights.items():
                if indicator.startswith(pattern):
                    score += weight
                    break
        
        # Adjust based on graph context
        if context.get('is_new_account', False):
            score *= 1.5  # New accounts are higher risk
        
        # Low connectivity is suspicious
        in_degree = context.get('inDegree', 0)
        out_degree = context.get('outDegree', 0)
        if in_degree + out_degree < 3:
            score += 0.2
        
        return min(1.0, score)  # Cap at 1.0
    
    def _mark_entity_suspicious(self, entity_id: str, fraud_score: float):
        """Mark entity as suspicious in graph"""
        try:
            query = f"""
                g.V().has('entity_id', '{entity_id}')
                .property('is_suspicious', true)
                .property('fraud_score', {fraud_score})
                .property('fraud_detected_at', '{datetime.utcnow().isoformat()}')
            """
            self.gremlin_client.submit(query).all().result()
        except Exception as e:
            logger.error(f"Failed to mark entity suspicious: {e}")


class GraphRecommendationProcessor(KeyedProcessFunction):
    """Generate real-time recommendations based on graph analysis"""
    
    def __init__(self, janusgraph_url: str, ignite_nodes: List[Tuple[str, int]]):
        self.janusgraph_url = janusgraph_url
        self.ignite_nodes = ignite_nodes
        self.gremlin_client = None
        self.ignite_client = None
        self.user_context = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize connections and state"""
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        self.ignite_client = Client()
        self.ignite_client.connect(self.ignite_nodes)
        
        self.user_context = runtime_context.get_state(
            ValueStateDescriptor(
                "user_context",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING()))
            )
        )
    
    def process_element(self, event: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Generate recommendations based on user activity"""
        user_id = ctx.get_current_key()
        event_type = event['event_type']
        
        # Update user context
        context = self.user_context.value() or {}
        if 'recent_items' not in context:
            context['recent_items'] = []
        
        if 'entity_id' in event:
            context['recent_items'].append(event['entity_id'])
            context['recent_items'] = context['recent_items'][-10:]  # Keep last 10
        
        self.user_context.update(context)
        
        # Generate recommendations for specific events
        if event_type in ['ASSET_VIEWED', 'PROJECT_JOINED', 'USER_FOLLOWED']:
            recommendations = self._generate_recommendations(user_id, event, context)
            
            if recommendations:
                # Cache recommendations
                cache_key = f"recommendations:{user_id}"
                self.ignite_client.put_all({
                    cache_key: {
                        "recommendations": recommendations,
                        "generated_at": datetime.utcnow().isoformat(),
                        "context": event_type
                    }
                })
                
                # Emit recommendation event
                yield {
                    "user_id": user_id,
                    "recommendations": recommendations,
                    "recommendation_type": self._get_recommendation_type(event_type),
                    "timestamp": datetime.utcnow().isoformat()
                }
    
    def _generate_recommendations(self, user_id: str, event: Dict[str, Any], 
                                  context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate personalized recommendations using graph traversal"""
        try:
            recommendations = []
            
            # Collaborative filtering: Find similar users
            similar_users_query = f"""
                g.V().has('entity_id', '{user_id}').as('user')
                .out('INTERACTED_WITH').aggregate('user_items')
                .in('INTERACTED_WITH').where(neq('user'))
                .group().by().by(out('INTERACTED_WITH').where(without('user_items')).count())
                .order(local).by(values, desc).limit(local, 5)
                .select(keys).unfold()
            """
            
            similar_users = self.gremlin_client.submit(similar_users_query).all().result()
            
            # Get items from similar users
            for similar_user in similar_users[:3]:  # Top 3 similar users
                items_query = f"""
                    g.V('{similar_user}').out('CREATED', 'LIKED', 'PURCHASED')
                    .where(has('quality_score', gte(0.7)))
                    .limit(3)
                    .project('id', 'type', 'name', 'score')
                    .by('entity_id')
                    .by('entity_type')
                    .by('name')
                    .by('quality_score')
                """
                
                items = self.gremlin_client.submit(items_query).all().result()
                for item in items:
                    recommendations.append({
                        'item_id': item['id'],
                        'item_type': item['type'],
                        'item_name': item['name'],
                        'score': item['score'],
                        'reason': 'similar_users',
                        'source_user': similar_user
                    })
            
            # Content-based: Find similar items
            if 'entity_id' in event:
                similar_items_query = f"""
                    g.V().has('entity_id', '{event['entity_id']}').as('source')
                    .both('SIMILAR_TO', 'TAGGED_WITH').dedup()
                    .where(neq('source'))
                    .limit(5)
                    .project('id', 'type', 'name', 'similarity')
                    .by('entity_id')
                    .by('entity_type')
                    .by('name')
                    .by(constant(0.8))
                """
                
                similar_items = self.gremlin_client.submit(similar_items_query).all().result()
                for item in similar_items:
                    recommendations.append({
                        'item_id': item['id'],
                        'item_type': item['type'],
                        'item_name': item['name'],
                        'score': item['similarity'],
                        'reason': 'content_similarity',
                        'source_item': event['entity_id']
                    })
            
            # Remove duplicates and sort by score
            seen = set()
            unique_recommendations = []
            for rec in sorted(recommendations, key=lambda x: x['score'], reverse=True):
                if rec['item_id'] not in seen and rec['item_id'] not in context.get('recent_items', []):
                    seen.add(rec['item_id'])
                    unique_recommendations.append(rec)
            
            return unique_recommendations[:10]  # Top 10 recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return []
    
    def _get_recommendation_type(self, event_type: str) -> str:
        """Map event type to recommendation type"""
        mapping = {
            'ASSET_VIEWED': 'similar_assets',
            'PROJECT_JOINED': 'related_projects',
            'USER_FOLLOWED': 'suggested_connections'
        }
        return mapping.get(event_type, 'general')


class GraphEvolutionTracker(ProcessFunction):
    """Track graph evolution and community changes over time"""
    
    def __init__(self, janusgraph_url: str):
        self.janusgraph_url = janusgraph_url
        self.gremlin_client = None
        self.evolution_buffer = defaultdict(lambda: {'nodes': 0, 'edges': 0})
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize connections"""
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
    
    def process_element(self, event: Dict[str, Any], ctx: ProcessFunction.Context):
        """Track graph changes"""
        tenant_id = event.get('tenant_id', 'default')
        event_type = event['event_type']
        
        # Update counters
        if event_type.endswith('_CREATED'):
            if 'USER' in event_type or 'ASSET' in event_type:
                self.evolution_buffer[tenant_id]['nodes'] += 1
            elif 'RELATIONSHIP' in event_type:
                self.evolution_buffer[tenant_id]['edges'] += 1
        
        # Periodically emit evolution metrics
        if sum(self.evolution_buffer[tenant_id].values()) % 100 == 0:
            # Get current graph stats
            stats_query = f"""
                g.V().has('tenant_id', '{tenant_id}').count().as('node_count')
                .V().has('tenant_id', '{tenant_id}').outE().count().as('edge_count')
                .select('node_count', 'edge_count')
            """
            
            try:
                stats = self.gremlin_client.submit(stats_query).all().result()
                if stats:
                    current_stats = stats[0]
                    
                    # Emit evolution event
                    yield {
                        'event_type': 'GRAPH_EVOLUTION',
                        'tenant_id': tenant_id,
                        'total_nodes': current_stats.get('node_count', 0),
                        'total_edges': current_stats.get('edge_count', 0),
                        'recent_node_additions': self.evolution_buffer[tenant_id]['nodes'],
                        'recent_edge_additions': self.evolution_buffer[tenant_id]['edges'],
                        'timestamp': datetime.utcnow().isoformat(),
                        'density': current_stats.get('edge_count', 0) / max(current_stats.get('node_count', 1), 1)
                    }
                    
                    # Reset buffer
                    self.evolution_buffer[tenant_id] = {'nodes': 0, 'edges': 0}
                    
            except Exception as e:
                logger.error(f"Failed to get graph stats: {e}")


def create_graph_analytics_job():
    """Create and configure the real-time graph analytics Flink job"""
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Configure checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    
    # Create table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Configuration
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    JANUSGRAPH_URL = "ws://janusgraph:8182/gremlin"
    IGNITE_NODES = [
        ('ignite-0.ignite', 10800),
        ('ignite-1.ignite', 10800),
        ('ignite-2.ignite', 10800)
    ]
    
    # Configure Pulsar source for graph events
    pulsar_source = FlinkKafkaConsumer(
        topics=[
            'graph-update-events',
            'trust-events',
            'user-activity-events',
            'asset-events',
            'collaboration-events'
        ],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': PULSAR_SERVICE_URL,
            'group.id': 'graph-analytics-group'
        }
    )
    
    # Create data stream with watermarks
    event_stream = env.add_source(pulsar_source) \
        .map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(lambda x: int(datetime.fromisoformat(x['timestamp']).timestamp() * 1000))
        )
    
    # Branch 1: Trust score processing
    trust_stream = event_stream \
        .filter(lambda x: x['event_type'] in [
            'ASSET_CREATED', 'ASSET_VERIFIED', 'POSITIVE_REVIEW',
            'NEGATIVE_REVIEW', 'COLLABORATION_COMPLETED', 'DISPUTE_RAISED'
        ]) \
        .key_by(lambda x: x['entity_id']) \
        .process(TrustScoreProcessor(JANUSGRAPH_URL, IGNITE_NODES))
    
    # Branch 2: Fraud detection
    fraud_stream = event_stream \
        .process(FraudDetectionProcessor(JANUSGRAPH_URL))
    
    # Branch 3: Recommendations
    recommendation_stream = event_stream \
        .filter(lambda x: 'user_id' in x) \
        .key_by(lambda x: x['user_id']) \
        .process(GraphRecommendationProcessor(JANUSGRAPH_URL, IGNITE_NODES))
    
    # Branch 4: Graph evolution tracking
    evolution_stream = event_stream \
        .process(GraphEvolutionTracker(JANUSGRAPH_URL))
    
    # Configure Pulsar sink for analytics results
    pulsar_sink = FlinkKafkaProducer(
        topic='graph-analytics-results',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': PULSAR_SERVICE_URL
        }
    )
    
    # Write all streams to sink
    trust_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    fraud_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    recommendation_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    evolution_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    
    # Complex Event Processing for fraud patterns
    # Pattern: Multiple failed verifications followed by successful transaction
    fraud_pattern = Pattern.begin("failed_verification").where(
        lambda x: x.get('event_type') == 'VERIFICATION_FAILED'
    ).times(3).consecutive().next("success").where(
        lambda x: x.get('event_type') in ['ASSET_CREATED', 'TRANSACTION_COMPLETED']
    ).within(Time.minutes(30))
    
    fraud_alerts = CEP.pattern(
        event_stream.key_by(lambda x: x.get('entity_id', '')),
        fraud_pattern
    ).select(
        lambda pattern: {
            'alert_type': 'SUSPICIOUS_PATTERN',
            'entity_id': pattern['failed_verification'][0]['entity_id'],
            'pattern': 'multiple_failed_verifications_then_success',
            'events': len(pattern['failed_verification']) + 1,
            'timestamp': datetime.utcnow().isoformat(),
            'severity': 'HIGH'
        }
    )
    
    fraud_alerts.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    
    # Execute job
    env.execute("Real-time Graph Analytics Job")


if __name__ == "__main__":
    create_graph_analytics_job() 