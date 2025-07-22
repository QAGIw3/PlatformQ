"""
Real-time Simulation Engine Stream Processing Flink Job

This job processes simulation data streams including:
- Multi-physics simulation state updates
- Agent-based simulation events
- Real-time convergence monitoring
- Collaborative simulation synchronization
- Complex Event Processing (CEP) for simulation patterns
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.cep import CEP, Pattern, PatternSelectFunction
from pyflink.datastream.functions import RuntimeContext
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationState:
    """Container for simulation state data"""
    def __init__(self):
        self.simulation_id: str = ""
        self.current_tick: int = 0
        self.domains: Dict[str, Dict[str, Any]] = {}
        self.agents: Dict[str, Dict[str, Any]] = {}
        self.convergence_history: List[float] = []
        self.metrics: Dict[str, float] = {}
        self.optimization_state: Optional[Dict[str, Any]] = None


class SimulationTimestampAssigner(TimestampAssigner):
    """Extract event time from simulation events"""
    
    def extract_timestamp(self, value: Dict[str, Any], record_timestamp: int) -> int:
        # Use simulation tick as event time
        simulation_tick = value.get('simulation_tick', 0)
        base_timestamp = value.get('timestamp', record_timestamp)
        
        # Convert simulation tick to milliseconds (assuming 100ms per tick)
        return base_timestamp + (simulation_tick * 100)


class MultiPhysicsStateProcessor(KeyedProcessFunction):
    """Process multi-physics simulation state updates with stateful processing"""
    
    def __init__(self):
        self.simulation_state = None
        self.convergence_window = None
        self.anomaly_detector = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state descriptors"""
        # Main simulation state
        self.simulation_state = runtime_context.get_state(
            ValueStateDescriptor(
                "simulation_state",
                type_info=DataTypes.PICKLED_BYTE_ARRAY()
            )
        )
        
        # Convergence window for monitoring
        self.convergence_window = runtime_context.get_list_state(
            ListStateDescriptor(
                "convergence_window",
                type_info=DataTypes.FLOAT()
            )
        )
        
        # Anomaly detection state
        self.anomaly_detector = runtime_context.get_map_state(
            MapStateDescriptor(
                "anomaly_metrics",
                key_type_info=DataTypes.STRING(),
                value_type_info=DataTypes.ROW([
                    DataTypes.FIELD("mean", DataTypes.FLOAT()),
                    DataTypes.FIELD("std", DataTypes.FLOAT()),
                    DataTypes.FIELD("count", DataTypes.INT())
                ])
            )
        )
        
    def process_element(self, value: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Process simulation state updates"""
        try:
            event_type = value.get('event_type')
            
            if event_type == 'DOMAIN_UPDATE':
                self._process_domain_update(value, ctx)
            elif event_type == 'CONVERGENCE_CHECK':
                self._check_convergence(value, ctx)
            elif event_type == 'OPTIMIZATION_UPDATE':
                self._process_optimization_update(value, ctx)
            elif event_type == 'AGENT_STATE_BATCH':
                self._process_agent_batch(value, ctx)
                
            # Register timer for periodic analysis
            ctx.timer_service().register_processing_time_timer(
                ctx.timestamp() + 5000  # 5 seconds
            )
            
        except Exception as e:
            logger.error(f"Error processing simulation state: {e}")
            self._emit_error(value.get('simulation_id'), str(e), ctx)
    
    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """Periodic analysis and anomaly detection"""
        state = self.simulation_state.value()
        if not state:
            return
            
        # Perform anomaly detection
        anomalies = self._detect_anomalies(state)
        if anomalies:
            self._emit_anomaly_alert(state.simulation_id, anomalies, ctx)
            
        # Emit periodic metrics
        self._emit_metrics_summary(state, ctx)
        
    def _process_domain_update(self, event: Dict[str, Any], ctx):
        """Process physics domain update"""
        state = self.simulation_state.value() or SimulationState()
        
        domain_id = event.get('domain_id')
        domain_data = event.get('domain_data', {})
        
        # Update domain state
        if domain_id not in state.domains:
            state.domains[domain_id] = {}
            
        state.domains[domain_id].update({
            'physics_type': domain_data.get('physics_type'),
            'solver': domain_data.get('solver'),
            'status': domain_data.get('status'),
            'metrics': domain_data.get('metrics', {}),
            'last_update': datetime.utcnow().isoformat()
        })
        
        # Extract convergence metric
        convergence = domain_data.get('metrics', {}).get('convergence_residual', float('inf'))
        state.convergence_history.append(convergence)
        
        # Keep only last 100 values
        if len(state.convergence_history) > 100:
            state.convergence_history = state.convergence_history[-100:]
            
        # Update state
        self.simulation_state.update(state)
        
        # Emit domain metrics
        metrics_event = {
            'simulation_id': event['simulation_id'],
            'event_type': 'DOMAIN_METRICS',
            'domain_id': domain_id,
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': domain_data.get('metrics', {})
        }
        ctx.output(metrics_event)
        
    def _check_convergence(self, event: Dict[str, Any], ctx):
        """Check simulation convergence"""
        state = self.simulation_state.value()
        if not state or len(state.convergence_history) < 10:
            return
            
        # Calculate convergence trend
        recent_history = state.convergence_history[-10:]
        convergence_rate = self._calculate_convergence_rate(recent_history)
        
        # Check if converged
        is_converged = all(r < 1e-4 for r in recent_history[-5:])
        
        # Emit convergence status
        convergence_event = {
            'simulation_id': event['simulation_id'],
            'event_type': 'CONVERGENCE_STATUS',
            'timestamp': datetime.utcnow().isoformat(),
            'is_converged': is_converged,
            'convergence_rate': convergence_rate,
            'latest_residual': recent_history[-1] if recent_history else None,
            'trend': 'improving' if convergence_rate < 0 else 'worsening'
        }
        ctx.output(convergence_event)
        
        # Trigger optimization if convergence is slow
        if not is_converged and convergence_rate > -0.01:
            self._trigger_optimization_adjustment(state, ctx)
            
    def _process_optimization_update(self, event: Dict[str, Any], ctx):
        """Process optimization state update"""
        state = self.simulation_state.value() or SimulationState()
        
        state.optimization_state = {
            'type': event.get('optimization_type'),
            'objective_value': event.get('objective_value'),
            'parameters': event.get('parameters', {}),
            'iteration': event.get('iteration', 0),
            'gradient_norm': event.get('gradient_norm'),
            'improvement': event.get('improvement', 0)
        }
        
        self.simulation_state.update(state)
        
        # Emit optimization metrics
        opt_metrics = {
            'simulation_id': event['simulation_id'],
            'event_type': 'OPTIMIZATION_METRICS',
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': state.optimization_state
        }
        ctx.output(opt_metrics)
        
    def _process_agent_batch(self, event: Dict[str, Any], ctx):
        """Process batch of agent state updates"""
        state = self.simulation_state.value() or SimulationState()
        
        agents = event.get('agents', [])
        
        # Update agent states
        for agent in agents:
            agent_id = agent.get('agent_id')
            state.agents[agent_id] = {
                'position': agent.get('position'),
                'velocity': agent.get('velocity'),
                'state': agent.get('state'),
                'energy': agent.get('energy', 0),
                'interactions': agent.get('interactions', 0)
            }
            
        # Calculate aggregate metrics
        if state.agents:
            positions = [a.get('position', [0, 0, 0]) for a in state.agents.values()]
            velocities = [a.get('velocity', [0, 0, 0]) for a in state.agents.values()]
            
            state.metrics['agent_count'] = len(state.agents)
            state.metrics['avg_velocity'] = np.mean([np.linalg.norm(v) for v in velocities])
            state.metrics['spatial_spread'] = self._calculate_spatial_spread(positions)
            state.metrics['total_energy'] = sum(a.get('energy', 0) for a in state.agents.values())
            
        self.simulation_state.update(state)
        
    def _calculate_convergence_rate(self, history: List[float]) -> float:
        """Calculate convergence rate using linear regression"""
        if len(history) < 2:
            return 0
            
        x = np.arange(len(history))
        y = np.log10(np.array(history) + 1e-10)  # Log scale for residuals
        
        # Linear regression
        A = np.vstack([x, np.ones(len(x))]).T
        m, c = np.linalg.lstsq(A, y, rcond=None)[0]
        
        return m  # Slope indicates convergence rate
        
    def _calculate_spatial_spread(self, positions: List[List[float]]) -> float:
        """Calculate spatial spread of agents"""
        if not positions:
            return 0
            
        positions_array = np.array(positions)
        centroid = np.mean(positions_array, axis=0)
        distances = np.linalg.norm(positions_array - centroid, axis=1)
        
        return np.std(distances)
        
    def _detect_anomalies(self, state: SimulationState) -> List[Dict[str, Any]]:
        """Detect anomalies in simulation metrics"""
        anomalies = []
        
        # Check each metric
        for metric_name, metric_value in state.metrics.items():
            # Get historical statistics
            stats = self.anomaly_detector.get(metric_name)
            
            if stats and stats['count'] > 10:
                mean = stats['mean']
                std = stats['std']
                
                # Z-score based anomaly detection
                z_score = abs((metric_value - mean) / (std + 1e-6))
                
                if z_score > 3:  # 3 sigma rule
                    anomalies.append({
                        'metric': metric_name,
                        'value': metric_value,
                        'expected_range': [mean - 3*std, mean + 3*std],
                        'z_score': z_score
                    })
                    
            # Update statistics
            self._update_metric_stats(metric_name, metric_value)
            
        return anomalies
        
    def _update_metric_stats(self, metric_name: str, value: float):
        """Update running statistics for metric"""
        stats = self.anomaly_detector.get(metric_name) or {'mean': 0, 'std': 0, 'count': 0}
        
        # Welford's online algorithm for mean and variance
        count = stats['count'] + 1
        delta = value - stats['mean']
        mean = stats['mean'] + delta / count
        delta2 = value - mean
        
        if count > 1:
            variance = ((count - 1) * stats['std']**2 + delta * delta2) / count
            std = math.sqrt(variance)
        else:
            std = 0
            
        self.anomaly_detector.put(metric_name, {
            'mean': mean,
            'std': std,
            'count': count
        })
        
    def _trigger_optimization_adjustment(self, state: SimulationState, ctx):
        """Trigger optimization parameter adjustment"""
        adjustment_event = {
            'simulation_id': state.simulation_id,
            'event_type': 'OPTIMIZATION_ADJUSTMENT_NEEDED',
            'timestamp': datetime.utcnow().isoformat(),
            'reason': 'slow_convergence',
            'current_residual': state.convergence_history[-1] if state.convergence_history else None
        }
        ctx.output(adjustment_event)
        
    def _emit_anomaly_alert(self, simulation_id: str, anomalies: List[Dict[str, Any]], ctx):
        """Emit anomaly alert event"""
        alert_event = {
            'simulation_id': simulation_id,
            'event_type': 'ANOMALY_DETECTED',
            'timestamp': datetime.utcnow().isoformat(),
            'anomalies': anomalies,
            'severity': 'high' if len(anomalies) > 3 else 'medium'
        }
        ctx.output(alert_event)
        
    def _emit_metrics_summary(self, state: SimulationState, ctx):
        """Emit periodic metrics summary"""
        summary_event = {
            'simulation_id': state.simulation_id,
            'event_type': 'METRICS_SUMMARY',
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': state.metrics,
            'domain_count': len(state.domains),
            'agent_count': len(state.agents),
            'convergence_trend': self._calculate_convergence_rate(state.convergence_history[-10:])
        }
        ctx.output(summary_event)
        
    def _emit_error(self, simulation_id: str, error: str, ctx):
        """Emit error event"""
        error_event = {
            'simulation_id': simulation_id,
            'event_type': 'SIMULATION_PROCESSING_ERROR',
            'timestamp': datetime.utcnow().isoformat(),
            'error': error
        }
        ctx.output(error_event)


class SimulationEventPatternDetector:
    """Complex Event Processing for simulation patterns"""
    
    @staticmethod
    def create_convergence_stall_pattern() -> Pattern:
        """Pattern to detect convergence stall"""
        return Pattern.begin("start").where(lambda x: x.get('event_type') == 'CONVERGENCE_STATUS') \
            .followed_by("stall").where(
                lambda x: x.get('event_type') == 'CONVERGENCE_STATUS' and 
                x.get('convergence_rate', -1) > -0.001
            ).times(5).consecutive()
            
    @staticmethod
    def create_oscillation_pattern() -> Pattern:
        """Pattern to detect oscillating convergence"""
        return Pattern.begin("high").where(
            lambda x: x.get('event_type') == 'DOMAIN_METRICS' and 
            x.get('metrics', {}).get('convergence_residual', 0) > 0.1
        ).followed_by("low").where(
            lambda x: x.get('event_type') == 'DOMAIN_METRICS' and 
            x.get('metrics', {}).get('convergence_residual', 1) < 0.01
        ).followed_by("high_again").where(
            lambda x: x.get('event_type') == 'DOMAIN_METRICS' and 
            x.get('metrics', {}).get('convergence_residual', 0) > 0.1
        ).within(Time.minutes(5))
        
    @staticmethod
    def create_cascade_failure_pattern() -> Pattern:
        """Pattern to detect cascade failures across domains"""
        return Pattern.begin("first_failure").where(
            lambda x: x.get('event_type') == 'DOMAIN_METRICS' and 
            x.get('metrics', {}).get('status') == 'failed'
        ).followed_by("cascade").where(
            lambda x: x.get('event_type') == 'DOMAIN_METRICS' and 
            x.get('metrics', {}).get('status') == 'failed'
        ).times(2).within(Time.seconds(30))


class SimulationMetricsAggregator(AggregateFunction):
    """Aggregate simulation metrics over windows"""
    
    def create_accumulator(self) -> Dict[str, Any]:
        return {
            'count': 0,
            'sum_residual': 0,
            'min_residual': float('inf'),
            'max_residual': 0,
            'domain_updates': 0,
            'agent_updates': 0,
            'anomalies': 0
        }
        
    def add(self, value: Dict[str, Any], accumulator: Dict[str, Any]) -> Dict[str, Any]:
        accumulator['count'] += 1
        
        if value.get('event_type') == 'DOMAIN_METRICS':
            residual = value.get('metrics', {}).get('convergence_residual', 0)
            accumulator['sum_residual'] += residual
            accumulator['min_residual'] = min(accumulator['min_residual'], residual)
            accumulator['max_residual'] = max(accumulator['max_residual'], residual)
            accumulator['domain_updates'] += 1
            
        elif value.get('event_type') == 'AGENT_STATE_BATCH':
            accumulator['agent_updates'] += value.get('agent_count', 0)
            
        elif value.get('event_type') == 'ANOMALY_DETECTED':
            accumulator['anomalies'] += len(value.get('anomalies', []))
            
        return accumulator
        
    def get_result(self, accumulator: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'window_event_count': accumulator['count'],
            'avg_residual': accumulator['sum_residual'] / max(1, accumulator['domain_updates']),
            'min_residual': accumulator['min_residual'] if accumulator['min_residual'] != float('inf') else 0,
            'max_residual': accumulator['max_residual'],
            'domain_updates': accumulator['domain_updates'],
            'agent_updates': accumulator['agent_updates'],
            'anomaly_count': accumulator['anomalies']
        }
        
    def merge(self, acc1: Dict[str, Any], acc2: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'count': acc1['count'] + acc2['count'],
            'sum_residual': acc1['sum_residual'] + acc2['sum_residual'],
            'min_residual': min(acc1['min_residual'], acc2['min_residual']),
            'max_residual': max(acc1['max_residual'], acc2['max_residual']),
            'domain_updates': acc1['domain_updates'] + acc2['domain_updates'],
            'agent_updates': acc1['agent_updates'] + acc2['agent_updates'],
            'anomalies': acc1['anomalies'] + acc2['anomalies']
        }


def create_simulation_engine_job():
    """Create and configure the simulation engine Flink job"""
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # Configure checkpointing
    env.enable_checkpointing(10000)  # 10 seconds
    
    # Create table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Configure Pulsar source for simulation events
    pulsar_source = FlinkKafkaConsumer(
        topics=[
            'simulation-state-events',
            'multi-physics-updates', 
            'agent-state-events',
            'optimization-events'
        ],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'simulation-engine-group'
        }
    )
    
    # Create data stream with watermarks
    simulation_stream = env.add_source(pulsar_source) \
        .map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(SimulationTimestampAssigner())
        )

    # Key by simulation ID for stateful processing
    keyed_stream = simulation_stream.key_by(lambda x: x['simulation_id'])
    
    # Apply stateful processing
    processed_stream = keyed_stream.process(MultiPhysicsStateProcessor())
    
    # Window aggregation for metrics
    windowed_metrics = processed_stream \
        .key_by(lambda x: x['simulation_id']) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .aggregate(SimulationMetricsAggregator())
    
    # Complex Event Processing
    # Detect convergence stall
    stall_pattern = SimulationEventPatternDetector.create_convergence_stall_pattern()
    stall_alerts = CEP.pattern(keyed_stream, stall_pattern).select(
        lambda pattern: {
            'simulation_id': pattern['start'][0]['simulation_id'],
            'event_type': 'CONVERGENCE_STALL_DETECTED',
            'timestamp': datetime.utcnow().isoformat(),
            'duration': len(pattern.get('stall', [])),
            'action': 'consider_parameter_adjustment'
        }
    )
    
    # Detect oscillation
    oscillation_pattern = SimulationEventPatternDetector.create_oscillation_pattern()
    oscillation_alerts = CEP.pattern(keyed_stream, oscillation_pattern).select(
        lambda pattern: {
            'simulation_id': pattern['high'][0]['simulation_id'],
            'event_type': 'CONVERGENCE_OSCILLATION_DETECTED',
            'timestamp': datetime.utcnow().isoformat(),
            'high_value': pattern['high'][0]['metrics']['convergence_residual'],
            'low_value': pattern['low'][0]['metrics']['convergence_residual'],
            'action': 'adjust_relaxation_factor'
        }
    )
    
    # Configure sinks
    pulsar_sink = FlinkKafkaProducer(
        topic='simulation-analysis-results',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    # Write all streams to sink
    processed_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    windowed_metrics.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    stall_alerts.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    oscillation_alerts.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    
    # Execute job
    env.execute("Real-time Simulation Engine Stream Processing")


if __name__ == "__main__":
    create_simulation_engine_job() 