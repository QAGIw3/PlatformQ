from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.udf import ScalarFunction, udf, udtf, TableFunction
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
import numpy as np
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationStateProcessor(ProcessFunction):
    """Process simulation state updates and compute analytics"""
    
    def __init__(self):
        self.state_descriptor = None
        self.metrics_state = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state"""
        # State for tracking simulation metrics
        self.state_descriptor = ValueStateDescriptor(
            "simulation_state",
            Dict[str, Any]
        )
        self.metrics_state = runtime_context.get_state(self.state_descriptor)
        
    def process_element(self, value: Dict[str, Any], ctx: ProcessFunction.Context):
        """Process simulation state updates"""
        try:
            event_type = value.get('state_type')
            simulation_id = value.get('simulation_id')
            
            # Get current state
            current_state = self.metrics_state.value() or {
                'agent_count': 0,
                'tick_rate': 0,
                'last_tick': 0,
                'spatial_distribution': {}
            }
            
            if event_type == 'INCREMENTAL_UPDATE':
                # Update metrics
                current_state['agent_count'] = value.get('agent_count', 0)
                current_state['last_tick'] = value.get('simulation_tick', 0)
                
                # Calculate tick rate
                if 'last_update_time' in current_state:
                    time_diff = value['timestamp'] - current_state['last_update_time']
                    tick_diff = current_state['last_tick'] - current_state.get('previous_tick', 0)
                    if time_diff > 0:
                        current_state['tick_rate'] = tick_diff / (time_diff / 1000.0)
                
                current_state['previous_tick'] = current_state['last_tick']
                current_state['last_update_time'] = value['timestamp']
                
            elif event_type == 'METRICS_UPDATE':
                # Merge metrics
                metrics = value.get('metrics', {})
                current_state.update(metrics)
                
            # Update state
            self.metrics_state.update(current_state)
            
            # Emit enriched event
            enriched_event = {
                **value,
                'computed_metrics': {
                    'tick_rate': current_state.get('tick_rate', 0),
                    'agent_density': self._calculate_density(current_state)
                }
            }
            
            yield enriched_event
            
        except Exception as e:
            logger.error(f"Error processing simulation state: {e}")
    
    def _calculate_density(self, state: Dict[str, Any]) -> float:
        """Calculate agent density metric"""
        agent_count = state.get('agent_count', 0)
        # Simplified density calculation
        return agent_count / 1000.0  # agents per unit volume


class AgentInteractionProcessor(ProcessFunction):
    """Process agent interactions and compute collision/influence metrics"""
    
    def __init__(self, interaction_radius: float = 5.0):
        self.interaction_radius = interaction_radius
        self.agent_positions = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state for agent tracking"""
        self.agent_positions_descriptor = ListStateDescriptor(
            "agent_positions",
            Tuple[str, List[float]]  # (agent_id, position)
        )
        self.agent_positions = runtime_context.get_list_state(
            self.agent_positions_descriptor
        )
    
    def process_element(self, value: Dict[str, Any], ctx: ProcessFunction.Context):
        """Process agent state updates and detect interactions"""
        try:
            if value.get('event_type') == 'AGENT_UPDATE_BATCH':
                agents = value.get('agents', [])
                simulation_id = value.get('simulation_id')
                
                # Update agent positions
                new_positions = []
                for agent in agents:
                    new_positions.append((
                        agent['id'],
                        agent['position']
                    ))
                
                # Clear old positions and add new ones
                self.agent_positions.clear()
                for pos in new_positions:
                    self.agent_positions.add(pos)
                
                # Detect interactions
                interactions = self._detect_interactions(new_positions)
                
                if interactions:
                    # Emit interaction events
                    yield {
                        'type': 'agent_interactions',
                        'simulation_id': simulation_id,
                        'timestamp': value['timestamp'],
                        'interactions': interactions,
                        'interaction_count': len(interactions)
                    }
                    
        except Exception as e:
            logger.error(f"Error processing agent interactions: {e}")
    
    def _detect_interactions(self, positions: List[Tuple[str, List[float]]]) -> List[Dict[str, Any]]:
        """Detect agents within interaction radius"""
        interactions = []
        
        # Simple O(n²) algorithm - in production use spatial indexing
        for i in range(len(positions)):
            for j in range(i + 1, len(positions)):
                agent1_id, pos1 = positions[i]
                agent2_id, pos2 = positions[j]
                
                # Calculate distance
                distance = np.linalg.norm(
                    np.array(pos1) - np.array(pos2)
                )
                
                if distance <= self.interaction_radius:
                    interactions.append({
                        'agent1_id': agent1_id,
                        'agent2_id': agent2_id,
                        'distance': float(distance),
                        'type': 'proximity'
                    })
        
        return interactions


def simulation_collaboration_job():
    """
    Main Flink job for real-time simulation collaboration processing
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(10000)  # 10 seconds
    
    t_env = StreamTableEnvironment.create(stream_environment=env)
    
    # Configuration
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    PULSAR_ADMIN_URL = "http://pulsar:8080"
    IGNITE_URL = "ignite://ignite:10800"
    CASSANDRA_HOST = "cassandra"
    CASSANDRA_PORT = 9042
    JANUSGRAPH_URL = "ws://janusgraph:8182/gremlin"
    
    # 1. Create source table for simulation collaboration events
    t_env.execute_sql(f"""
        CREATE TABLE simulation_collaboration_events (
            tenant_id STRING,
            simulation_id STRING,
            session_id STRING,
            user_id STRING,
            event_id STRING,
            event_type STRING,
            operation_data BYTES,
            vector_clock MAP<STRING, BIGINT>,
            parent_operations ARRAY<STRING>,
            simulation_time BIGINT,
            timestamp TIMESTAMP(3),
            WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/simulation-collaboration-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # 2. Create source table for simulation state events
    t_env.execute_sql(f"""
        CREATE TABLE simulation_state_events (
            tenant_id STRING,
            simulation_id STRING,
            state_type STRING,
            simulation_tick BIGINT,
            agent_count INT,
            state_data BYTES,
            metrics MAP<STRING, DOUBLE>,
            state_uri STRING,
            timestamp TIMESTAMP(3),
            WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/simulation-state-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # 3. Create sink table for aggregated metrics
    t_env.execute_sql(f"""
        CREATE TABLE simulation_metrics_sink (
            tenant_id STRING,
            simulation_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            avg_tick_rate DOUBLE,
            max_agent_count INT,
            operation_count BIGINT,
            active_users INT,
            avg_agents_per_user DOUBLE,
            PRIMARY KEY (tenant_id, simulation_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'cassandra',
            'cassandra.host' = '{CASSANDRA_HOST}',
            'cassandra.port' = '{CASSANDRA_PORT}',
            'cassandra.keyspace' = 'platformq',
            'cassandra.table' = 'simulation_metrics',
            'cassandra.consistency' = 'LOCAL_QUORUM'
        )
    """)
    
    # 4. Create sink for JanusGraph lineage tracking
    t_env.execute_sql(f"""
        CREATE TABLE simulation_lineage_sink (
            simulation_id STRING,
            operation_id STRING,
            user_id STRING,
            operation_type STRING,
            timestamp TIMESTAMP(3),
            parent_operations ARRAY<STRING>,
            branch_id STRING,
            simulation_tick BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:janusgraph:{JANUSGRAPH_URL}',
            'table-name' = 'simulation_lineage',
            'driver' = 'org.janusgraph.driver.JanusGraphDriver'
        )
    """)
    
    # 5. Aggregate simulation metrics
    simulation_metrics = t_env.sql_query("""
        SELECT
            tenant_id,
            simulation_id,
            TUMBLE_START(timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(CAST(metrics['tick_rate'] AS DOUBLE)) AS avg_tick_rate,
            MAX(agent_count) AS max_agent_count,
            COUNT(DISTINCT event_id) AS operation_count,
            COUNT(DISTINCT user_id) AS active_users,
            CAST(MAX(agent_count) AS DOUBLE) / COUNT(DISTINCT user_id) AS avg_agents_per_user
        FROM (
            SELECT 
                c.tenant_id,
                c.simulation_id,
                c.event_id,
                c.user_id,
                c.timestamp,
                s.agent_count,
                s.metrics
            FROM simulation_collaboration_events c
            LEFT JOIN simulation_state_events s
            ON c.simulation_id = s.simulation_id
            AND c.timestamp BETWEEN s.timestamp - INTERVAL '1' SECOND 
                                AND s.timestamp + INTERVAL '1' SECOND
        )
        GROUP BY 
            tenant_id,
            simulation_id,
            TUMBLE(timestamp, INTERVAL '1' MINUTE)
    """)
    
    # 6. Track operation lineage
    operation_lineage = t_env.sql_query("""
        SELECT
            simulation_id,
            event_id AS operation_id,
            user_id,
            event_type AS operation_type,
            timestamp,
            parent_operations,
            'main' AS branch_id,  -- TODO: Extract from operation_data
            simulation_time AS simulation_tick
        FROM simulation_collaboration_events
        WHERE event_type IN (
            'PARAMETER_CHANGED',
            'AGENT_ADDED',
            'AGENT_REMOVED',
            'AGENT_MODIFIED',
            'BRANCH_CREATED',
            'CHECKPOINT_CREATED'
        )
    """)
    
    # 7. Insert into sinks
    t_env.execute_sql("""
        INSERT INTO simulation_metrics_sink
        SELECT * FROM simulation_metrics
    """)
    
    t_env.execute_sql("""
        INSERT INTO simulation_lineage_sink
        SELECT * FROM operation_lineage
    """)
    
    # 8. Process state updates with custom logic
    state_stream = t_env.to_data_stream(
        t_env.from_path("simulation_state_events")
    )
    
    # Apply state processing
    processed_states = state_stream.process(SimulationStateProcessor())
    
    # 9. Process agent interactions
    # Convert collaboration events to agent updates
    agent_updates = t_env.sql_query("""
        SELECT
            tenant_id,
            simulation_id,
            'AGENT_UPDATE_BATCH' AS event_type,
            timestamp,
            -- Extract agents from operation_data (simplified)
            CAST(operation_data AS STRING) AS agents_json
        FROM simulation_collaboration_events
        WHERE event_type IN ('AGENT_ADDED', 'AGENT_MODIFIED')
    """)
    
    agent_stream = t_env.to_data_stream(agent_updates)
    
    # Process interactions
    interactions = agent_stream.process(AgentInteractionProcessor())
    
    # 10. Output interaction events to Ignite for real-time access
    # (In production, this would use an Ignite sink)
    interactions.print()
    
    # Execute the job
    env.execute("Simulation Collaboration Processing")


# User-defined functions for data transformation
class ExtractBranchId(ScalarFunction):
    def eval(self, operation_data: bytes) -> str:
        """Extract branch ID from operation data"""
        try:
            data = json.loads(operation_data.decode())
            return data.get('branch_id', 'main')
        except:
            return 'main'


class ParseAgentData(TableFunction):
    def eval(self, agents_json: str):
        """Parse agent data from JSON"""
        try:
            data = json.loads(agents_json)
            agents = data.get('agents', [])
            for agent in agents:
                yield agent['id'], agent['position'], agent.get('velocity', [0, 0, 0])
        except:
            pass


# Register UDFs
extract_branch_id = udf(ExtractBranchId(), result_type=DataTypes.STRING())
parse_agent_data = udtf(ParseAgentData(), result_type=DataTypes.ROW([
    DataTypes.FIELD("agent_id", DataTypes.STRING()),
    DataTypes.FIELD("position", DataTypes.ARRAY(DataTypes.FLOAT())),
    DataTypes.FIELD("velocity", DataTypes.ARRAY(DataTypes.FLOAT()))
]))

if __name__ == "__main__":
    simulation_collaboration_job() 