# In a new file: processing/flink/graph-ingestion-job/src/main.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JanusGraphSink(ProcessFunction):
    """Custom ProcessFunction to write graph data to JanusGraph"""
    
    def __init__(self, janusgraph_url: str):
        self.janusgraph_url = janusgraph_url
        self.gremlin_client = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize Gremlin client"""
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
    def close(self):
        """Close Gremlin client"""
        if self.gremlin_client:
            self.gremlin_client.close()
            
    def process_element(self, value: Dict[str, Any], ctx: ProcessFunction.Context):
        """Process each event and update the graph"""
        try:
            event_type = value.get('event_type')
            tenant_id = value.get('tenant_id')
            
            if event_type == 'USER_CREATED':
                self._create_user_vertex(value, tenant_id)
            elif event_type == 'DOCUMENT_UPDATED':
                self._create_document_and_edge(value, tenant_id)
            elif event_type == 'ASSET_CREATED':
                self._create_asset_and_relationships(value, tenant_id)
            elif event_type == 'PROJECT_CREATED':
                self._create_project_and_membership(value, tenant_id)
            elif event_type == 'WORKFLOW_COMPLETED':
                self._create_workflow_relationships(value, tenant_id)
                
        except Exception as e:
            logger.error(f"Error processing graph update: {e}")
            
    def _create_user_vertex(self, event: Dict[str, Any], tenant_id: str):
        """Create or update a user vertex"""
        user_id = event['user_id']
        details = event.get('details', {})
        
        query = """
            g.V().has('user', 'user_id', user_id).has('tenant_id', tenant_id)
            .fold()
            .coalesce(
                unfold(),
                addV('user')
                    .property('user_id', user_id)
                    .property('tenant_id', tenant_id)
            )
            .property('email', email)
            .property('full_name', full_name)
            .property('created_at', created_at)
            .property('last_activity', timestamp)
        """
        
        bindings = {
            'user_id': user_id,
            'tenant_id': tenant_id,
            'email': details.get('email', ''),
            'full_name': details.get('full_name', ''),
            'created_at': event['event_timestamp'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.gremlin_client.submit(query, bindings).all().result()
        
    def _create_document_and_edge(self, event: Dict[str, Any], tenant_id: str):
        """Create document vertex and edge from user to document"""
        user_id = event['user_id']
        document_id = event['entity_id']
        details = event.get('details', {})
        
        # Create document vertex
        doc_query = """
            g.V().has('document', 'document_id', document_id).has('tenant_id', tenant_id)
            .fold()
            .coalesce(
                unfold(),
                addV('document')
                    .property('document_id', document_id)
                    .property('tenant_id', tenant_id)
            )
            .property('document_name', document_name)
            .property('path', path)
            .property('file_type', file_type)
            .property('updated_at', timestamp)
            .as('doc')
            .V().has('user', 'user_id', user_id).has('tenant_id', tenant_id).as('user')
            .coalesce(
                g.E().has('action', 'EDITED').where(outV().as('user')).where(inV().as('doc')),
                addE('EDITED').from('user').to('doc').property('action', 'EDITED')
            )
            .property('timestamp', timestamp)
            .property('version', version)
        """
        
        bindings = {
            'document_id': document_id,
            'tenant_id': tenant_id,
            'document_name': details.get('document_name', ''),
            'path': details.get('path', ''),
            'file_type': details.get('file_type', ''),
            'user_id': user_id,
            'timestamp': event['event_timestamp'],
            'version': int(details.get('version', 1))
        }
        
        self.gremlin_client.submit(doc_query, bindings).all().result()
        
    def _create_asset_and_relationships(self, event: Dict[str, Any], tenant_id: str):
        """Create digital asset vertex and relationships"""
        user_id = event['user_id']
        asset_id = event['entity_id']
        details = event.get('details', {})
        
        # Create asset vertex
        asset_query = """
            g.V().has('digital_asset', 'asset_id', asset_id).has('tenant_id', tenant_id)
            .fold()
            .coalesce(
                unfold(),
                addV('digital_asset')
                    .property('asset_id', asset_id)
                    .property('tenant_id', tenant_id)
            )
            .property('asset_name', asset_name)
            .property('asset_type', asset_type)
            .property('source_tool', source_tool)
            .property('created_at', timestamp)
            .as('asset')
            .V().has('user', 'user_id', user_id).has('tenant_id', tenant_id).as('user')
            .addE('CREATED').from('user').to('asset')
            .property('timestamp', timestamp)
        """
        
        bindings = {
            'asset_id': asset_id,
            'tenant_id': tenant_id,
            'asset_name': details.get('asset_name', ''),
            'asset_type': details.get('asset_type', ''),
            'source_tool': details.get('source_tool', 'unknown'),
            'user_id': user_id,
            'timestamp': event['event_timestamp']
        }
        
        self.gremlin_client.submit(asset_query, bindings).all().result()
        
        # Create relationships to other assets based on metadata
        if 'related_assets' in details:
            self._create_asset_relationships(asset_id, details['related_assets'], tenant_id)
            
    def _create_project_and_membership(self, event: Dict[str, Any], tenant_id: str):
        """Create project vertex and membership edges"""
        user_id = event['user_id']
        project_id = event['entity_id']
        details = event.get('details', {})
        
        # Create project vertex and creator edge
        project_query = """
            g.V().has('project', 'project_id', project_id).has('tenant_id', tenant_id)
            .fold()
            .coalesce(
                unfold(),
                addV('project')
                    .property('project_id', project_id)
                    .property('tenant_id', tenant_id)
            )
            .property('project_name', project_name)
            .property('status', status)
            .property('created_at', timestamp)
            .as('project')
            .V().has('user', 'user_id', user_id).has('tenant_id', tenant_id).as('user')
            .addE('CREATED').from('user').to('project')
            .property('timestamp', timestamp)
            .property('role', 'owner')
        """
        
        bindings = {
            'project_id': project_id,
            'tenant_id': tenant_id,
            'project_name': details.get('project_name', ''),
            'status': details.get('status', 'active'),
            'user_id': user_id,
            'timestamp': event['event_timestamp']
        }
        
        self.gremlin_client.submit(project_query, bindings).all().result()
        
    def _create_workflow_relationships(self, event: Dict[str, Any], tenant_id: str):
        """Create workflow relationships between entities"""
        workflow_id = event.get('workflow_id')
        details = event.get('details', {})
        
        # Create workflow vertex
        workflow_query = """
            g.addV('workflow')
            .property('workflow_id', workflow_id)
            .property('tenant_id', tenant_id)
            .property('workflow_type', workflow_type)
            .property('status', status)
            .property('completed_at', timestamp)
            .as('workflow')
        """
        
        # Link workflow to involved entities
        if 'involved_entities' in details:
            for entity in details['involved_entities']:
                link_query = """
                    g.V().has(entity_type, entity_id_field, entity_id).has('tenant_id', tenant_id).as('entity')
                    .V().has('workflow', 'workflow_id', workflow_id).has('tenant_id', tenant_id).as('workflow')
                    .addE('INVOLVED_IN').from('entity').to('workflow')
                    .property('role', role)
                """
                
                link_bindings = {
                    'entity_type': entity['type'],
                    'entity_id_field': f"{entity['type']}_id",
                    'entity_id': entity['id'],
                    'tenant_id': tenant_id,
                    'workflow_id': workflow_id,
                    'role': entity.get('role', 'participant')
                }
                
                self.gremlin_client.submit(link_query, link_bindings).all().result()
                
    def _create_asset_relationships(self, asset_id: str, related_assets: List[str], 
                                  tenant_id: str):
        """Create relationships between related assets"""
        for related_id in related_assets:
            rel_query = """
                g.V().has('digital_asset', 'asset_id', asset_id).has('tenant_id', tenant_id).as('source')
                .V().has('digital_asset', 'asset_id', related_id).has('tenant_id', tenant_id).as('target')
                .coalesce(
                    g.E().has('relationship', 'RELATED_TO').where(outV().as('source')).where(inV().as('target')),
                    addE('RELATED_TO').from('source').to('target').property('relationship', 'RELATED_TO')
                )
                .property('discovered_at', timestamp)
            """
            
            rel_bindings = {
                'asset_id': asset_id,
                'related_id': related_id,
                'tenant_id': tenant_id,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            try:
                self.gremlin_client.submit(rel_query, rel_bindings).all().result()
            except Exception as e:
                logger.error(f"Error creating asset relationship: {e}")

def graph_ingestion_job():
    """
    Main Flink job that ingests activity stream data and builds the graph in JanusGraph
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30000)  # 30 seconds
    
    t_env = StreamTableEnvironment.create(stream_environment=env)
    
    # Configuration
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    PULSAR_ADMIN_URL = "http://pulsar:8080"
    JANUSGRAPH_URL = "ws://janusgraph:8182/gremlin"
    
    # Create source table for unified activity stream
    t_env.execute_sql(f"""
        CREATE TABLE activity_stream_source (
            tenant_id STRING,
            event_timestamp TIMESTAMP(3),
            event_id STRING,
            user_id STRING,
            event_source STRING,
            event_type STRING,
            entity_type STRING,
            entity_id STRING,
            details MAP<STRING, STRING>,
            workflow_id STRING,
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/activity-stream',
            'scan.startup.mode' = 'latest',
            'format' = 'json'
        )
    """)
    
    # Query to select and enrich events for graph processing
    enriched_events = t_env.sql_query("""
        SELECT
            tenant_id,
            event_timestamp,
            event_id,
            user_id,
            event_source,
            event_type,
            entity_type,
            entity_id,
            details,
            workflow_id,
            -- Add computed fields for graph processing
            CASE 
                WHEN event_type = 'USER_CREATED' THEN 'create_vertex'
                WHEN event_type IN ('DOCUMENT_UPDATED', 'ASSET_CREATED', 'PROJECT_CREATED') THEN 'create_vertex_and_edge'
                WHEN event_type = 'WORKFLOW_COMPLETED' THEN 'create_workflow_graph'
                ELSE 'update_properties'
            END AS graph_operation
        FROM activity_stream_source
        WHERE event_type IN (
            'USER_CREATED',
            'DOCUMENT_UPDATED', 
            'ASSET_CREATED',
            'PROJECT_CREATED',
            'WORKFLOW_COMPLETED',
            'USER_LOGGED_IN',
            'PERMISSION_GRANTED',
            'TEAM_MEMBER_ADDED'
        )
    """)
    
    # Convert to DataStream for custom processing
    event_stream = t_env.to_data_stream(enriched_events)
    
    # Apply custom JanusGraph sink
    event_stream.process(JanusGraphSink(JANUSGRAPH_URL))
    
    # Also create aggregate views for graph analytics
    t_env.execute_sql(f"""
        CREATE TABLE graph_metrics_sink (
            tenant_id STRING,
            metric_type STRING,
            metric_name STRING,
            metric_value DOUBLE,
            computed_at TIMESTAMP(3),
            PRIMARY KEY (tenant_id, metric_type, metric_name) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'graph-metrics',
            'properties.bootstrap.servers' = 'kafka:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)
    
    # Compute real-time graph metrics
    graph_metrics = t_env.sql_query("""
        SELECT
            tenant_id,
            'user_activity' AS metric_type,
            CONCAT('active_users_', CAST(TUMBLE_END(event_timestamp, INTERVAL '5' MINUTE) AS STRING)) AS metric_name,
            COUNT(DISTINCT user_id) AS metric_value,
            TUMBLE_END(event_timestamp, INTERVAL '5' MINUTE) AS computed_at
        FROM activity_stream_source
        GROUP BY 
            tenant_id,
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE)
            
        UNION ALL
        
        SELECT
            tenant_id,
            'entity_creation' AS metric_type,
            CONCAT(entity_type, '_created_', CAST(TUMBLE_END(event_timestamp, INTERVAL '1' HOUR) AS STRING)) AS metric_name,
            COUNT(*) AS metric_value,
            TUMBLE_END(event_timestamp, INTERVAL '1' HOUR) AS computed_at
        FROM activity_stream_source
        WHERE event_type LIKE '%_CREATED'
        GROUP BY 
            tenant_id,
            entity_type,
            TUMBLE(event_timestamp, INTERVAL '1' HOUR)
    """)
    
    # Insert metrics into sink
    t_env.execute_sql("""
        INSERT INTO graph_metrics_sink
        SELECT * FROM graph_metrics_view
    """)
    
    # Execute the job
    env.execute("Real-time Graph Ingestion and Analytics")

if __name__ == "__main__":
    graph_ingestion_job() 