import logging
from .janusgraph import JanusGraph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

logger = logging.getLogger(__name__)

class SchemaManager:
    def __init__(self, graph: JanusGraph):
        self.g = graph.g
        self.graph = graph

    def create_schema(self, overwrite=False):
        """
        Defines and creates the graph schema including vertex labels, edge labels,
        property keys, and indexes.
        """
        logger.info("Defining graph schema...")
        
        try:
            self.graph.connect()
            
            # Use the JanusGraph management API
            mgmt = self.graph.connection.client.submit("graph.openManagement()").all().result()[0]

            try:
                # Define Property Keys
                self._define_property_key(mgmt, 'asset_id', 'String', 'A unique identifier for the asset.')
                self._define_property_key(mgmt, 'user_id', 'String', 'A unique identifier for the user.')
                self._define_property_key(mgmt, 'project_id', 'String', 'A unique identifier for a project.')
                self._define_property_key(mgmt, 'name', 'String', 'A human-readable name.')
                self._define_property_key(mgmt, 'asset_type', 'String', 'The type of the asset (e.g., 3d-model, dataset).')
                self._define_property_key(mgmt, 'created_at', 'Date', 'Timestamp of creation.')
                self._define_property_key(mgmt, 'processor_name', 'String', 'The name of the processor.')
                self._define_property_key(mgmt, 'processor_version', 'String', 'The version of the processor.')
                self._define_property_key(mgmt, 'parameters', 'String', 'The parameters used by the processor.')
                
                # Define Vertex Labels
                self._define_vertex_label(mgmt, 'asset')
                self._define_vertex_label(mgmt, 'user')
                self._define_vertex_label(mgmt, 'project')
                self._define_vertex_label(mgmt, 'processor')

                # Define Edge Labels
                self._define_edge_label(mgmt, 'OWNS')
                self._define_edge_label(mgmt, 'CREATED_BY')
                self._define_edge_label(mgmt, 'USED_IN')
                self._define_edge_label(mgmt, 'DERIVED_FROM')
                self._define_edge_label(mgmt, 'PROCESSED_BY')

                # Define Indexes
                # Composite index on asset_id for fast lookups
                self._create_composite_index(mgmt, 'by_asset_id', 'asset_id', unique=True)
                self._create_composite_index(mgmt, 'by_user_id', 'user_id', unique=True)
                self._create_composite_index(mgmt, 'by_project_id', 'project_id', unique=True)
                self._create_composite_index(mgmt, 'by_processor_name', 'processor_name', unique=False)
                
                logger.info("Committing schema changes...")
                mgmt.commit()
                logger.info("Schema definition completed successfully.")

            except Exception as e:
                logger.error(f"Error during schema definition, rolling back: {e}")
                mgmt.rollback()
                raise
        
        finally:
            self.graph.close()

    def _define_property_key(self, mgmt, name, data_type, description=""):
        if not mgmt.getPropertyKey(name):
            pk = mgmt.makePropertyKey(name).dataType(eval(data_type)).description(description).make()
            logger.info(f"Created Property Key: {name}")
        else:
            logger.info(f"Property Key '{name}' already exists.")

    def _define_vertex_label(self, mgmt, name):
        if not mgmt.getVertexLabel(name):
            mgmt.makeVertexLabel(name).make()
            logger.info(f"Created Vertex Label: {name}")
        else:
            logger.info(f"Vertex Label '{name}' already exists.")

    def _define_edge_label(self, mgmt, name):
        if not mgmt.getEdgeLabel(name):
            mgmt.makeEdgeLabel(name).make()
            logger.info(f"Created Edge Label: {name}")
        else:
            logger.info(f"Edge Label '{name}' already exists.")
            
    def _create_composite_index(self, mgmt, index_name, property_key_name, unique=False):
        if not mgmt.getGraphIndex(index_name):
            index = mgmt.buildIndex(index_name, T.Vertex).addKey(mgmt.getPropertyKey(property_key_name))
            if unique:
                index = index.unique()
            index.buildCompositeIndex()
            logger.info(f"Created Composite Index '{index_name}' on property '{property_key_name}'.")
        else:
            logger.info(f"Index '{index_name}' already exists.")

    def wait_for_index_status(self, index_name, status='REGISTERED'):
        """
        Waits for a graph index to reach a specific status.
        Needed after creating indexes before they are usable.
        """
        logger.info(f"Waiting for index '{index_name}' to become {status}...")
        
        try:
            self.graph.connect()
            mgmt = self.graph.connection.client.submit("graph.openManagement()").all().result()[0]
            
            try:
                graph_index = mgmt.getGraphIndex(index_name)
                if not graph_index:
                    logger.warning(f"Index '{index_name}' does not exist.")
                    return

                # This is a simplified wait loop. In a real-world scenario, you might
                # need a more robust async wait or use JanusGraph's management features.
                import time
                while True:
                    # Re-read index state
                    graph_index = mgmt.getGraphIndex(index_name)
                    # Check the state of each property key in the index
                    all_registered = True
                    for pk in graph_index.getFieldKeys():
                        if graph_index.getIndexedElement().getSimpleName() == 'Vertex':
                            key_status = graph_index.getfield(pk).getStatus()
                            if key_status.toString() != status:
                                all_registered = False
                                break
                    
                    if all_registered:
                        logger.info(f"Index '{index_name}' is now {status}.")
                        break
                    
                    logger.info(f"Index '{index_name}' status is not yet {status}. Retrying in 10 seconds...")
                    time.sleep(10)
            finally:
                mgmt.commit() # Commit to close the management transaction
        
        finally:
            self.graph.close() 