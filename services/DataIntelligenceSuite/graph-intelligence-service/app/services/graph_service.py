import logging
from ..db.janusgraph import JanusGraph
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class GraphService:
    def __init__(self):
        self.graph = JanusGraph() # Assuming a JanusGraph connection class exists

    async def add_asset_to_graph(self, asset_id: str, asset_name: str, asset_type: str, owner_id: str):
        """
        Adds a new asset and its owner to the graph if they don't already exist.
        Creates an 'OWNS' relationship between them.
        """
        logger.info(f"Processing asset {asset_id} for graph update.")
        
        try:
            self.graph.connect() # Connect before using the graph
            # Idempotent creation of the asset vertex
            g = self.graph.g
            asset_vertex = g.V().has('asset', 'asset_id', asset_id).fold().coalesce(
                g.addV('asset').property('asset_id', asset_id),
                g.V().has('asset', 'asset_id', asset_id)
            ).next()
            
            # Update properties
            g.V(asset_vertex).property('name', asset_name).property('type', asset_type).iterate()

            # Idempotent creation of the user vertex
            user_vertex = g.V().has('user', 'user_id', owner_id).fold().coalesce(
                g.addV('user').property('user_id', owner_id),
                g.V().has('user', 'user_id', owner_id)
            ).next()

            # Idempotent creation of the 'OWNS' edge from user to asset
            g.V(user_vertex).outE('OWNS').where(g.inV().is_(asset_vertex)).fold().coalesce(
                g.V(user_vertex).addE('OWNS').to(asset_vertex),
                g.V(user_vertex).outE('OWNS').where(g.inV().is_(asset_vertex))
            ).iterate()

            logger.info(f"Successfully added/updated asset '{asset_id}' and user '{owner_id}' in the graph.")

        except Exception as e:
            logger.error(f"Error updating graph for asset {asset_id}: {e}")
            # Depending on the desired retry logic, we might want to raise the exception
            raise
        finally:
            self.graph.close()

    async def get_downstream_lineage(self, asset_id: str) -> Dict:
        """
        Traverses the 'DERIVED_FROM' edges to build a lineage graph.
        """
        logger.info(f"Fetching downstream lineage for asset {asset_id}")
        try:
            self.graph.connect()
            g = self.graph.g
            
            # This query finds all assets derived from the starting asset,
            # and all assets derived from those, and so on.
            lineage_path = g.V().has('asset', 'asset_id', asset_id).repeat(
                __.in_('DERIVED_FROM')
            ).emit().path().by(__.valueMap('asset_id', 'name', 'asset_type')).toList()

            if not lineage_path:
                return {}
            
            # The result is a list of paths. We can format this into a more structured tree.
            # For simplicity here, we'll return the paths directly.
            return {"asset_id": asset_id, "lineage": lineage_path}
            
        except Exception as e:
            logger.error(f"Error fetching lineage for asset {asset_id}: {e}")
            raise
        finally:
            self.graph.close()

    async def get_user_assets(self, user_id: str) -> List[Dict]:
        """
        Finds all assets directly owned by a user.
        """
        logger.info(f"Fetching assets for user {user_id}")
        try:
            self.graph.connect()
            g = self.graph.g

            # Find the user and then traverse to all assets they own.
            assets = g.V().has('user', 'user_id', user_id).out('OWNS').valueMap(
                'asset_id', 'name', 'asset_type'
            ).toList()
            
            return assets
            
        except Exception as e:
            logger.error(f"Error fetching assets for user {user_id}: {e}")
            raise
        finally:
            self.graph.close()

    async def update_lineage(self, lineage_data: Dict[str, Any]):
        """
        Updates the graph with new lineage information.
        """
        try:
            self.graph.connect()
            g = self.graph.g
            
            processor_name = lineage_data.get("processor_name")
            processor_version = lineage_data.get("processor_version")
            
            # Create or get processor vertex
            processor_vertex = g.V().has('processor', 'processor_name', processor_name).has('version', processor_version).fold().coalesce(
                g.addV('processor').property('processor_name', processor_name).property('version', processor_version),
                g.V().has('processor', 'processor_name', processor_name).has('version', processor_version)
            ).next()

            # Process input and output assets
            for asset in lineage_data.get("output_assets", []):
                output_asset_id = asset.get("asset_id")
                output_vertex = g.V().has('asset', 'asset_id', output_asset_id).fold().coalesce(
                    g.addV('asset').property('asset_id', output_asset_id),
                    g.V().has('asset', 'asset_id', output_asset_id)
                ).next()
                
                # Link output asset to processor
                g.V(output_vertex).addE('PROCESSED_BY').to(processor_vertex).iterate()
                
                # Link to input assets
                for input_asset in lineage_data.get("input_assets", []):
                    input_asset_id = input_asset.get("asset_id")
                    input_vertex = g.V().has('asset', 'asset_id', input_asset_id).next()
                    g.V(output_vertex).addE('DERIVED_FROM').to(input_vertex).iterate()
                    
        except Exception as e:
            logger.error(f"Error updating lineage: {e}")
            raise
        finally:
            self.graph.close()

graph_service = GraphService() 