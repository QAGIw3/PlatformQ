import logging
from ..db.janusgraph import JanusGraph
from typing import Dict, List

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
            self.graph.close() # Ensure connection is closed

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


graph_service = GraphService() 