"""
MongoDB Connector Plugin

Provides MongoDB database connectivity for the Integration Hub.
"""

from typing import Dict, Any, List, Optional, Union
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import UpdateOne, InsertOne, DeleteOne
from bson import ObjectId

from ...core.base import BaseConnector, ConnectorConfig
from data_intelligence_common.core.patterns.resilience import ResiliencePolicy
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MongoDBConnector(BaseConnector):
    """MongoDB database connector implementation"""
    
    def __init__(self, config: ConnectorConfig, resilience_policy: ResiliencePolicy):
        super().__init__(config, resilience_policy)
        self.supports_pooling = True  # Motor handles pooling internally
        self.supports_credential_rotation = True
        
        # Connection details
        self._client: Optional[AsyncIOMotorClient] = None
        self._database: Optional[AsyncIOMotorDatabase] = None
        self._connection_string = self._build_connection_string()
        
    def _build_connection_string(self) -> str:
        """Build MongoDB connection string"""
        conn_config = self.config.connection_config
        
        # Build basic connection string
        auth = ""
        if conn_config.username and conn_config.password:
            auth = f"{conn_config.username}:{conn_config.password}@"
            
        protocol = "mongodb+srv" if conn_config.ssl_enabled else "mongodb"
        base_url = f"{protocol}://{auth}{conn_config.host}:{conn_config.port}"
        
        # Add database
        database = conn_config.database or "admin"
        connection_string = f"{base_url}/{database}"
        
        # Add additional parameters
        params = []
        if conn_config.ssl_enabled:
            params.append("ssl=true")
        if conn_config.additional_params.get("replica_set"):
            params.append(f"replicaSet={conn_config.additional_params['replica_set']}")
        if conn_config.additional_params.get("auth_source"):
            params.append(f"authSource={conn_config.additional_params['auth_source']}")
            
        if params:
            connection_string += "?" + "&".join(params)
            
        return connection_string
        
    async def initialize(self):
        """Initialize MongoDB connection"""
        try:
            # Create client with resilience
            @self.resilience_policy.apply()
            async def create_client():
                client = AsyncIOMotorClient(
                    self._connection_string,
                    serverSelectionTimeoutMS=self.config.connection_config.timeout * 1000,
                    connectTimeoutMS=self.config.connection_config.timeout * 1000,
                    maxPoolSize=self.config.connection_config.pool_size
                )
                
                # Test connection
                await client.server_info()
                return client
                
            self._client = await create_client()
            self._database = self._client[self.config.connection_config.database]
            
            logger.info(f"MongoDB connector initialized for {self.config.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connector: {e}")
            raise
            
    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            await self._client.server_info()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
            
    async def query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        # MongoDB uses collection.method syntax
        # Parse query string format: "collection.find|aggregate|etc"
        parts = query.split(".", 1)
        if len(parts) != 2:
            raise ValueError("Query must be in format: collection.method")
            
        collection_name, method_call = parts
        collection = self._database[collection_name]
        
        # Parse method and arguments
        if method_call.startswith("find"):
            # Extract filter from parameters or method call
            filter_doc = parameters or {}
            projection = None
            
            if "(" in method_call and ")" in method_call:
                # Parse find(filter, projection) syntax
                import json
                args_str = method_call[method_call.index("(")+1:method_call.rindex(")")]
                if args_str:
                    args = json.loads(f"[{args_str}]")
                    if len(args) > 0:
                        filter_doc = args[0]
                    if len(args) > 1:
                        projection = args[1]
                        
            @self.resilience_policy.apply()
            async def execute_find():
                cursor = collection.find(filter_doc, projection)
                results = []
                async for doc in cursor:
                    # Convert ObjectId to string
                    if "_id" in doc and isinstance(doc["_id"], ObjectId):
                        doc["_id"] = str(doc["_id"])
                    results.append(doc)
                return results
                
            return await execute_find()
            
        elif method_call.startswith("aggregate"):
            # Extract pipeline from parameters
            pipeline = parameters.get("pipeline", [])
            
            @self.resilience_policy.apply()
            async def execute_aggregate():
                cursor = collection.aggregate(pipeline)
                results = []
                async for doc in cursor:
                    if "_id" in doc and isinstance(doc["_id"], ObjectId):
                        doc["_id"] = str(doc["_id"])
                    results.append(doc)
                return results
                
            return await execute_aggregate()
            
        else:
            raise ValueError(f"Unsupported method: {method_call}")
            
    async def write(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Write data to MongoDB"""
        write_config = config or {}
        collection_name = write_config.get('collection')
        if not collection_name:
            raise ValueError("Collection name is required in write config")
            
        collection = self._database[collection_name]
        
        # Ensure data is a list
        records = data if isinstance(data, list) else [data]
        if not records:
            return {"documents_affected": 0}
            
        @self.resilience_policy.apply()
        async def execute_write():
            operation = write_config.get('operation', 'insert')
            
            if operation == 'insert':
                # Bulk insert
                result = await collection.insert_many(records)
                return len(result.inserted_ids)
                
            elif operation == 'upsert':
                # Bulk upsert
                operations = []
                for record in records:
                    # Extract filter fields
                    filter_fields = write_config.get('filter_fields', ['_id'])
                    filter_doc = {
                        field: record.get(field) 
                        for field in filter_fields 
                        if field in record
                    }
                    
                    if not filter_doc:
                        # No filter, just insert
                        operations.append(InsertOne(record))
                    else:
                        # Upsert
                        operations.append(
                            UpdateOne(
                                filter_doc,
                                {"$set": record},
                                upsert=True
                            )
                        )
                        
                if operations:
                    result = await collection.bulk_write(operations)
                    return result.modified_count + result.upserted_count + result.inserted_count
                return 0
                
            elif operation == 'update':
                # Bulk update
                operations = []
                for record in records:
                    filter_fields = write_config.get('filter_fields', ['_id'])
                    filter_doc = {
                        field: record.get(field) 
                        for field in filter_fields 
                        if field in record
                    }
                    
                    if filter_doc:
                        # Remove filter fields from update
                        update_doc = {
                            k: v for k, v in record.items() 
                            if k not in filter_fields
                        }
                        
                        operations.append(
                            UpdateOne(
                                filter_doc,
                                {"$set": update_doc}
                            )
                        )
                        
                if operations:
                    result = await collection.bulk_write(operations)
                    return result.modified_count
                return 0
                
            else:
                raise ValueError(f"Unsupported operation: {operation}")
                
        try:
            documents_affected = await execute_write()
            
            logger.info(
                "Data written",
                connector=self.config.name,
                collection=collection_name,
                documents_affected=documents_affected
            )
            
            return {
                "documents_affected": documents_affected,
                "collection": collection_name,
                "operation": write_config.get('operation', 'insert'),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Write operation failed: {e}", collection=collection_name)
            raise
            
    async def check_health(self) -> Dict[str, Any]:
        """Check connector health"""
        try:
            # Get server info
            server_info = await self._client.server_info()
            
            # Get database stats
            stats = await self._database.command("dbStats")
            
            return {
                "healthy": True,
                "version": server_info.get("version"),
                "collections": stats.get("collections", 0),
                "objects": stats.get("objects", 0),
                "dataSize": stats.get("dataSize", 0)
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            }
            
    async def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info(f"MongoDB connector {self.config.name} closed")
            
    async def update_credentials(self, credentials: Dict[str, Any]):
        """Update connection credentials"""
        # Update connection string
        self.config.connection_config.username = credentials.get('username')
        self.config.connection_config.password = credentials.get('password')
        self._connection_string = self._build_connection_string()
        
        # Recreate client
        old_client = self._client
        await self.initialize()
        
        # Close old client
        if old_client:
            old_client.close()
            
        logger.info(f"Credentials updated for {self.config.name}")
        
    # Additional MongoDB-specific methods
    
    async def create_index(
        self,
        collection: str,
        index_spec: Union[str, List[tuple]],
        **kwargs
    ) -> str:
        """Create an index on a collection"""
        coll = self._database[collection]
        
        @self.resilience_policy.apply()
        async def create_index():
            return await coll.create_index(index_spec, **kwargs)
            
        index_name = await create_index()
        
        logger.info(
            "Index created",
            connector=self.config.name,
            collection=collection,
            index_name=index_name
        )
        
        return index_name
        
    async def execute_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a database command"""
        @self.resilience_policy.apply()
        async def execute_command():
            return await self._database.command(command)
            
        return await execute_command()
        
    async def watch_changes(
        self,
        collection: Optional[str] = None,
        pipeline: Optional[List[Dict[str, Any]]] = None,
        full_document: str = "updateLookup"
    ):
        """Watch for changes using change streams"""
        target = self._database[collection] if collection else self._database
        
        async with target.watch(
            pipeline=pipeline,
            full_document=full_document
        ) as change_stream:
            async for change in change_stream:
                yield change
                
    async def get_collection_stats(self, collection: str) -> Dict[str, Any]:
        """Get collection statistics"""
        coll = self._database[collection]
        
        stats = await self._database.command("collStats", collection)
        count = await coll.count_documents({})
        
        return {
            "collection": collection,
            "count": count,
            "size": stats.get("size", 0),
            "avgObjSize": stats.get("avgObjSize", 0),
            "storageSize": stats.get("storageSize", 0),
            "indexes": stats.get("nindexes", 0)
        }


# Plugin registration
__connector_class__ = MongoDBConnector
__connector_type__ = "mongodb" 