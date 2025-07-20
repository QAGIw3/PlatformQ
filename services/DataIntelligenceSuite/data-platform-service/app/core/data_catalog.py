"""
Data Catalog for managing metadata about data assets
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.errors import ValidationError, NotFoundError

logger = get_logger(__name__)


class DataCatalog:
    """Data catalog for asset metadata management"""
    
    def __init__(self):
        self.es_client = AsyncElasticsearch(["elasticsearch:9200"])
        self.cache = IgniteClient()
        self.index_name = "data_catalog"
        
    async def register_asset(self, asset_data: Dict[str, Any]) -> Dict[str, Any]:
        """Register a new data asset"""
        try:
            # Validate required fields
            required_fields = ["name", "type", "location", "format"]
            for field in required_fields:
                if field not in asset_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate asset ID
            asset_id = f"asset_{asset_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Prepare document
            doc = {
                "asset_id": asset_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "status": "active",
                **asset_data
            }
            
            # Index in Elasticsearch
            await self.es_client.index(
                index=self.index_name,
                id=asset_id,
                body=doc
            )
            
            # Cache for quick access
            await self.cache.put(f"catalog:asset:{asset_id}", doc, ttl=3600)
            
            logger.info(f"Registered asset: {asset_id}")
            return doc
            
        except Exception as e:
            logger.error(f"Failed to register asset: {str(e)}")
            raise
    
    async def get_asset(self, asset_id: str) -> Dict[str, Any]:
        """Get asset metadata"""
        # Check cache first
        cached = await self.cache.get(f"catalog:asset:{asset_id}")
        if cached:
            return cached
        
        # Query Elasticsearch
        try:
            result = await self.es_client.get(
                index=self.index_name,
                id=asset_id
            )
            asset = result["_source"]
            
            # Cache for future access
            await self.cache.put(f"catalog:asset:{asset_id}", asset, ttl=3600)
            
            return asset
        except Exception as e:
            raise NotFoundError(f"Asset {asset_id} not found")
    
    async def update_asset(self, asset_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update asset metadata"""
        try:
            # Get existing asset
            asset = await self.get_asset(asset_id)
            
            # Update fields
            asset.update(updates)
            asset["updated_at"] = datetime.utcnow().isoformat()
            
            # Update in Elasticsearch
            await self.es_client.update(
                index=self.index_name,
                id=asset_id,
                body={"doc": updates}
            )
            
            # Update cache
            await self.cache.put(f"catalog:asset:{asset_id}", asset, ttl=3600)
            
            logger.info(f"Updated asset: {asset_id}")
            return asset
            
        except Exception as e:
            logger.error(f"Failed to update asset: {str(e)}")
            raise
    
    async def search_assets(self, 
                          query: Optional[str] = None,
                          filters: Optional[Dict] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Search for assets"""
        try:
            # Build search query
            search_body = {
                "size": limit,
                "query": {
                    "bool": {
                        "must": []
                    }
                }
            }
            
            # Add text search if query provided
            if query:
                search_body["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query,
                        "fields": ["name^2", "description", "tags", "type"]
                    }
                })
            
            # Add filters
            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        search_body["query"]["bool"]["must"].append({
                            "terms": {field: value}
                        })
                    else:
                        search_body["query"]["bool"]["must"].append({
                            "term": {field: value}
                        })
            
            # Execute search
            result = await self.es_client.search(
                index=self.index_name,
                body=search_body
            )
            
            assets = [hit["_source"] for hit in result["hits"]["hits"]]
            
            return assets
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise
    
    async def add_column_metadata(self, asset_id: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Add column metadata to an asset"""
        try:
            asset = await self.get_asset(asset_id)
            
            # Process column metadata
            processed_columns = []
            for col in columns:
                column_doc = {
                    "name": col["name"],
                    "type": col["type"],
                    "nullable": col.get("nullable", True),
                    "description": col.get("description", ""),
                    "tags": col.get("tags", []),
                    "statistics": col.get("statistics", {})
                }
                processed_columns.append(column_doc)
            
            # Update asset
            updates = {
                "columns": processed_columns,
                "column_count": len(processed_columns),
                "schema_updated_at": datetime.utcnow().isoformat()
            }
            
            return await self.update_asset(asset_id, updates)
            
        except Exception as e:
            logger.error(f"Failed to add column metadata: {str(e)}")
            raise
    
    async def get_asset_lineage(self, asset_id: str) -> Dict[str, Any]:
        """Get asset lineage information"""
        # Query lineage from graph database (via lineage service)
        # For now, return mock data
        return {
            "asset_id": asset_id,
            "upstream": [
                {"asset_id": "asset_123", "relationship": "derived_from"},
                {"asset_id": "asset_456", "relationship": "joined_with"}
            ],
            "downstream": [
                {"asset_id": "asset_789", "relationship": "feeds_into"}
            ]
        }
    
    async def add_tags(self, asset_id: str, tags: List[str]) -> Dict[str, Any]:
        """Add tags to an asset"""
        asset = await self.get_asset(asset_id)
        existing_tags = set(asset.get("tags", []))
        existing_tags.update(tags)
        
        return await self.update_asset(asset_id, {"tags": list(existing_tags)})
    
    async def deprecate_asset(self, asset_id: str, reason: str) -> Dict[str, Any]:
        """Mark an asset as deprecated"""
        updates = {
            "status": "deprecated",
            "deprecated_at": datetime.utcnow().isoformat(),
            "deprecation_reason": reason
        }
        
        asset = await self.update_asset(asset_id, updates)
        
        # Remove from cache to force refresh
        await self.cache.delete(f"catalog:asset:{asset_id}")
        
        return asset
    
    async def get_asset_statistics(self, asset_id: str) -> Dict[str, Any]:
        """Get asset statistics"""
        asset = await self.get_asset(asset_id)
        
        # Return existing statistics or compute them
        if "statistics" in asset:
            return asset["statistics"]
        
        # Mock statistics for now
        return {
            "row_count": 1000000,
            "size_bytes": 1024 * 1024 * 100,  # 100MB
            "last_updated": asset.get("updated_at"),
            "access_frequency": "daily",
            "quality_score": 0.85
        }
    
    async def bulk_import(self, assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk import assets"""
        success_count = 0
        failed_assets = []
        
        for asset_data in assets:
            try:
                await self.register_asset(asset_data)
                success_count += 1
            except Exception as e:
                failed_assets.append({
                    "asset": asset_data.get("name", "unknown"),
                    "error": str(e)
                })
        
        return {
            "total": len(assets),
            "success": success_count,
            "failed": len(failed_assets),
            "failures": failed_assets
        } 