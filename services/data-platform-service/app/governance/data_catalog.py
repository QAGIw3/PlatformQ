"""
Enhanced Data Catalog Manager with auto-discovery and business metadata
"""
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
import json
from enum import Enum

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import networkx as nx

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, NotFoundError
from ..core.connection_manager import UnifiedConnectionManager
from ..core.cache_manager import DataCacheManager

logger = get_logger(__name__)


class AssetType(str, Enum):
    """Types of data assets"""
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    REPORT = "report"
    DASHBOARD = "dashboard"
    MODEL = "model"


class DataClassification(str, Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"
    PHI = "phi"


class DataCatalogManager:
    """
    Enhanced data catalog for comprehensive metadata management.
    
    Features:
    - Auto-discovery of data assets
    - Business metadata and glossary
    - Data classification
    - Ownership and stewardship
    - Usage tracking
    - Search and discovery
    - Integration with lineage
    """
    
    def __init__(self,
                 connection_manager: UnifiedConnectionManager,
                 atlas_url: Optional[str] = None,
                 enable_auto_discovery: bool = True):
        self.connection_manager = connection_manager
        self.atlas_url = atlas_url
        self.enable_auto_discovery = enable_auto_discovery
        
        # Elasticsearch client
        self.es_client: Optional[AsyncElasticsearch] = None
        
        # Cache manager
        self.cache = DataCacheManager()
        
        # Catalog indices
        self.asset_index = "platformq_catalog_assets"
        self.glossary_index = "platformq_catalog_glossary"
        self.tag_index = "platformq_catalog_tags"
        
        # Discovery state
        self.discovered_assets: Set[str] = set()
        self.discovery_running = False
        
        # Statistics
        self.stats = {
            "total_assets": 0,
            "discovered_assets": 0,
            "classified_assets": 0,
            "tagged_assets": 0
        }
    
    async def initialize(self) -> None:
        """Initialize the catalog manager"""
        try:
            # Get Elasticsearch client from connection manager
            self.es_client = self.connection_manager.get_elasticsearch_client()
            
            # Create indices if not exist
            await self._create_indices()
            
            # Load existing catalog
            await self._load_catalog_stats()
            
            # Start auto-discovery if enabled
            if self.enable_auto_discovery:
                asyncio.create_task(self._auto_discovery_loop())
            
            logger.info("Data catalog manager initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize catalog manager: {e}")
            raise
    
    async def _create_indices(self) -> None:
        """Create Elasticsearch indices for catalog"""
        # Asset index mapping
        asset_mapping = {
            "mappings": {
                "properties": {
                    "asset_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "display_name": {"type": "text"},
                    "description": {"type": "text"},
                    "asset_type": {"type": "keyword"},
                    "location": {"type": "keyword"},
                    "format": {"type": "keyword"},
                    "classification": {"type": "keyword"},
                    "owner": {"type": "keyword"},
                    "steward": {"type": "keyword"},
                    "team": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "business_terms": {"type": "keyword"},
                    "schema": {"type": "object", "enabled": False},
                    "statistics": {"type": "object"},
                    "quality_score": {"type": "float"},
                    "usage_count": {"type": "integer"},
                    "last_accessed": {"type": "date"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "discovered_at": {"type": "date"},
                    "tenant_id": {"type": "keyword"},
                    "custom_properties": {"type": "object", "enabled": False}
                }
            }
        }
        
        # Create asset index
        if not await self.es_client.indices.exists(index=self.asset_index):
            await self.es_client.indices.create(index=self.asset_index, body=asset_mapping)
            logger.info(f"Created index: {self.asset_index}")
        
        # Glossary index mapping
        glossary_mapping = {
            "mappings": {
                "properties": {
                    "term_id": {"type": "keyword"},
                    "term": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "definition": {"type": "text"},
                    "synonyms": {"type": "text"},
                    "related_terms": {"type": "keyword"},
                    "domain": {"type": "keyword"},
                    "owner": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            }
        }
        
        # Create glossary index
        if not await self.es_client.indices.exists(index=self.glossary_index):
            await self.es_client.indices.create(index=self.glossary_index, body=glossary_mapping)
            logger.info(f"Created index: {self.glossary_index}")
    
    async def register_asset(self,
                           asset_data: Dict[str, Any],
                           tenant_id: str) -> Dict[str, Any]:
        """Register a new data asset"""
        try:
            # Validate required fields
            required_fields = ["name", "asset_type", "location"]
            for field in required_fields:
                if field not in asset_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate asset ID
            asset_id = f"{tenant_id}_{asset_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Prepare document
            doc = {
                "asset_id": asset_id,
                "tenant_id": tenant_id,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "discovered_at": datetime.utcnow(),
                "usage_count": 0,
                "quality_score": 0.0,
                **asset_data
            }
            
            # Auto-classify if not provided
            if "classification" not in doc:
                doc["classification"] = await self._auto_classify(asset_data)
            
            # Index in Elasticsearch
            await self.es_client.index(
                index=self.asset_index,
                id=asset_id,
                body=doc
            )
            
            # Update cache
            await self.cache.set_metadata("asset", asset_id, doc)
            
            # Update statistics
            self.stats["total_assets"] += 1
            
            logger.info(f"Registered asset: {asset_id}")
            
            return doc
            
        except Exception as e:
            logger.error(f"Failed to register asset: {e}")
            raise
    
    async def update_asset(self,
                         asset_id: str,
                         updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update asset metadata"""
        try:
            # Get existing asset
            result = await self.es_client.get(index=self.asset_index, id=asset_id)
            asset = result["_source"]
            
            # Update fields
            asset.update(updates)
            asset["updated_at"] = datetime.utcnow()
            
            # Update in Elasticsearch
            await self.es_client.update(
                index=self.asset_index,
                id=asset_id,
                body={"doc": asset}
            )
            
            # Update cache
            await self.cache.set_metadata("asset", asset_id, asset)
            
            logger.info(f"Updated asset: {asset_id}")
            
            return asset
            
        except Exception as e:
            logger.error(f"Failed to update asset: {e}")
            raise
    
    async def search_assets(self,
                          query: str,
                          tenant_id: str,
                          filters: Optional[Dict[str, Any]] = None,
                          limit: int = 50,
                          offset: int = 0) -> Dict[str, Any]:
        """Search for assets with filters"""
        try:
            # Build search query
            must_clauses = [
                {"term": {"tenant_id": tenant_id}}
            ]
            
            # Add text search
            if query:
                must_clauses.append({
                    "multi_match": {
                        "query": query,
                        "fields": ["name^3", "display_name^2", "description", "tags"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                })
            
            # Add filters
            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        must_clauses.append({"terms": {field: value}})
                    else:
                        must_clauses.append({"term": {field: value}})
            
            # Build aggregations
            aggs = {
                "asset_types": {"terms": {"field": "asset_type"}},
                "classifications": {"terms": {"field": "classification"}},
                "owners": {"terms": {"field": "owner"}},
                "tags": {"terms": {"field": "tags", "size": 20}}
            }
            
            # Execute search
            search_body = {
                "query": {"bool": {"must": must_clauses}},
                "aggs": aggs,
                "from": offset,
                "size": limit,
                "highlight": {
                    "fields": {
                        "name": {},
                        "description": {}
                    }
                }
            }
            
            result = await self.es_client.search(
                index=self.asset_index,
                body=search_body
            )
            
            # Format response
            assets = []
            for hit in result["hits"]["hits"]:
                asset = hit["_source"]
                asset["_score"] = hit["_score"]
                if "highlight" in hit:
                    asset["_highlight"] = hit["highlight"]
                assets.append(asset)
            
            return {
                "assets": assets,
                "total": result["hits"]["total"]["value"],
                "aggregations": result.get("aggregations", {})
            }
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def add_business_glossary_term(self,
                                       term: str,
                                       definition: str,
                                       domain: str,
                                       owner: str,
                                       synonyms: Optional[List[str]] = None,
                                       related_terms: Optional[List[str]] = None) -> Dict[str, Any]:
        """Add a business glossary term"""
        try:
            term_id = f"term_{term.lower().replace(' ', '_')}_{datetime.utcnow().timestamp()}"
            
            doc = {
                "term_id": term_id,
                "term": term,
                "definition": definition,
                "domain": domain,
                "owner": owner,
                "synonyms": synonyms or [],
                "related_terms": related_terms or [],
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            await self.es_client.index(
                index=self.glossary_index,
                id=term_id,
                body=doc
            )
            
            logger.info(f"Added glossary term: {term}")
            
            return doc
            
        except Exception as e:
            logger.error(f"Failed to add glossary term: {e}")
            raise
    
    async def link_business_terms(self,
                                asset_id: str,
                                term_ids: List[str]) -> None:
        """Link business terms to an asset"""
        try:
            # Update asset with business terms
            await self.es_client.update(
                index=self.asset_index,
                id=asset_id,
                body={
                    "doc": {
                        "business_terms": term_ids,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            logger.info(f"Linked {len(term_ids)} business terms to asset {asset_id}")
            
        except Exception as e:
            logger.error(f"Failed to link business terms: {e}")
            raise
    
    async def classify_asset(self,
                           asset_id: str,
                           classification: DataClassification,
                           reason: Optional[str] = None) -> None:
        """Classify an asset"""
        try:
            updates = {
                "classification": classification.value,
                "classification_reason": reason,
                "classification_date": datetime.utcnow()
            }
            
            await self.update_asset(asset_id, updates)
            
            self.stats["classified_assets"] += 1
            
            logger.info(f"Classified asset {asset_id} as {classification.value}")
            
        except Exception as e:
            logger.error(f"Failed to classify asset: {e}")
            raise
    
    async def _auto_classify(self, asset_data: Dict[str, Any]) -> str:
        """Auto-classify an asset based on metadata"""
        name = asset_data.get("name", "").lower()
        description = asset_data.get("description", "").lower()
        tags = asset_data.get("tags", [])
        
        # Check for PII indicators
        pii_indicators = ["email", "ssn", "phone", "address", "credit_card", "passport"]
        for indicator in pii_indicators:
            if indicator in name or indicator in description or indicator in tags:
                return DataClassification.PII.value
        
        # Check for PHI indicators
        phi_indicators = ["patient", "medical", "health", "diagnosis", "prescription"]
        for indicator in phi_indicators:
            if indicator in name or indicator in description or indicator in tags:
                return DataClassification.PHI.value
        
        # Check for confidential indicators
        confidential_indicators = ["salary", "revenue", "profit", "financial", "strategic"]
        for indicator in confidential_indicators:
            if indicator in name or indicator in description or indicator in tags:
                return DataClassification.CONFIDENTIAL.value
        
        # Default to internal
        return DataClassification.INTERNAL.value
    
    async def track_usage(self,
                        asset_id: str,
                        user_id: str,
                        access_type: str = "read") -> None:
        """Track asset usage"""
        try:
            # Update usage count and last accessed
            await self.es_client.update(
                index=self.asset_index,
                id=asset_id,
                body={
                    "script": {
                        "source": "ctx._source.usage_count += 1; ctx._source.last_accessed = params.now",
                        "params": {"now": datetime.utcnow()}
                    }
                }
            )
            
            # Log usage event
            usage_event = {
                "asset_id": asset_id,
                "user_id": user_id,
                "access_type": access_type,
                "timestamp": datetime.utcnow()
            }
            
            # In production, would store in usage tracking system
            logger.debug(f"Tracked usage: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to track usage: {e}")
    
    async def get_popular_assets(self,
                               tenant_id: str,
                               days: int = 7,
                               limit: int = 10) -> List[Dict[str, Any]]:
        """Get most popular assets by usage"""
        try:
            # Query for most used assets in time period
            query = {
                "bool": {
                    "must": [
                        {"term": {"tenant_id": tenant_id}},
                        {"range": {"last_accessed": {"gte": f"now-{days}d"}}}
                    ]
                }
            }
            
            result = await self.es_client.search(
                index=self.asset_index,
                body={
                    "query": query,
                    "sort": [{"usage_count": {"order": "desc"}}],
                    "size": limit
                }
            )
            
            assets = [hit["_source"] for hit in result["hits"]["hits"]]
            
            return assets
            
        except Exception as e:
            logger.error(f"Failed to get popular assets: {e}")
            raise
    
    async def _auto_discovery_loop(self) -> None:
        """Background task for auto-discovering assets"""
        while True:
            try:
                if not self.discovery_running:
                    self.discovery_running = True
                    
                    # Discover from various sources
                    await self._discover_database_assets()
                    await self._discover_file_assets()
                    await self._discover_api_assets()
                    
                    self.discovery_running = False
                
                # Run discovery every hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Auto-discovery error: {e}")
                self.discovery_running = False
                await asyncio.sleep(300)  # Retry in 5 minutes
    
    async def _discover_database_assets(self) -> None:
        """Discover assets from connected databases"""
        try:
            # PostgreSQL discovery
            if "postgresql" in self.connection_manager.connections:
                await self._discover_postgresql_assets()
            
            # Cassandra discovery
            if "cassandra" in self.connection_manager.connections:
                await self._discover_cassandra_assets()
            
            # MongoDB discovery
            if "mongodb" in self.connection_manager.connections:
                await self._discover_mongodb_assets()
                
        except Exception as e:
            logger.error(f"Database discovery error: {e}")
    
    async def _discover_postgresql_assets(self) -> None:
        """Discover PostgreSQL tables and views"""
        try:
            async with self.connection_manager.get_postgresql_connection() as conn:
                # Query information schema
                query = """
                SELECT 
                    table_schema,
                    table_name,
                    table_type,
                    obj_description(pgc.oid) as comment
                FROM information_schema.tables t
                JOIN pg_class pgc ON pgc.relname = t.table_name
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                """
                
                rows = await conn.fetch(query)
                
                for row in rows:
                    asset_key = f"postgresql_{row['table_schema']}_{row['table_name']}"
                    
                    if asset_key not in self.discovered_assets:
                        asset_data = {
                            "name": f"{row['table_schema']}.{row['table_name']}",
                            "display_name": row['table_name'],
                            "asset_type": AssetType.TABLE if row['table_type'] == 'BASE TABLE' else AssetType.VIEW,
                            "location": f"postgresql://{row['table_schema']}.{row['table_name']}",
                            "format": "postgresql",
                            "description": row['comment'] or f"PostgreSQL {row['table_type'].lower()}",
                            "tags": ["postgresql", "discovered", row['table_schema']]
                        }
                        
                        # Get column information
                        col_query = """
                        SELECT 
                            column_name,
                            data_type,
                            is_nullable,
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                        ORDER BY ordinal_position
                        """
                        
                        columns = await conn.fetch(col_query, row['table_schema'], row['table_name'])
                        
                        schema_info = []
                        for col in columns:
                            schema_info.append({
                                "name": col['column_name'],
                                "type": col['data_type'],
                                "nullable": col['is_nullable'] == 'YES',
                                "default": col['column_default']
                            })
                        
                        asset_data["schema"] = {"columns": schema_info}
                        
                        # Register asset
                        await self.register_asset(asset_data, "system")
                        
                        self.discovered_assets.add(asset_key)
                        self.stats["discovered_assets"] += 1
                        
        except Exception as e:
            logger.error(f"PostgreSQL discovery error: {e}")
    
    async def _discover_cassandra_assets(self) -> None:
        """Discover Cassandra tables"""
        try:
            session = self.connection_manager.get_cassandra_session()
            
            # Get all keyspaces
            keyspaces_query = "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name NOT LIKE 'system%'"
            keyspaces = session.execute(keyspaces_query)
            
            for keyspace_row in keyspaces:
                keyspace = keyspace_row.keyspace_name
                
                # Get tables in keyspace
                tables_query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
                tables = session.execute(tables_query)
                
                for table_row in tables:
                    table = table_row.table_name
                    asset_key = f"cassandra_{keyspace}_{table}"
                    
                    if asset_key not in self.discovered_assets:
                        asset_data = {
                            "name": f"{keyspace}.{table}",
                            "display_name": table,
                            "asset_type": AssetType.TABLE,
                            "location": f"cassandra://{keyspace}.{table}",
                            "format": "cassandra",
                            "description": f"Cassandra table in {keyspace} keyspace",
                            "tags": ["cassandra", "discovered", keyspace, "nosql"]
                        }
                        
                        # Get column information
                        columns_query = f"""
                        SELECT column_name, type, kind 
                        FROM system_schema.columns 
                        WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'
                        """
                        columns = session.execute(columns_query)
                        
                        schema_info = []
                        for col in columns:
                            schema_info.append({
                                "name": col.column_name,
                                "type": col.type,
                                "kind": col.kind  # partition_key, clustering, regular
                            })
                        
                        asset_data["schema"] = {"columns": schema_info}
                        
                        # Register asset
                        await self.register_asset(asset_data, "system")
                        
                        self.discovered_assets.add(asset_key)
                        self.stats["discovered_assets"] += 1
                        
        except Exception as e:
            logger.error(f"Cassandra discovery error: {e}")
    
    async def _discover_mongodb_assets(self) -> None:
        """Discover MongoDB collections"""
        try:
            async with self.connection_manager.get_mongodb_database("admin") as admin_db:
                # List all databases
                databases = await admin_db.list_database_names()
                
                for db_name in databases:
                    if db_name not in ["admin", "local", "config"]:
                        async with self.connection_manager.get_mongodb_database(db_name) as db:
                            # List collections
                            collections = await db.list_collection_names()
                            
                            for collection in collections:
                                asset_key = f"mongodb_{db_name}_{collection}"
                                
                                if asset_key not in self.discovered_assets:
                                    asset_data = {
                                        "name": f"{db_name}.{collection}",
                                        "display_name": collection,
                                        "asset_type": AssetType.TABLE,
                                        "location": f"mongodb://{db_name}.{collection}",
                                        "format": "mongodb",
                                        "description": f"MongoDB collection in {db_name} database",
                                        "tags": ["mongodb", "discovered", db_name, "nosql", "document"]
                                    }
                                    
                                    # Sample documents to infer schema
                                    sample_docs = await db[collection].find().limit(100).to_list(100)
                                    
                                    if sample_docs:
                                        # Infer schema from sample
                                        schema_info = self._infer_mongodb_schema(sample_docs)
                                        asset_data["schema"] = {"fields": schema_info}
                                        
                                        # Get collection stats
                                        stats = await db.command("collStats", collection)
                                        asset_data["statistics"] = {
                                            "count": stats.get("count", 0),
                                            "size": stats.get("size", 0),
                                            "avgObjSize": stats.get("avgObjSize", 0)
                                        }
                                    
                                    # Register asset
                                    await self.register_asset(asset_data, "system")
                                    
                                    self.discovered_assets.add(asset_key)
                                    self.stats["discovered_assets"] += 1
                                    
        except Exception as e:
            logger.error(f"MongoDB discovery error: {e}")
    
    def _infer_mongodb_schema(self, documents: List[Dict]) -> List[Dict[str, Any]]:
        """Infer schema from MongoDB documents"""
        field_info = {}
        
        for doc in documents:
            self._analyze_document(doc, field_info)
        
        # Convert to list format
        schema = []
        for field, info in field_info.items():
            schema.append({
                "name": field,
                "types": list(info["types"]),
                "occurrence": info["count"] / len(documents),
                "nullable": info["count"] < len(documents)
            })
        
        return schema
    
    def _analyze_document(self, doc: Dict, field_info: Dict, prefix: str = "") -> None:
        """Recursively analyze document structure"""
        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            
            if field_path not in field_info:
                field_info[field_path] = {"types": set(), "count": 0}
            
            field_info[field_path]["count"] += 1
            
            if value is None:
                field_info[field_path]["types"].add("null")
            elif isinstance(value, bool):
                field_info[field_path]["types"].add("boolean")
            elif isinstance(value, int):
                field_info[field_path]["types"].add("integer")
            elif isinstance(value, float):
                field_info[field_path]["types"].add("float")
            elif isinstance(value, str):
                field_info[field_path]["types"].add("string")
            elif isinstance(value, list):
                field_info[field_path]["types"].add("array")
            elif isinstance(value, dict):
                field_info[field_path]["types"].add("object")
                # Recurse into nested objects
                self._analyze_document(value, field_info, field_path)
            else:
                field_info[field_path]["types"].add(type(value).__name__)
    
    async def _discover_file_assets(self) -> None:
        """Discover file-based assets from data lake"""
        # This would integrate with the lake manager
        # to discover datasets in Bronze/Silver/Gold zones
        pass
    
    async def _discover_api_assets(self) -> None:
        """Discover API endpoints as data assets"""
        # This would discover REST/GraphQL APIs
        # registered in the platform
        pass
    
    async def _load_catalog_stats(self) -> None:
        """Load catalog statistics"""
        try:
            # Count total assets
            count_result = await self.es_client.count(index=self.asset_index)
            self.stats["total_assets"] = count_result["count"]
            
            # Count by classification
            agg_result = await self.es_client.search(
                index=self.asset_index,
                body={
                    "size": 0,
                    "aggs": {
                        "classifications": {
                            "terms": {"field": "classification"}
                        }
                    }
                }
            )
            
            classifications = agg_result["aggregations"]["classifications"]["buckets"]
            classified_count = sum(c["doc_count"] for c in classifications if c["key"] != "unclassified")
            self.stats["classified_assets"] = classified_count
            
        except Exception as e:
            logger.error(f"Failed to load catalog stats: {e}")
    
    async def get_asset_count(self) -> int:
        """Get total asset count"""
        return self.stats["total_assets"]
    
    async def get_catalog_statistics(self) -> Dict[str, Any]:
        """Get comprehensive catalog statistics"""
        try:
            # Get various aggregations
            result = await self.es_client.search(
                index=self.asset_index,
                body={
                    "size": 0,
                    "aggs": {
                        "by_type": {"terms": {"field": "asset_type"}},
                        "by_classification": {"terms": {"field": "classification"}},
                        "by_owner": {"terms": {"field": "owner", "size": 20}},
                        "by_format": {"terms": {"field": "format"}},
                        "quality_avg": {"avg": {"field": "quality_score"}},
                        "recently_updated": {
                            "date_histogram": {
                                "field": "updated_at",
                                "calendar_interval": "day",
                                "min_doc_count": 1
                            }
                        }
                    }
                }
            )
            
            return {
                "total_assets": self.stats["total_assets"],
                "discovered_assets": self.stats["discovered_assets"],
                "classified_assets": self.stats["classified_assets"],
                "aggregations": result["aggregations"]
            }
            
        except Exception as e:
            logger.error(f"Failed to get catalog statistics: {e}")
            raise
    
    async def export_catalog(self,
                           tenant_id: str,
                           format: str = "json") -> Dict[str, Any]:
        """Export catalog data"""
        try:
            # Scroll through all assets
            assets = []
            
            # Initial search
            result = await self.es_client.search(
                index=self.asset_index,
                body={
                    "query": {"term": {"tenant_id": tenant_id}},
                    "size": 1000
                },
                scroll="2m"
            )
            
            scroll_id = result["_scroll_id"]
            assets.extend([hit["_source"] for hit in result["hits"]["hits"]])
            
            # Continue scrolling
            while len(result["hits"]["hits"]) > 0:
                result = await self.es_client.scroll(
                    scroll_id=scroll_id,
                    scroll="2m"
                )
                assets.extend([hit["_source"] for hit in result["hits"]["hits"]])
            
            # Clear scroll
            await self.es_client.clear_scroll(scroll_id=scroll_id)
            
            # Format output
            if format == "json":
                return {
                    "export_date": datetime.utcnow().isoformat(),
                    "tenant_id": tenant_id,
                    "asset_count": len(assets),
                    "assets": assets
                }
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Failed to export catalog: {e}")
            raise
    
    async def shutdown(self) -> None:
        """Shutdown catalog manager"""
        self.discovery_running = False
        logger.info("Data catalog manager shut down") 