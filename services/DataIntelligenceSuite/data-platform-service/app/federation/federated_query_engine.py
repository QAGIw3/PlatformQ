"""
Federated Query Engine using Apache Trino
"""
import asyncio
import uuid
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor

import trino
import pandas as pd
from sqlparse import parse as sql_parse, format as sql_format

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError
from ..core.cache_manager import DataCacheManager
from ..core.query_router import QueryRouter, QueryPlan

logger = get_logger(__name__)


class QueryResult:
    """Represents a federated query result"""
    
    def __init__(self, query_id: str):
        self.query_id = query_id
        self.status = "pending"
        self.columns: List[Dict[str, Any]] = []
        self.rows: List[List[Any]] = []
        self.row_count = 0
        self.execution_time_ms = 0
        self.accessed_assets: List[str] = []
        self.error: Optional[str] = None
        self.stats: Dict[str, Any] = {}
        

class FederatedQueryEngine:
    """
    Federated query engine using Apache Trino.
    
    Features:
    - Cross-datasource queries
    - Query optimization
    - Result caching
    - Query monitoring
    - Cost-based optimization
    - Adaptive query execution
    """
    
    def __init__(self,
                 trino_host: str,
                 trino_port: int,
                 cache_manager: DataCacheManager,
                 query_router: QueryRouter,
                 max_workers: int = 10):
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.cache_manager = cache_manager
        self.query_router = query_router
        
        # Thread pool for blocking Trino operations
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Active queries tracking
        self.active_queries: Dict[str, QueryResult] = {}
        
        # Query statistics
        self.stats = {
            "total_queries": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "cache_hits": 0,
            "total_execution_time_ms": 0
        }
        
        # Trino connection parameters
        self.trino_params = {
            "host": trino_host,
            "port": trino_port,
            "user": "platformq",
            "catalog": "hive",
            "schema": "default",
            "http_scheme": "http",
            "verify": False
        }
    
    async def initialize(self) -> None:
        """Initialize the federated query engine"""
        try:
            # Test Trino connection
            await self._test_connection()
            
            # Load catalog metadata
            await self._load_catalogs()
            
            logger.info(f"Federated query engine initialized with Trino at {self.trino_host}:{self.trino_port}")
            
        except Exception as e:
            logger.error(f"Failed to initialize federated query engine: {e}")
            raise
    
    async def _test_connection(self) -> None:
        """Test Trino connection"""
        loop = asyncio.get_event_loop()
        
        def test_query():
            conn = trino.dbapi.connect(**self.trino_params)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result
        
        result = await loop.run_in_executor(self.executor, test_query)
        if result[0] != 1:
            raise ServiceError("Trino connection test failed")
    
    async def _load_catalogs(self) -> None:
        """Load available catalogs from Trino"""
        catalogs = await self._execute_system_query("SHOW CATALOGS")
        logger.info(f"Available catalogs: {[cat[0] for cat in catalogs]}")
    
    async def execute_query(self,
                          query: str,
                          tenant_id: str,
                          user_id: str,
                          catalog: Optional[str] = None,
                          schema: Optional[str] = None,
                          parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a federated query"""
        query_id = str(uuid.uuid4())
        result = QueryResult(query_id)
        
        try:
            # Track query
            self.active_queries[query_id] = result
            self.stats["total_queries"] += 1
            
            # Check cache first
            cache_key = self._generate_cache_key(query, parameters)
            cached_result = await self.cache_manager.get_query_result(query, parameters)
            
            if cached_result:
                logger.info(f"Cache hit for query {query_id}")
                self.stats["cache_hits"] += 1
                
                # Convert cached result
                result.status = "completed"
                result.columns = cached_result.get("columns", [])
                result.rows = cached_result.get("rows", [])
                result.row_count = cached_result.get("row_count", 0)
                result.execution_time_ms = 0  # Cached, so no execution time
                
                return result
            
            # Route query to optimal source
            plan = await self.query_router.route_query(
                query=query,
                catalog=catalog,
                schema=schema,
                hints={"tenant_id": tenant_id}
            )
            
            # Execute based on routing decision
            if plan.source == "trino":
                await self._execute_trino_query(result, plan, catalog, schema, parameters)
            else:
                # Delegate to specific data source
                await self._execute_delegated_query(result, plan, parameters)
            
            # Cache successful results
            if result.status == "completed" and result.error is None:
                await self._cache_result(cache_key, result)
                self.stats["successful_queries"] += 1
            else:
                self.stats["failed_queries"] += 1
            
            # Track execution time
            self.stats["total_execution_time_ms"] += result.execution_time_ms
            
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            result.status = "failed"
            result.error = str(e)
            self.stats["failed_queries"] += 1
            return result
            
        finally:
            # Clean up tracking
            if query_id in self.active_queries:
                del self.active_queries[query_id]
    
    async def _execute_trino_query(self,
                                 result: QueryResult,
                                 plan: QueryPlan,
                                 catalog: Optional[str],
                                 schema: Optional[str],
                                 parameters: Optional[Dict[str, Any]]) -> None:
        """Execute query using Trino"""
        start_time = datetime.utcnow()
        
        # Prepare connection parameters
        conn_params = self.trino_params.copy()
        if catalog:
            conn_params["catalog"] = catalog
        if schema:
            conn_params["schema"] = schema
        
        # Execute in thread pool
        loop = asyncio.get_event_loop()
        
        def execute_query():
            conn = trino.dbapi.connect(**conn_params)
            cursor = conn.cursor()
            
            try:
                # Set query properties for optimization
                cursor.execute("SET SESSION query_max_memory = '8GB'")
                cursor.execute("SET SESSION query_max_memory_per_node = '4GB'")
                cursor.execute("SET SESSION query_max_cpu_time = '1h'")
                
                # Execute the actual query
                if parameters:
                    # Parameterized query execution
                    cursor.execute(plan.query, parameters)
                else:
                    cursor.execute(plan.query)
                
                # Fetch column metadata
                columns = []
                if cursor.description:
                    columns = [
                        {
                            "name": desc[0],
                            "type": str(desc[1]) if desc[1] else "unknown"
                        }
                        for desc in cursor.description
                    ]
                
                # Fetch results
                rows = cursor.fetchall()
                
                # Get query stats
                stats = {}
                if hasattr(cursor, 'stats'):
                    stats = cursor.stats
                
                return columns, rows, stats
                
            finally:
                cursor.close()
                conn.close()
        
        try:
            columns, rows, stats = await loop.run_in_executor(
                self.executor, execute_query
            )
            
            # Update result
            result.status = "completed"
            result.columns = columns
            result.rows = [list(row) for row in rows]  # Convert tuples to lists
            result.row_count = len(rows)
            result.stats = stats
            
            # Extract accessed assets from plan
            result.accessed_assets = list(plan.tables)
            
            # Calculate execution time
            end_time = datetime.utcnow()
            result.execution_time_ms = int((end_time - start_time).total_seconds() * 1000)
            
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            raise
    
    async def _execute_delegated_query(self,
                                     result: QueryResult,
                                     plan: QueryPlan,
                                     parameters: Optional[Dict[str, Any]]) -> None:
        """Execute query on delegated data source"""
        # This would delegate to specific data source handlers
        # For now, we'll use Trino's ability to query multiple sources
        
        # Rewrite query to use Trino federation
        federated_query = self._rewrite_for_federation(plan.query, plan.source)
        
        # Update plan with federated query
        plan.query = federated_query
        
        # Execute via Trino
        await self._execute_trino_query(result, plan, None, None, parameters)
    
    def _rewrite_for_federation(self, query: str, source: str) -> str:
        """Rewrite query for Trino federation"""
        # Add catalog prefix based on source
        catalog_map = {
            "postgresql": "postgresql",
            "cassandra": "cassandra",
            "elasticsearch": "elasticsearch",
            "mongodb": "mongodb"
        }
        
        catalog = catalog_map.get(source, "hive")
        
        # Simple rewrite - in production, use proper SQL parser
        # This assumes table names don't have catalog prefixes
        parsed = sql_parse(query)[0]
        
        # Add catalog prefix to table references
        # This is simplified - real implementation would be more sophisticated
        rewritten = query
        
        # For now, just prepend catalog to FROM clause tables
        # Real implementation would parse and rewrite properly
        if catalog != "hive":
            rewritten = query.replace("FROM ", f"FROM {catalog}.default.")
            rewritten = rewritten.replace("JOIN ", f"JOIN {catalog}.default.")
        
        return rewritten
    
    async def _execute_system_query(self, query: str) -> List[Tuple]:
        """Execute a system query (SHOW CATALOGS, etc.)"""
        loop = asyncio.get_event_loop()
        
        def execute():
            conn = trino.dbapi.connect(**self.trino_params)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results
        
        return await loop.run_in_executor(self.executor, execute)
    
    def _generate_cache_key(self, query: str, parameters: Optional[Dict]) -> str:
        """Generate cache key for query"""
        key_data = {
            "query": query,
            "parameters": parameters or {}
        }
        return json.dumps(key_data, sort_keys=True)
    
    async def _cache_result(self, cache_key: str, result: QueryResult) -> None:
        """Cache query result"""
        cache_data = {
            "columns": result.columns,
            "rows": result.rows[:1000],  # Limit cached rows
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "cached_at": datetime.utcnow().isoformat()
        }
        
        # Determine TTL based on query characteristics
        ttl = 300  # Default 5 minutes
        
        # Longer TTL for aggregation queries
        if any(keyword in cache_key.upper() for keyword in ["GROUP BY", "SUM", "COUNT", "AVG"]):
            ttl = 3600  # 1 hour
        
        await self.cache_manager.set_query_result(
            query=cache_key,
            result=cache_data,
            ttl=ttl
        )
    
    async def cancel_query(self, query_id: str) -> bool:
        """Cancel a running query"""
        if query_id not in self.active_queries:
            return False
        
        # In Trino, we would need to track the query ID from Trino
        # and use the Trino REST API to cancel it
        # For now, just mark as cancelled
        
        result = self.active_queries.get(query_id)
        if result:
            result.status = "cancelled"
            result.error = "Query cancelled by user"
        
        return True
    
    async def get_query_status(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a query"""
        result = self.active_queries.get(query_id)
        
        if not result:
            # Check if we have a completed result in cache
            return None
        
        return {
            "query_id": query_id,
            "status": result.status,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "error": result.error
        }
    
    async def explain_query(self,
                          query: str,
                          catalog: Optional[str] = None,
                          schema: Optional[str] = None) -> Dict[str, Any]:
        """Get query execution plan"""
        explain_query = f"EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) {query}"
        
        # Execute explain query
        loop = asyncio.get_event_loop()
        
        def execute_explain():
            conn_params = self.trino_params.copy()
            if catalog:
                conn_params["catalog"] = catalog
            if schema:
                conn_params["schema"] = schema
            
            conn = trino.dbapi.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute(explain_query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return result[0] if result else None
        
        plan_json = await loop.run_in_executor(self.executor, execute_explain)
        
        if plan_json:
            plan = json.loads(plan_json)
            
            # Extract key information
            return {
                "query": query,
                "plan_type": "distributed",
                "estimated_cost": self._extract_cost_from_plan(plan),
                "plan": plan
            }
        
        return {"query": query, "error": "Failed to generate execution plan"}
    
    def _extract_cost_from_plan(self, plan: Dict[str, Any]) -> float:
        """Extract estimated cost from execution plan"""
        # Simplified cost extraction
        # Real implementation would parse the plan tree
        
        total_cost = 0.0
        
        # Look for cost estimates in plan
        if isinstance(plan, dict):
            if "cost" in plan:
                total_cost += float(plan["cost"])
            
            # Recursively search for costs
            for value in plan.values():
                if isinstance(value, (dict, list)):
                    if isinstance(value, dict):
                        total_cost += self._extract_cost_from_plan(value)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                total_cost += self._extract_cost_from_plan(item)
        
        return total_cost
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get query engine metrics"""
        total_queries = self.stats["total_queries"]
        
        return {
            "total_queries": total_queries,
            "successful_queries": self.stats["successful_queries"],
            "failed_queries": self.stats["failed_queries"],
            "cache_hit_rate": (
                self.stats["cache_hits"] / total_queries * 100 
                if total_queries > 0 else 0
            ),
            "average_execution_time_ms": (
                self.stats["total_execution_time_ms"] / total_queries
                if total_queries > 0 else 0
            ),
            "active_queries": len(self.active_queries)
        }
    
    async def health_check(self) -> None:
        """Check Trino health"""
        await self._test_connection()
    
    async def shutdown(self) -> None:
        """Shutdown the query engine"""
        # Cancel active queries
        for query_id in list(self.active_queries.keys()):
            await self.cancel_query(query_id)
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        logger.info("Federated query engine shut down") 