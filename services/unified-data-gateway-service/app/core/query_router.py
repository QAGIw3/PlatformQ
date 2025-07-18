"""
Query Router

Intelligently routes queries to the appropriate database based on query type,
data characteristics, and performance requirements.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
import re
import json
import sqlparse

from .connection_manager import UnifiedConnectionManager
from .cache_manager import IgniteCacheManager
from ..monitoring import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class QueryPlan:
    """Execution plan for a query"""
    database_type: str
    database_name: str
    query: str
    use_cache: bool = True
    cache_ttl: Optional[int] = None
    read_preference: str = "primary"  # primary, secondary, nearest
    timeout: Optional[int] = None
    explain: bool = False


@dataclass
class QueryResult:
    """Result of a query execution"""
    data: Any
    execution_time_ms: float
    cached: bool = False
    database_type: str = ""
    error: Optional[str] = None


class QueryRouter:
    """Routes queries to appropriate databases with optimization"""
    
    def __init__(self, connection_manager: UnifiedConnectionManager,
                 cache_manager: IgniteCacheManager,
                 metrics_collector: MetricsCollector):
        self.connection_manager = connection_manager
        self.cache_manager = cache_manager
        self.metrics = metrics_collector
        
        # Query routing rules
        self.routing_rules = self._initialize_routing_rules()
        
    def _initialize_routing_rules(self) -> Dict[str, Any]:
        """Initialize routing rules for different query patterns"""
        return {
            # Time-series queries go to InfluxDB
            "time_series": {
                "patterns": [
                    r"SELECT.*FROM.*WHERE.*time.*",
                    r".*timestamp.*BETWEEN.*",
                    r".*GROUP BY time.*"
                ],
                "database_type": "influxdb",
                "cache_ttl": 60
            },
            
            # Graph queries go to JanusGraph
            "graph": {
                "patterns": [
                    r"g\.V\(\).*",
                    r"g\.E\(\).*",
                    r".*MATCH.*\-\[.*\]\-.*",
                    r".*vertices.*edges.*"
                ],
                "database_type": "janusgraph",
                "cache_ttl": 300
            },
            
            # Full-text search goes to Elasticsearch
            "search": {
                "patterns": [
                    r".*MATCH.*AGAINST.*",
                    r".*_search.*",
                    r".*fuzzy.*",
                    r".*highlight.*"
                ],
                "database_type": "elasticsearch",
                "cache_ttl": 300
            },
            
            # Analytics queries can use Ignite SQL
            "analytics": {
                "patterns": [
                    r".*GROUP BY.*HAVING.*",
                    r".*WITH.*AS.*SELECT.*",
                    r".*WINDOW.*OVER.*"
                ],
                "database_type": "ignite",
                "cache_ttl": 300
            },
            
            # Document queries go to MongoDB
            "document": {
                "patterns": [
                    r"\{.*\:.*\}",
                    r".*find\(.*\).*",
                    r".*aggregate\(.*\).*"
                ],
                "database_type": "mongodb",
                "cache_ttl": 300
            },
            
            # Wide-column queries go to Cassandra
            "wide_column": {
                "patterns": [
                    r".*FROM.*WHERE.*partition_key.*",
                    r".*ALLOW FILTERING.*",
                    r".*TOKEN\(.*\).*"
                ],
                "database_type": "cassandra",
                "cache_ttl": 300
            }
        }
        
    async def route_query(self, query: str, context: Dict[str, Any]) -> QueryPlan:
        """Determine the best database for a query"""
        # Extract metadata from context
        tenant_id = context.get("tenant_id")
        query_type = context.get("query_type", "unknown")
        target_db = context.get("target_database")
        
        # If target database is specified, use it
        if target_db:
            return QueryPlan(
                database_type=target_db.get("type"),
                database_name=target_db.get("name"),
                query=query,
                use_cache=context.get("use_cache", True),
                cache_ttl=context.get("cache_ttl"),
                read_preference=context.get("read_preference", "primary")
            )
            
        # Otherwise, analyze query pattern
        db_type, cache_ttl = self._analyze_query_pattern(query)
        
        # Get database name based on type and tenant
        db_name = self._get_database_name(db_type, tenant_id)
        
        return QueryPlan(
            database_type=db_type,
            database_name=db_name,
            query=query,
            use_cache=context.get("use_cache", True),
            cache_ttl=cache_ttl or context.get("cache_ttl"),
            read_preference=context.get("read_preference", "primary"),
            timeout=context.get("timeout"),
            explain=context.get("explain", False)
        )
        
    def _analyze_query_pattern(self, query: str) -> Tuple[str, Optional[int]]:
        """Analyze query pattern to determine database type"""
        query_lower = query.lower().strip()
        
        # Check each routing rule
        for rule_name, rule_config in self.routing_rules.items():
            for pattern in rule_config["patterns"]:
                if re.search(pattern, query_lower, re.IGNORECASE):
                    return rule_config["database_type"], rule_config.get("cache_ttl")
                    
        # Default to PostgreSQL for standard SQL
        if any(keyword in query_lower for keyword in ["select", "insert", "update", "delete"]):
            return "postgresql", 300
            
        # Default to MongoDB for document-style queries
        return "mongodb", 300
        
    def _get_database_name(self, db_type: str, tenant_id: Optional[str]) -> str:
        """Get database name based on type and tenant"""
        if tenant_id:
            return f"{db_type}_{tenant_id}"
        return f"{db_type}_default"
        
    async def execute_query(self, plan: QueryPlan, context: Dict[str, Any]) -> QueryResult:
        """Execute a query according to the plan"""
        start_time = asyncio.get_event_loop().time()
        tenant_id = context.get("tenant_id")
        
        # Check cache if enabled
        if plan.use_cache:
            cache_key = [plan.database_type, plan.database_name, plan.query]
            cached_result = await self.cache_manager.get(
                "query_results", 
                cache_key,
                tenant_id
            )
            
            if cached_result is not None:
                execution_time = (asyncio.get_event_loop().time() - start_time) * 1000
                await self.metrics.record_query(
                    database_type=plan.database_type,
                    execution_time_ms=execution_time,
                    cached=True,
                    tenant_id=tenant_id
                )
                
                return QueryResult(
                    data=cached_result,
                    execution_time_ms=execution_time,
                    cached=True,
                    database_type=plan.database_type
                )
                
        # Execute query on appropriate database
        try:
            result = await self._execute_on_database(plan, context)
            
            execution_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            # Cache result if successful
            if plan.use_cache and result is not None:
                cache_key = [plan.database_type, plan.database_name, plan.query]
                await self.cache_manager.put(
                    "query_results",
                    cache_key,
                    result,
                    ttl_seconds=plan.cache_ttl,
                    tenant_id=tenant_id
                )
                
            # Record metrics
            await self.metrics.record_query(
                database_type=plan.database_type,
                execution_time_ms=execution_time,
                cached=False,
                tenant_id=tenant_id
            )
            
            return QueryResult(
                data=result,
                execution_time_ms=execution_time,
                cached=False,
                database_type=plan.database_type
            )
            
        except Exception as e:
            logger.error(f"Error executing query on {plan.database_type}: {e}")
            execution_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            await self.metrics.record_query_error(
                database_type=plan.database_type,
                error=str(e),
                tenant_id=tenant_id
            )
            
            return QueryResult(
                data=None,
                execution_time_ms=execution_time,
                cached=False,
                database_type=plan.database_type,
                error=str(e)
            )
            
    async def _execute_on_database(self, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute query on specific database type"""
        async with self.connection_manager.get_connection(
            plan.database_type,
            plan.database_name
        ) as conn:
            
            if plan.database_type == "postgresql":
                return await self._execute_postgresql(conn, plan, context)
                
            elif plan.database_type == "cassandra":
                return await self._execute_cassandra(conn, plan, context)
                
            elif plan.database_type == "mongodb":
                return await self._execute_mongodb(conn, plan, context)
                
            elif plan.database_type == "ignite":
                return await self._execute_ignite(conn, plan, context)
                
            elif plan.database_type == "janusgraph":
                return await self._execute_janusgraph(conn, plan, context)
                
            elif plan.database_type == "elasticsearch":
                return await self._execute_elasticsearch(conn, plan, context)
                
            elif plan.database_type == "influxdb":
                return await self._execute_influxdb(conn, plan, context)
                
            else:
                raise ValueError(f"Unsupported database type: {plan.database_type}")
                
    async def _execute_postgresql(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute PostgreSQL query"""
        # Parse query to determine type
        parsed = sqlparse.parse(plan.query)[0]
        query_type = parsed.get_type()
        
        if query_type == "SELECT":
            if plan.explain:
                explain_query = f"EXPLAIN ANALYZE {plan.query}"
                return await conn.fetch(explain_query)
            return await conn.fetch(plan.query)
            
        elif query_type in ["INSERT", "UPDATE", "DELETE"]:
            return await conn.execute(plan.query)
            
        else:
            # For DDL or other statements
            return await conn.execute(plan.query)
            
    async def _execute_cassandra(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute Cassandra query"""
        # Use async session
        async_session = conn['async_session']
        
        # Prepare statement for better performance
        prepared = await async_session.prepare_async(plan.query)
        
        # Execute with timeout
        result = await async_session.execute_async(
            prepared,
            timeout=plan.timeout or 30
        )
        
        # Convert to list of dicts
        return [dict(row._asdict()) for row in result]
        
    async def _execute_mongodb(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute MongoDB query"""
        # Parse MongoDB-style query
        try:
            query_doc = json.loads(plan.query)
        except:
            # Fallback for non-JSON queries
            query_doc = {"$match": {}}
            
        # Determine operation type
        if "find" in query_doc:
            collection = conn[context.get("collection", "default")]
            cursor = collection.find(query_doc["find"])
            return await cursor.to_list(length=query_doc.get("limit", 1000))
            
        elif "aggregate" in query_doc:
            collection = conn[context.get("collection", "default")]
            cursor = collection.aggregate(query_doc["aggregate"])
            return await cursor.to_list(length=None)
            
        else:
            # Default find operation
            collection = conn[context.get("collection", "default")]
            cursor = collection.find(query_doc)
            return await cursor.to_list(length=1000)
            
    async def _execute_ignite(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute Ignite SQL query"""
        # Ignite supports SQL queries
        cache_name = context.get("cache_name", "SQL_PUBLIC_DEFAULT")
        
        # Get cache
        cache = await conn.get_or_create_cache(cache_name)
        
        # Execute SQL query
        cursor = cache.query(plan.query)
        
        # Collect results
        results = []
        async for row in cursor:
            results.append(row)
            
        return results
        
    async def _execute_janusgraph(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute JanusGraph Gremlin query"""
        # Submit Gremlin query
        result_set = conn.submit(plan.query)
        
        # Collect all results
        results = []
        for result in result_set:
            results.append(result)
            
        return results
        
    async def _execute_elasticsearch(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute Elasticsearch query"""
        # Parse query as Elasticsearch DSL
        try:
            query_dsl = json.loads(plan.query)
        except:
            # Fallback to match_all
            query_dsl = {"query": {"match_all": {}}}
            
        # Execute search
        index = context.get("index", "_all")
        response = await conn.search(
            index=index,
            body=query_dsl,
            timeout=f"{plan.timeout or 30}s"
        )
        
        return response
        
    async def _execute_influxdb(self, conn, plan: QueryPlan, context: Dict[str, Any]) -> Any:
        """Execute InfluxDB query"""
        # Get query API
        query_api = conn.query_api()
        
        # Execute query
        tables = await query_api.query(plan.query)
        
        # Convert to records
        results = []
        for table in tables:
            for record in table.records:
                results.append(record.values)
                
        return results
        
    async def execute_multi_query(self, queries: List[Dict[str, Any]], 
                                 context: Dict[str, Any]) -> List[QueryResult]:
        """Execute multiple queries in parallel"""
        tasks = []
        
        for query_spec in queries:
            query = query_spec["query"]
            query_context = {**context, **query_spec.get("context", {})}
            
            # Route query
            plan = await self.route_query(query, query_context)
            
            # Create execution task
            task = self.execute_query(plan, query_context)
            tasks.append(task)
            
        # Execute all queries in parallel
        results = await asyncio.gather(*tasks)
        
        return results
        
    async def explain_query(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Explain query execution plan"""
        # Route query
        plan = await self.route_query(query, context)
        
        # Add explain flag
        plan.explain = True
        
        # Execute with explain
        result = await self.execute_query(plan, context)
        
        return {
            "routing_plan": {
                "database_type": plan.database_type,
                "database_name": plan.database_name,
                "use_cache": plan.use_cache,
                "cache_ttl": plan.cache_ttl,
                "read_preference": plan.read_preference
            },
            "execution_plan": result.data if result.data else None,
            "estimated_cost": self._estimate_query_cost(query, plan)
        }
        
    def _estimate_query_cost(self, query: str, plan: QueryPlan) -> Dict[str, Any]:
        """Estimate query execution cost"""
        # Simple cost estimation based on query complexity
        query_lower = query.lower()
        
        cost_factors = {
            "joins": query_lower.count("join"),
            "subqueries": query_lower.count("select") - 1,
            "aggregations": sum(1 for agg in ["group by", "sum", "avg", "count"] if agg in query_lower),
            "full_scan": "where" not in query_lower
        }
        
        # Estimate based on database type
        base_costs = {
            "postgresql": 1.0,
            "cassandra": 0.8,
            "mongodb": 0.9,
            "ignite": 0.7,  # In-memory
            "janusgraph": 1.5,  # Graph traversal
            "elasticsearch": 1.2,
            "influxdb": 0.9
        }
        
        base_cost = base_costs.get(plan.database_type, 1.0)
        
        # Calculate total cost
        total_cost = base_cost
        total_cost *= (1 + cost_factors["joins"] * 0.5)
        total_cost *= (1 + cost_factors["subqueries"] * 0.3)
        total_cost *= (1 + cost_factors["aggregations"] * 0.2)
        total_cost *= (2.0 if cost_factors["full_scan"] else 1.0)
        
        return {
            "estimated_cost": round(total_cost, 2),
            "cost_factors": cost_factors,
            "optimization_hints": self._get_optimization_hints(cost_factors)
        }
        
    def _get_optimization_hints(self, cost_factors: Dict[str, Any]) -> List[str]:
        """Get query optimization hints"""
        hints = []
        
        if cost_factors["full_scan"]:
            hints.append("Consider adding WHERE clause to avoid full table scan")
            
        if cost_factors["joins"] > 2:
            hints.append("Consider denormalizing data to reduce joins")
            
        if cost_factors["subqueries"] > 1:
            hints.append("Consider using CTEs or materializing subquery results")
            
        return hints 