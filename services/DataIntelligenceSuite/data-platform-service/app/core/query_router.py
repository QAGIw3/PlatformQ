"""
Query Router for intelligent query routing and optimization
"""
import re
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Token
from sqlparse.tokens import Keyword, DML

from platformq_shared.utils.logger import get_logger
from .cache_manager import DataCacheManager
from .connection_manager import UnifiedConnectionManager

logger = get_logger(__name__)


class QueryPlan:
    """Represents a query execution plan"""
    
    def __init__(self, query: str, source: str):
        self.query = query
        self.source = source
        self.tables: Set[str] = set()
        self.joins: List[Dict[str, Any]] = []
        self.predicates: List[str] = []
        self.aggregations: List[str] = []
        self.estimated_cost: float = 0.0
        self.cache_hit_probability: float = 0.0
        self.partitions: List[str] = []
        

class QueryRouter:
    """
    Routes queries to appropriate data sources and optimizes execution.
    
    Features:
    - Query parsing and analysis
    - Source selection based on query pattern
    - Cost-based optimization
    - Query rewriting for performance
    - Cache-aware routing
    - Load balancing across sources
    """
    
    def __init__(self, 
                 connection_manager: UnifiedConnectionManager,
                 cache_manager: DataCacheManager):
        self.connection_manager = connection_manager
        self.cache_manager = cache_manager
        
        # Catalog metadata cache
        self.table_catalog: Dict[str, Dict[str, Any]] = {}
        self.source_capabilities: Dict[str, Set[str]] = {
            "trino": {"federated", "analytical", "joins", "aggregations"},
            "postgresql": {"transactional", "relational", "joins", "indexes"},
            "cassandra": {"nosql", "timeseries", "partition_key_queries"},
            "elasticsearch": {"search", "aggregations", "text_search"},
            "mongodb": {"nosql", "document", "aggregations"}
        }
        
        # Query patterns for source selection
        self.query_patterns = {
            r"(?i)WITH\s+RECURSIVE": ["postgresql"],
            r"(?i)MATCH\s*\(": ["elasticsearch"],
            r"(?i)SELECT.*FROM.*\..*\.": ["trino"],  # Cross-catalog queries
            r"(?i)ALLOW FILTERING": ["cassandra"],
            r"(?i)\$lookup": ["mongodb"],
            r"(?i)SELECT.*OVER\s*\(": ["trino", "postgresql"],  # Window functions
        }
        
        # Performance metrics
        self.metrics = {
            "queries_routed": 0,
            "cache_hits": 0,
            "query_rewrites": 0,
            "source_usage": {}
        }
    
    async def route_query(self, 
                         query: str,
                         catalog: Optional[str] = None,
                         schema: Optional[str] = None,
                         hints: Optional[Dict[str, Any]] = None) -> QueryPlan:
        """Route a query to the optimal data source"""
        try:
            # Parse query
            parsed = sqlparse.parse(query)[0]
            
            # Extract query components
            tables = self._extract_tables(parsed)
            query_type = self._get_query_type(parsed)
            
            # Check cache probability
            cache_probability = await self._estimate_cache_hit(query, tables)
            
            # Determine best source
            source = await self._select_source(
                query=query,
                tables=tables,
                query_type=query_type,
                catalog=catalog,
                hints=hints
            )
            
            # Create query plan
            plan = QueryPlan(query, source)
            plan.tables = tables
            plan.cache_hit_probability = cache_probability
            
            # Optimize query for selected source
            optimized_query = await self._optimize_query(query, source, plan)
            plan.query = optimized_query
            
            # Estimate cost
            plan.estimated_cost = await self._estimate_cost(plan)
            
            # Update metrics
            self.metrics["queries_routed"] += 1
            self.metrics["source_usage"][source] = \
                self.metrics["source_usage"].get(source, 0) + 1
            
            logger.info(f"Routed query to {source} with cost {plan.estimated_cost}")
            
            return plan
            
        except Exception as e:
            logger.error(f"Error routing query: {e}")
            # Fallback to default source
            return QueryPlan(query, "trino")
    
    def _extract_tables(self, parsed) -> Set[str]:
        """Extract table names from parsed query"""
        tables = set()
        
        def extract_from_identifiers(token_list):
            for token in token_list.tokens:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.add(str(identifier).strip())
                elif isinstance(token, Identifier):
                    tables.add(str(token).strip())
                elif token.ttype is None:
                    extract_from_identifiers(token)
        
        extract_from_identifiers(parsed)
        return tables
    
    def _get_query_type(self, parsed) -> str:
        """Determine query type (SELECT, INSERT, etc.)"""
        for token in parsed.tokens:
            if token.ttype is DML:
                return token.value.upper()
        return "UNKNOWN"
    
    async def _estimate_cache_hit(self, 
                                 query: str,
                                 tables: Set[str]) -> float:
        """Estimate probability of cache hit"""
        # Check if query result is cached
        cache_key = hashlib.md5(query.encode()).hexdigest()
        cached_result = await self.cache_manager.get_query_result(query)
        
        if cached_result:
            return 1.0
        
        # Estimate based on table access patterns
        hit_probability = 0.0
        
        for table in tables:
            # Check if table metadata is cached
            table_meta = await self.cache_manager.get_metadata("table", table)
            if table_meta:
                # Recently accessed tables have higher cache probability
                last_access = table_meta.get("last_access")
                if last_access:
                    age = (datetime.utcnow() - datetime.fromisoformat(last_access)).seconds
                    if age < 300:  # Within 5 minutes
                        hit_probability += 0.3
                    elif age < 3600:  # Within 1 hour
                        hit_probability += 0.1
        
        return min(hit_probability, 0.9)
    
    async def _select_source(self,
                           query: str,
                           tables: Set[str],
                           query_type: str,
                           catalog: Optional[str],
                           hints: Optional[Dict[str, Any]]) -> str:
        """Select the best data source for the query"""
        # Check hints first
        if hints and "source" in hints:
            return hints["source"]
        
        # Check query patterns
        for pattern, sources in self.query_patterns.items():
            if re.search(pattern, query):
                # Return first available source
                for source in sources:
                    if await self._is_source_available(source):
                        return source
        
        # Check table locations
        source_scores: Dict[str, float] = {}
        
        for table in tables:
            # Look up table in catalog
            table_info = await self._lookup_table(table, catalog)
            if table_info:
                source = table_info.get("source", "trino")
                source_scores[source] = source_scores.get(source, 0) + 1
        
        # If tables are from mixed sources, use federation engine
        if len(source_scores) > 1:
            return "trino"
        
        # Return source with most tables
        if source_scores:
            return max(source_scores, key=source_scores.get)
        
        # Default based on query type
        if query_type in ["INSERT", "UPDATE", "DELETE"]:
            return "postgresql"  # Transactional
        else:
            return "trino"  # Analytical
    
    async def _is_source_available(self, source: str) -> bool:
        """Check if a data source is available"""
        try:
            health = await self.connection_manager.health_check()
            return health["sources"].get(source) == "healthy"
        except:
            return False
    
    async def _lookup_table(self, 
                          table: str,
                          catalog: Optional[str]) -> Optional[Dict[str, Any]]:
        """Look up table information in catalog"""
        # Check cache first
        cache_key = f"{catalog}.{table}" if catalog else table
        
        if cache_key in self.table_catalog:
            return self.table_catalog[cache_key]
        
        # Query metadata from catalog
        # This would query Elasticsearch or other metadata store
        table_info = await self.cache_manager.get_metadata("table", cache_key)
        
        if table_info:
            self.table_catalog[cache_key] = table_info
            
        return table_info
    
    async def _optimize_query(self, 
                            query: str,
                            source: str,
                            plan: QueryPlan) -> str:
        """Optimize query for specific data source"""
        optimized = query
        
        if source == "cassandra":
            # Add ALLOW FILTERING if needed
            if "WHERE" in query.upper() and "ALLOW FILTERING" not in query.upper():
                if not self._has_partition_key_filter(query, plan.tables):
                    optimized = f"{query} ALLOW FILTERING"
        
        elif source == "elasticsearch":
            # Convert SQL to Elasticsearch query DSL
            # This is simplified - real implementation would be more complex
            optimized = self._sql_to_es_query(query)
        
        elif source == "trino":
            # Add query hints for optimization
            if plan.tables and len(plan.tables) > 2:
                # Suggest broadcast join for small tables
                optimized = self._add_join_hints(query, plan)
        
        if optimized != query:
            self.metrics["query_rewrites"] += 1
            logger.debug(f"Optimized query for {source}")
        
        return optimized
    
    def _has_partition_key_filter(self, query: str, tables: Set[str]) -> bool:
        """Check if query filters on partition key"""
        # Simplified check - real implementation would analyze WHERE clause
        # against table schema
        return "WHERE" in query.upper() and any(
            key in query.upper() 
            for key in ["ID", "DATE", "TIMESTAMP", "USER_ID"]
        )
    
    def _sql_to_es_query(self, sql: str) -> str:
        """Convert SQL to Elasticsearch query (simplified)"""
        # This is a very basic conversion
        # Real implementation would use a proper SQL parser
        if "SELECT" in sql.upper():
            # Extract basic components
            match = re.search(r"SELECT\s+(.*?)\s+FROM\s+(\w+)", sql, re.IGNORECASE)
            if match:
                fields = match.group(1)
                index = match.group(2)
                
                # Build basic ES query
                es_query = {
                    "query": {"match_all": {}},
                    "_source": fields.split(",") if fields != "*" else True
                }
                
                # Add WHERE clause if present
                where_match = re.search(r"WHERE\s+(.+)", sql, re.IGNORECASE)
                if where_match:
                    # Very simplified WHERE parsing
                    es_query["query"] = {
                        "query_string": {
                            "query": where_match.group(1)
                        }
                    }
                
                return str(es_query)
        
        return sql
    
    def _add_join_hints(self, query: str, plan: QueryPlan) -> str:
        """Add join hints for query optimization"""
        # Analyze table sizes and add broadcast hints
        # This is simplified - real implementation would check actual table stats
        
        small_tables = self._identify_small_tables(plan.tables)
        
        if small_tables:
            # Add Trino-specific hints
            hint = f"-- +broadcaster({','.join(small_tables)})\n"
            return hint + query
        
        return query
    
    def _identify_small_tables(self, tables: Set[str]) -> List[str]:
        """Identify small tables suitable for broadcast joins"""
        # Simplified - would check actual table statistics
        small_table_patterns = ["dim_", "lookup_", "ref_"]
        
        small_tables = []
        for table in tables:
            if any(pattern in table.lower() for pattern in small_table_patterns):
                small_tables.append(table)
        
        return small_tables
    
    async def _estimate_cost(self, plan: QueryPlan) -> float:
        """Estimate query execution cost"""
        base_cost = 1.0
        
        # Factor in number of tables (joins are expensive)
        base_cost *= len(plan.tables)
        
        # Factor in query complexity
        if "GROUP BY" in plan.query.upper():
            base_cost *= 1.5
        if "ORDER BY" in plan.query.upper():
            base_cost *= 1.2
        if "DISTINCT" in plan.query.upper():
            base_cost *= 1.3
        
        # Reduce cost if cache hit is likely
        if plan.cache_hit_probability > 0.5:
            base_cost *= (1 - plan.cache_hit_probability * 0.8)
        
        # Source-specific cost factors
        source_costs = {
            "trino": 1.0,      # Optimized for analytics
            "postgresql": 0.8,  # Fast for small queries
            "cassandra": 0.5,   # Fast for partition key queries
            "elasticsearch": 0.7,  # Fast for search
            "mongodb": 0.6      # Fast for document queries
        }
        
        base_cost *= source_costs.get(plan.source, 1.0)
        
        return round(base_cost, 2)
    
    async def analyze_query_performance(self, 
                                      query: str,
                                      execution_time: float) -> Dict[str, Any]:
        """Analyze query performance for optimization"""
        analysis = {
            "query": query[:100] + "..." if len(query) > 100 else query,
            "execution_time": execution_time,
            "recommendations": []
        }
        
        # Check if query is slow
        if execution_time > 5.0:
            # Analyze for common performance issues
            if "SELECT *" in query.upper():
                analysis["recommendations"].append(
                    "Avoid SELECT *, specify only required columns"
                )
            
            if not re.search(r"WHERE|LIMIT", query, re.IGNORECASE):
                analysis["recommendations"].append(
                    "Add WHERE clause or LIMIT to reduce data scanned"
                )
            
            # Check for missing indexes (simplified)
            if "WHERE" in query.upper():
                analysis["recommendations"].append(
                    "Consider adding indexes on WHERE clause columns"
                )
        
        # Store performance data for learning
        await self.cache_manager.set_metadata(
            "query_performance",
            hashlib.md5(query.encode()).hexdigest(),
            {
                "execution_time": execution_time,
                "timestamp": datetime.utcnow().isoformat(),
                "analysis": analysis
            }
        )
        
        return analysis
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get router metrics"""
        total_queries = self.metrics["queries_routed"]
        
        return {
            "total_queries": total_queries,
            "cache_hit_rate": (
                self.metrics["cache_hits"] / total_queries * 100 
                if total_queries > 0 else 0
            ),
            "query_rewrites": self.metrics["query_rewrites"],
            "source_distribution": self.metrics["source_usage"]
        } 