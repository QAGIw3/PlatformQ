"""
Query Engine for federated query execution
"""
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import sqlparse
import trino
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.errors import ValidationError, ServiceError

logger = get_logger(__name__)


class QueryEngine:
    """Federated query engine using Trino"""
    
    def __init__(self):
        self.trino_host = "trino"
        self.trino_port = 8080
        self.trino_user = "platform"
        self.cache = IgniteClient()
        self.active_queries: Dict[str, Dict] = {}
        
    async def execute_query(self, 
                          query: str, 
                          catalog: Optional[str] = None,
                          schema: Optional[str] = None,
                          params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a federated query"""
        try:
            # Validate and parse query
            parsed = sqlparse.parse(query)[0]
            if not parsed:
                raise ValidationError("Invalid SQL query")
            
            # Create query ID
            query_id = f"query_{datetime.utcnow().timestamp()}"
            
            # Store query metadata
            self.active_queries[query_id] = {
                "query": query,
                "status": "running",
                "started_at": datetime.utcnow(),
                "catalog": catalog,
                "schema": schema
            }
            
            # Connect to Trino
            conn = trino.dbapi.connect(
                host=self.trino_host,
                port=self.trino_port,
                user=self.trino_user,
                catalog=catalog or "hive",
                schema=schema or "default"
            )
            
            # Execute query asynchronously
            result = await self._execute_async(conn, query, params)
            
            # Update query status
            self.active_queries[query_id]["status"] = "completed"
            self.active_queries[query_id]["completed_at"] = datetime.utcnow()
            
            # Cache results if small enough
            if len(result.get("data", [])) < 1000:
                await self.cache.put(f"query_result:{query_id}", result, ttl=3600)
            
            return {
                "query_id": query_id,
                "status": "completed",
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            if query_id in self.active_queries:
                self.active_queries[query_id]["status"] = "failed"
                self.active_queries[query_id]["error"] = str(e)
            raise ServiceError(f"Query execution failed: {str(e)}")
    
    async def _execute_async(self, conn, query: str, params: Optional[Dict]) -> Dict:
        """Execute query asynchronously"""
        loop = asyncio.get_event_loop()
        
        def run_query():
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            data = cursor.fetchall()
            return {
                "columns": columns,
                "data": data,
                "row_count": len(data)
            }
        
        return await loop.run_in_executor(None, run_query)
    
    async def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Get status of a query"""
        query_info = self.active_queries.get(query_id)
        if not query_info:
            # Check cache
            cached = await self.cache.get(f"query_result:{query_id}")
            if cached:
                return {
                    "query_id": query_id,
                    "status": "completed",
                    "cached": True
                }
            raise ValidationError(f"Query {query_id} not found")
        
        return {
            "query_id": query_id,
            **query_info
        }
    
    async def cancel_query(self, query_id: str) -> Dict[str, Any]:
        """Cancel a running query"""
        query_info = self.active_queries.get(query_id)
        if not query_info:
            raise ValidationError(f"Query {query_id} not found")
        
        if query_info["status"] != "running":
            raise ValidationError(f"Query {query_id} is not running")
        
        # TODO: Implement actual query cancellation in Trino
        query_info["status"] = "cancelled"
        query_info["cancelled_at"] = datetime.utcnow()
        
        return {
            "query_id": query_id,
            "status": "cancelled"
        }
    
    async def get_catalogs(self) -> List[str]:
        """Get available catalogs"""
        conn = trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user
        )
        cursor = conn.cursor()
        cursor.execute("SHOW CATALOGS")
        return [row[0] for row in cursor.fetchall()]
    
    async def get_schemas(self, catalog: str) -> List[str]:
        """Get schemas in a catalog"""
        conn = trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog=catalog
        )
        cursor = conn.cursor()
        cursor.execute("SHOW SCHEMAS")
        return [row[0] for row in cursor.fetchall()]
    
    async def get_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """Get tables in a schema"""
        conn = trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog=catalog,
            schema=schema
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        
        tables = []
        for row in cursor.fetchall():
            table_name = row[0]
            # Get table columns
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [{"name": col[0], "type": col[1]} for col in cursor.fetchall()]
            
            tables.append({
                "name": table_name,
                "catalog": catalog,
                "schema": schema,
                "columns": columns
            })
        
        return tables
    
    async def explain_query(self, query: str, catalog: Optional[str] = None, 
                          schema: Optional[str] = None) -> Dict[str, Any]:
        """Get query execution plan"""
        conn = trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog=catalog or "hive",
            schema=schema or "default"
        )
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN {query}")
        
        plan = []
        for row in cursor.fetchall():
            plan.append(row[0])
        
        return {
            "query": query,
            "plan": plan,
            "estimated_cost": self._estimate_cost(plan)
        }
    
    def _estimate_cost(self, plan: List[str]) -> Dict[str, Any]:
        """Estimate query cost from execution plan"""
        # Simple cost estimation based on plan
        scan_count = sum(1 for line in plan if "Scan" in line)
        join_count = sum(1 for line in plan if "Join" in line)
        
        return {
            "scan_operations": scan_count,
            "join_operations": join_count,
            "estimated_rows": scan_count * 10000,  # Rough estimate
            "complexity": "high" if join_count > 2 else "medium" if join_count > 0 else "low"
        } 