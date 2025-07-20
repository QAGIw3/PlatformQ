"""
Trino Client for query execution
"""
import asyncio
from typing import Dict, Any, List, Optional, Tuple, AsyncIterator
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor

import trino
from trino.auth import BasicAuthentication
from trino.constants import HEADER_USER

from platformq_shared.utils.logger import get_logger

logger = get_logger(__name__)


class TrinoQueryExecutor:
    """
    Async wrapper for Trino query execution.
    
    Features:
    - Async query execution
    - Streaming results
    - Query progress monitoring
    - Resource management
    - Query cancellation
    """
    
    def __init__(self,
                 host: str = "trino-coordinator",
                 port: int = 8080,
                 user: str = "platformq",
                 catalog: str = "hive",
                 schema: str = "default",
                 http_scheme: str = "http",
                 auth: Optional[BasicAuthentication] = None,
                 max_workers: int = 10):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        self.auth = auth
        
        # Thread pool for blocking operations
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Connection pool
        self.connections: List[trino.dbapi.Connection] = []
        self.max_connections = max_workers
        
        # Base connection parameters
        self.base_params = {
            "host": host,
            "port": port,
            "user": user,
            "catalog": catalog,
            "schema": schema,
            "http_scheme": http_scheme,
            "auth": auth,
            "verify": False
        }
    
    async def execute(self,
                     query: str,
                     parameters: Optional[Dict[str, Any]] = None,
                     session_properties: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Execute a query and return all results"""
        loop = asyncio.get_event_loop()
        
        def run_query():
            conn = self._get_connection(session_properties)
            cursor = conn.cursor()
            
            try:
                # Execute query
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                # Get column info
                columns = []
                if cursor.description:
                    columns = [
                        {
                            "name": desc[0],
                            "type": str(desc[1]) if desc[1] else "unknown"
                        }
                        for desc in cursor.description
                    ]
                
                # Fetch all results
                rows = cursor.fetchall()
                
                # Get query stats
                stats = self._get_query_stats(cursor)
                
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "stats": stats
                }
                
            finally:
                cursor.close()
                self._release_connection(conn)
        
        return await loop.run_in_executor(self.executor, run_query)
    
    async def execute_streaming(self,
                              query: str,
                              parameters: Optional[Dict[str, Any]] = None,
                              session_properties: Optional[Dict[str, str]] = None,
                              batch_size: int = 1000) -> AsyncIterator[List[Tuple]]:
        """Execute a query and stream results in batches"""
        loop = asyncio.get_event_loop()
        
        # Create queue for streaming
        queue = asyncio.Queue(maxsize=10)
        
        def stream_query():
            conn = self._get_connection(session_properties)
            cursor = conn.cursor()
            
            try:
                # Execute query
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                # Stream results in batches
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    
                    # Put batch in queue
                    loop.call_soon_threadsafe(
                        lambda b=batch: asyncio.create_task(queue.put(b))
                    )
                
                # Signal end of results
                loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(queue.put(None))
                )
                
            except Exception as e:
                # Put exception in queue
                loop.call_soon_threadsafe(
                    lambda ex=e: asyncio.create_task(queue.put(ex))
                )
            finally:
                cursor.close()
                self._release_connection(conn)
        
        # Start streaming in thread
        loop.run_in_executor(self.executor, stream_query)
        
        # Yield results from queue
        while True:
            result = await queue.get()
            
            if result is None:
                # End of results
                break
            elif isinstance(result, Exception):
                # Error occurred
                raise result
            else:
                # Yield batch
                yield result
    
    async def get_query_info(self, query_id: str) -> Dict[str, Any]:
        """Get information about a running or completed query"""
        loop = asyncio.get_event_loop()
        
        def fetch_info():
            # Use Trino REST API to get query info
            import requests
            
            url = f"{self.http_scheme}://{self.host}:{self.port}/v1/query/{query_id}"
            headers = {HEADER_USER: self.user}
            
            response = requests.get(url, headers=headers, verify=False)
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
        
        info = await loop.run_in_executor(self.executor, fetch_info)
        
        if info:
            return {
                "query_id": query_id,
                "state": info.get("state"),
                "progress": {
                    "running_drivers": info.get("stats", {}).get("runningDrivers", 0),
                    "queued_drivers": info.get("stats", {}).get("queuedDrivers", 0),
                    "completed_drivers": info.get("stats", {}).get("completedDrivers", 0),
                    "total_drivers": info.get("stats", {}).get("totalDrivers", 0),
                    "cpu_time_millis": info.get("stats", {}).get("cpuTimeMillis", 0),
                    "wall_time_millis": info.get("stats", {}).get("wallTimeMillis", 0),
                    "processed_rows": info.get("stats", {}).get("processedRows", 0),
                    "processed_bytes": info.get("stats", {}).get("processedBytes", 0)
                },
                "error": info.get("error")
            }
        
        return {"query_id": query_id, "error": "Query not found"}
    
    async def cancel_query(self, query_id: str) -> bool:
        """Cancel a running query"""
        loop = asyncio.get_event_loop()
        
        def cancel():
            import requests
            
            url = f"{self.http_scheme}://{self.host}:{self.port}/v1/query/{query_id}"
            headers = {HEADER_USER: self.user}
            
            response = requests.delete(url, headers=headers, verify=False)
            
            return response.status_code in [204, 200]
        
        return await loop.run_in_executor(self.executor, cancel)
    
    async def show_catalogs(self) -> List[str]:
        """Get list of available catalogs"""
        result = await self.execute("SHOW CATALOGS")
        return [row[0] for row in result["rows"]]
    
    async def show_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """Get list of schemas in a catalog"""
        if catalog:
            query = f"SHOW SCHEMAS FROM {catalog}"
        else:
            query = "SHOW SCHEMAS"
        
        result = await self.execute(query)
        return [row[0] for row in result["rows"]]
    
    async def show_tables(self, 
                         catalog: Optional[str] = None,
                         schema: Optional[str] = None) -> List[str]:
        """Get list of tables"""
        if catalog and schema:
            query = f"SHOW TABLES FROM {catalog}.{schema}"
        elif schema:
            query = f"SHOW TABLES FROM {schema}"
        else:
            query = "SHOW TABLES"
        
        result = await self.execute(query)
        return [row[0] for row in result["rows"]]
    
    async def describe_table(self,
                           table: str,
                           catalog: Optional[str] = None,
                           schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get table schema"""
        if catalog and schema:
            full_table = f"{catalog}.{schema}.{table}"
        elif schema:
            full_table = f"{schema}.{table}"
        else:
            full_table = table
        
        query = f"DESCRIBE {full_table}"
        result = await self.execute(query)
        
        columns = []
        for row in result["rows"]:
            columns.append({
                "name": row[0],
                "type": row[1],
                "extra": row[2] if len(row) > 2 else None,
                "comment": row[3] if len(row) > 3 else None
            })
        
        return columns
    
    async def analyze_table(self,
                          table: str,
                          catalog: Optional[str] = None,
                          schema: Optional[str] = None) -> Dict[str, Any]:
        """Analyze table statistics"""
        if catalog and schema:
            full_table = f"{catalog}.{schema}.{table}"
        elif schema:
            full_table = f"{schema}.{table}"
        else:
            full_table = table
        
        # Get table stats
        stats_query = f"SHOW STATS FOR {full_table}"
        stats_result = await self.execute(stats_query)
        
        # Get row count
        count_query = f"SELECT COUNT(*) FROM {full_table}"
        count_result = await self.execute(count_query)
        
        row_count = count_result["rows"][0][0] if count_result["rows"] else 0
        
        # Parse stats
        column_stats = {}
        for row in stats_result["rows"]:
            if row[0]:  # Column name
                column_stats[row[0]] = {
                    "null_fraction": row[1],
                    "distinct_values": row[2],
                    "min": row[3],
                    "max": row[4],
                    "data_size": row[5]
                }
        
        return {
            "table": full_table,
            "row_count": row_count,
            "column_stats": column_stats
        }
    
    def _get_connection(self, session_properties: Optional[Dict[str, str]] = None) -> trino.dbapi.Connection:
        """Get a connection from the pool or create a new one"""
        if self.connections:
            return self.connections.pop()
        
        # Create new connection
        params = self.base_params.copy()
        
        # Add session properties
        if session_properties:
            params["session_properties"] = session_properties
        
        return trino.dbapi.connect(**params)
    
    def _release_connection(self, conn: trino.dbapi.Connection) -> None:
        """Release a connection back to the pool"""
        if len(self.connections) < self.max_connections:
            self.connections.append(conn)
        else:
            # Close excess connection
            conn.close()
    
    def _get_query_stats(self, cursor) -> Dict[str, Any]:
        """Extract query statistics from cursor"""
        stats = {}
        
        # Trino cursor may have stats attribute
        if hasattr(cursor, 'stats'):
            stats = cursor.stats
        
        # Extract key metrics
        return {
            "rows_processed": stats.get("processed_rows", 0),
            "bytes_processed": stats.get("processed_bytes", 0),
            "cpu_time_ms": stats.get("cpu_time_millis", 0),
            "wall_time_ms": stats.get("wall_time_millis", 0),
            "queued_time_ms": stats.get("queued_time_millis", 0),
            "peak_memory_bytes": stats.get("peak_memory_bytes", 0)
        }
    
    async def close(self) -> None:
        """Close all connections and shutdown executor"""
        # Close all pooled connections
        while self.connections:
            conn = self.connections.pop()
            conn.close()
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        logger.info("Trino client closed") 