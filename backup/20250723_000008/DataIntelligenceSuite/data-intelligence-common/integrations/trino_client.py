"""
Trino Client Integration

Provides high-level client for Trino distributed SQL query engine.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Iterator
from dataclasses import dataclass, field
from datetime import datetime
import requests
from urllib.parse import quote

logger = logging.getLogger(__name__)


@dataclass
class TrinoConfig:
    """Configuration for Trino client"""
    host: str = "localhost"
    port: int = 8080
    
    # Authentication
    user: str = "trino"
    password: Optional[str] = None
    auth_type: Optional[str] = None  # "basic", "kerberos", "jwt"
    
    # Session properties
    catalog: Optional[str] = None
    schema: Optional[str] = None
    source: str = "trino-python-client"
    session_properties: Dict[str, str] = field(default_factory=dict)
    
    # Query settings
    query_max_memory: Optional[str] = None
    query_max_total_memory: Optional[str] = None
    query_max_execution_time: Optional[str] = None
    
    # Connection settings
    http_scheme: str = "http"
    verify_ssl: bool = True
    request_timeout: int = 30
    
    # Client settings
    client_tags: List[str] = field(default_factory=list)
    trace_token: Optional[str] = None
    
    # Timezone
    timezone: Optional[str] = None


@dataclass
class QueryStats:
    """Trino query statistics"""
    state: str
    queued: bool
    scheduled: bool
    nodes: int
    total_splits: int
    queued_splits: int
    running_splits: int
    completed_splits: int
    cpu_time_millis: int
    wall_time_millis: int
    queued_time_millis: int
    elapsed_time_millis: int
    processed_rows: int
    processed_bytes: int
    peak_memory_bytes: int
    spilled_bytes: int
    root_stage: Optional[Dict[str, Any]] = None


@dataclass
class QueryResult:
    """Trino query result"""
    query_id: str
    columns: List[Dict[str, Any]]
    data: List[List[Any]]
    stats: Optional[QueryStats] = None
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    update_type: Optional[str] = None
    update_count: Optional[int] = None


@dataclass
class TableInfo:
    """Trino table information"""
    catalog: str
    schema: str
    table: str
    type: str  # "TABLE" or "VIEW"
    columns: List[Dict[str, Any]] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


class TrinoClient:
    """
    High-level client for Trino operations.
    
    Features:
    - SQL query execution
    - Catalog and schema management
    - Table operations
    - Query monitoring
    - Session management
    """
    
    def __init__(self, config: TrinoConfig):
        self.config = config
        self._session = requests.Session()
        self._base_url = f"{config.http_scheme}://{config.host}:{config.port}"
        
        # Set up headers
        self._headers = {
            "X-Trino-User": config.user,
            "X-Trino-Source": config.source,
            "User-Agent": config.source
        }
        
        if config.catalog:
            self._headers["X-Trino-Catalog"] = config.catalog
        if config.schema:
            self._headers["X-Trino-Schema"] = config.schema
        if config.timezone:
            self._headers["X-Trino-Time-Zone"] = config.timezone
        if config.trace_token:
            self._headers["X-Trino-Trace-Token"] = config.trace_token
        if config.client_tags:
            self._headers["X-Trino-Client-Tags"] = ",".join(config.client_tags)
            
        # Session properties
        if config.session_properties:
            props = ",".join(
                f"{k}={v}" for k, v in config.session_properties.items()
            )
            self._headers["X-Trino-Session"] = props
            
        # Authentication
        if config.auth_type == "basic" and config.password:
            from requests.auth import HTTPBasicAuth
            self._session.auth = HTTPBasicAuth(config.user, config.password)
            
    def execute(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        session_properties: Optional[Dict[str, str]] = None
    ) -> QueryResult:
        """Execute a SQL query"""
        # Prepare headers
        headers = self._headers.copy()
        
        if catalog:
            headers["X-Trino-Catalog"] = catalog
        if schema:
            headers["X-Trino-Schema"] = schema
        if session_properties:
            props = ",".join(
                f"{k}={v}" for k, v in session_properties.items()
            )
            headers["X-Trino-Session"] = props
            
        # Submit query
        response = self._session.post(
            f"{self._base_url}/v1/statement",
            data=query.encode("utf-8"),
            headers=headers,
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        
        # Process results
        return self._fetch_results(response.json())
        
    def _fetch_results(self, initial_response: Dict[str, Any]) -> QueryResult:
        """Fetch all results for a query"""
        query_id = initial_response["id"]
        columns = initial_response.get("columns", [])
        data = []
        warnings = []
        stats = None
        update_type = None
        update_count = None
        
        current = initial_response
        
        while True:
            # Collect data
            if "data" in current:
                data.extend(current["data"])
                
            # Collect warnings
            if "warnings" in current:
                warnings.extend(current["warnings"])
                
            # Get stats
            if "stats" in current:
                stats = self._parse_stats(current["stats"])
                
            # Check for updates
            if "updateType" in current:
                update_type = current["updateType"]
            if "updateCount" in current:
                update_count = current["updateCount"]
                
            # Check if done
            if "nextUri" not in current:
                break
                
            # Fetch next batch
            response = self._session.get(
                current["nextUri"],
                timeout=self.config.request_timeout
            )
            response.raise_for_status()
            current = response.json()
            
        return QueryResult(
            query_id=query_id,
            columns=columns,
            data=data,
            stats=stats,
            warnings=warnings,
            update_type=update_type,
            update_count=update_count
        )
        
    def _parse_stats(self, stats_data: Dict[str, Any]) -> QueryStats:
        """Parse query statistics"""
        return QueryStats(
            state=stats_data.get("state", "UNKNOWN"),
            queued=stats_data.get("queued", False),
            scheduled=stats_data.get("scheduled", False),
            nodes=stats_data.get("nodes", 0),
            total_splits=stats_data.get("totalSplits", 0),
            queued_splits=stats_data.get("queuedSplits", 0),
            running_splits=stats_data.get("runningSplits", 0),
            completed_splits=stats_data.get("completedSplits", 0),
            cpu_time_millis=stats_data.get("cpuTimeMillis", 0),
            wall_time_millis=stats_data.get("wallTimeMillis", 0),
            queued_time_millis=stats_data.get("queuedTimeMillis", 0),
            elapsed_time_millis=stats_data.get("elapsedTimeMillis", 0),
            processed_rows=stats_data.get("processedRows", 0),
            processed_bytes=stats_data.get("processedBytes", 0),
            peak_memory_bytes=stats_data.get("peakMemoryBytes", 0),
            spilled_bytes=stats_data.get("spilledBytes", 0),
            root_stage=stats_data.get("rootStage")
        )
        
    def execute_async(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        session_properties: Optional[Dict[str, str]] = None
    ) -> str:
        """Submit query and return query ID without waiting for results"""
        # Prepare headers
        headers = self._headers.copy()
        
        if catalog:
            headers["X-Trino-Catalog"] = catalog
        if schema:
            headers["X-Trino-Schema"] = schema
        if session_properties:
            props = ",".join(
                f"{k}={v}" for k, v in session_properties.items()
            )
            headers["X-Trino-Session"] = props
            
        # Submit query
        response = self._session.post(
            f"{self._base_url}/v1/statement",
            data=query.encode("utf-8"),
            headers=headers,
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        result = response.json()
        
        return result["id"]
        
    def get_query_info(self, query_id: str) -> Dict[str, Any]:
        """Get query information"""
        response = self._session.get(
            f"{self._base_url}/v1/query/{query_id}",
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json()
        
    def cancel_query(self, query_id: str) -> bool:
        """Cancel a running query"""
        try:
            response = self._session.delete(
                f"{self._base_url}/v1/query/{query_id}",
                timeout=self.config.request_timeout
            )
            response.raise_for_status()
            return True
        except Exception:
            return False
            
    # Catalog operations
    
    def list_catalogs(self) -> List[str]:
        """List all catalogs"""
        result = self.execute("SHOW CATALOGS")
        return [row[0] for row in result.data]
        
    def list_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """List schemas in a catalog"""
        catalog = catalog or self.config.catalog
        if not catalog:
            raise ValueError("Catalog must be specified")
            
        result = self.execute(f"SHOW SCHEMAS FROM {catalog}")
        return [row[0] for row in result.data]
        
    def list_tables(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[TableInfo]:
        """List tables in a schema"""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        
        if not catalog or not schema:
            raise ValueError("Both catalog and schema must be specified")
            
        result = self.execute(
            f"SHOW TABLES FROM {catalog}.{schema}"
        )
        
        tables = []
        for row in result.data:
            table_name = row[0]
            tables.append(TableInfo(
                catalog=catalog,
                schema=schema,
                table=table_name,
                type="TABLE"  # Would need additional query to determine type
            ))
            
        return tables
        
    def describe_table(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> TableInfo:
        """Describe a table"""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        
        if not catalog or not schema:
            raise ValueError("Both catalog and schema must be specified")
            
        # Get columns
        result = self.execute(
            f"DESCRIBE {catalog}.{schema}.{table}"
        )
        
        columns = []
        for row in result.data:
            columns.append({
                "name": row[0],
                "type": row[1],
                "extra": row[2] if len(row) > 2 else None,
                "comment": row[3] if len(row) > 3 else None
            })
            
        # Get table properties
        try:
            props_result = self.execute(
                f"SHOW CREATE TABLE {catalog}.{schema}.{table}"
            )
            # Parse properties from CREATE TABLE statement
            properties = {}  # Would need to parse the statement
        except Exception:
            properties = {}
            
        return TableInfo(
            catalog=catalog,
            schema=schema,
            table=table,
            type="TABLE",
            columns=columns,
            properties=properties
        )
        
    def table_exists(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> bool:
        """Check if table exists"""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        
        if not catalog or not schema:
            raise ValueError("Both catalog and schema must be specified")
            
        try:
            self.execute(
                f"SELECT 1 FROM {catalog}.{schema}.{table} LIMIT 0"
            )
            return True
        except Exception:
            return False
            
    # Utility methods
    
    def explain(
        self,
        query: str,
        analyze: bool = False,
        verbose: bool = False
    ) -> str:
        """Explain query execution plan"""
        explain_query = "EXPLAIN "
        
        if analyze:
            explain_query += "ANALYZE "
        if verbose:
            explain_query += "VERBOSE "
            
        explain_query += query
        
        result = self.execute(explain_query)
        
        # Combine all rows into a single string
        return "\n".join(row[0] for row in result.data)
        
    def get_session_properties(self) -> Dict[str, Any]:
        """Get current session properties"""
        result = self.execute("SHOW SESSION")
        
        properties = {}
        for row in result.data:
            name = row[0]
            value = row[1]
            default = row[2]
            type = row[3]
            description = row[4]
            
            properties[name] = {
                "value": value,
                "default": default,
                "type": type,
                "description": description
            }
            
        return properties
        
    def set_session_property(self, name: str, value: str):
        """Set session property"""
        self.execute(f"SET SESSION {name} = '{value}'")
        
    def reset_session_property(self, name: str):
        """Reset session property to default"""
        self.execute(f"RESET SESSION {name}")
        
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        response = self._session.get(
            f"{self._base_url}/v1/info",
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json()
        
    def get_node_info(self) -> List[Dict[str, Any]]:
        """Get information about cluster nodes"""
        response = self._session.get(
            f"{self._base_url}/v1/node",
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json()
        
    def create_table_as(
        self,
        table_name: str,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        if_not_exists: bool = False
    ) -> QueryResult:
        """Create table as select"""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        
        if not catalog or not schema:
            raise ValueError("Both catalog and schema must be specified")
            
        create_query = f"CREATE TABLE "
        
        if if_not_exists:
            create_query += "IF NOT EXISTS "
            
        create_query += f"{catalog}.{schema}.{table_name} "
        
        if properties:
            props_str = ", ".join(
                f"{k} = {v}" for k, v in properties.items()
            )
            create_query += f"WITH ({props_str}) "
            
        create_query += f"AS {query}"
        
        return self.execute(create_query)
        
    def analyze_table(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> QueryResult:
        """Analyze table statistics"""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        
        if not catalog or not schema:
            raise ValueError("Both catalog and schema must be specified")
            
        return self.execute(
            f"ANALYZE {catalog}.{schema}.{table}"
        ) 