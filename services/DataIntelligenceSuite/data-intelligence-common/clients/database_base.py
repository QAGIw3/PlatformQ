"""
Unified Database Client Base

Provides a common interface and implementation for all database clients.
"""

from typing import Any, Dict, List, Optional, Union, AsyncIterator, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import abstractmethod
import asyncio
from contextlib import asynccontextmanager

from .base import BaseClient, ClientConfig
from ..core.patterns.resilience import RetryConfig, CircuitBreakerConfig
from ..monitoring import StructuredLogger
from ..caching import cached

logger = StructuredLogger.get_logger(__name__)


class DatabaseType(str, Enum):
    """Supported database types"""
    RELATIONAL = "relational"      # PostgreSQL, MySQL, etc.
    NOSQL_DOCUMENT = "document"     # MongoDB, CouchDB
    NOSQL_KEYVALUE = "keyvalue"     # Redis, Ignite
    NOSQL_COLUMN = "column"         # Cassandra, HBase
    NOSQL_GRAPH = "graph"           # JanusGraph, Neo4j
    TIMESERIES = "timeseries"       # InfluxDB, TimescaleDB
    ANALYTICAL = "analytical"       # ClickHouse, Druid
    SEARCH = "search"               # Elasticsearch, Solr


@dataclass
class DatabaseConfig(ClientConfig):
    """Unified database configuration"""
    # Database settings
    database_type: DatabaseType = DatabaseType.RELATIONAL
    database_name: Optional[str] = None
    schema: Optional[str] = None
    
    # Connection pooling
    pool_size: int = 10
    pool_overflow: int = 5
    pool_timeout: float = 30.0
    pool_recycle: int = 3600
    
    # Query settings
    query_timeout: float = 30.0
    fetch_size: int = 1000
    max_results: Optional[int] = None
    
    # Consistency (for distributed databases)
    consistency_level: str = "quorum"
    read_preference: str = "primary"
    
    # Transactions
    auto_commit: bool = True
    isolation_level: Optional[str] = None
    
    # Performance
    enable_query_cache: bool = True
    query_cache_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    enable_prepared_statements: bool = True
    
    # Schema management
    auto_create_schema: bool = False
    schema_version: Optional[str] = None


class DatabaseClient(BaseClient):
    """
    Unified database client with common operations.
    
    Features:
    - Connection pooling
    - Query caching
    - Prepared statements
    - Transaction management
    - Schema versioning
    - Batch operations
    - Streaming results
    """
    
    def __init__(self, config: DatabaseConfig, **kwargs):
        super().__init__(config, **kwargs)
        self.config: DatabaseConfig = config
        
        # Connection pool
        self._pool = None
        self._prepared_statements: Dict[str, Any] = {}
        
    # Abstract methods to be implemented by specific database clients
    
    @abstractmethod
    async def _create_pool(self) -> Any:
        """Create database-specific connection pool"""
        pass
        
    @abstractmethod
    async def _execute_raw(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        **kwargs
    ) -> Any:
        """Execute raw query with database-specific implementation"""
        pass
        
    @abstractmethod
    async def _fetch_all(self, result: Any) -> List[Dict[str, Any]]:
        """Fetch all results from database-specific result object"""
        pass
        
    @abstractmethod
    async def _fetch_one(self, result: Any) -> Optional[Dict[str, Any]]:
        """Fetch one result from database-specific result object"""
        pass
        
    # Common interface methods
    
    async def initialize(self):
        """Initialize database client"""
        await super().initialize()
        
        # Create connection pool
        self._pool = await self._create_pool()
        
        # Initialize schema if needed
        if self.config.auto_create_schema:
            await self.ensure_schema()
            
        logger.info(f"Database client initialized for {self.config.name}")
        
    async def shutdown(self):
        """Shutdown database client"""
        # Close prepared statements
        self._prepared_statements.clear()
        
        # Close pool
        if self._pool:
            await self._close_pool()
            
        await super().shutdown()
        
    @abstractmethod
    async def _close_pool(self):
        """Close database-specific connection pool"""
        pass
        
    # Query execution methods
    
    @cached(key_func=lambda self, query, params, **kwargs: f"query:{hash(query)}:{hash(str(params))}")
    async def query(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        fetch_size: Optional[int] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Execute query and return all results.
        
        Args:
            query: SQL or query string
            parameters: Query parameters
            fetch_size: Number of rows to fetch at once
            timeout: Query timeout in seconds
            **kwargs: Additional database-specific options
            
        Returns:
            List of result dictionaries
        """
        result = await self._execute_raw(
            query,
            parameters,
            fetch_size=fetch_size or self.config.fetch_size,
            timeout=timeout or self.config.query_timeout,
            **kwargs
        )
        
        return await self._fetch_all(result)
        
    async def query_one(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """Execute query and return first result"""
        result = await self._execute_raw(
            query,
            parameters,
            timeout=timeout or self.config.query_timeout,
            **kwargs
        )
        
        return await self._fetch_one(result)
        
    async def query_stream(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        batch_size: int = 1000,
        timeout: Optional[float] = None,
        **kwargs
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Execute query and stream results.
        
        Yields results one at a time to handle large datasets efficiently.
        """
        result = await self._execute_raw(
            query,
            parameters,
            fetch_size=batch_size,
            timeout=timeout or self.config.query_timeout,
            stream=True,
            **kwargs
        )
        
        async for row in self._stream_results(result):
            yield row
            
    @abstractmethod
    async def _stream_results(self, result: Any) -> AsyncIterator[Dict[str, Any]]:
        """Stream results from database-specific result object"""
        pass
        
    # Write operations
    
    async def execute(
        self,
        query: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute a write operation (INSERT, UPDATE, DELETE).
        
        Returns:
            Dictionary with execution metadata (rows_affected, etc.)
        """
        result = await self._execute_raw(
            query,
            parameters,
            timeout=timeout or self.config.query_timeout,
            **kwargs
        )
        
        return await self._get_execution_metadata(result)
        
    @abstractmethod
    async def _get_execution_metadata(self, result: Any) -> Dict[str, Any]:
        """Get execution metadata from database-specific result"""
        pass
        
    # Batch operations
    
    async def execute_batch(
        self,
        queries: List[Union[str, Tuple[str, Any]]],
        batch_size: int = 100,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple queries in batches.
        
        Args:
            queries: List of queries or (query, parameters) tuples
            batch_size: Number of queries per batch
            
        Returns:
            List of execution results
        """
        results = []
        
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            batch_results = await self._execute_batch(batch, **kwargs)
            results.extend(batch_results)
            
        return results
        
    @abstractmethod
    async def _execute_batch(
        self,
        queries: List[Union[str, Tuple[str, Any]]],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Execute a batch of queries with database-specific implementation"""
        pass
        
    # Transaction management
    
    @asynccontextmanager
    async def transaction(self, isolation_level: Optional[str] = None):
        """
        Transaction context manager.
        
        Usage:
            async with client.transaction():
                await client.execute("INSERT ...")
                await client.execute("UPDATE ...")
        """
        conn = await self._get_connection()
        
        try:
            await self._begin_transaction(conn, isolation_level)
            yield conn
            await self._commit_transaction(conn)
        except Exception as e:
            await self._rollback_transaction(conn)
            raise
        finally:
            await self._release_connection(conn)
            
    @abstractmethod
    async def _get_connection(self) -> Any:
        """Get a connection from the pool"""
        pass
        
    @abstractmethod
    async def _release_connection(self, conn: Any):
        """Release connection back to pool"""
        pass
        
    @abstractmethod
    async def _begin_transaction(self, conn: Any, isolation_level: Optional[str]):
        """Begin transaction with database-specific implementation"""
        pass
        
    @abstractmethod
    async def _commit_transaction(self, conn: Any):
        """Commit transaction"""
        pass
        
    @abstractmethod
    async def _rollback_transaction(self, conn: Any):
        """Rollback transaction"""
        pass
        
    # Schema management
    
    async def ensure_schema(self) -> bool:
        """Ensure database schema exists and is up to date"""
        if self.config.database_name:
            await self.create_database_if_not_exists(self.config.database_name)
            
        if self.config.schema:
            await self.create_schema_if_not_exists(self.config.schema)
            
        if self.config.schema_version:
            await self.migrate_schema(self.config.schema_version)
            
        return True
        
    @abstractmethod
    async def create_database_if_not_exists(self, database: str) -> bool:
        """Create database if it doesn't exist"""
        pass
        
    @abstractmethod
    async def create_schema_if_not_exists(self, schema: str) -> bool:
        """Create schema if it doesn't exist"""
        pass
        
    async def migrate_schema(self, target_version: str) -> bool:
        """Migrate schema to target version"""
        # Default implementation - can be overridden
        logger.info(f"Schema migration to version {target_version} not implemented")
        return True
        
    # Prepared statements
    
    async def prepare(self, name: str, query: str) -> bool:
        """Prepare a statement for repeated execution"""
        if self.config.enable_prepared_statements:
            prepared = await self._prepare_statement(name, query)
            self._prepared_statements[name] = prepared
            return True
        return False
        
    @abstractmethod
    async def _prepare_statement(self, name: str, query: str) -> Any:
        """Prepare statement with database-specific implementation"""
        pass
        
    async def execute_prepared(
        self,
        name: str,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        **kwargs
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute a prepared statement"""
        if name not in self._prepared_statements:
            raise ValueError(f"Prepared statement '{name}' not found")
            
        return await self._execute_prepared(
            self._prepared_statements[name],
            parameters,
            **kwargs
        )
        
    @abstractmethod
    async def _execute_prepared(
        self,
        prepared: Any,
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None,
        **kwargs
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute prepared statement with database-specific implementation"""
        pass
        
    # Utility methods
    
    async def ping(self) -> bool:
        """Check database connectivity"""
        try:
            await self.query_one("SELECT 1")
            return True
        except Exception:
            return False
            
    async def get_table_info(self, table: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata"""
        return await self._get_table_info(table, schema or self.config.schema)
        
    @abstractmethod
    async def _get_table_info(self, table: str, schema: Optional[str]) -> Dict[str, Any]:
        """Get table info with database-specific implementation"""
        pass
        
    async def table_exists(self, table: str, schema: Optional[str] = None) -> bool:
        """Check if table exists"""
        info = await self.get_table_info(table, schema)
        return info is not None and info.get("exists", False)
        
    # High-level convenience methods
    
    async def insert(
        self,
        table: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        returning: Optional[List[str]] = None,
        **kwargs
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Insert data into table"""
        if isinstance(data, dict):
            return await self._insert_one(table, data, returning, **kwargs)
        else:
            return await self._insert_many(table, data, returning, **kwargs)
            
    @abstractmethod
    async def _insert_one(
        self,
        table: str,
        data: Dict[str, Any],
        returning: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Insert single row"""
        pass
        
    @abstractmethod
    async def _insert_many(
        self,
        table: str,
        data: List[Dict[str, Any]],
        returning: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Insert multiple rows"""
        pass
        
    async def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Update rows in table"""
        return await self._update(table, data, where, **kwargs)
        
    @abstractmethod
    async def _update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Update implementation"""
        pass
        
    async def delete(
        self,
        table: str,
        where: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Delete rows from table"""
        return await self._delete(table, where, **kwargs)
        
    @abstractmethod
    async def _delete(
        self,
        table: str,
        where: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Delete implementation"""
        pass
        
    async def upsert(
        self,
        table: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        conflict_columns: List[str],
        **kwargs
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Insert or update data"""
        if isinstance(data, dict):
            return await self._upsert_one(table, data, conflict_columns, **kwargs)
        else:
            return await self._upsert_many(table, data, conflict_columns, **kwargs)
            
    @abstractmethod
    async def _upsert_one(
        self,
        table: str,
        data: Dict[str, Any],
        conflict_columns: List[str],
        **kwargs
    ) -> Dict[str, Any]:
        """Upsert single row"""
        pass
        
    @abstractmethod
    async def _upsert_many(
        self,
        table: str,
        data: List[Dict[str, Any]],
        conflict_columns: List[str],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Upsert multiple rows"""
        pass


# Export main classes
__all__ = ['DatabaseClient', 'DatabaseConfig', 'DatabaseType'] 