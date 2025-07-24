"""
PostgreSQL Connector Plugin

Provides PostgreSQL database connectivity for the Integration Hub.
"""

from typing import Dict, Any, List, Optional, Union
import asyncio
import asyncpg
from datetime import datetime

from ...core.base import BaseConnector, ConnectorConfig
from data_intelligence_common.core.patterns.resilience import ResiliencePolicy
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector implementation"""
    
    def __init__(self, config: ConnectorConfig, resilience_policy: ResiliencePolicy):
        super().__init__(config, resilience_policy)
        self.supports_pooling = True
        self.supports_credential_rotation = True
        
        # Connection details
        self._pool: Optional[asyncpg.Pool] = None
        self._connection_params = self._build_connection_params()
        
    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from config"""
        conn_config = self.config.connection_config
        
        return {
            'host': conn_config.host,
            'port': conn_config.port,
            'database': conn_config.database,
            'user': conn_config.username,
            'password': conn_config.password,
            'ssl': conn_config.ssl_enabled,
            'timeout': conn_config.timeout,
            'command_timeout': conn_config.timeout,
            'server_settings': conn_config.additional_params.get('server_settings', {})
        }
        
    async def initialize(self):
        """Initialize PostgreSQL connection pool"""
        try:
            # Create connection pool with resilience
            @self.resilience_policy.apply()
            async def create_pool():
                return await asyncpg.create_pool(
                    **self._connection_params,
                    min_size=2,
                    max_size=self.config.connection_config.pool_size,
                    max_queries=50000,
                    max_inactive_connection_lifetime=300
                )
                
            self._pool = await create_pool()
            logger.info(f"PostgreSQL connector initialized for {self.config.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connector: {e}")
            raise
            
    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
            
    async def query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        @self.resilience_policy.apply()
        async def execute_query():
            async with self._pool.acquire() as conn:
                # Prepare query with parameters
                if parameters:
                    # Convert dict parameters to positional
                    param_values = list(parameters.values())
                    rows = await conn.fetch(query, *param_values)
                else:
                    rows = await conn.fetch(query)
                    
                # Convert to list of dicts
                return [dict(row) for row in rows]
                
        try:
            result = await execute_query()
            
            # Record metrics
            logger.info(
                "Query executed",
                connector=self.config.name,
                rows_returned=len(result)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}", query=query)
            raise
            
    async def write(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Write data to PostgreSQL"""
        write_config = config or {}
        table = write_config.get('table')
        if not table:
            raise ValueError("Table name is required in write config")
            
        # Ensure data is a list
        records = data if isinstance(data, list) else [data]
        if not records:
            return {"rows_affected": 0}
            
        # Get column names from first record
        columns = list(records[0].keys())
        
        @self.resilience_policy.apply()
        async def execute_write():
            async with self._pool.acquire() as conn:
                # Build insert query
                placeholders = []
                values = []
                
                for i, record in enumerate(records):
                    row_placeholders = []
                    for j, col in enumerate(columns):
                        placeholder = f"${len(values) + 1}"
                        row_placeholders.append(placeholder)
                        values.append(record.get(col))
                    placeholders.append(f"({','.join(row_placeholders)})")
                    
                insert_query = f"""
                    INSERT INTO {table} ({','.join(columns)})
                    VALUES {','.join(placeholders)}
                """
                
                # Handle conflict resolution
                conflict_resolution = write_config.get('conflict_resolution', 'fail')
                if conflict_resolution == 'ignore':
                    insert_query += " ON CONFLICT DO NOTHING"
                elif conflict_resolution == 'update':
                    update_cols = write_config.get('update_columns', columns)
                    update_clause = ','.join([
                        f"{col} = EXCLUDED.{col}" for col in update_cols
                    ])
                    insert_query += f" ON CONFLICT ({write_config.get('conflict_columns', 'id')}) DO UPDATE SET {update_clause}"
                    
                # Execute insert
                result = await conn.execute(insert_query, *values)
                
                # Extract rows affected
                rows_affected = int(result.split()[-1])
                
                return rows_affected
                
        try:
            rows_affected = await execute_write()
            
            logger.info(
                "Data written",
                connector=self.config.name,
                table=table,
                rows_affected=rows_affected
            )
            
            return {
                "rows_affected": rows_affected,
                "table": table,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Write operation failed: {e}", table=table)
            raise
            
    async def check_health(self) -> Dict[str, Any]:
        """Check connector health"""
        try:
            # Check pool status
            pool_size = self._pool.get_size() if self._pool else 0
            idle_size = self._pool.get_idle_size() if self._pool else 0
            
            # Test query
            test_passed = await self.test_connection()
            
            return {
                "healthy": test_passed and pool_size > 0,
                "pool_size": pool_size,
                "idle_connections": idle_size,
                "active_connections": pool_size - idle_size
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            }
            
    async def close(self):
        """Close connection pool"""
        if self._pool:
            await self._pool.close()
            logger.info(f"PostgreSQL connector {self.config.name} closed")
            
    async def update_credentials(self, credentials: Dict[str, Any]):
        """Update connection credentials"""
        # Update connection parameters
        self._connection_params['user'] = credentials.get('username')
        self._connection_params['password'] = credentials.get('password')
        
        # Recreate pool with new credentials
        old_pool = self._pool
        await self.initialize()
        
        # Close old pool
        if old_pool:
            await old_pool.close()
            
        logger.info(f"Credentials updated for {self.config.name}")
        
    # Additional PostgreSQL-specific methods
    
    async def execute_transaction(
        self,
        queries: List[Dict[str, Any]]
    ) -> List[Any]:
        """Execute multiple queries in a transaction"""
        @self.resilience_policy.apply()
        async def execute_transaction():
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    results = []
                    
                    for query_info in queries:
                        query = query_info['query']
                        params = query_info.get('parameters', [])
                        
                        if query.strip().upper().startswith('SELECT'):
                            result = await conn.fetch(query, *params)
                            results.append([dict(row) for row in result])
                        else:
                            result = await conn.execute(query, *params)
                            results.append(result)
                            
                    return results
                    
        return await execute_transaction()
        
    async def copy_from(
        self,
        table: str,
        source: Union[str, List[Dict[str, Any]]],
        columns: Optional[List[str]] = None
    ) -> int:
        """Bulk copy data using COPY command"""
        @self.resilience_policy.apply()
        async def execute_copy():
            async with self._pool.acquire() as conn:
                if isinstance(source, str):
                    # Copy from file
                    result = await conn.copy_to_table(
                        table,
                        source=source,
                        columns=columns
                    )
                else:
                    # Copy from records
                    result = await conn.copy_records_to_table(
                        table,
                        records=source,
                        columns=columns
                    )
                    
                return result
                
        rows_copied = await execute_copy()
        
        logger.info(
            "Bulk copy completed",
            connector=self.config.name,
            table=table,
            rows_copied=rows_copied
        )
        
        return rows_copied
        
    async def get_table_schema(self, table: str) -> Dict[str, Any]:
        """Get table schema information"""
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """
        
        columns = await self.query(query, {"table": table})
        
        return {
            "table": table,
            "columns": columns,
            "column_count": len(columns)
        }


# Plugin registration
__connector_class__ = PostgreSQLConnector
__connector_type__ = "postgresql" 