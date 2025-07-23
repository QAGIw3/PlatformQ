"""
ClickHouse Client Integration

Provides column-oriented database for real-time analytics.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from clickhouse_driver import Client as SyncClient
from aiochclient import ChClient
import pandas as pd

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class Engine(str, Enum):
    """ClickHouse table engines"""
    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    SUMMING_MERGE_TREE = "SummingMergeTree"
    AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
    COLLAPSING_MERGE_TREE = "CollapsingMergeTree"
    VERSIONED_COLLAPSING_MERGE_TREE = "VersionedCollapsingMergeTree"
    DISTRIBUTED = "Distributed"
    REPLICATED_MERGE_TREE = "ReplicatedMergeTree"
    KAFKA = "Kafka"
    MATERIALIZED_VIEW = "MaterializedView"


class DataType(str, Enum):
    """Common ClickHouse data types"""
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    STRING = "String"
    FIXED_STRING = "FixedString"
    DATE = "Date"
    DATETIME = "DateTime"
    DATETIME64 = "DateTime64"
    ARRAY = "Array"
    NESTED = "Nested"
    TUPLE = "Tuple"
    ENUM = "Enum"
    UUID = "UUID"
    IPV4 = "IPv4"
    IPV6 = "IPv6"


@dataclass
class ClickHouseConfig(ClientConfig):
    """Configuration for ClickHouse client"""
    # Connection settings
    host: str = "localhost"
    port: int = 9000
    http_port: int = 8123
    database: str = "default"
    
    # Authentication
    user: str = "default"
    password: Optional[str] = None
    
    # Query settings
    max_execution_time: int = 300
    max_memory_usage: int = 10 * 1024 * 1024 * 1024  # 10GB
    max_rows_to_read: int = 1000000000
    
    # Connection pool
    pool_size: int = 10
    
    # Compression
    compression: bool = True
    
    # Cluster settings
    cluster_name: Optional[str] = None
    distributed_ddl_timeout: int = 180
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "clickhouse"


@dataclass
class TableColumn:
    """Table column definition"""
    name: str
    type: Union[DataType, str]
    nullable: bool = False
    default: Optional[str] = None
    codec: Optional[str] = None
    ttl: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert to SQL column definition"""
        sql = f"`{self.name}` "
        
        if self.nullable:
            sql += f"Nullable({self.type})"
        else:
            sql += str(self.type)
        
        if self.default:
            sql += f" DEFAULT {self.default}"
        
        if self.codec:
            sql += f" CODEC({self.codec})"
        
        if self.ttl:
            sql += f" TTL {self.ttl}"
        
        return sql


@dataclass
class TableDefinition:
    """Table definition"""
    name: str
    columns: List[TableColumn]
    engine: Engine
    
    # Engine parameters
    partition_by: Optional[str] = None
    order_by: Optional[List[str]] = None
    primary_key: Optional[List[str]] = None
    sample_by: Optional[str] = None
    ttl: Optional[str] = None
    settings: Dict[str, Any] = field(default_factory=dict)
    
    # For replicated tables
    zk_path: Optional[str] = None
    replica_name: Optional[str] = None
    
    # For distributed tables
    cluster: Optional[str] = None
    sharding_key: Optional[str] = None
    
    def to_create_sql(self, database: Optional[str] = None) -> str:
        """Generate CREATE TABLE SQL"""
        table_name = f"{database}.{self.name}" if database else self.name
        
        # Columns
        columns_sql = ",\n    ".join(col.to_sql() for col in self.columns)
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name}\n(\n    {columns_sql}\n)\n"
        
        # Engine
        if self.engine == Engine.REPLICATED_MERGE_TREE:
            sql += f"ENGINE = {self.engine.value}('{self.zk_path}', '{self.replica_name}')\n"
        elif self.engine == Engine.DISTRIBUTED:
            local_table = self.settings.get('local_table', f"{self.name}_local")
            sql += f"ENGINE = {self.engine.value}('{self.cluster}', currentDatabase(), '{local_table}', {self.sharding_key or 'rand()'})\n"
        else:
            sql += f"ENGINE = {self.engine.value}()\n"
        
        # Engine clauses
        if self.partition_by:
            sql += f"PARTITION BY {self.partition_by}\n"
        
        if self.order_by:
            sql += f"ORDER BY ({', '.join(self.order_by)})\n"
        elif self.engine in [Engine.MERGE_TREE, Engine.REPLICATED_MERGE_TREE]:
            # MergeTree family requires ORDER BY
            sql += "ORDER BY tuple()\n"
        
        if self.primary_key:
            sql += f"PRIMARY KEY ({', '.join(self.primary_key)})\n"
        
        if self.sample_by:
            sql += f"SAMPLE BY {self.sample_by}\n"
        
        if self.ttl:
            sql += f"TTL {self.ttl}\n"
        
        # Settings
        if self.settings:
            settings_str = ", ".join(f"{k} = {v}" for k, v in self.settings.items())
            sql += f"SETTINGS {settings_str}"
        
        return sql


@dataclass
class QueryResult:
    """Query execution result"""
    data: List[Tuple[Any, ...]]
    columns: List[str]
    types: List[str]
    row_count: int
    execution_time: float
    bytes_read: int = 0
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        return pd.DataFrame(self.data, columns=self.columns)
    
    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries"""
        return [dict(zip(self.columns, row)) for row in self.data]


class ClickHouseClient(BaseServiceClient):
    """
    ClickHouse client for column-oriented analytics.
    
    Features:
    - Columnar storage
    - Real-time query performance
    - Data compression
    - Replication and sharding
    - Materialized views
    - Time-series optimization
    """
    
    def __init__(
        self,
        config: Optional[ClickHouseConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = ClickHouseConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: ClickHouseConfig = config
        self._sync_client: Optional[SyncClient] = None
        self._async_client: Optional[ChClient] = None
        
    async def connect(self):
        """Connect to ClickHouse"""
        await super().connect()
        
        try:
            # Get credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.user = creds.get("user", self.config.user)
                    self.config.password = creds.get("password")
            
            # Create sync client for DDL operations
            self._sync_client = SyncClient(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                compression=self.config.compression,
                secure=self.config.use_ssl,
                verify=self.config.verify_ssl,
                settings={
                    'max_execution_time': self.config.max_execution_time,
                    'max_memory_usage': self.config.max_memory_usage,
                    'max_rows_to_read': self.config.max_rows_to_read
                }
            )
            
            # Create async client for queries
            url = f"http://{self.config.host}:{self.config.http_port}"
            if self.config.use_ssl:
                url = f"https://{self.config.host}:{self.config.http_port}"
            
            self._async_client = ChClient(
                url=url,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                compress_response=self.config.compression
            )
            
            # Test connection
            await self.execute_query("SELECT 1")
            
            logger.info(f"Connected to ClickHouse: {self.config.host}:{self.config.port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    async def create_database(
        self,
        database: str,
        on_cluster: bool = False,
        engine: str = "Atomic"
    ) -> bool:
        """
        Create database.
        
        Args:
            database: Database name
            on_cluster: Create on cluster
            engine: Database engine
            
        Returns:
            Success status
        """
        try:
            sql = f"CREATE DATABASE IF NOT EXISTS {database}"
            
            if on_cluster and self.config.cluster_name:
                sql += f" ON CLUSTER {self.config.cluster_name}"
            
            sql += f" ENGINE = {engine}"
            
            self._sync_client.execute(sql)
            logger.info(f"Created database: {database}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
    
    async def create_table(
        self,
        table: TableDefinition,
        database: Optional[str] = None,
        on_cluster: bool = False
    ) -> bool:
        """
        Create table.
        
        Args:
            table: Table definition
            database: Target database
            on_cluster: Create on cluster
            
        Returns:
            Success status
        """
        try:
            sql = table.to_create_sql(database or self.config.database)
            
            if on_cluster and self.config.cluster_name:
                sql = sql.replace("CREATE TABLE", f"CREATE TABLE ON CLUSTER {self.config.cluster_name}")
            
            self._sync_client.execute(sql)
            logger.info(f"Created table: {table.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    async def insert_data(
        self,
        table: str,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        database: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> int:
        """
        Insert data into table.
        
        Args:
            table: Table name
            data: Data to insert
            database: Target database
            columns: Column names (auto-detected if not provided)
            
        Returns:
            Number of rows inserted
        """
        try:
            table_name = f"{database or self.config.database}.{table}"
            
            # Convert DataFrame to list of dicts
            if isinstance(data, pd.DataFrame):
                data = data.to_dict('records')
            
            if not data:
                return 0
            
            # Auto-detect columns if not provided
            if not columns:
                columns = list(data[0].keys())
            
            # Prepare values
            values = []
            for row in data:
                values.append(tuple(row.get(col) for col in columns))
            
            # Insert data
            columns_str = ", ".join(f"`{col}`" for col in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES"
            
            self._sync_client.execute(query, values)
            
            logger.info(f"Inserted {len(values)} rows into {table_name}")
            return len(values)
            
        except Exception as e:
            logger.error(f"Failed to insert data: {e}")
            raise
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        with_column_types: bool = True
    ) -> QueryResult:
        """
        Execute query.
        
        Args:
            query: SQL query
            params: Query parameters
            with_column_types: Include column types in result
            
        Returns:
            Query result
        """
        try:
            start_time = datetime.now()
            
            # Execute query
            if params:
                result = await self._async_client.execute(query, params)
            else:
                result = await self._async_client.execute(query)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Parse result
            if with_column_types and hasattr(result, 'column_names'):
                columns = result.column_names
                types = result.column_types if hasattr(result, 'column_types') else []
                data = result.data
            else:
                # Simple result
                columns = []
                types = []
                data = result
            
            return QueryResult(
                data=data,
                columns=columns,
                types=types,
                row_count=len(data),
                execution_time=execution_time
            )
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    async def create_materialized_view(
        self,
        view_name: str,
        select_query: str,
        to_table: Optional[str] = None,
        populate: bool = True,
        on_cluster: bool = False
    ) -> bool:
        """
        Create materialized view.
        
        Args:
            view_name: View name
            select_query: SELECT query for the view
            to_table: Target table (auto-created if not specified)
            populate: Populate with existing data
            on_cluster: Create on cluster
            
        Returns:
            Success status
        """
        try:
            sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}"
            
            if on_cluster and self.config.cluster_name:
                sql += f" ON CLUSTER {self.config.cluster_name}"
            
            if to_table:
                sql += f" TO {to_table}"
            
            sql += f" AS {select_query}"
            
            if not populate:
                sql = sql.replace("CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW EMPTY")
            
            self._sync_client.execute(sql)
            logger.info(f"Created materialized view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create materialized view: {e}")
            return False
    
    async def optimize_table(
        self,
        table: str,
        partition: Optional[str] = None,
        final: bool = False,
        deduplicate: bool = False
    ) -> bool:
        """
        Optimize table.
        
        Args:
            table: Table name
            partition: Specific partition to optimize
            final: Force final merge
            deduplicate: Remove duplicates
            
        Returns:
            Success status
        """
        try:
            sql = f"OPTIMIZE TABLE {table}"
            
            if partition:
                sql += f" PARTITION {partition}"
            
            if final:
                sql += " FINAL"
            
            if deduplicate:
                sql += " DEDUPLICATE"
            
            self._sync_client.execute(sql)
            logger.info(f"Optimized table: {table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            return False
    
    async def get_table_stats(
        self,
        table: str,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get table statistics.
        
        Args:
            table: Table name
            database: Database name
            
        Returns:
            Table statistics
        """
        try:
            db = database or self.config.database
            
            # Get basic stats
            stats_query = f"""
            SELECT
                sum(rows) as total_rows,
                sum(bytes_on_disk) as total_bytes,
                sum(data_compressed_bytes) as compressed_bytes,
                sum(data_uncompressed_bytes) as uncompressed_bytes,
                count() as parts_count,
                max(modification_time) as last_modified
            FROM system.parts
            WHERE database = '{db}' AND table = '{table}' AND active
            """
            
            result = await self.execute_query(stats_query)
            stats = result.to_dict_list()[0] if result.data else {}
            
            # Get column stats
            columns_query = f"""
            SELECT
                name,
                type,
                data_compressed_bytes,
                data_uncompressed_bytes,
                marks_bytes
            FROM system.columns
            WHERE database = '{db}' AND table = '{table}'
            """
            
            columns_result = await self.execute_query(columns_query)
            stats['columns'] = columns_result.to_dict_list()
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}
    
    async def create_distributed_table(
        self,
        table_name: str,
        local_table: TableDefinition,
        cluster: Optional[str] = None,
        sharding_key: Optional[str] = None
    ) -> bool:
        """
        Create distributed table across cluster.
        
        Args:
            table_name: Distributed table name
            local_table: Local table definition
            cluster: Cluster name
            sharding_key: Sharding expression
            
        Returns:
            Success status
        """
        try:
            cluster = cluster or self.config.cluster_name
            if not cluster:
                raise ValueError("Cluster name required for distributed table")
            
            # Create local table on all nodes
            local_table_name = f"{table_name}_local"
            local_table.name = local_table_name
            
            await self.create_table(local_table, on_cluster=True)
            
            # Create distributed table
            distributed_table = TableDefinition(
                name=table_name,
                columns=local_table.columns,
                engine=Engine.DISTRIBUTED,
                cluster=cluster,
                sharding_key=sharding_key,
                settings={'local_table': local_table_name}
            )
            
            await self.create_table(distributed_table, on_cluster=True)
            
            logger.info(f"Created distributed table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create distributed table: {e}")
            return False
    
    async def execute_mutations(
        self,
        table: str,
        update_expr: Optional[str] = None,
        delete_where: Optional[str] = None
    ) -> str:
        """
        Execute mutations (UPDATE/DELETE).
        
        Args:
            table: Table name
            update_expr: UPDATE expression
            delete_where: DELETE WHERE clause
            
        Returns:
            Mutation ID
        """
        try:
            if update_expr and delete_where:
                raise ValueError("Cannot UPDATE and DELETE in same mutation")
            
            if update_expr:
                sql = f"ALTER TABLE {table} UPDATE {update_expr}"
            elif delete_where:
                sql = f"ALTER TABLE {table} DELETE WHERE {delete_where}"
            else:
                raise ValueError("Either update_expr or delete_where required")
            
            result = self._sync_client.execute(sql)
            
            # Get mutation ID
            mutation_query = f"""
            SELECT mutation_id
            FROM system.mutations
            WHERE database = '{self.config.database}' AND table = '{table}'
            ORDER BY create_time DESC
            LIMIT 1
            """
            
            mutation_result = self._sync_client.execute(mutation_query)
            mutation_id = mutation_result[0][0] if mutation_result else "unknown"
            
            logger.info(f"Started mutation {mutation_id} on table: {table}")
            return mutation_id
            
        except Exception as e:
            logger.error(f"Failed to execute mutation: {e}")
            raise
    
    async def wait_for_mutation(
        self,
        table: str,
        mutation_id: str,
        timeout: int = 300
    ) -> bool:
        """
        Wait for mutation to complete.
        
        Args:
            table: Table name
            mutation_id: Mutation ID
            timeout: Timeout in seconds
            
        Returns:
            Success status
        """
        try:
            start_time = datetime.now()
            
            while (datetime.now() - start_time).total_seconds() < timeout:
                query = f"""
                SELECT is_done
                FROM system.mutations
                WHERE database = '{self.config.database}'
                    AND table = '{table}'
                    AND mutation_id = '{mutation_id}'
                """
                
                result = self._sync_client.execute(query)
                
                if result and result[0][0]:
                    logger.info(f"Mutation {mutation_id} completed")
                    return True
                
                await asyncio.sleep(1)
            
            logger.warning(f"Mutation {mutation_id} timed out")
            return False
            
        except Exception as e:
            logger.error(f"Failed to wait for mutation: {e}")
            return False
    
    async def close(self):
        """Close ClickHouse connections"""
        if self._sync_client:
            self._sync_client.disconnect()
        
        if self._async_client:
            await self._async_client.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get ClickHouse specific configuration"""
        return {
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "cluster_name": self.config.cluster_name,
            "max_execution_time": self.config.max_execution_time,
            "compression": self.config.compression
        } 