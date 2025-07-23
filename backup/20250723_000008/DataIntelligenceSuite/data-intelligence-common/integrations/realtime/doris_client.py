"""
Apache Doris Client Integration

Provides real-time analytical database capabilities with MPP architecture.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
import asyncio
import pymysql
import pandas as pd
from urllib.parse import quote_plus

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TableModel(str, Enum):
    """Doris table models"""
    DUPLICATE = "DUPLICATE"
    AGGREGATE = "AGGREGATE"
    UNIQUE = "UNIQUE"


class PartitionType(str, Enum):
    """Partition types"""
    RANGE = "RANGE"
    LIST = "LIST"


class AggregationType(str, Enum):
    """Aggregation types for AGGREGATE model"""
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    REPLACE = "REPLACE"
    HLL_UNION = "HLL_UNION"
    BITMAP_UNION = "BITMAP_UNION"
    REPLACE_IF_NOT_NULL = "REPLACE_IF_NOT_NULL"


class IndexType(str, Enum):
    """Index types"""
    BITMAP = "BITMAP"
    BLOOM_FILTER = "BLOOM_FILTER"
    INVERTED = "INVERTED"


@dataclass
class DorisConfig(ClientConfig):
    """Configuration for Doris client"""
    # Connection settings
    fe_host: str = "localhost"  # Frontend host
    fe_port: int = 9030         # Frontend MySQL port
    fe_http_port: int = 8030    # Frontend HTTP port
    be_http_port: int = 8040    # Backend HTTP port
    database: str = "default"
    
    # Authentication
    user: str = "root"
    password: Optional[str] = None
    
    # Query settings
    query_timeout: int = 300
    enable_profile: bool = False
    
    # Load settings
    max_filter_ratio: float = 0.0
    strict_mode: bool = True
    
    # Connection pool
    pool_size: int = 10
    pool_recycle: int = 3600
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "doris"


@dataclass
class Column:
    """Column definition"""
    name: str
    type: str
    nullable: bool = True
    default: Optional[str] = None
    aggregation_type: Optional[AggregationType] = None
    comment: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert to SQL column definition"""
        sql = f"`{self.name}` {self.type}"
        
        if self.aggregation_type:
            sql += f" {self.aggregation_type.value}"
        
        if not self.nullable:
            sql += " NOT NULL"
        
        if self.default is not None:
            sql += f" DEFAULT '{self.default}'"
        
        if self.comment:
            sql += f" COMMENT '{self.comment}'"
        
        return sql


@dataclass
class TableDefinition:
    """Doris table definition"""
    name: str
    columns: List[Column]
    model: TableModel = TableModel.DUPLICATE
    
    # Keys
    duplicate_keys: Optional[List[str]] = None
    aggregate_keys: Optional[List[str]] = None
    unique_keys: Optional[List[str]] = None
    
    # Distribution
    distributed_by: List[str] = field(default_factory=list)
    buckets: int = 10
    
    # Partition
    partition_by: Optional[str] = None
    partition_type: PartitionType = PartitionType.RANGE
    partitions: Optional[List[Dict[str, Any]]] = None
    
    # Properties
    replication_num: int = 1
    storage_format: str = "V2"
    compression: str = "LZ4"
    
    # Indexes
    bitmap_indexes: List[str] = field(default_factory=list)
    bloom_filter_columns: List[str] = field(default_factory=list)
    
    def to_create_sql(self, database: Optional[str] = None) -> str:
        """Generate CREATE TABLE SQL"""
        table_name = f"{database}.{self.name}" if database else self.name
        
        # Columns
        columns_sql = ",\n    ".join(col.to_sql() for col in self.columns)
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name}\n(\n    {columns_sql}\n)\n"
        
        # Table model and keys
        if self.model == TableModel.DUPLICATE:
            if self.duplicate_keys:
                sql += f"DUPLICATE KEY({', '.join(f'`{k}`' for k in self.duplicate_keys)})\n"
        elif self.model == TableModel.AGGREGATE:
            if self.aggregate_keys:
                sql += f"AGGREGATE KEY({', '.join(f'`{k}`' for k in self.aggregate_keys)})\n"
        elif self.model == TableModel.UNIQUE:
            if self.unique_keys:
                sql += f"UNIQUE KEY({', '.join(f'`{k}`' for k in self.unique_keys)})\n"
        
        # Partition
        if self.partition_by:
            sql += f"PARTITION BY {self.partition_type.value}({self.partition_by})\n"
            if self.partitions:
                sql += "(\n"
                partition_defs = []
                for p in self.partitions:
                    if self.partition_type == PartitionType.RANGE:
                        partition_defs.append(
                            f"    PARTITION {p['name']} VALUES LESS THAN ({p['value']})"
                        )
                    else:  # LIST
                        values = ", ".join(f"'{v}'" for v in p['values'])
                        partition_defs.append(
                            f"    PARTITION {p['name']} VALUES IN ({values})"
                        )
                sql += ",\n".join(partition_defs) + "\n)\n"
        
        # Distribution
        if self.distributed_by:
            sql += f"DISTRIBUTED BY HASH({', '.join(f'`{k}`' for k in self.distributed_by)}) "
        else:
            # Default to first key column
            key_cols = self.duplicate_keys or self.aggregate_keys or self.unique_keys or [self.columns[0].name]
            sql += f"DISTRIBUTED BY HASH(`{key_cols[0]}`) "
        
        sql += f"BUCKETS {self.buckets}\n"
        
        # Properties
        properties = [
            f"'replication_num' = '{self.replication_num}'",
            f"'storage_format' = '{self.storage_format}'",
            f"'compression' = '{self.compression}'"
        ]
        
        # Indexes
        if self.bitmap_indexes:
            for col in self.bitmap_indexes:
                properties.append(f"'bitmap_index' = '{col}'")
        
        if self.bloom_filter_columns:
            properties.append(f"'bloom_filter_columns' = '{','.join(self.bloom_filter_columns)}'")
        
        sql += f"PROPERTIES (\n    {',\\n    '.join(properties)}\n)"
        
        return sql


@dataclass
class StreamLoadResult:
    """Stream load result"""
    txn_id: str
    label: str
    status: str
    message: str
    number_total_rows: int
    number_loaded_rows: int
    number_filtered_rows: int
    number_unselected_rows: int
    load_bytes: int
    load_time_ms: int
    
    @property
    def is_success(self) -> bool:
        return self.status == "Success"


class DorisClient(BaseServiceClient):
    """
    Apache Doris client for real-time analytical database.
    
    Features:
    - MPP architecture
    - Real-time data ingestion
    - Materialized views
    - Vectorized execution
    - Tiered storage
    - SQL compatibility
    """
    
    def __init__(
        self,
        config: Optional[DorisConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = DorisConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: DorisConfig = config
        self._connection = None
        self._pool = None
        
    async def connect(self):
        """Connect to Doris cluster"""
        await super().connect()
        
        try:
            # Get credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.user = creds.get("user", self.config.user)
                    self.config.password = creds.get("password")
            
            # Create connection
            self._connection = pymysql.connect(
                host=self.config.fe_host,
                port=self.config.fe_port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=self.config.connection_timeout
            )
            
            # Test connection
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                result = cursor.fetchone()
                logger.info(f"Connected to Doris: {result['VERSION()']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Doris: {e}")
            raise
    
    async def create_database(
        self,
        database: str,
        properties: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create database.
        
        Args:
            database: Database name
            properties: Database properties
            
        Returns:
            Success status
        """
        try:
            sql = f"CREATE DATABASE IF NOT EXISTS `{database}`"
            
            if properties:
                props = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
                sql += f" PROPERTIES ({props})"
            
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
            
            self._connection.commit()
            logger.info(f"Created database: {database}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            self._connection.rollback()
            return False
    
    async def create_table(
        self,
        table: TableDefinition,
        database: Optional[str] = None
    ) -> bool:
        """
        Create table.
        
        Args:
            table: Table definition
            database: Target database
            
        Returns:
            Success status
        """
        try:
            sql = table.to_create_sql(database or self.config.database)
            
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
            
            self._connection.commit()
            logger.info(f"Created table: {table.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            self._connection.rollback()
            return False
    
    async def stream_load(
        self,
        table: str,
        data: Union[str, bytes, pd.DataFrame],
        format: str = "csv",
        column_separator: str = ",",
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        max_filter_ratio: Optional[float] = None,
        label: Optional[str] = None
    ) -> StreamLoadResult:
        """
        Load data using Stream Load.
        
        Args:
            table: Target table
            data: Data to load
            format: Data format (csv, json)
            column_separator: Column separator for CSV
            columns: Target columns
            where: WHERE clause for filtering
            max_filter_ratio: Maximum filter ratio
            label: Load label
            
        Returns:
            Stream load result
        """
        try:
            import requests
            import uuid
            
            # Convert DataFrame to CSV if needed
            if isinstance(data, pd.DataFrame):
                data = data.to_csv(index=False, sep=column_separator)
            
            # Convert string to bytes
            if isinstance(data, str):
                data = data.encode('utf-8')
            
            # Generate label if not provided
            if not label:
                label = f"load_{table}_{uuid.uuid4().hex[:8]}"
            
            # Build URL
            url = f"http://{self.config.fe_host}:{self.config.fe_http_port}/api/{self.config.database}/{table}/_stream_load"
            
            # Build headers
            headers = {
                "label": label,
                "format": format,
                "column_separator": column_separator,
                "strict_mode": str(self.config.strict_mode).lower(),
                "max_filter_ratio": str(max_filter_ratio or self.config.max_filter_ratio)
            }
            
            if columns:
                headers["columns"] = ",".join(columns)
            
            if where:
                headers["where"] = where
            
            # Add authentication
            auth = (self.config.user, self.config.password) if self.config.password else None
            
            # Execute stream load
            response = requests.put(
                url,
                data=data,
                headers=headers,
                auth=auth,
                timeout=self.config.query_timeout
            )
            
            # Parse response
            result = response.json()
            
            return StreamLoadResult(
                txn_id=result.get("TxnId", ""),
                label=result.get("Label", label),
                status=result.get("Status", ""),
                message=result.get("Message", ""),
                number_total_rows=result.get("NumberTotalRows", 0),
                number_loaded_rows=result.get("NumberLoadedRows", 0),
                number_filtered_rows=result.get("NumberFilteredRows", 0),
                number_unselected_rows=result.get("NumberUnselectedRows", 0),
                load_bytes=result.get("LoadBytes", 0),
                load_time_ms=result.get("LoadTimeMs", 0)
            )
            
        except Exception as e:
            logger.error(f"Failed to execute stream load: {e}")
            raise
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch_all: bool = True
    ) -> Union[List[Dict[str, Any]], None]:
        """
        Execute SQL query.
        
        Args:
            query: SQL query
            params: Query parameters
            fetch_all: Fetch all results
            
        Returns:
            Query results
        """
        try:
            with self._connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch_all and cursor.description:
                    return cursor.fetchall()
                elif cursor.description:
                    return cursor.fetchone()
                else:
                    self._connection.commit()
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            self._connection.rollback()
            raise
    
    async def create_materialized_view(
        self,
        view_name: str,
        base_table: str,
        select_query: str,
        keys: List[str],
        distributed_by: Optional[List[str]] = None,
        refresh_method: str = "ASYNC",
        refresh_interval: Optional[int] = None
    ) -> bool:
        """
        Create materialized view.
        
        Args:
            view_name: View name
            base_table: Base table name
            select_query: SELECT query
            keys: Key columns
            distributed_by: Distribution columns
            refresh_method: ASYNC, SYNC, or MANUAL
            refresh_interval: Refresh interval in seconds
            
        Returns:
            Success status
        """
        try:
            sql = f"""
            CREATE MATERIALIZED VIEW {view_name}
            AS {select_query}
            KEY({', '.join(f'`{k}`' for k in keys)})
            """
            
            if distributed_by:
                sql += f"\nDISTRIBUTED BY HASH({', '.join(f'`{k}`' for k in distributed_by)})"
            else:
                sql += f"\nDISTRIBUTED BY HASH(`{keys[0]}`)"
            
            sql += f"\nREFRESH {refresh_method}"
            
            if refresh_interval:
                sql += f" START WITH INTERVAL {refresh_interval} SECOND"
            
            sql += f"\nPROPERTIES ('replication_num' = '{self.config.replication_num}')"
            
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
            
            self._connection.commit()
            logger.info(f"Created materialized view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create materialized view: {e}")
            self._connection.rollback()
            return False
    
    async def create_rollup(
        self,
        table: str,
        rollup_name: str,
        columns: List[str],
        keys: List[str],
        aggregations: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create rollup index.
        
        Args:
            table: Base table
            rollup_name: Rollup name
            columns: Columns to include
            keys: Key columns
            aggregations: Column aggregations
            
        Returns:
            Success status
        """
        try:
            # Build column list with aggregations
            column_defs = []
            for col in columns:
                if aggregations and col in aggregations:
                    column_defs.append(f"`{col}` WITH {aggregations[col]}")
                else:
                    column_defs.append(f"`{col}`")
            
            sql = f"""
            ALTER TABLE {table}
            ADD ROLLUP {rollup_name}
            ({', '.join(column_defs)})
            """
            
            if keys:
                sql += f" KEY({', '.join(f'`{k}`' for k in keys)})"
            
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
            
            self._connection.commit()
            logger.info(f"Created rollup: {rollup_name} on table {table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create rollup: {e}")
            self._connection.rollback()
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
                TABLE_ROWS as row_count,
                AVG_ROW_LENGTH as avg_row_length,
                DATA_LENGTH as data_size,
                INDEX_LENGTH as index_size,
                CREATE_TIME as created_at,
                UPDATE_TIME as updated_at
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}'
            """
            
            result = await self.execute_query(stats_query, fetch_all=False)
            stats = result if result else {}
            
            # Get partition info
            partition_query = f"""
            SHOW PARTITIONS FROM {db}.{table}
            """
            
            try:
                partitions = await self.execute_query(partition_query)
                stats['partitions'] = partitions
            except:
                stats['partitions'] = []
            
            # Get tablet info
            tablet_query = f"""
            SHOW TABLET FROM {db}.{table}
            """
            
            try:
                tablets = await self.execute_query(tablet_query)
                stats['tablet_count'] = len(tablets)
                stats['tablets'] = tablets[:10]  # First 10 tablets
            except:
                stats['tablet_count'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}
    
    async def optimize_table(
        self,
        table: str,
        partition: Optional[str] = None
    ) -> bool:
        """
        Optimize table by compacting.
        
        Args:
            table: Table name
            partition: Specific partition
            
        Returns:
            Success status
        """
        try:
            sql = f"ALTER TABLE {table}"
            
            if partition:
                sql += f" PARTITION ({partition})"
            
            sql += " COMPACT"
            
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
            
            self._connection.commit()
            logger.info(f"Started compaction for table: {table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            self._connection.rollback()
            return False
    
    async def export_data(
        self,
        query: str,
        output_path: str,
        format: str = "csv",
        column_separator: str = ",",
        line_delimiter: str = "\n"
    ) -> bool:
        """
        Export query results.
        
        Args:
            query: SELECT query
            output_path: Output file path
            format: Output format
            column_separator: Column separator
            line_delimiter: Line delimiter
            
        Returns:
            Success status
        """
        try:
            export_sql = f"""
            EXPORT TABLE (
                {query}
            ) TO '{output_path}'
            PROPERTIES (
                'format' = '{format}',
                'column_separator' = '{column_separator}',
                'line_delimiter' = '{line_delimiter}'
            )
            """
            
            with self._connection.cursor() as cursor:
                cursor.execute(export_sql)
            
            self._connection.commit()
            logger.info(f"Exported data to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            self._connection.rollback()
            return False
    
    async def close(self):
        """Close Doris connection"""
        if self._connection:
            self._connection.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Doris specific configuration"""
        return {
            "fe_host": self.config.fe_host,
            "fe_port": self.config.fe_port,
            "database": self.config.database,
            "query_timeout": self.config.query_timeout,
            "strict_mode": self.config.strict_mode
        } 