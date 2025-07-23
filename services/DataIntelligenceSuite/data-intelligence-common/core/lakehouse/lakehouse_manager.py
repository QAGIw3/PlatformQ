"""
Unified Lakehouse Manager

Provides a unified interface for managing multiple lakehouse table formats.
"""

from typing import Any, Dict, List, Optional, Union, Type
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import pyarrow as pa

from .iceberg_client import IcebergClient, IcebergConfig, TableSchema as IcebergSchema
from .delta_client import DeltaLakeClient, DeltaConfig, MergeBuilder
from .hudi_client import HudiClient, HudiConfig, IncrementalQuery

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class LakehouseFormat(str, Enum):
    """Supported lakehouse formats"""
    ICEBERG = "iceberg"
    DELTA = "delta"
    HUDI = "hudi"


class TableOperation(str, Enum):
    """Table operations"""
    CREATE = "create"
    READ = "read"
    WRITE = "write"
    MERGE = "merge"
    DELETE = "delete"
    OPTIMIZE = "optimize"
    TIME_TRAVEL = "time_travel"


@dataclass
class UnifiedTable:
    """Unified table metadata"""
    name: str
    format: LakehouseFormat
    path: str
    schema: Dict[str, str]
    partitions: List[str]
    version: Union[int, str]
    records: int
    size_bytes: int
    created_at: datetime
    updated_at: datetime
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "format": self.format.value,
            "path": self.path,
            "schema": self.schema,
            "partitions": self.partitions,
            "version": self.version,
            "records": self.records,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "properties": self.properties
        }


@dataclass
class LakehouseConfig:
    """Unified lakehouse configuration"""
    default_format: LakehouseFormat = LakehouseFormat.ICEBERG
    
    # Format-specific configs
    iceberg_config: Optional[IcebergConfig] = None
    delta_config: Optional[DeltaConfig] = None
    hudi_config: Optional[HudiConfig] = None
    
    # Storage
    base_path: str = "s3://datalake"
    
    # Optimization
    auto_optimize: bool = True
    optimize_threshold_gb: float = 1.0
    
    # Retention
    retention_days: int = 30
    
    def __post_init__(self):
        # Initialize default configs if not provided
        if not self.iceberg_config:
            self.iceberg_config = IcebergConfig(
                warehouse_path=f"{self.base_path}/iceberg"
            )
        if not self.delta_config:
            self.delta_config = DeltaConfig(
                table_path=f"{self.base_path}/delta"
            )
        if not self.hudi_config:
            self.hudi_config = HudiConfig(
                table_path=f"{self.base_path}/hudi"
            )


class LakehouseManager:
    """
    Unified manager for multiple lakehouse formats.
    
    Features:
    - Format-agnostic table operations
    - Automatic format selection
    - Cross-format data migration
    - Unified metadata management
    - Performance optimization
    """
    
    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.config = config or LakehouseConfig()
        self.vault_client = vault_client
        self.consul_client = consul_client
        
        # Initialize format clients
        self._clients: Dict[LakehouseFormat, Union[IcebergClient, DeltaLakeClient, HudiClient]] = {}
        self._table_registry: Dict[str, UnifiedTable] = {}
        
    async def initialize(self):
        """Initialize lakehouse clients"""
        logger.info("Initializing Lakehouse Manager")
        
        # Initialize Iceberg client
        self._clients[LakehouseFormat.ICEBERG] = IcebergClient(
            self.config.iceberg_config,
            self.vault_client,
            self.consul_client
        )
        
        # Initialize Delta client
        self._clients[LakehouseFormat.DELTA] = DeltaLakeClient(
            self.config.delta_config,
            self.vault_client,
            self.consul_client
        )
        
        # Initialize Hudi client
        self._clients[LakehouseFormat.HUDI] = HudiClient(
            self.config.hudi_config,
            self.vault_client,
            self.consul_client
        )
        
        # Connect all clients
        for client in self._clients.values():
            await client.connect()
        
        # Load table registry
        await self._load_table_registry()
        
        logger.info("Lakehouse Manager initialized")
    
    async def create_table(
        self,
        name: str,
        schema: Union[pa.Schema, pd.DataFrame, Dict[str, str]],
        format: Optional[LakehouseFormat] = None,
        partition_by: Optional[List[str]] = None,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> UnifiedTable:
        """
        Create a table in specified format.
        
        Args:
            name: Table name
            schema: Table schema
            format: Lakehouse format (defaults to config)
            partition_by: Partition columns
            properties: Table properties
            **kwargs: Format-specific options
            
        Returns:
            Unified table metadata
        """
        format = format or self.config.default_format
        client = self._clients[format]
        
        logger.info(f"Creating {format.value} table: {name}")
        
        try:
            if format == LakehouseFormat.ICEBERG:
                # Convert schema to Iceberg format
                if isinstance(schema, dict):
                    iceberg_schema = IcebergSchema(
                        fields=[(k, v, True) for k, v in schema.items()]
                    )
                else:
                    # Handle pa.Schema or pd.DataFrame
                    iceberg_schema = schema
                
                table = await client.create_table(
                    "default",  # namespace
                    name,
                    iceberg_schema,
                    properties
                )
                
                unified_table = UnifiedTable(
                    name=name,
                    format=format,
                    path=table.path,
                    schema=table.schema,
                    partitions=partition_by or [],
                    version=table.version,
                    records=0,
                    size_bytes=0,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    properties=properties or {}
                )
                
            elif format == LakehouseFormat.DELTA:
                table = await client.create_table(
                    name,
                    schema,
                    partition_by,
                    properties
                )
                
                unified_table = UnifiedTable(
                    name=name,
                    format=format,
                    path=table.path,
                    schema=table.schema,
                    partitions=table.partitions,
                    version=table.version,
                    records=table.num_records,
                    size_bytes=table.size_bytes,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    properties=table.properties
                )
                
            elif format == LakehouseFormat.HUDI:
                # Hudi requires additional parameters
                record_key = kwargs.get("record_key", "id")
                precombine_field = kwargs.get("precombine_field", "timestamp")
                
                table = await client.create_table(
                    name,
                    schema,
                    record_key,
                    partition_by[0] if partition_by else None,
                    precombine_field
                )
                
                unified_table = UnifiedTable(
                    name=name,
                    format=format,
                    path=table.path,
                    schema=table.schema,
                    partitions=table.partitions,
                    version=table.latest_commit,
                    records=table.total_records,
                    size_bytes=0,  # Hudi doesn't provide this easily
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    properties=properties or {}
                )
            
            # Register table
            self._table_registry[name] = unified_table
            await self._save_table_registry()
            
            return unified_table
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    async def write_data(
        self,
        table_name: str,
        data: Union[pa.Table, pd.DataFrame, List[Dict[str, Any]]],
        mode: str = "append",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Write data to table.
        
        Args:
            table_name: Table name
            data: Data to write
            mode: Write mode
            **kwargs: Format-specific options
            
        Returns:
            Write statistics
        """
        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")
        
        table = self._table_registry[table_name]
        client = self._clients[table.format]
        
        logger.info(f"Writing data to {table.format.value} table: {table_name}")
        
        try:
            if table.format == LakehouseFormat.ICEBERG:
                stats = await client.write_data(
                    "default",  # namespace
                    table_name,
                    data,
                    mode
                )
                
            elif table.format == LakehouseFormat.DELTA:
                stats = await client.write_data(
                    table_name,
                    data,
                    mode
                )
                
            elif table.format == LakehouseFormat.HUDI:
                # Convert mode to Hudi WriteMode
                from .hudi_client import WriteMode
                hudi_mode = WriteMode.UPSERT if mode == "append" else WriteMode.INSERT_OVERWRITE
                
                stats = await client.write_data(
                    table_name,
                    data,
                    hudi_mode
                )
            
            # Update table metadata
            table.updated_at = datetime.now()
            if "records_written" in stats:
                table.records += stats["records_written"]
            
            # Check if optimization needed
            if self.config.auto_optimize:
                await self._check_and_optimize(table_name)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    async def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        version: Optional[Union[int, str]] = None,
        timestamp: Optional[datetime] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read data from table with optional time travel.
        
        Args:
            table_name: Table name
            columns: Columns to read
            filter_expr: Filter expression
            version: Version for time travel
            timestamp: Timestamp for time travel
            limit: Row limit
            **kwargs: Format-specific options
            
        Returns:
            DataFrame with results
        """
        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")
        
        table = self._table_registry[table_name]
        client = self._clients[table.format]
        
        logger.info(f"Reading from {table.format.value} table: {table_name}")
        
        try:
            if table.format == LakehouseFormat.ICEBERG:
                df = await client.read_table(
                    "default",  # namespace
                    table_name,
                    columns,
                    filter_expr,
                    snapshot_id=version,
                    as_of_timestamp=timestamp,
                    limit=limit
                )
                
            elif table.format == LakehouseFormat.DELTA:
                df = await client.read_table(
                    table_name,
                    columns,
                    filter_expr,
                    version=version,
                    timestamp=timestamp,
                    limit=limit
                )
                
            elif table.format == LakehouseFormat.HUDI:
                # Handle incremental query if specified
                incremental = None
                if "begin_instant" in kwargs:
                    incremental = IncrementalQuery(
                        begin_instant_time=kwargs["begin_instant"],
                        end_instant_time=kwargs.get("end_instant")
                    )
                
                df = await client.read_table(
                    table_name,
                    columns,
                    filter_expr,
                    as_of_instant=version,
                    incremental=incremental,
                    limit=limit
                )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise
    
    async def optimize_table(
        self,
        table_name: str,
        strategy: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Optimize table storage.
        
        Args:
            table_name: Table name
            strategy: Optimization strategy
            **kwargs: Strategy-specific options
            
        Returns:
            Optimization statistics
        """
        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")
        
        table = self._table_registry[table_name]
        client = self._clients[table.format]
        
        logger.info(f"Optimizing {table.format.value} table: {table_name}")
        
        try:
            if table.format == LakehouseFormat.ICEBERG:
                stats = await client.compact_table(
                    "default",  # namespace
                    table_name
                )
                
            elif table.format == LakehouseFormat.DELTA:
                from .delta_client import OptimizeConfig, OptimizeStrategy
                
                config = OptimizeConfig(
                    strategy=OptimizeStrategy.COMPACT if strategy == "compact" else OptimizeStrategy.VACUUM
                )
                stats = await client.optimize_table(table_name, config)
                
            elif table.format == LakehouseFormat.HUDI:
                if strategy == "cluster":
                    from .hudi_client import ClusteringConfig
                    clustering_config = ClusteringConfig(
                        columns=kwargs.get("columns", [])
                    )
                    stats = await client.cluster_table(table_name, clustering_config)
                else:
                    stats = await client.compact_table(table_name)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    async def migrate_table(
        self,
        source_table: str,
        target_table: str,
        target_format: LakehouseFormat,
        batch_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Migrate table between formats.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            target_format: Target format
            batch_size: Batch size for migration
            
        Returns:
            Migration statistics
        """
        logger.info(f"Migrating {source_table} to {target_format.value} format")
        
        try:
            # Read source table metadata
            if source_table not in self._table_registry:
                raise ValueError(f"Source table not found: {source_table}")
            
            source_meta = self._table_registry[source_table]
            
            # Create target table with same schema
            target_meta = await self.create_table(
                target_table,
                source_meta.schema,
                target_format,
                source_meta.partitions,
                source_meta.properties
            )
            
            # Migrate data in batches
            offset = 0
            total_records = 0
            
            while True:
                # Read batch from source
                batch_df = await self.read_table(
                    source_table,
                    limit=batch_size
                )
                
                if batch_df.empty:
                    break
                
                # Write to target
                await self.write_data(target_table, batch_df)
                
                total_records += len(batch_df)
                offset += batch_size
                
                logger.info(f"Migrated {total_records} records")
            
            stats = {
                "source_table": source_table,
                "target_table": target_table,
                "source_format": source_meta.format.value,
                "target_format": target_format.value,
                "records_migrated": total_records,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Migration completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to migrate table: {e}")
            raise
    
    async def list_tables(
        self,
        format: Optional[LakehouseFormat] = None
    ) -> List[UnifiedTable]:
        """
        List registered tables.
        
        Args:
            format: Filter by format
            
        Returns:
            List of tables
        """
        tables = list(self._table_registry.values())
        
        if format:
            tables = [t for t in tables if t.format == format]
        
        return tables
    
    async def get_table_info(self, table_name: str) -> UnifiedTable:
        """Get table information"""
        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")
        
        return self._table_registry[table_name]
    
    async def drop_table(self, table_name: str) -> bool:
        """Drop table"""
        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")
        
        # Remove from registry
        del self._table_registry[table_name]
        await self._save_table_registry()
        
        logger.info(f"Dropped table: {table_name}")
        return True
    
    async def _check_and_optimize(self, table_name: str):
        """Check if table needs optimization"""
        table = self._table_registry[table_name]
        
        # Simple size-based check
        if table.size_bytes > self.config.optimize_threshold_gb * 1024 * 1024 * 1024:
            logger.info(f"Table {table_name} exceeds optimization threshold, triggering optimization")
            await self.optimize_table(table_name, strategy="compact")
    
    async def _load_table_registry(self):
        """Load table registry from storage"""
        # TODO: Implement persistent registry storage
        pass
    
    async def _save_table_registry(self):
        """Save table registry to storage"""
        # TODO: Implement persistent registry storage
        pass
    
    async def close(self):
        """Close all clients"""
        for client in self._clients.values():
            await client.close()
        
        logger.info("Lakehouse Manager closed") 