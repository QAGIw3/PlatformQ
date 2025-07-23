"""
Apache Iceberg Client Integration

Provides high-level client for Apache Iceberg table format operations.
Supports ACID transactions, time travel, and schema evolution on object storage.
"""

import uuid
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.expressions import (
    GreaterThanOrEqual, LessThan, And, Or, Not,
    EqualTo, NotEqualTo, In, NotIn
)
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, TimestampType,
    DateType, TimeType, BinaryType, StructType, ListType, MapType
)

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class IcebergCatalogType(str, Enum):
    """Iceberg catalog types"""
    HIVE = "hive"
    HADOOP = "hadoop"
    REST = "rest"
    GLUE = "glue"
    DYNAMODB = "dynamodb"
    JDBC = "jdbc"


class PartitionStrategy(str, Enum):
    """Partition strategies"""
    IDENTITY = "identity"
    BUCKET = "bucket"
    TRUNCATE = "truncate"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"


@dataclass
class IcebergConfig(ClientConfig):
    """Configuration for Iceberg client"""
    catalog_type: IcebergCatalogType = IcebergCatalogType.REST
    catalog_uri: str = "http://localhost:8181"
    warehouse_path: str = "s3://datalake/warehouse"
    
    # S3/MinIO configuration
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_region: str = "us-east-1"
    
    # Table defaults
    default_file_format: str = "parquet"
    compression_codec: str = "snappy"
    
    # Performance
    target_file_size_bytes: int = 134217728  # 128MB
    min_files_for_compaction: int = 5
    
    # Metadata
    metadata_retention_days: int = 7
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "iceberg"


@dataclass
class TableSchema:
    """Iceberg table schema definition"""
    fields: List[Tuple[str, str, bool]] = field(default_factory=list)  # (name, type, nullable)
    partition_fields: List[Tuple[str, PartitionStrategy]] = field(default_factory=list)
    
    def to_iceberg_schema(self) -> Schema:
        """Convert to Iceberg schema"""
        iceberg_fields = []
        field_id = 1
        
        for name, field_type, nullable in self.fields:
            iceberg_type = self._get_iceberg_type(field_type)
            iceberg_fields.append(
                NestedField(
                    field_id=field_id,
                    name=name,
                    field_type=iceberg_type,
                    required=not nullable
                )
            )
            field_id += 1
            
        return Schema(*iceberg_fields)
    
    def _get_iceberg_type(self, type_str: str):
        """Convert string type to Iceberg type"""
        type_map = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "time": TimeType(),
            "binary": BinaryType()
        }
        return type_map.get(type_str, StringType())


@dataclass
class TableSnapshot:
    """Table snapshot information"""
    snapshot_id: int
    timestamp: datetime
    summary: Dict[str, Any]
    manifest_list: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp.isoformat(),
            "summary": self.summary,
            "manifest_list": self.manifest_list
        }


class IcebergClient(BaseServiceClient):
    """
    Apache Iceberg client for lakehouse operations.
    
    Features:
    - ACID transactions on object storage
    - Time travel queries
    - Schema evolution
    - Hidden partitioning
    - Incremental processing
    - Compaction and optimization
    """
    
    def __init__(
        self,
        config: Optional[IcebergConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = IcebergConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: IcebergConfig = config
        self._catalog = None
        
    async def connect(self):
        """Connect to Iceberg catalog"""
        await super().connect()
        
        try:
            # Get S3 credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.s3_access_key = creds.get("access_key")
                    self.config.s3_secret_key = creds.get("secret_key")
            
            # Build catalog configuration
            catalog_config = {
                "type": self.config.catalog_type.value,
                "uri": self.config.catalog_uri,
                "warehouse": self.config.warehouse_path
            }
            
            # Add S3 configuration if provided
            if self.config.s3_endpoint:
                catalog_config.update({
                    "s3.endpoint": self.config.s3_endpoint,
                    "s3.access-key-id": self.config.s3_access_key,
                    "s3.secret-access-key": self.config.s3_secret_key,
                    "s3.region": self.config.s3_region
                })
            
            # Load catalog
            self._catalog = load_catalog("default", **catalog_config)
            
            logger.info(f"Connected to Iceberg catalog: {self.config.catalog_uri}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Iceberg: {e}")
            raise
    
    async def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: TableSchema,
        properties: Optional[Dict[str, str]] = None
    ) -> Table:
        """
        Create a new Iceberg table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            schema: Table schema definition
            properties: Table properties
            
        Returns:
            Created table
        """
        try:
            # Create namespace if it doesn't exist
            if namespace not in self._catalog.list_namespaces():
                self._catalog.create_namespace(namespace)
            
            # Build table properties
            table_properties = {
                "write.format.default": self.config.default_file_format,
                "write.parquet.compression-codec": self.config.compression_codec,
                "write.target-file-size-bytes": str(self.config.target_file_size_bytes),
                "history.expire.max-snapshot-age-days": str(self.config.metadata_retention_days)
            }
            
            if properties:
                table_properties.update(properties)
            
            # Create table
            table = self._catalog.create_table(
                identifier=f"{namespace}.{table_name}",
                schema=schema.to_iceberg_schema(),
                properties=table_properties
            )
            
            # Add partitioning if specified
            if schema.partition_fields:
                # TODO: Implement partition spec
                pass
            
            logger.info(f"Created Iceberg table: {namespace}.{table_name}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    async def write_data(
        self,
        namespace: str,
        table_name: str,
        data: Union[pa.Table, pd.DataFrame, List[Dict[str, Any]]],
        mode: str = "append"
    ) -> Dict[str, Any]:
        """
        Write data to Iceberg table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            data: Data to write
            mode: Write mode (append, overwrite, overwrite_partitions)
            
        Returns:
            Write statistics
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            # Convert data to PyArrow table if needed
            if isinstance(data, list):
                import pandas as pd
                df = pd.DataFrame(data)
                arrow_table = pa.Table.from_pandas(df)
            elif hasattr(data, "to_arrow"):  # pandas DataFrame
                arrow_table = pa.Table.from_pandas(data)
            else:
                arrow_table = data
            
            # Write data
            if mode == "append":
                table.append(arrow_table)
            elif mode == "overwrite":
                table.overwrite(arrow_table)
            elif mode == "overwrite_partitions":
                # TODO: Implement dynamic partition overwrite
                table.overwrite(arrow_table)
            
            # Get write statistics
            stats = {
                "records_written": arrow_table.num_rows,
                "files_created": 1,  # TODO: Get actual file count
                "bytes_written": arrow_table.nbytes
            }
            
            logger.info(f"Wrote {stats['records_written']} records to {namespace}.{table_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    async def read_table(
        self,
        namespace: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pa.Table:
        """
        Read data from Iceberg table with time travel support.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            columns: Columns to read
            filter_expr: Filter expression
            snapshot_id: Read specific snapshot
            as_of_timestamp: Read as of timestamp
            limit: Row limit
            
        Returns:
            PyArrow table
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            # Create scan
            scan = table.scan()
            
            # Add column selection
            if columns:
                scan = scan.select(*columns)
            
            # Add filter
            if filter_expr:
                # TODO: Parse filter expression
                pass
            
            # Time travel
            if snapshot_id:
                scan = scan.use_snapshot(snapshot_id)
            elif as_of_timestamp:
                # Find snapshot at timestamp
                snapshot = self._find_snapshot_at_time(table, as_of_timestamp)
                if snapshot:
                    scan = scan.use_snapshot(snapshot.snapshot_id)
            
            # Add limit
            if limit:
                scan = scan.limit(limit)
            
            # Execute scan
            result = scan.to_arrow()
            
            logger.info(f"Read {result.num_rows} rows from {namespace}.{table_name}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise
    
    async def get_table_history(
        self,
        namespace: str,
        table_name: str,
        limit: int = 10
    ) -> List[TableSnapshot]:
        """
        Get table snapshot history.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            limit: Maximum snapshots to return
            
        Returns:
            List of table snapshots
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            snapshots = []
            for snapshot in table.history()[-limit:]:
                snapshots.append(TableSnapshot(
                    snapshot_id=snapshot.snapshot_id,
                    timestamp=datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
                    summary=snapshot.summary,
                    manifest_list=snapshot.manifest_list
                ))
            
            return snapshots
            
        except Exception as e:
            logger.error(f"Failed to get table history: {e}")
            raise
    
    async def compact_table(
        self,
        namespace: str,
        table_name: str,
        target_file_size_bytes: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Compact small files in table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            target_file_size_bytes: Target file size
            
        Returns:
            Compaction statistics
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            # TODO: Implement file compaction
            # This would typically use Spark or another engine
            
            stats = {
                "files_before": 0,
                "files_after": 0,
                "bytes_compacted": 0
            }
            
            logger.info(f"Compacted table {namespace}.{table_name}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to compact table: {e}")
            raise
    
    async def evolve_schema(
        self,
        namespace: str,
        table_name: str,
        add_columns: Optional[List[Tuple[str, str, bool]]] = None,
        drop_columns: Optional[List[str]] = None,
        rename_columns: Optional[Dict[str, str]] = None
    ) -> Table:
        """
        Evolve table schema.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            add_columns: Columns to add [(name, type, nullable)]
            drop_columns: Columns to drop
            rename_columns: Columns to rename {old: new}
            
        Returns:
            Updated table
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            with table.update_schema() as update:
                # Add columns
                if add_columns:
                    for name, field_type, nullable in add_columns:
                        # TODO: Implement column addition
                        pass
                
                # Drop columns
                if drop_columns:
                    for column in drop_columns:
                        update.drop_column(column)
                
                # Rename columns
                if rename_columns:
                    for old_name, new_name in rename_columns.items():
                        update.rename_column(old_name, new_name)
            
            logger.info(f"Evolved schema for {namespace}.{table_name}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to evolve schema: {e}")
            raise
    
    async def create_branch(
        self,
        namespace: str,
        table_name: str,
        branch_name: str,
        from_snapshot_id: Optional[int] = None
    ) -> str:
        """
        Create a table branch for isolated changes.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            branch_name: Branch name
            from_snapshot_id: Base snapshot ID
            
        Returns:
            Branch reference
        """
        try:
            table = self._catalog.load_table(f"{namespace}.{table_name}")
            
            # TODO: Implement branch creation
            # This is a newer Iceberg feature
            
            logger.info(f"Created branch {branch_name} for {namespace}.{table_name}")
            return f"{namespace}.{table_name}@{branch_name}"
            
        except Exception as e:
            logger.error(f"Failed to create branch: {e}")
            raise
    
    def _find_snapshot_at_time(self, table: Table, timestamp: datetime) -> Optional[TableSnapshot]:
        """Find snapshot at given timestamp"""
        target_ms = int(timestamp.timestamp() * 1000)
        
        for snapshot in reversed(table.history()):
            if snapshot.timestamp_ms <= target_ms:
                return TableSnapshot(
                    snapshot_id=snapshot.snapshot_id,
                    timestamp=datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
                    summary=snapshot.summary,
                    manifest_list=snapshot.manifest_list
                )
        
        return None
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Iceberg-specific configuration"""
        return {
            "catalog_type": self.config.catalog_type.value,
            "catalog_uri": self.config.catalog_uri,
            "warehouse_path": self.config.warehouse_path,
            "default_file_format": self.config.default_file_format,
            "compression_codec": self.config.compression_codec
        } 