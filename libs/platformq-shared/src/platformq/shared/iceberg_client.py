"""
Apache Iceberg Client for PlatformQ

Provides easy access to Iceberg tables with time-travel queries,
ACID transactions, and schema evolution capabilities.
"""

import os
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import logging
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.expressions import (
    EqualTo, NotEqualTo, LessThan, LessThanOrEqual,
    GreaterThan, GreaterThanOrEqual, In, NotIn,
    IsNull, NotNull, And, Or
)
from platformq_shared.config import ConfigLoader

logger = logging.getLogger(__name__)


class IcebergClient:
    """Client for interacting with Apache Iceberg tables"""
    
    def __init__(self,
                 catalog_name: str = "platformq",
                 catalog_uri: Optional[str] = None,
                 s3_endpoint: Optional[str] = None,
                 s3_access_key: Optional[str] = None,
                 s3_secret_key: Optional[str] = None,
                 config_loader: Optional[ConfigLoader] = None):
        """
        Initialize Iceberg client
        
        Args:
            catalog_name: Name of the Iceberg catalog
            catalog_uri: URI of the catalog (e.g., thrift://hive-metastore:9083)
            s3_endpoint: S3/MinIO endpoint URL
            s3_access_key: S3 access key
            s3_secret_key: S3 secret key
            config_loader: Optional config loader for getting configuration
        """
        self.config_loader = config_loader or ConfigLoader()
        
        # Load configuration
        if not catalog_uri:
            catalog_uri = self.config_loader.get_config(
                "ICEBERG_CATALOG_URI",
                "thrift://hive-metastore:9083"
            )
            
        if not s3_endpoint:
            s3_endpoint = self.config_loader.get_config(
                "MINIO_ENDPOINT",
                "http://minio:9000"
            )
            
        if not s3_access_key:
            s3_access_key = self.config_loader.get_config(
                "MINIO_ACCESS_KEY",
                "minioadmin"
            )
            
        if not s3_secret_key:
            s3_secret_key = self.config_loader.get_secret("MINIO_SECRET_KEY")
            
        # Initialize catalog
        self.catalog = load_catalog(
            catalog_name,
            **{
                "uri": catalog_uri,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
                "s3.path-style-access": "true",
            }
        )
        
        self._table_cache: Dict[str, Table] = {}
        
    def list_namespaces(self) -> List[str]:
        """List all namespaces in the catalog"""
        return [ns[0] for ns in self.catalog.list_namespaces()]
        
    def list_tables(self, namespace: str = "default") -> List[str]:
        """List all tables in a namespace"""
        return self.catalog.list_tables(namespace)
        
    def get_table(self, 
                  table_name: str, 
                  namespace: str = "default",
                  refresh: bool = False) -> Table:
        """
        Get an Iceberg table
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            refresh: Whether to refresh the table metadata
            
        Returns:
            Iceberg Table object
        """
        table_id = f"{namespace}.{table_name}"
        
        if refresh or table_id not in self._table_cache:
            self._table_cache[table_id] = self.catalog.load_table(table_id)
            
        return self._table_cache[table_id]
        
    def read_table(self,
                   table_name: str,
                   namespace: str = "default",
                   columns: Optional[List[str]] = None,
                   filter_expr: Optional[Any] = None,
                   limit: Optional[int] = None,
                   as_of: Optional[Union[datetime, str, int]] = None) -> pd.DataFrame:
        """
        Read data from an Iceberg table
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            columns: List of columns to read (None for all)
            filter_expr: Filter expression
            limit: Maximum number of rows to read
            as_of: Read table as of specific time (time travel)
                  Can be datetime, ISO string, or snapshot ID
            
        Returns:
            Pandas DataFrame
        """
        table = self.get_table(table_name, namespace)
        
        # Build scan
        scan = table.scan()
        
        # Apply column selection
        if columns:
            scan = scan.select(*columns)
            
        # Apply filter
        if filter_expr:
            scan = scan.filter(filter_expr)
            
        # Apply limit
        if limit:
            scan = scan.limit(limit)
            
        # Apply time travel
        if as_of:
            if isinstance(as_of, datetime):
                # Find snapshot at or before the given time
                snapshot = self._find_snapshot_by_time(table, as_of)
                if snapshot:
                    scan = scan.use_snapshot(snapshot.snapshot_id)
            elif isinstance(as_of, str):
                # Parse ISO datetime string
                as_of_dt = datetime.fromisoformat(as_of.replace('Z', '+00:00'))
                snapshot = self._find_snapshot_by_time(table, as_of_dt)
                if snapshot:
                    scan = scan.use_snapshot(snapshot.snapshot_id)
            elif isinstance(as_of, int):
                # Use snapshot ID directly
                scan = scan.use_snapshot(as_of)
                
        # Execute scan and convert to pandas
        return scan.to_pandas()
        
    def write_table(self,
                    data: Union[pd.DataFrame, pa.Table],
                    table_name: str,
                    namespace: str = "default",
                    mode: str = "append",
                    partition_by: Optional[List[str]] = None,
                    properties: Optional[Dict[str, str]] = None):
        """
        Write data to an Iceberg table
        
        Args:
            data: Data to write (pandas DataFrame or PyArrow Table)
            table_name: Name of the table
            namespace: Namespace of the table
            mode: Write mode - "append", "overwrite", or "create"
            partition_by: List of columns to partition by (for new tables)
            properties: Table properties (for new tables)
        """
        table_id = f"{namespace}.{table_name}"
        
        # Convert pandas to PyArrow if needed
        if isinstance(data, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(data)
        else:
            arrow_table = data
            
        try:
            # Try to load existing table
            table = self.get_table(table_name, namespace)
            
            if mode == "append":
                table.append(arrow_table)
            elif mode == "overwrite":
                table.overwrite(arrow_table)
            else:
                raise ValueError(f"Invalid mode '{mode}' for existing table")
                
        except Exception as e:
            if mode == "create" or "NoSuchTableError" in str(e):
                # Create new table
                schema = self._arrow_to_iceberg_schema(arrow_table.schema)
                
                # Build partition spec if specified
                partition_spec = None
                if partition_by:
                    from pyiceberg.partitioning import PartitionSpec, PartitionField
                    from pyiceberg.transforms import IdentityTransform
                    
                    fields = []
                    for i, col in enumerate(partition_by):
                        if col in arrow_table.schema.names:
                            field_id = arrow_table.schema.names.index(col) + 1
                            fields.append(PartitionField(
                                source_id=field_id,
                                field_id=1000 + i,
                                name=col,
                                transform=IdentityTransform()
                            ))
                    partition_spec = PartitionSpec(*fields)
                    
                # Set default properties
                if not properties:
                    properties = {}
                    
                properties.update({
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "zstd",
                    "write.metadata.compression-codec": "gzip",
                })
                
                # Create table
                table = self.catalog.create_table(
                    identifier=table_id,
                    schema=schema,
                    partition_spec=partition_spec,
                    properties=properties
                )
                
                # Write initial data
                table.append(arrow_table)
            else:
                raise
                
    def delete_where(self,
                     table_name: str,
                     namespace: str = "default",
                     filter_expr: Any = None):
        """
        Delete rows from a table matching a filter
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            filter_expr: Filter expression for rows to delete
        """
        table = self.get_table(table_name, namespace)
        
        if filter_expr:
            table.delete(filter_expr)
        else:
            raise ValueError("filter_expr is required for delete operation")
            
    def update_table(self,
                     table_name: str,
                     namespace: str = "default",
                     updates: Dict[str, Any] = None,
                     filter_expr: Any = None):
        """
        Update rows in a table
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            updates: Dictionary of column -> new value
            filter_expr: Filter expression for rows to update
        """
        table = self.get_table(table_name, namespace)
        
        if updates and filter_expr:
            table.update(updates, filter_expr)
        else:
            raise ValueError("Both updates and filter_expr are required")
            
    def get_table_history(self,
                          table_name: str,
                          namespace: str = "default",
                          limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get table history (snapshots)
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            limit: Maximum number of snapshots to return
            
        Returns:
            List of snapshot information
        """
        table = self.get_table(table_name, namespace)
        
        history = []
        for snapshot in list(table.metadata.snapshots)[-limit:]:
            history.append({
                "snapshot_id": snapshot.snapshot_id,
                "timestamp": datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
                "parent_id": snapshot.parent_snapshot_id,
                "operation": snapshot.summary.get("operation", "unknown"),
                "added_files": snapshot.summary.get("added-data-files", 0),
                "deleted_files": snapshot.summary.get("deleted-data-files", 0),
                "added_records": snapshot.summary.get("added-records", 0),
                "deleted_records": snapshot.summary.get("deleted-records", 0),
            })
            
        return history
        
    def rollback_table(self,
                       table_name: str,
                       namespace: str = "default",
                       snapshot_id: Optional[int] = None,
                       timestamp: Optional[datetime] = None):
        """
        Rollback table to a previous snapshot
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            snapshot_id: Snapshot ID to rollback to
            timestamp: Timestamp to rollback to (finds nearest snapshot)
        """
        table = self.get_table(table_name, namespace, refresh=True)
        
        if snapshot_id:
            table.rollback(snapshot_id)
        elif timestamp:
            snapshot = self._find_snapshot_by_time(table, timestamp)
            if snapshot:
                table.rollback(snapshot.snapshot_id)
            else:
                raise ValueError(f"No snapshot found before {timestamp}")
        else:
            raise ValueError("Either snapshot_id or timestamp is required")
            
    def compact_table(self,
                      table_name: str,
                      namespace: str = "default",
                      target_file_size_mb: int = 512):
        """
        Compact small files in a table
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            target_file_size_mb: Target file size in MB
        """
        table = self.get_table(table_name, namespace)
        
        # Compact data files
        table.compact(target_file_size_bytes=target_file_size_mb * 1024 * 1024)
        
        # Rewrite manifests
        table.rewrite_manifests()
        
        logger.info(f"Compacted table {namespace}.{table_name}")
        
    def expire_snapshots(self,
                         table_name: str,
                         namespace: str = "default",
                         older_than_days: int = 7):
        """
        Expire old snapshots
        
        Args:
            table_name: Name of the table
            namespace: Namespace of the table
            older_than_days: Expire snapshots older than this many days
        """
        table = self.get_table(table_name, namespace)
        
        older_than = datetime.now() - timedelta(days=older_than_days)
        table.expire_snapshots(older_than=older_than.timestamp() * 1000)
        
        logger.info(f"Expired snapshots older than {older_than_days} days for {namespace}.{table_name}")
        
    def _find_snapshot_by_time(self, table: Table, timestamp: datetime):
        """Find the snapshot that was current at a given time"""
        target_ts = int(timestamp.timestamp() * 1000)
        
        # Find the latest snapshot before the target time
        best_snapshot = None
        for snapshot in table.metadata.snapshots:
            if snapshot.timestamp_ms <= target_ts:
                if not best_snapshot or snapshot.timestamp_ms > best_snapshot.timestamp_ms:
                    best_snapshot = snapshot
                    
        return best_snapshot
        
    def _arrow_to_iceberg_schema(self, arrow_schema: pa.Schema):
        """Convert PyArrow schema to Iceberg schema"""
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            NestedField, StringType, LongType, DoubleType,
            TimestampType, BooleanType, BinaryType
        )
        
        fields = []
        for i, field in enumerate(arrow_schema):
            # Map Arrow types to Iceberg types
            if pa.types.is_string(field.type):
                iceberg_type = StringType()
            elif pa.types.is_int64(field.type):
                iceberg_type = LongType()
            elif pa.types.is_float64(field.type):
                iceberg_type = DoubleType()
            elif pa.types.is_timestamp(field.type):
                iceberg_type = TimestampType()
            elif pa.types.is_boolean(field.type):
                iceberg_type = BooleanType()
            elif pa.types.is_binary(field.type):
                iceberg_type = BinaryType()
            else:
                # Default to string for unsupported types
                iceberg_type = StringType()
                
            iceberg_field = NestedField(
                field_id=i + 1,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            )
            fields.append(iceberg_field)
            
        return Schema(*fields)
        
    # Query builder helpers
    @staticmethod
    def eq(column: str, value: Any):
        """Equal to filter"""
        return EqualTo(column, value)
        
    @staticmethod
    def ne(column: str, value: Any):
        """Not equal to filter"""
        return NotEqualTo(column, value)
        
    @staticmethod
    def lt(column: str, value: Any):
        """Less than filter"""
        return LessThan(column, value)
        
    @staticmethod
    def lte(column: str, value: Any):
        """Less than or equal filter"""
        return LessThanOrEqual(column, value)
        
    @staticmethod
    def gt(column: str, value: Any):
        """Greater than filter"""
        return GreaterThan(column, value)
        
    @staticmethod
    def gte(column: str, value: Any):
        """Greater than or equal filter"""
        return GreaterThanOrEqual(column, value)
        
    @staticmethod
    def in_(column: str, values: List[Any]):
        """In filter"""
        return In(column, values)
        
    @staticmethod
    def not_in(column: str, values: List[Any]):
        """Not in filter"""
        return NotIn(column, values)
        
    @staticmethod
    def is_null(column: str):
        """Is null filter"""
        return IsNull(column)
        
    @staticmethod
    def not_null(column: str):
        """Not null filter"""
        return NotNull(column)
        
    @staticmethod
    def and_(*filters):
        """AND multiple filters"""
        return And(*filters)
        
    @staticmethod
    def or_(*filters):
        """OR multiple filters"""
        return Or(*filters) 