"""
Delta Lake Client Integration

Provides high-level client for Delta Lake table format operations.
Supports ACID transactions, time travel, and unified batch/streaming.
"""

import os
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable as DeltaTableNative, write_deltalake
from deltalake.schema import Schema as DeltaSchema

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MergeType(str, Enum):
    """Delta Lake merge types"""
    UPSERT = "upsert"
    DELETE = "delete"
    UPDATE = "update"
    INSERT = "insert"


class OptimizeStrategy(str, Enum):
    """Optimization strategies"""
    COMPACT = "compact"
    Z_ORDER = "z_order"
    VACUUM = "vacuum"
    REORG = "reorg"


@dataclass
class DeltaConfig(ClientConfig):
    """Configuration for Delta Lake client"""
    table_path: str = "s3://datalake/delta"
    
    # S3/MinIO configuration
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_region: str = "us-east-1"
    
    # Table defaults
    compression: str = "snappy"
    target_file_size: int = 134217728  # 128MB
    
    # Optimization
    auto_compact: bool = True
    compact_threshold: int = 10
    vacuum_retention_hours: int = 168  # 7 days
    
    # Performance
    enable_caching: bool = True
    cache_size_mb: int = 1024
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "delta-lake"


@dataclass
class DeltaTable:
    """Delta table metadata"""
    path: str
    version: int
    num_files: int
    size_bytes: int
    num_records: int
    schema: Dict[str, str]
    partitions: List[str]
    properties: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "path": self.path,
            "version": self.version,
            "num_files": self.num_files,
            "size_bytes": self.size_bytes,
            "num_records": self.num_records,
            "schema": self.schema,
            "partitions": self.partitions,
            "properties": self.properties
        }


@dataclass
class OptimizeConfig:
    """Configuration for table optimization"""
    strategy: OptimizeStrategy = OptimizeStrategy.COMPACT
    target_file_size: Optional[int] = None
    z_order_columns: Optional[List[str]] = None
    vacuum_hours: Optional[int] = None
    dry_run: bool = False


@dataclass
class MergeBuilder:
    """Builder for Delta merge operations"""
    source_alias: str = "source"
    target_alias: str = "target"
    merge_condition: str = ""
    when_matched_update: Optional[Dict[str, str]] = None
    when_matched_delete: Optional[str] = None
    when_not_matched_insert: Optional[Dict[str, str]] = None
    when_not_matched_by_source_update: Optional[Dict[str, str]] = None
    when_not_matched_by_source_delete: Optional[str] = None


class DeltaLakeClient(BaseServiceClient):
    """
    Delta Lake client for lakehouse operations.
    
    Features:
    - ACID transactions
    - Time travel and versioning
    - Schema enforcement and evolution
    - Unified batch and streaming
    - Compaction and optimization
    - Change data capture (CDC)
    """
    
    def __init__(
        self,
        config: Optional[DeltaConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = DeltaConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: DeltaConfig = config
        self._storage_options = {}
        
    async def connect(self):
        """Connect to Delta Lake storage"""
        await super().connect()
        
        try:
            # Get S3 credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.s3_access_key = creds.get("access_key")
                    self.config.s3_secret_key = creds.get("secret_key")
            
            # Build storage options
            if self.config.s3_endpoint:
                self._storage_options = {
                    "AWS_ENDPOINT_URL": self.config.s3_endpoint,
                    "AWS_ACCESS_KEY_ID": self.config.s3_access_key,
                    "AWS_SECRET_ACCESS_KEY": self.config.s3_secret_key,
                    "AWS_REGION": self.config.s3_region,
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
                }
            
            logger.info(f"Connected to Delta Lake storage: {self.config.table_path}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Delta Lake: {e}")
            raise
    
    async def create_table(
        self,
        table_name: str,
        schema: Union[pa.Schema, pd.DataFrame],
        partition_by: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> DeltaTable:
        """
        Create a new Delta table.
        
        Args:
            table_name: Table name
            schema: PyArrow schema or pandas DataFrame
            partition_by: Partition columns
            properties: Table properties
            
        Returns:
            Created table metadata
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Create empty DataFrame with schema if PyArrow schema provided
            if isinstance(schema, pa.Schema):
                df = pd.DataFrame(columns=[field.name for field in schema])
            else:
                df = schema.head(0)  # Empty DataFrame with schema
            
            # Write initial table
            write_deltalake(
                table_path,
                df,
                mode="error",  # Fail if exists
                partition_by=partition_by,
                storage_options=self._storage_options,
                engine="pyarrow",
                compression=self.config.compression
            )
            
            # Set table properties
            if properties:
                delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
                # TODO: Set properties via ALTER TABLE
            
            # Get table metadata
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            table_meta = DeltaTable(
                path=table_path,
                version=delta_table.version(),
                num_files=len(delta_table.files()),
                size_bytes=sum(f.get("size", 0) for f in delta_table.files()),
                num_records=0,
                schema={field.name: str(field.type) for field in delta_table.schema().fields},
                partitions=partition_by or [],
                properties=properties or {}
            )
            
            logger.info(f"Created Delta table: {table_name}")
            return table_meta
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    async def write_data(
        self,
        table_name: str,
        data: Union[pa.Table, pd.DataFrame, List[Dict[str, Any]]],
        mode: str = "append",
        partition_overwrite_mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Write data to Delta table.
        
        Args:
            table_name: Table name
            data: Data to write
            mode: Write mode (append, overwrite, error, ignore)
            partition_overwrite_mode: Dynamic or static partition overwrite
            
        Returns:
            Write statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Convert data to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, pa.Table):
                df = data.to_pandas()
            else:
                df = data
            
            # Write data
            write_deltalake(
                table_path,
                df,
                mode=mode,
                storage_options=self._storage_options,
                engine="pyarrow",
                compression=self.config.compression,
                overwrite_schema=mode == "overwrite"
            )
            
            # Get write statistics
            stats = {
                "records_written": len(df),
                "mode": mode,
                "timestamp": datetime.now().isoformat()
            }
            
            # Check if compaction needed
            if self.config.auto_compact:
                delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
                if len(delta_table.files()) > self.config.compact_threshold:
                    await self.optimize_table(table_name, OptimizeConfig(strategy=OptimizeStrategy.COMPACT))
            
            logger.info(f"Wrote {stats['records_written']} records to {table_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    async def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        version: Optional[int] = None,
        timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read data from Delta table with time travel support.
        
        Args:
            table_name: Table name
            columns: Columns to read
            filter_expr: Filter expression
            version: Read specific version
            timestamp: Read as of timestamp
            limit: Row limit
            
        Returns:
            DataFrame with results
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Load table with version/timestamp
            if version is not None:
                delta_table = DeltaTableNative(
                    table_path,
                    version=version,
                    storage_options=self._storage_options
                )
            elif timestamp is not None:
                # TODO: Implement timestamp-based time travel
                delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            else:
                delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            # Read data
            if filter_expr:
                # TODO: Parse and apply filter
                df = delta_table.to_pandas()
            else:
                df = delta_table.to_pandas()
            
            # Select columns
            if columns:
                df = df[columns]
            
            # Apply limit
            if limit:
                df = df.head(limit)
            
            logger.info(f"Read {len(df)} rows from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise
    
    async def merge_data(
        self,
        table_name: str,
        source_data: Union[pd.DataFrame, pa.Table],
        merge_config: MergeBuilder
    ) -> Dict[str, Any]:
        """
        Perform merge operation on Delta table.
        
        Args:
            table_name: Target table name
            source_data: Source data for merge
            merge_config: Merge configuration
            
        Returns:
            Merge statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            # Convert source data
            if isinstance(source_data, pa.Table):
                source_df = source_data.to_pandas()
            else:
                source_df = source_data
            
            # Build merge operation
            merge_builder = (
                delta_table.merge(
                    source_df,
                    predicate=merge_config.merge_condition,
                    source_alias=merge_config.source_alias,
                    target_alias=merge_config.target_alias
                )
            )
            
            # Add matched clauses
            if merge_config.when_matched_update:
                merge_builder = merge_builder.when_matched_update(
                    updates=merge_config.when_matched_update
                )
            
            if merge_config.when_matched_delete:
                merge_builder = merge_builder.when_matched_delete(
                    predicate=merge_config.when_matched_delete
                )
            
            # Add not matched clauses
            if merge_config.when_not_matched_insert:
                merge_builder = merge_builder.when_not_matched_insert(
                    updates=merge_config.when_not_matched_insert
                )
            
            # Execute merge
            merge_metrics = merge_builder.execute()
            
            stats = {
                "rows_inserted": merge_metrics.get("num_target_rows_inserted", 0),
                "rows_updated": merge_metrics.get("num_target_rows_updated", 0),
                "rows_deleted": merge_metrics.get("num_target_rows_deleted", 0),
                "files_added": merge_metrics.get("num_target_files_added", 0),
                "files_removed": merge_metrics.get("num_target_files_removed", 0)
            }
            
            logger.info(f"Merge completed on {table_name}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to merge data: {e}")
            raise
    
    async def get_table_history(
        self,
        table_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get table version history.
        
        Args:
            table_name: Table name
            limit: Maximum versions to return
            
        Returns:
            List of version history
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            history = delta_table.history(limit=limit)
            
            return [
                {
                    "version": h.get("version"),
                    "timestamp": h.get("timestamp"),
                    "operation": h.get("operation"),
                    "parameters": h.get("operationParameters", {}),
                    "metrics": h.get("operationMetrics", {}),
                    "user": h.get("userName"),
                    "job": h.get("job", {})
                }
                for h in history
            ]
            
        except Exception as e:
            logger.error(f"Failed to get table history: {e}")
            raise
    
    async def optimize_table(
        self,
        table_name: str,
        config: OptimizeConfig
    ) -> Dict[str, Any]:
        """
        Optimize Delta table.
        
        Args:
            table_name: Table name
            config: Optimization configuration
            
        Returns:
            Optimization statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            stats = {}
            
            if config.strategy == OptimizeStrategy.COMPACT:
                # Compact small files
                metrics = delta_table.optimize.compact()
                stats["files_before"] = metrics.get("numFilesRemoved", 0)
                stats["files_after"] = metrics.get("numFilesAdded", 0)
                stats["bytes_removed"] = metrics.get("bytesRemoved", 0)
                
            elif config.strategy == OptimizeStrategy.Z_ORDER:
                # Z-order optimization
                if config.z_order_columns:
                    metrics = delta_table.optimize.z_order(config.z_order_columns)
                    stats["files_optimized"] = metrics.get("numFilesAdded", 0)
                    
            elif config.strategy == OptimizeStrategy.VACUUM:
                # Remove old files
                retention_hours = config.vacuum_hours or self.config.vacuum_retention_hours
                if not config.dry_run:
                    removed_files = delta_table.vacuum(retention_hours)
                    stats["files_removed"] = len(removed_files)
                else:
                    stats["files_to_remove"] = len(delta_table.vacuum(retention_hours, dry_run=True))
            
            logger.info(f"Optimized table {table_name}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    async def get_table_details(
        self,
        table_name: str
    ) -> DeltaTable:
        """
        Get detailed table information.
        
        Args:
            table_name: Table name
            
        Returns:
            Table metadata
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            # Get table details
            details = delta_table.detail()
            files = delta_table.files()
            
            return DeltaTable(
                path=table_path,
                version=delta_table.version(),
                num_files=len(files),
                size_bytes=sum(f.get("size", 0) for f in files),
                num_records=details.get("numRecords", 0),
                schema={field.name: str(field.type) for field in delta_table.schema().fields},
                partitions=details.get("partitionColumns", []),
                properties=details.get("properties", {})
            )
            
        except Exception as e:
            logger.error(f"Failed to get table details: {e}")
            raise
    
    async def restore_table(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Restore table to previous version.
        
        Args:
            table_name: Table name
            version: Target version
            timestamp: Target timestamp
            
        Returns:
            Restore statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            delta_table = DeltaTableNative(table_path, storage_options=self._storage_options)
            
            if version is not None:
                metrics = delta_table.restore(version)
            elif timestamp is not None:
                metrics = delta_table.restore(timestamp)
            else:
                raise ValueError("Either version or timestamp must be specified")
            
            stats = {
                "restored_version": version,
                "restored_timestamp": timestamp.isoformat() if timestamp else None,
                "files_restored": metrics.get("numRestoredFiles", 0),
                "files_removed": metrics.get("numRemovedFiles", 0)
            }
            
            logger.info(f"Restored table {table_name}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to restore table: {e}")
            raise
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Delta Lake specific configuration"""
        return {
            "table_path": self.config.table_path,
            "compression": self.config.compression,
            "auto_compact": self.config.auto_compact,
            "compact_threshold": self.config.compact_threshold,
            "vacuum_retention_hours": self.config.vacuum_retention_hours
        } 