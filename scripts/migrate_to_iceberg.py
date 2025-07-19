#!/usr/bin/env python3
"""
Migrate existing Parquet tables to Apache Iceberg format

This script converts existing Parquet files stored in MinIO to Iceberg tables,
enabling ACID transactions, schema evolution, and time-travel queries.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, 
    TimestampType, BooleanType, BinaryType, DecimalType,
    StructType, ListType, MapType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, DayTransform, HourTransform, BucketTransform
import boto3
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IcebergMigrator:
    """Migrates Parquet tables to Iceberg format"""
    
    def __init__(self, 
                 catalog_name: str = "platformq",
                 catalog_uri: str = "thrift://hive-metastore:9083",
                 s3_endpoint: str = "http://minio:9000",
                 s3_access_key: str = "minioadmin",
                 s3_secret_key: Optional[str] = None):
        """Initialize Iceberg migrator"""
        
        # Configure S3 client for MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key or os.getenv('MINIO_SECRET_KEY'),
            config=boto3.session.Config(signature_version='s3v4')
        )
        
        # Configure Iceberg catalog
        self.catalog = load_catalog(
            catalog_name,
            **{
                "uri": catalog_uri,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key or os.getenv('MINIO_SECRET_KEY'),
            }
        )
        
        self.migration_stats = {
            "tables_migrated": 0,
            "tables_failed": 0,
            "total_records": 0,
            "total_size_bytes": 0
        }
        
    def migrate_bucket(self, bucket_name: str, namespace: str = "default") -> Dict[str, Any]:
        """Migrate all Parquet tables in a bucket to Iceberg"""
        logger.info(f"Starting migration for bucket: {bucket_name}")
        
        # List all Parquet files in bucket
        parquet_files = self._list_parquet_files(bucket_name)
        
        # Group files by table (assuming directory = table)
        tables = self._group_files_by_table(parquet_files)
        
        results = {}
        for table_name, files in tables.items():
            try:
                logger.info(f"Migrating table: {table_name}")
                result = self._migrate_table(
                    bucket_name, 
                    table_name, 
                    files, 
                    namespace
                )
                results[table_name] = result
                self.migration_stats["tables_migrated"] += 1
                
            except Exception as e:
                logger.error(f"Failed to migrate table {table_name}: {e}")
                results[table_name] = {"status": "failed", "error": str(e)}
                self.migration_stats["tables_failed"] += 1
                
        return results
        
    def _list_parquet_files(self, bucket_name: str) -> List[str]:
        """List all Parquet files in a bucket"""
        files = []
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files.append(obj['Key'])
                    
        logger.info(f"Found {len(files)} Parquet files in bucket {bucket_name}")
        return files
        
    def _group_files_by_table(self, files: List[str]) -> Dict[str, List[str]]:
        """Group Parquet files by table name (directory)"""
        tables = {}
        
        for file_path in files:
            parts = file_path.split('/')
            if len(parts) > 1:
                # Use parent directory as table name
                table_name = parts[-2]
            else:
                # Use file name without extension
                table_name = parts[-1].replace('.parquet', '')
                
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(file_path)
            
        return tables
        
    def _migrate_table(self, 
                      bucket_name: str, 
                      table_name: str, 
                      files: List[str], 
                      namespace: str) -> Dict[str, Any]:
        """Migrate a single table to Iceberg format"""
        
        # Read schema from first Parquet file
        first_file = f"s3a://{bucket_name}/{files[0]}"
        parquet_schema = pq.read_schema(first_file, 
                                      filesystem=self._get_s3_filesystem())
        
        # Convert Parquet schema to Iceberg schema
        iceberg_schema = self._convert_schema(parquet_schema)
        
        # Determine partitioning strategy
        partition_spec = self._determine_partitioning(table_name, parquet_schema)
        
        # Create Iceberg table
        table_identifier = f"{namespace}.{table_name}"
        
        # Check if table already exists
        try:
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Table {table_identifier} already exists, appending data")
        except:
            # Create new table
            table = self.catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "zstd",
                    "write.metadata.compression-codec": "gzip",
                    "write.summary.partition-limit": "100",
                    "history.expire.max-snapshot-age-days": "7",
                    "write.parquet.dict-size-bytes": "2097152",  # 2MB
                    "write.parquet.row-group-size-bytes": "134217728",  # 128MB
                }
            )
            logger.info(f"Created Iceberg table: {table_identifier}")
            
        # Migrate data from Parquet files
        total_records = 0
        for file_path in files:
            records = self._migrate_file(
                bucket_name, 
                file_path, 
                table
            )
            total_records += records
            
        self.migration_stats["total_records"] += total_records
        
        # Run table maintenance
        self._optimize_table(table)
        
        return {
            "status": "success",
            "files_migrated": len(files),
            "records_migrated": total_records,
            "table_location": table.metadata_location
        }
        
    def _convert_schema(self, parquet_schema: pa.Schema) -> Schema:
        """Convert Parquet schema to Iceberg schema"""
        fields = []
        
        for i, field in enumerate(parquet_schema):
            iceberg_type = self._convert_type(field.type)
            iceberg_field = NestedField(
                field_id=i + 1,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            )
            fields.append(iceberg_field)
            
        return Schema(*fields)
        
    def _convert_type(self, arrow_type: pa.DataType) -> Any:
        """Convert Arrow type to Iceberg type"""
        if pa.types.is_string(arrow_type):
            return StringType()
        elif pa.types.is_int64(arrow_type):
            return LongType()
        elif pa.types.is_float64(arrow_type):
            return DoubleType()
        elif pa.types.is_timestamp(arrow_type):
            return TimestampType()
        elif pa.types.is_boolean(arrow_type):
            return BooleanType()
        elif pa.types.is_binary(arrow_type):
            return BinaryType()
        elif pa.types.is_decimal(arrow_type):
            return DecimalType(arrow_type.precision, arrow_type.scale)
        elif pa.types.is_list(arrow_type):
            element_type = self._convert_type(arrow_type.value_type)
            return ListType(element_id=1, element_type=element_type, element_required=False)
        elif pa.types.is_struct(arrow_type):
            fields = []
            for i, field in enumerate(arrow_type):
                fields.append(NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=self._convert_type(field.type),
                    required=not field.nullable
                ))
            return StructType(*fields)
        else:
            # Default to string for unknown types
            return StringType()
            
    def _determine_partitioning(self, 
                               table_name: str, 
                               schema: pa.Schema) -> PartitionSpec:
        """Determine partitioning strategy based on table name and schema"""
        
        # Common partitioning patterns
        if any(col in schema.names for col in ['event_date', 'date', 'dt']):
            # Daily partitioning for event data
            return PartitionSpec(
                PartitionField(
                    source_id=schema.names.index('event_date') + 1 
                    if 'event_date' in schema.names 
                    else schema.names.index('date') + 1,
                    field_id=1000,
                    name="event_day",
                    transform=DayTransform()
                )
            )
            
        elif 'timestamp' in schema.names:
            # Hourly partitioning for high-volume streaming data
            return PartitionSpec(
                PartitionField(
                    source_id=schema.names.index('timestamp') + 1,
                    field_id=1000,
                    name="event_hour",
                    transform=HourTransform()
                )
            )
            
        elif 'tenant_id' in schema.names and 'created_at' in schema.names:
            # Multi-level partitioning for multi-tenant data
            return PartitionSpec(
                PartitionField(
                    source_id=schema.names.index('tenant_id') + 1,
                    field_id=1000,
                    name="tenant",
                    transform=IdentityTransform()
                ),
                PartitionField(
                    source_id=schema.names.index('created_at') + 1,
                    field_id=1001,
                    name="created_day",
                    transform=DayTransform()
                )
            )
            
        elif 'user_id' in schema.names:
            # Bucket partitioning for user data
            return PartitionSpec(
                PartitionField(
                    source_id=schema.names.index('user_id') + 1,
                    field_id=1000,
                    name="user_bucket",
                    transform=BucketTransform(n_buckets=32)
                )
            )
            
        else:
            # No partitioning for small tables
            return PartitionSpec()
            
    def _migrate_file(self, 
                     bucket_name: str, 
                     file_path: str, 
                     table: Any) -> int:
        """Migrate a single Parquet file to Iceberg table"""
        
        # Read Parquet file
        s3_path = f"s3a://{bucket_name}/{file_path}"
        df = pq.read_table(s3_path, filesystem=self._get_s3_filesystem())
        
        # Write to Iceberg table
        table.overwrite(df)
        
        record_count = len(df)
        logger.info(f"Migrated {record_count} records from {file_path}")
        
        return record_count
        
    def _optimize_table(self, table: Any):
        """Run optimization on Iceberg table"""
        try:
            # Compact small files
            table.compact()
            
            # Expire old snapshots
            table.expire_snapshots(
                older_than=datetime.now().timestamp() - 7 * 24 * 60 * 60
            )
            
            # Rewrite manifests
            table.rewrite_manifests()
            
            logger.info(f"Optimized table {table.name()}")
            
        except Exception as e:
            logger.warning(f"Failed to optimize table: {e}")
            
    def _get_s3_filesystem(self):
        """Get PyArrow S3 filesystem for MinIO"""
        return pa.fs.S3FileSystem(
            endpoint_override=self.s3_client._endpoint.host,
            access_key=self.s3_client._request_signer._credentials.access_key,
            secret_key=self.s3_client._request_signer._credentials.secret_key
        )
        
    def migrate_medallion_architecture(self):
        """Migrate entire medallion architecture (bronze, silver, gold)"""
        logger.info("Starting medallion architecture migration")
        
        results = {}
        
        # Migrate each layer
        for layer in ['bronze', 'silver', 'gold']:
            bucket_name = f"platformq-{layer}"
            logger.info(f"Migrating {layer} layer")
            
            try:
                layer_results = self.migrate_bucket(bucket_name, namespace=layer)
                results[layer] = layer_results
            except Exception as e:
                logger.error(f"Failed to migrate {layer} layer: {e}")
                results[layer] = {"error": str(e)}
                
        # Print summary
        logger.info("=" * 50)
        logger.info("Migration Summary:")
        logger.info(f"Tables migrated: {self.migration_stats['tables_migrated']}")
        logger.info(f"Tables failed: {self.migration_stats['tables_failed']}")
        logger.info(f"Total records: {self.migration_stats['total_records']:,}")
        logger.info("=" * 50)
        
        return results


def main():
    """Main migration function"""
    migrator = IcebergMigrator(
        s3_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        s3_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.getenv("MINIO_SECRET_KEY")
    )
    
    # Run migration
    results = migrator.migrate_medallion_architecture()
    
    # Save results
    import json
    with open("iceberg_migration_results.json", "w") as f:
        json.dump(results, f, indent=2)
        
    logger.info("Migration complete! Results saved to iceberg_migration_results.json")


if __name__ == "__main__":
    main() 