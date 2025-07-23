"""
Medallion Architecture Manager

Manages the Bronze, Silver, and Gold layers of the data lake
with automated transitions, quality checks, and lifecycle policies.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
from pathlib import Path

from minio import Minio
from minio.error import S3Error
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from delta import DeltaTable, write_deltalake, configure_spark_with_delta_pip
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from app.core.config import Settings
from app.core.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class DataLayer(str, Enum):
    """Data layer types in medallion architecture"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class DataQualityLevel(str, Enum):
    """Data quality levels for transitions"""
    RAW = "raw"
    VALIDATED = "validated"
    CLEANSED = "cleansed"
    ENRICHED = "enriched"
    AGGREGATED = "aggregated"


class MedallionArchitectureManager:
    """Manages data lake medallion architecture"""
    
    def __init__(self, settings: Settings, schema_registry: SchemaRegistry, spark_session: Optional[SparkSession] = None):
        self.settings = settings
        self.schema_registry = schema_registry
        self.spark = spark_session
        
        # MinIO client for object storage
        self.minio_client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure
        )
        
        # Bucket names for each layer
        self.layer_buckets = {
            DataLayer.BRONZE: settings.minio_bucket_bronze or "lake-bronze",
            DataLayer.SILVER: settings.minio_bucket_silver or "lake-silver",
            DataLayer.GOLD: settings.minio_bucket_gold or "lake-gold"
        }
        
        # Retention policies per layer (in days)
        self.retention_policies = {
            DataLayer.BRONZE: settings.bronze_retention_days or 90,
            DataLayer.SILVER: settings.silver_retention_days or 365,
            DataLayer.GOLD: settings.gold_retention_days or 1825  # 5 years
        }
        
        # Initialize buckets
        self._initialize_buckets()
        
    def _initialize_buckets(self):
        """Ensure all layer buckets exist"""
        for layer, bucket in self.layer_buckets.items():
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
    
    async def ingest_to_bronze(
        self,
        data: Any,
        dataset_name: str,
        source_info: Dict[str, Any],
        format: str = "parquet"
    ) -> Dict[str, Any]:
        """Ingest raw data to bronze layer"""
        try:
            # Generate unique path
            timestamp = datetime.utcnow()
            path = f"{dataset_name}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/{timestamp.isoformat()}.{format}"
            
            # Add metadata
            metadata = {
                "ingestion_timestamp": timestamp.isoformat(),
                "source": source_info,
                "dataset": dataset_name,
                "layer": DataLayer.BRONZE,
                "quality_level": DataQualityLevel.RAW,
                "format": format
            }
            
            # Store data based on format
            if format == "parquet":
                # Convert to parquet if needed
                if isinstance(data, pd.DataFrame):
                    table = pa.Table.from_pandas(data)
                else:
                    table = data
                    
                # Write to MinIO
                import io
                buf = io.BytesIO()
                pq.write_table(table, buf)
                buf.seek(0)
                
                self.minio_client.put_object(
                    self.layer_buckets[DataLayer.BRONZE],
                    path,
                    buf,
                    length=buf.getbuffer().nbytes,
                    metadata=metadata
                )
            else:
                # Handle other formats (CSV, JSON, etc.)
                import io
                if format == "json":
                    content = json.dumps(data).encode('utf-8')
                elif format == "csv" and isinstance(data, pd.DataFrame):
                    content = data.to_csv(index=False).encode('utf-8')
                else:
                    content = str(data).encode('utf-8')
                
                self.minio_client.put_object(
                    self.layer_buckets[DataLayer.BRONZE],
                    path,
                    io.BytesIO(content),
                    length=len(content),
                    metadata=metadata
                )
            
            logger.info(f"Ingested data to bronze layer: {path}")
            
            return {
                "status": "success",
                "layer": DataLayer.BRONZE,
                "path": path,
                "bucket": self.layer_buckets[DataLayer.BRONZE],
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Error ingesting to bronze layer: {e}")
            raise
    
    async def transform_bronze_to_silver(
        self,
        dataset_name: str,
        transformations: List[Dict[str, Any]],
        quality_rules: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Transform data from bronze to silver layer with quality checks"""
        try:
            if not self.spark:
                raise ValueError("Spark session required for transformations")
            
            # Read from bronze layer
            bronze_path = f"s3a://{self.layer_buckets[DataLayer.BRONZE]}/{dataset_name}/"
            df = self.spark.read.parquet(bronze_path)
            
            # Apply transformations
            for transform in transformations:
                df = self._apply_transformation(df, transform)
            
            # Apply quality checks
            if quality_rules:
                quality_report = self._check_data_quality(df, quality_rules)
                if not quality_report["passed"]:
                    logger.warning(f"Data quality checks failed: {quality_report}")
                    # Optionally halt transformation
                    if self.settings.halt_on_quality_failure:
                        raise ValueError("Data quality checks failed")
            
            # Write to silver layer with Delta format
            silver_path = f"s3a://{self.layer_buckets[DataLayer.SILVER]}/{dataset_name}"
            
            # Add metadata columns
            df = df.withColumn("_quality_level", F.lit(DataQualityLevel.CLEANSED))
            df = df.withColumn("_transformation_timestamp", F.current_timestamp())
            
            # Write as Delta table
            write_deltalake(silver_path, df, mode="overwrite", partition_by=["year", "month"])
            
            # Register schema if not exists
            schema = self._extract_schema(df)
            await self.schema_registry.register_schema(
                f"{dataset_name}_silver",
                schema,
                "delta"
            )
            
            logger.info(f"Transformed data to silver layer: {silver_path}")
            
            return {
                "status": "success",
                "layer": DataLayer.SILVER,
                "path": silver_path,
                "bucket": self.layer_buckets[DataLayer.SILVER],
                "quality_report": quality_report if quality_rules else None,
                "row_count": df.count()
            }
            
        except Exception as e:
            logger.error(f"Error transforming to silver layer: {e}")
            raise
    
    async def aggregate_silver_to_gold(
        self,
        dataset_name: str,
        aggregations: List[Dict[str, Any]],
        business_rules: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Aggregate data from silver to gold layer for business use"""
        try:
            if not self.spark:
                raise ValueError("Spark session required for aggregations")
            
            # Read from silver layer
            silver_path = f"s3a://{self.layer_buckets[DataLayer.SILVER]}/{dataset_name}"
            df = self.spark.read.format("delta").load(silver_path)
            
            # Apply business rules
            if business_rules:
                for rule in business_rules:
                    df = self._apply_business_rule(df, rule)
            
            # Apply aggregations
            for agg in aggregations:
                df = self._apply_aggregation(df, agg)
            
            # Write to gold layer
            gold_path = f"s3a://{self.layer_buckets[DataLayer.GOLD]}/{dataset_name}"
            
            # Add metadata
            df = df.withColumn("_quality_level", F.lit(DataQualityLevel.AGGREGATED))
            df = df.withColumn("_aggregation_timestamp", F.current_timestamp())
            
            # Write as optimized Delta table
            write_deltalake(
                gold_path,
                df,
                mode="overwrite",
                partition_by=self._determine_partitions(dataset_name)
            )
            
            # Optimize for analytics
            delta_table = DeltaTable.forPath(self.spark, gold_path)
            delta_table.optimize().executeCompaction()
            
            logger.info(f"Aggregated data to gold layer: {gold_path}")
            
            return {
                "status": "success",
                "layer": DataLayer.GOLD,
                "path": gold_path,
                "bucket": self.layer_buckets[DataLayer.GOLD],
                "row_count": df.count()
            }
            
        except Exception as e:
            logger.error(f"Error aggregating to gold layer: {e}")
            raise
    
    def _apply_transformation(self, df, transform: Dict[str, Any]):
        """Apply a transformation to a DataFrame"""
        transform_type = transform.get("type")
        config = transform.get("config", {})
        
        if transform_type == "clean":
            # Remove nulls from specified columns
            for col in config.get("remove_nulls", []):
                df = df.filter(df[col].isNotNull())
            
            # Standardize strings
            if config.get("standardize_strings"):
                for col in df.columns:
                    if dict(df.dtypes)[col] == "string":
                        df = df.withColumn(col, F.trim(F.lower(df[col])))
            
            # Deduplicate
            if config.get("deduplicate"):
                df = df.dropDuplicates(config.get("deduplicate"))
                
        elif transform_type == "enrich":
            # Add derived columns
            for col_name, expression in config.get("derived_columns", {}).items():
                df = df.withColumn(col_name, F.expr(expression))
                
        elif transform_type == "filter":
            # Apply filters
            for condition in config.get("conditions", []):
                df = df.filter(condition)
                
        return df
    
    def _check_data_quality(self, df, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check data quality against rules"""
        results = []
        passed = True
        
        for rule in rules:
            rule_type = rule.get("type")
            config = rule.get("config", {})
            
            if rule_type == "completeness":
                # Check completeness for columns
                for col in config.get("columns", []):
                    null_count = df.filter(df[col].isNull()).count()
                    total_count = df.count()
                    completeness = (total_count - null_count) / total_count if total_count > 0 else 0
                    
                    threshold = config.get("threshold", 0.95)
                    rule_passed = completeness >= threshold
                    
                    results.append({
                        "rule": f"completeness_{col}",
                        "passed": rule_passed,
                        "value": completeness,
                        "threshold": threshold
                    })
                    
                    if not rule_passed:
                        passed = False
                        
            elif rule_type == "uniqueness":
                # Check uniqueness
                for col in config.get("columns", []):
                    distinct_count = df.select(col).distinct().count()
                    total_count = df.count()
                    uniqueness = distinct_count / total_count if total_count > 0 else 0
                    
                    threshold = config.get("threshold", 0.99)
                    rule_passed = uniqueness >= threshold
                    
                    results.append({
                        "rule": f"uniqueness_{col}",
                        "passed": rule_passed,
                        "value": uniqueness,
                        "threshold": threshold
                    })
                    
                    if not rule_passed:
                        passed = False
        
        return {
            "passed": passed,
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _apply_business_rule(self, df, rule: Dict[str, Any]):
        """Apply business rule to DataFrame"""
        rule_type = rule.get("type")
        config = rule.get("config", {})
        
        if rule_type == "currency_conversion":
            # Convert currency columns
            df = df.withColumn(
                config["target_column"],
                df[config["source_column"]] * config["rate"]
            )
        elif rule_type == "date_dimension":
            # Add date dimensions
            date_col = config["date_column"]
            df = df.withColumn("year", F.year(date_col))
            df = df.withColumn("month", F.month(date_col))
            df = df.withColumn("quarter", F.quarter(date_col))
            df = df.withColumn("day_of_week", F.dayofweek(date_col))
            
        return df
    
    def _apply_aggregation(self, df, agg: Dict[str, Any]):
        """Apply aggregation to DataFrame"""
        agg_type = agg.get("type")
        config = agg.get("config", {})
        
        if agg_type == "rollup":
            # Group by and aggregate
            group_cols = config.get("group_by", [])
            agg_exprs = []
            
            for metric in config.get("metrics", []):
                col = metric["column"]
                func = metric["function"]
                alias = metric.get("alias", f"{func}_{col}")
                
                if func == "sum":
                    agg_exprs.append(F.sum(col).alias(alias))
                elif func == "avg":
                    agg_exprs.append(F.avg(col).alias(alias))
                elif func == "count":
                    agg_exprs.append(F.count(col).alias(alias))
                elif func == "max":
                    agg_exprs.append(F.max(col).alias(alias))
                elif func == "min":
                    agg_exprs.append(F.min(col).alias(alias))
            
            df = df.groupBy(*group_cols).agg(*agg_exprs)
            
        elif agg_type == "window":
            # Window functions
            window_spec = config.get("window_spec", {})
            # Implement window aggregations
            
        return df
    
    def _extract_schema(self, df) -> Dict[str, Any]:
        """Extract schema from DataFrame"""
        return {
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                }
                for field in df.schema.fields
            ]
        }
    
    def _determine_partitions(self, dataset_name: str) -> List[str]:
        """Determine partition columns based on dataset"""
        # Common partition strategies
        if "sales" in dataset_name:
            return ["year", "month", "region"]
        elif "events" in dataset_name:
            return ["year", "month", "day", "hour"]
        elif "users" in dataset_name:
            return ["registration_year", "country"]
        else:
            return ["year", "month"]
    
    async def apply_lifecycle_policies(self):
        """Apply retention and archival policies to all layers"""
        try:
            for layer, bucket in self.layer_buckets.items():
                retention_days = self.retention_policies[layer]
                cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
                
                # List objects in bucket
                objects = self.minio_client.list_objects(bucket, recursive=True)
                
                for obj in objects:
                    if obj.last_modified.replace(tzinfo=None) < cutoff_date:
                        if layer == DataLayer.BRONZE:
                            # Archive to cold storage
                            await self._archive_object(bucket, obj.object_name)
                        elif layer == DataLayer.SILVER and self.settings.archive_silver:
                            # Optionally archive silver
                            await self._archive_object(bucket, obj.object_name)
                        # Gold layer typically not archived
                        
                logger.info(f"Applied lifecycle policies to {layer} layer")
                
        except Exception as e:
            logger.error(f"Error applying lifecycle policies: {e}")
            raise
    
    async def _archive_object(self, bucket: str, object_name: str):
        """Archive object to cold storage"""
        try:
            # Copy to archive bucket
            archive_bucket = f"{bucket}-archive"
            
            # Ensure archive bucket exists
            if not self.minio_client.bucket_exists(archive_bucket):
                self.minio_client.make_bucket(archive_bucket)
            
            # Copy object
            self.minio_client.copy_object(
                archive_bucket,
                object_name,
                f"/{bucket}/{object_name}"
            )
            
            # Delete from primary bucket
            self.minio_client.remove_object(bucket, object_name)
            
            logger.info(f"Archived {object_name} from {bucket}")
            
        except Exception as e:
            logger.error(f"Error archiving object: {e}")
            raise
    
    async def optimize_storage(self, layer: DataLayer, dataset_name: str):
        """Optimize storage for a dataset in a specific layer"""
        try:
            if layer == DataLayer.SILVER or layer == DataLayer.GOLD:
                # Optimize Delta tables
                path = f"s3a://{self.layer_buckets[layer]}/{dataset_name}"
                delta_table = DeltaTable.forPath(self.spark, path)
                
                # Compact small files
                delta_table.optimize().executeCompaction()
                
                # Z-order by common query columns
                if layer == DataLayer.GOLD:
                    # Determine columns to z-order by
                    z_order_cols = self._get_z_order_columns(dataset_name)
                    if z_order_cols:
                        delta_table.optimize().executeZOrderBy(*z_order_cols)
                
                logger.info(f"Optimized storage for {dataset_name} in {layer} layer")
                
        except Exception as e:
            logger.error(f"Error optimizing storage: {e}")
            raise
    
    def _get_z_order_columns(self, dataset_name: str) -> List[str]:
        """Get columns to z-order by for optimization"""
        # Common patterns
        if "sales" in dataset_name:
            return ["customer_id", "product_id", "date"]
        elif "events" in dataset_name:
            return ["user_id", "event_type", "timestamp"]
        else:
            return [] 