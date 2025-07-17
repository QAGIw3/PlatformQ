"""
Medallion Architecture for Data Lake

Implements the Bronze, Silver, and Gold layer pattern for data quality and processing.
Integrates with MinIO for object storage and Apache Spark for processing.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
import hashlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta import DeltaTable, configure_spark_with_delta_pip
from minio import Minio
from minio.error import S3Error
from platformq.shared.event_publisher import EventPublisher
from platformq.events import DatasetLineageEvent, IndexableEntityEvent
import uuid

logger = logging.getLogger(__name__)


class DataLayer(Enum):
    """Data lake layers in medallion architecture"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class DataQualityRule:
    """Data quality rule definition"""
    rule_id: str
    name: str
    description: str
    check_function: Callable[[DataFrame], DataFrame]
    severity: str = "warning"  # warning, error
    active: bool = True


@dataclass
class DataLineage:
    """Data lineage tracking"""
    dataset_id: str
    source_layer: DataLayer
    target_layer: DataLayer
    transformation_id: str
    input_datasets: List[str]
    output_dataset: str
    timestamp: datetime
    spark_job_id: Optional[str] = None
    quality_checks_passed: List[str] = field(default_factory=list)
    quality_checks_failed: List[str] = field(default_factory=list)


@dataclass
class SchemaEvolution:
    """Schema evolution tracking"""
    dataset_id: str
    version: int
    schema: StructType
    timestamp: datetime
    changes: List[Dict[str, Any]]
    backward_compatible: bool = True


class MedallionArchitecture:
    """
    Implements medallion architecture for data lake with Bronze, Silver, Gold layers
    """
    
    def __init__(self,
                 spark_session: SparkSession,
                 minio_client: Minio,
                 event_publisher: EventPublisher,
                 bucket_name: str = "platformq-data-lake",
                 delta_enabled: bool = True):
        """
        Initialize medallion architecture
        
        Args:
            spark_session: Spark session for processing
            minio_client: MinIO client for object storage
            event_publisher: Event publisher for sending lineage events
            bucket_name: MinIO bucket name
            delta_enabled: Whether to use Delta Lake format
        """
        self.spark = spark_session
        self.minio = minio_client
        self.event_publisher = event_publisher
        self.bucket_name = bucket_name
        self.delta_enabled = delta_enabled
        
        # Configure Spark for Delta Lake if enabled
        if delta_enabled:
            self.spark = configure_spark_with_delta_pip(spark_session).builder \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
                
        # Data quality rules registry
        self.quality_rules: Dict[DataLayer, List[DataQualityRule]] = {
            DataLayer.BRONZE: [],
            DataLayer.SILVER: [],
            DataLayer.GOLD: []
        }
        
        # Schema registry
        self.schema_registry: Dict[str, List[SchemaEvolution]] = {}
        
        # Lineage tracker
        self.lineage_tracker: List[DataLineage] = []
        
        # Initialize default quality rules
        self._init_default_quality_rules()
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
        
    def _ensure_bucket_exists(self):
        """Ensure MinIO bucket exists"""
        try:
            if not self.minio.bucket_exists(self.bucket_name):
                self.minio.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            
    def _init_default_quality_rules(self):
        """Initialize default data quality rules"""
        # Bronze layer rules - basic validation
        self.add_quality_rule(
            DataLayer.BRONZE,
            DataQualityRule(
                rule_id="bronze_not_null_id",
                name="Not Null ID Check",
                description="Ensure ID fields are not null",
                check_function=lambda df: df.filter(col("id").isNotNull()),
                severity="error"
            )
        )
        
        self.add_quality_rule(
            DataLayer.BRONZE,
            DataQualityRule(
                rule_id="bronze_timestamp_valid",
                name="Valid Timestamp Check",
                description="Ensure timestamps are valid",
                check_function=lambda df: df.filter(col("timestamp").isNotNull()),
                severity="error"
            )
        )
        
        # Silver layer rules - data cleansing
        self.add_quality_rule(
            DataLayer.SILVER,
            DataQualityRule(
                rule_id="silver_no_duplicates",
                name="No Duplicates Check",
                description="Remove duplicate records",
                check_function=lambda df: df.dropDuplicates(["id", "timestamp"]),
                severity="warning"
            )
        )
        
        self.add_quality_rule(
            DataLayer.SILVER,
            DataQualityRule(
                rule_id="silver_data_types",
                name="Data Type Validation",
                description="Ensure correct data types",
                check_function=self._validate_data_types,
                severity="error"
            )
        )
        
        # Gold layer rules - business logic
        self.add_quality_rule(
            DataLayer.GOLD,
            DataQualityRule(
                rule_id="gold_completeness",
                name="Data Completeness Check",
                description="Ensure all required fields are present",
                check_function=self._check_completeness,
                severity="error"
            )
        )
        
    def add_quality_rule(self, layer: DataLayer, rule: DataQualityRule):
        """Add a data quality rule to a layer"""
        self.quality_rules[layer].append(rule)
        logger.info(f"Added quality rule {rule.rule_id} to {layer.value} layer")
        
    def ingest_to_bronze(self,
                        data: Union[pd.DataFrame, List[Dict], str],
                        dataset_name: str,
                        format: str = "parquet",
                        partition_by: Optional[List[str]] = None,
                        metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Ingest raw data into Bronze layer
        
        Args:
            data: Input data (DataFrame, list of dicts, or file path)
            dataset_name: Name of the dataset
            format: Storage format (parquet, delta, json)
            partition_by: Columns to partition by
            metadata: Additional metadata
            
        Returns:
            Path to ingested data
        """
        try:
            # Convert input to Spark DataFrame
            if isinstance(data, pd.DataFrame):
                df = self.spark.createDataFrame(data)
            elif isinstance(data, list):
                df = self.spark.createDataFrame(data)
            elif isinstance(data, str):
                # Assume file path
                if format == "json":
                    df = self.spark.read.json(data)
                elif format == "csv":
                    df = self.spark.read.csv(data, header=True, inferSchema=True)
                elif format == "parquet":
                    df = self.spark.read.parquet(data)
                else:
                    raise ValueError(f"Unsupported format: {format}")
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
                
            # Add metadata columns
            df = self._add_bronze_metadata(df, dataset_name, metadata)
            
            # Apply Bronze layer quality rules
            df, quality_results = self._apply_quality_rules(df, DataLayer.BRONZE)
            
            # Generate path
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = f"{DataLayer.BRONZE.value}/{dataset_name}/{timestamp}"
            
            # Write to MinIO
            if self.delta_enabled and format == "delta":
                self._write_delta(df, path, partition_by)
            else:
                self._write_parquet(df, path, partition_by)
                
            # Track lineage
            lineage = DataLineage(
                dataset_id=f"{dataset_name}_{timestamp}",
                source_layer=DataLayer.BRONZE,
                target_layer=DataLayer.BRONZE,
                transformation_id="ingest",
                input_datasets=[],
                output_dataset=path,
                timestamp=datetime.now(timezone.utc),
                quality_checks_passed=[r["rule_id"] for r in quality_results if r["passed"]],
                quality_checks_failed=[r["rule_id"] for r in quality_results if not r["passed"]]
            )
            self.lineage_tracker.append(lineage)
            
            # Register schema
            self._register_schema(dataset_name, df.schema)
            
            # Publish lineage event
            self._publish_lineage_event(
                dataset_id=f"{dataset_name}_{timestamp}",
                dataset_name=dataset_name,
                layer=DataLayer.BRONZE,
                source_datasets=[],
                output_path=path,
                schema=df.schema,
                quality_results=quality_results,
                triggered_by="ingestion_pipeline"
            )

            logger.info(f"Ingested data to Bronze layer: {path}")
            return path
            
        except Exception as e:
            logger.error(f"Error ingesting to Bronze: {e}")
            raise
            
    def _add_bronze_metadata(self,
                           df: DataFrame,
                           dataset_name: str,
                           metadata: Optional[Dict[str, Any]]) -> DataFrame:
        """Add Bronze layer metadata columns"""
        df = df.withColumn("_bronze_timestamp", current_timestamp()) \
               .withColumn("_bronze_dataset", lit(dataset_name)) \
               .withColumn("_bronze_row_id", sha2(concat_ws("||", *df.columns), 256))
               
        if metadata:
            for key, value in metadata.items():
                df = df.withColumn(f"_bronze_meta_{key}", lit(str(value)))
                
        return df
        
    def process_to_silver(self,
                         bronze_path: str,
                         dataset_name: str,
                         transformations: List[Callable[[DataFrame], DataFrame]],
                         quality_threshold: float = 0.95) -> str:
        """
        Process Bronze data to Silver layer with cleansing and standardization
        
        Args:
            bronze_path: Path to Bronze data
            dataset_name: Name of the dataset
            transformations: List of transformation functions
            quality_threshold: Minimum quality score required
            
        Returns:
            Path to Silver data
        """
        try:
            # Read Bronze data
            df = self._read_data(bronze_path)
            
            # Apply transformations
            for transform in transformations:
                df = transform(df)
                
            # Standardize column names
            df = self._standardize_columns(df)
            
            # Add Silver metadata
            df = self._add_silver_metadata(df, dataset_name)
            
            # Apply Silver layer quality rules
            df, quality_results = self._apply_quality_rules(df, DataLayer.SILVER)
            
            # Check quality threshold
            quality_score = self._calculate_quality_score(quality_results)
            if quality_score < quality_threshold:
                raise ValueError(f"Data quality score {quality_score} below threshold {quality_threshold}")
                
            # Generate path
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = f"{DataLayer.SILVER.value}/{dataset_name}/{timestamp}"
            
            # Write to MinIO
            if self.delta_enabled:
                self._write_delta(df, path, ["_silver_date"])
            else:
                self._write_parquet(df, path, ["_silver_date"])
                
            # Track lineage
            lineage = DataLineage(
                dataset_id=f"{dataset_name}_{timestamp}",
                source_layer=DataLayer.BRONZE,
                target_layer=DataLayer.SILVER,
                transformation_id="cleanse_standardize",
                input_datasets=[bronze_path],
                output_dataset=path,
                timestamp=datetime.now(timezone.utc),
                quality_checks_passed=[r["rule_id"] for r in quality_results if r["passed"]],
                quality_checks_failed=[r["rule_id"] for r in quality_results if not r["passed"]]
            )
            self.lineage_tracker.append(lineage)
            
            # Publish lineage event
            self._publish_lineage_event(
                dataset_id=f"{dataset_name}_{timestamp}",
                dataset_name=dataset_name,
                layer=DataLayer.SILVER,
                source_datasets=[bronze_path],
                output_path=path,
                schema=df.schema,
                quality_results=quality_results,
                triggered_by="processing_pipeline"
            )

            logger.info(f"Processed data to Silver layer: {path}")
            return path
            
        except Exception as e:
            logger.error(f"Error processing to Silver: {e}")
            raise
            
    def _add_silver_metadata(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Add Silver layer metadata columns"""
        return df.withColumn("_silver_timestamp", current_timestamp()) \
                 .withColumn("_silver_dataset", lit(dataset_name)) \
                 .withColumn("_silver_date", current_timestamp().cast("date")) \
                 .withColumn("_silver_version", lit("1.0"))
                 
    def _standardize_columns(self, df: DataFrame) -> DataFrame:
        """Standardize column names to lowercase with underscores"""
        for col_name in df.columns:
            if not col_name.startswith("_"):  # Don't rename metadata columns
                new_name = col_name.lower().replace(" ", "_").replace("-", "_")
                df = df.withColumnRenamed(col_name, new_name)
        return df
        
    def aggregate_to_gold(self,
                         silver_paths: List[str],
                         dataset_name: str,
                         aggregations: List[Callable[[DataFrame], DataFrame]],
                         business_rules: List[Callable[[DataFrame], DataFrame]]) -> str:
        """
        Create Gold layer datasets with business-level aggregations
        
        Args:
            silver_paths: List of Silver layer paths to aggregate
            dataset_name: Name of the Gold dataset
            aggregations: List of aggregation functions
            business_rules: List of business rule functions
            
        Returns:
            Path to Gold data
        """
        try:
            # Read and union Silver data
            dfs = [self._read_data(path) for path in silver_paths]
            df = dfs[0]
            for other_df in dfs[1:]:
                df = df.union(other_df)
                
            # Apply aggregations
            for agg_func in aggregations:
                df = agg_func(df)
                
            # Apply business rules
            for rule_func in business_rules:
                df = rule_func(df)
                
            # Add Gold metadata
            df = self._add_gold_metadata(df, dataset_name)
            
            # Apply Gold layer quality rules
            df, quality_results = self._apply_quality_rules(df, DataLayer.GOLD)
            
            # Generate path
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = f"{DataLayer.GOLD.value}/{dataset_name}/{timestamp}"
            
            # Write to MinIO with optimization
            if self.delta_enabled:
                self._write_delta(df, path, mode="overwrite", optimize=True)
            else:
                self._write_parquet(df, path, mode="overwrite", coalesce=True)
                
            # Track lineage
            lineage = DataLineage(
                dataset_id=f"{dataset_name}_{timestamp}",
                source_layer=DataLayer.SILVER,
                target_layer=DataLayer.GOLD,
                transformation_id="aggregate_business",
                input_datasets=silver_paths,
                output_dataset=path,
                timestamp=datetime.now(timezone.utc),
                quality_checks_passed=[r["rule_id"] for r in quality_results if r["passed"]],
                quality_checks_failed=[r["rule_id"] for r in quality_results if not r["passed"]]
            )
            self.lineage_tracker.append(lineage)
            
            # Publish lineage event
            self._publish_lineage_event(
                dataset_id=f"{dataset_name}_{timestamp}",
                dataset_name=dataset_name,
                layer=DataLayer.GOLD,
                source_datasets=silver_paths,
                output_path=path,
                schema=df.schema,
                quality_results=quality_results,
                triggered_by="aggregation_pipeline"
            )

            logger.info(f"Created Gold layer dataset: {path}")
            return path
            
        except Exception as e:
            logger.error(f"Error creating Gold layer: {e}")
            raise
            
    def _add_gold_metadata(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Add Gold layer metadata columns"""
        return df.withColumn("_gold_timestamp", current_timestamp()) \
                 .withColumn("_gold_dataset", lit(dataset_name)) \
                 .withColumn("_gold_version", lit("1.0")) \
                 .withColumn("_gold_sla_compliant", lit(True))
                 
    def _apply_quality_rules(self,
                           df: DataFrame,
                           layer: DataLayer) -> tuple[DataFrame, List[Dict[str, Any]]]:
        """Apply quality rules for a specific layer"""
        results = []
        
        for rule in self.quality_rules[layer]:
            if not rule.active:
                continue
                
            try:
                # Apply rule
                original_count = df.count()
                df_checked = rule.check_function(df)
                checked_count = df_checked.count()
                
                # Record result
                passed = (checked_count == original_count) or rule.severity == "warning"
                results.append({
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "passed": passed,
                    "original_count": original_count,
                    "checked_count": checked_count,
                    "dropped_count": original_count - checked_count
                })
                
                if passed or rule.severity == "warning":
                    df = df_checked
                else:
                    logger.error(f"Quality rule {rule.rule_id} failed with severity {rule.severity}")
                    
            except Exception as e:
                logger.error(f"Error applying quality rule {rule.rule_id}: {e}")
                results.append({
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "passed": False,
                    "error": str(e)
                })
                
        return df, results
        
    def _calculate_quality_score(self, quality_results: List[Dict[str, Any]]) -> float:
        """Calculate overall quality score"""
        if not quality_results:
            return 1.0
            
        passed = sum(1 for r in quality_results if r.get("passed", False))
        return passed / len(quality_results)
        
    def _validate_data_types(self, df: DataFrame) -> DataFrame:
        """Validate and fix data types"""
        # Example implementation - customize based on your needs
        for field in df.schema.fields:
            if "timestamp" in field.name.lower() and field.dataType != TimestampType():
                df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
        return df
        
    def _check_completeness(self, df: DataFrame) -> DataFrame:
        """Check data completeness"""
        # Example implementation - check for nulls in required fields
        required_fields = [f.name for f in df.schema.fields if not f.name.startswith("_")]
        
        for field in required_fields:
            df = df.filter(col(field).isNotNull())
            
        return df
        
    def _write_delta(self,
                    df: DataFrame,
                    path: str,
                    partition_by: Optional[List[str]] = None,
                    mode: str = "append",
                    optimize: bool = False):
        """Write DataFrame in Delta format"""
        full_path = f"s3a://{self.bucket_name}/{path}"
        
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            
        writer.format("delta").save(full_path)
        
        if optimize:
            delta_table = DeltaTable.forPath(self.spark, full_path)
            delta_table.optimize().executeCompaction()
            
    def _write_parquet(self,
                      df: DataFrame,
                      path: str,
                      partition_by: Optional[List[str]] = None,
                      mode: str = "append",
                      coalesce: bool = False):
        """Write DataFrame in Parquet format"""
        full_path = f"s3a://{self.bucket_name}/{path}"
        
        if coalesce:
            df = df.coalesce(1)  # Single file for Gold layer
            
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            
        writer.parquet(full_path)
        
    def _read_data(self, path: str) -> DataFrame:
        """Read data from storage"""
        full_path = f"s3a://{self.bucket_name}/{path}"
        
        if self.delta_enabled and "delta" in path:
            return self.spark.read.format("delta").load(full_path)
        else:
            return self.spark.read.parquet(full_path)
            
    def _register_schema(self, dataset_name: str, schema: StructType):
        """Register schema evolution"""
        if dataset_name not in self.schema_registry:
            self.schema_registry[dataset_name] = []
            
        # Check for schema changes
        if self.schema_registry[dataset_name]:
            last_schema = self.schema_registry[dataset_name][-1].schema
            changes = self._detect_schema_changes(last_schema, schema)
            version = self.schema_registry[dataset_name][-1].version + 1
        else:
            changes = []
            version = 1
            
        evolution = SchemaEvolution(
            dataset_id=dataset_name,
            version=version,
            schema=schema,
            timestamp=datetime.now(timezone.utc),
            changes=changes,
            backward_compatible=self._is_backward_compatible(changes)
        )
        
        self.schema_registry[dataset_name].append(evolution)
        
    def _detect_schema_changes(self,
                              old_schema: StructType,
                              new_schema: StructType) -> List[Dict[str, Any]]:
        """Detect changes between schemas"""
        changes = []
        
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Check for added fields
        for name, field in new_fields.items():
            if name not in old_fields:
                changes.append({
                    "type": "add_field",
                    "field": name,
                    "dataType": str(field.dataType)
                })
                
        # Check for removed fields
        for name in old_fields:
            if name not in new_fields:
                changes.append({
                    "type": "remove_field",
                    "field": name
                })
                
        # Check for type changes
        for name, field in new_fields.items():
            if name in old_fields and field.dataType != old_fields[name].dataType:
                changes.append({
                    "type": "change_type",
                    "field": name,
                    "old_type": str(old_fields[name].dataType),
                    "new_type": str(field.dataType)
                })
                
        return changes
        
    def _is_backward_compatible(self, changes: List[Dict[str, Any]]) -> bool:
        """Check if schema changes are backward compatible"""
        for change in changes:
            # Removing fields or changing types breaks compatibility
            if change["type"] in ["remove_field", "change_type"]:
                return False
        return True
        
    def get_lineage(self, dataset_id: str) -> List[DataLineage]:
        """Get lineage information for a dataset"""
        return [l for l in self.lineage_tracker if dataset_id in l.output_dataset]
        
    def get_schema_history(self, dataset_name: str) -> List[SchemaEvolution]:
        """Get schema evolution history for a dataset"""
        return self.schema_registry.get(dataset_name, [])
        
    def compact_delta_tables(self, layer: DataLayer):
        """Compact Delta tables in a layer for better performance"""
        if not self.delta_enabled:
            logger.warning("Delta Lake not enabled")
            return
            
        layer_path = f"s3a://{self.bucket_name}/{layer.value}"
        
        # List all datasets in the layer
        datasets = self.spark.read.format("delta").load(layer_path).select("_dataset").distinct().collect()
        
        for row in datasets:
            dataset = row["_dataset"]
            path = f"{layer_path}/{dataset}"
            
            try:
                delta_table = DeltaTable.forPath(self.spark, path)
                
                # Optimize
                delta_table.optimize().executeCompaction()
                
                # Vacuum old files
                delta_table.vacuum(168)  # 7 days
                
                logger.info(f"Compacted Delta table: {path}")
                
            except Exception as e:
                logger.error(f"Error compacting {path}: {e}")
                
    def generate_data_catalog(self) -> Dict[str, Any]:
        """Generate a data catalog for all datasets"""
        catalog = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "layers": {}
        }
        
        for layer in DataLayer:
            catalog["layers"][layer.value] = {
                "datasets": [],
                "quality_rules": [
                    {
                        "id": rule.rule_id,
                        "name": rule.name,
                        "description": rule.description,
                        "severity": rule.severity
                    }
                    for rule in self.quality_rules[layer]
                ]
            }
            
            # List datasets in layer
            layer_path = f"s3a://{self.bucket_name}/{layer.value}"
            try:
                objects = self.minio.list_objects(self.bucket_name, prefix=f"{layer.value}/", recursive=False)
                
                for obj in objects:
                    if obj.is_dir:
                        dataset_name = obj.object_name.split("/")[1]
                        
                        # Get latest version
                        versions = self.minio.list_objects(
                            self.bucket_name,
                            prefix=f"{layer.value}/{dataset_name}/",
                            recursive=False
                        )
                        
                        latest_version = None
                        for v in versions:
                            if v.is_dir:
                                latest_version = v.object_name
                                
                        if latest_version:
                            # Get schema if registered
                            schema_info = None
                            if dataset_name in self.schema_registry:
                                latest_schema = self.schema_registry[dataset_name][-1]
                                schema_info = {
                                    "version": latest_schema.version,
                                    "fields": [
                                        {
                                            "name": f.name,
                                            "type": str(f.dataType),
                                            "nullable": f.nullable
                                        }
                                        for f in latest_schema.schema.fields
                                    ]
                                }
                                
                            catalog["layers"][layer.value]["datasets"].append({
                                "name": dataset_name,
                                "latest_version": latest_version,
                                "schema": schema_info
                            })
                            
            except Exception as e:
                logger.error(f"Error listing datasets in {layer.value}: {e}")
                
        return catalog 

    def _publish_lineage_event(self, dataset_id: str, dataset_name: str, layer: DataLayer, source_datasets: List[str],
                               output_path: str, schema: StructType, quality_results: List[Dict], triggered_by: str):
        # Publish lineage event
        lineage_event = DatasetLineageEvent(
            event_id=str(uuid.uuid4()),
            event_timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            source_service="data-lake-service",
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            layer=layer.value,
            source_datasets=source_datasets,
            output_path=output_path,
            schema={
                "fields": [
                    {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                    for f in schema.fields
                ]
            },
            quality_report={
                "overall_score": self._calculate_quality_score(quality_results),
                "metrics": [
                    {
                        "name": r["name"],
                        "dimension": "unknown",  # This could be improved
                        "value": r.get("checked_count", 0) / r.get("original_count", 1),
                        "passed": r["passed"]
                    }
                    for r in quality_results
                ]
            },
            is_gold_layer=layer == DataLayer.GOLD,
            triggered_by=triggered_by
        )
        self.event_publisher.publish(
            topic="persistent://platformq/public/dataset-lineage",
            schema_class=DatasetLineageEvent,
            data=lineage_event
        )

        # Publish search event
        search_event = IndexableEntityEvent(
            event_id=str(uuid.uuid4()),
            event_timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            source_service="data-lake-service",
            entity_id=dataset_id,
            entity_type="Dataset",
            event_type="CREATED", # Or UPDATED, depending on the logic
            data={
                "name": dataset_name,
                "description": f"{layer.value} layer dataset",
                "layer": layer.value,
                "path": output_path,
                "quality_score": self._calculate_quality_score(quality_results),
                "tags": [layer.value.lower(), "dataset"]
            }
        )
        self.event_publisher.publish(
            topic="persistent://platformq/public/search-indexing",
            schema_class=IndexableEntityEvent,
            data=search_event
        ) 