"""
Layer Processing Pipeline

Handles processing and transformation of data through the medallion architecture layers:
Bronze -> Silver -> Gold
"""

import logging
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor
import asyncio

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType
from delta import DeltaTable
import pandas as pd

from ..models.medallion_architecture import MedallionArchitecture, DataLayer
from ..quality.data_quality_framework import DataQualityFramework, QualityProfile
from ..jobs import JobRepository

logger = logging.getLogger(__name__)


@dataclass
class ProcessingConfig:
    """Configuration for layer processing"""
    source_layer: DataLayer
    target_layer: DataLayer
    dataset_name: str
    transformations: List[Dict[str, Any]]
    quality_threshold: float = 0.90
    partition_strategy: Optional[Dict[str, Any]] = None
    optimization_config: Optional[Dict[str, Any]] = None
    schedule: Optional[str] = None  # cron expression


@dataclass
class ProcessingJob:
    """Processing job tracking"""
    job_id: str
    config: ProcessingConfig
    status: str  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    source_paths: List[str] = None
    target_path: Optional[str] = None
    quality_score: Optional[float] = None
    records_processed: int = 0
    errors: List[str] = None


class TransformationRegistry:
    """Registry of reusable transformations"""
    
    def __init__(self):
        self.transformations = {}
        self._register_default_transformations()
        
    def _register_default_transformations(self):
        """Register default transformations"""
        # Data cleansing transformations
        self.register("remove_duplicates", self._remove_duplicates)
        self.register("handle_nulls", self._handle_nulls)
        self.register("standardize_strings", self._standardize_strings)
        self.register("fix_timestamps", self._fix_timestamps)
        
        # Data enrichment transformations
        self.register("add_derived_columns", self._add_derived_columns)
        self.register("geocode_addresses", self._geocode_addresses)
        self.register("currency_conversion", self._currency_conversion)
        
        # Aggregation transformations
        self.register("time_window_aggregation", self._time_window_aggregation)
        self.register("dimensional_rollup", self._dimensional_rollup)
        self.register("statistical_summary", self._statistical_summary)
        
        # Data quality transformations
        self.register("outlier_capping", self._outlier_capping)
        self.register("data_validation", self._data_validation)
        self.register("referential_integrity", self._referential_integrity)
        
    def register(self, name: str, func: Callable[[DataFrame, Dict[str, Any]], DataFrame]):
        """Register a transformation function"""
        self.transformations[name] = func
        
    def get(self, name: str) -> Optional[Callable]:
        """Get a transformation function"""
        return self.transformations.get(name)
        
    # Default transformation implementations
    
    def _remove_duplicates(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Remove duplicate records"""
        key_columns = params.get("key_columns", df.columns)
        keep = params.get("keep", "first")  # first, last, none
        
        if keep == "first":
            window = Window.partitionBy(*key_columns).orderBy(F.lit(1))
            return df.withColumn("_rn", F.row_number().over(window)) \
                    .filter(F.col("_rn") == 1) \
                    .drop("_rn")
        elif keep == "last":
            window = Window.partitionBy(*key_columns).orderBy(F.lit(1).desc())
            return df.withColumn("_rn", F.row_number().over(window)) \
                    .filter(F.col("_rn") == 1) \
                    .drop("_rn")
        else:
            return df.dropDuplicates(key_columns)
            
    def _handle_nulls(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Handle null values"""
        strategy = params.get("strategy", "drop")  # drop, fill, interpolate
        columns = params.get("columns", df.columns)
        
        if strategy == "drop":
            return df.dropna(subset=columns)
        elif strategy == "fill":
            fill_values = params.get("fill_values", {})
            for col in columns:
                if col in fill_values:
                    df = df.fillna({col: fill_values[col]})
                else:
                    # Default fill based on data type
                    dtype = dict(df.dtypes)[col]
                    if dtype in ["int", "bigint", "float", "double"]:
                        df = df.fillna({col: 0})
                    else:
                        df = df.fillna({col: ""})
        elif strategy == "interpolate":
            # Time-based interpolation for numeric columns
            time_column = params.get("time_column", "timestamp")
            for col in columns:
                dtype = dict(df.dtypes)[col]
                if dtype in ["int", "bigint", "float", "double"]:
                    window = Window.orderBy(time_column).rowsBetween(-1, 1)
                    df = df.withColumn(
                        col,
                        F.when(F.col(col).isNull(), F.avg(col).over(window))
                         .otherwise(F.col(col))
                    )
                    
        return df
        
    def _standardize_strings(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Standardize string values"""
        columns = params.get("columns", [col for col, dtype in df.dtypes if dtype == "string"])
        
        for col in columns:
            if col in df.columns:
                # Trim whitespace
                df = df.withColumn(col, F.trim(F.col(col)))
                
                # Handle case
                case_strategy = params.get("case", "lower")
                if case_strategy == "lower":
                    df = df.withColumn(col, F.lower(F.col(col)))
                elif case_strategy == "upper":
                    df = df.withColumn(col, F.upper(F.col(col)))
                elif case_strategy == "title":
                    df = df.withColumn(col, F.initcap(F.col(col)))
                    
                # Replace special characters if specified
                if params.get("remove_special_chars", False):
                    df = df.withColumn(col, F.regexp_replace(F.col(col), "[^a-zA-Z0-9\\s]", ""))
                    
        return df
        
    def _fix_timestamps(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Fix and standardize timestamps"""
        columns = params.get("columns", [col for col, dtype in df.dtypes if "timestamp" in col.lower()])
        target_timezone = params.get("target_timezone", "UTC")
        
        for col in columns:
            if col in df.columns:
                # Convert to timestamp if string
                df = df.withColumn(col, F.to_timestamp(F.col(col)))
                
                # Convert timezone
                if target_timezone:
                    df = df.withColumn(col, F.from_utc_timestamp(F.col(col), target_timezone))
                    
                # Handle outliers (e.g., future dates)
                max_date = params.get("max_date", datetime.now(timezone.utc))
                min_date = params.get("min_date", datetime(2000, 1, 1, tzinfo=timezone.utc))
                
                df = df.withColumn(
                    col,
                    F.when(F.col(col) > F.lit(max_date), F.lit(max_date))
                     .when(F.col(col) < F.lit(min_date), F.lit(min_date))
                     .otherwise(F.col(col))
                )
                
        return df
        
    def _add_derived_columns(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Add derived columns based on expressions"""
        derivations = params.get("derivations", {})
        
        for new_col, expression in derivations.items():
            if isinstance(expression, str):
                # SQL expression
                df = df.withColumn(new_col, F.expr(expression))
            elif isinstance(expression, dict):
                # Complex derivation
                operation = expression.get("operation")
                
                if operation == "concat":
                    columns = expression.get("columns", [])
                    separator = expression.get("separator", "")
                    df = df.withColumn(new_col, F.concat_ws(separator, *columns))
                    
                elif operation == "date_diff":
                    start_col = expression.get("start_column")
                    end_col = expression.get("end_column")
                    unit = expression.get("unit", "days")
                    
                    if unit == "days":
                        df = df.withColumn(new_col, F.datediff(F.col(end_col), F.col(start_col)))
                    elif unit == "hours":
                        df = df.withColumn(
                            new_col,
                            (F.unix_timestamp(F.col(end_col)) - F.unix_timestamp(F.col(start_col))) / 3600
                        )
                        
                elif operation == "bucket":
                    source_col = expression.get("source_column")
                    buckets = expression.get("buckets", [])
                    
                    when_expr = F.when(F.col(source_col) < buckets[0], f"< {buckets[0]}")
                    for i in range(len(buckets) - 1):
                        when_expr = when_expr.when(
                            (F.col(source_col) >= buckets[i]) & (F.col(source_col) < buckets[i + 1]),
                            f"{buckets[i]}-{buckets[i + 1]}"
                        )
                    when_expr = when_expr.otherwise(f">= {buckets[-1]}")
                    
                    df = df.withColumn(new_col, when_expr)
                    
        return df
        
    def _geocode_addresses(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Geocode addresses (placeholder - would integrate with geocoding service)"""
        address_column = params.get("address_column")
        
        if address_column and address_column in df.columns:
            # In production, this would call a geocoding service
            df = df.withColumn("latitude", F.lit(0.0))
            df = df.withColumn("longitude", F.lit(0.0))
            df = df.withColumn("geocoding_confidence", F.lit(0.0))
            
        return df
        
    def _currency_conversion(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Convert currency values"""
        amount_column = params.get("amount_column")
        currency_column = params.get("currency_column")
        target_currency = params.get("target_currency", "USD")
        
        if amount_column and amount_column in df.columns:
            # In production, this would use real exchange rates
            exchange_rates = params.get("exchange_rates", {
                "USD": 1.0,
                "EUR": 1.18,
                "GBP": 1.38,
                "JPY": 0.009
            })
            
            # Build conversion expression
            when_expr = F.col(amount_column)
            for currency, rate in exchange_rates.items():
                when_expr = F.when(
                    F.col(currency_column) == currency,
                    F.col(amount_column) * rate / exchange_rates[target_currency]
                ).otherwise(when_expr)
                
            df = df.withColumn(f"{amount_column}_{target_currency}", when_expr)
            
        return df
        
    def _time_window_aggregation(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Perform time window aggregations"""
        time_column = params.get("time_column", "timestamp")
        window_duration = params.get("window_duration", "1 hour")
        group_by = params.get("group_by", [])
        aggregations = params.get("aggregations", {})
        
        # Convert to time windows
        df = df.withColumn("time_window", F.window(F.col(time_column), window_duration))
        
        # Build aggregation expressions
        agg_exprs = []
        for col, agg_funcs in aggregations.items():
            if isinstance(agg_funcs, str):
                agg_funcs = [agg_funcs]
                
            for func in agg_funcs:
                if func == "sum":
                    agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
                elif func == "avg":
                    agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
                elif func == "max":
                    agg_exprs.append(F.max(col).alias(f"{col}_max"))
                elif func == "min":
                    agg_exprs.append(F.min(col).alias(f"{col}_min"))
                elif func == "count":
                    agg_exprs.append(F.count(col).alias(f"{col}_count"))
                elif func == "stddev":
                    agg_exprs.append(F.stddev(col).alias(f"{col}_stddev"))
                    
        # Perform aggregation
        group_cols = ["time_window"] + group_by
        result = df.groupBy(*group_cols).agg(*agg_exprs)
        
        # Extract window start and end
        result = result.withColumn("window_start", F.col("time_window.start")) \
                      .withColumn("window_end", F.col("time_window.end")) \
                      .drop("time_window")
                      
        return result
        
    def _dimensional_rollup(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Perform dimensional rollup aggregations"""
        dimensions = params.get("dimensions", [])
        measures = params.get("measures", {})
        
        # Build aggregation expressions
        agg_exprs = []
        for measure, agg_func in measures.items():
            if agg_func == "sum":
                agg_exprs.append(F.sum(measure).alias(f"{measure}_total"))
            elif agg_func == "avg":
                agg_exprs.append(F.avg(measure).alias(f"{measure}_avg"))
            elif agg_func == "count":
                agg_exprs.append(F.countDistinct(measure).alias(f"{measure}_distinct"))
                
        # Perform rollup
        result = df.rollup(*dimensions).agg(*agg_exprs)
        
        # Add rollup level indicator
        result = result.withColumn(
            "rollup_level",
            F.when(F.col(dimensions[0]).isNull(), "grand_total")
             .otherwise(F.concat_ws("_", *[F.when(F.col(d).isNotNull(), d).otherwise("ALL") for d in dimensions]))
        )
        
        return result
        
    def _statistical_summary(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Generate statistical summary"""
        numeric_columns = params.get("columns", [col for col, dtype in df.dtypes 
                                               if dtype in ["int", "bigint", "float", "double"]])
        group_by = params.get("group_by", [])
        
        # Build summary expressions
        summary_exprs = []
        for col in numeric_columns:
            if col in df.columns:
                summary_exprs.extend([
                    F.count(col).alias(f"{col}_count"),
                    F.mean(col).alias(f"{col}_mean"),
                    F.stddev(col).alias(f"{col}_stddev"),
                    F.min(col).alias(f"{col}_min"),
                    F.max(col).alias(f"{col}_max"),
                    F.expr(f"percentile_approx({col}, 0.25)").alias(f"{col}_q1"),
                    F.expr(f"percentile_approx({col}, 0.50)").alias(f"{col}_median"),
                    F.expr(f"percentile_approx({col}, 0.75)").alias(f"{col}_q3")
                ])
                
        if group_by:
            result = df.groupBy(*group_by).agg(*summary_exprs)
        else:
            result = df.agg(*summary_exprs)
            
        return result
        
    def _outlier_capping(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Cap outliers using statistical methods"""
        columns = params.get("columns", [])
        method = params.get("method", "iqr")  # iqr, zscore, percentile
        
        for col in columns:
            if col in df.columns:
                if method == "iqr":
                    # Calculate Q1, Q3, and IQR
                    quantiles = df.select(
                        F.expr(f"percentile_approx({col}, 0.25)").alias("q1"),
                        F.expr(f"percentile_approx({col}, 0.75)").alias("q3")
                    ).collect()[0]
                    
                    q1 = quantiles["q1"]
                    q3 = quantiles["q3"]
                    iqr = q3 - q1
                    
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    df = df.withColumn(
                        col,
                        F.when(F.col(col) < lower_bound, lower_bound)
                         .when(F.col(col) > upper_bound, upper_bound)
                         .otherwise(F.col(col))
                    )
                    
                elif method == "zscore":
                    threshold = params.get("zscore_threshold", 3)
                    
                    stats = df.select(
                        F.mean(col).alias("mean"),
                        F.stddev(col).alias("stddev")
                    ).collect()[0]
                    
                    if stats["stddev"] and stats["stddev"] > 0:
                        df = df.withColumn(
                            f"{col}_zscore",
                            (F.col(col) - stats["mean"]) / stats["stddev"]
                        )
                        
                        df = df.withColumn(
                            col,
                            F.when(F.abs(F.col(f"{col}_zscore")) > threshold, stats["mean"])
                             .otherwise(F.col(col))
                        ).drop(f"{col}_zscore")
                        
                elif method == "percentile":
                    lower_pct = params.get("lower_percentile", 0.01)
                    upper_pct = params.get("upper_percentile", 0.99)
                    
                    bounds = df.select(
                        F.expr(f"percentile_approx({col}, {lower_pct})").alias("lower"),
                        F.expr(f"percentile_approx({col}, {upper_pct})").alias("upper")
                    ).collect()[0]
                    
                    df = df.withColumn(
                        col,
                        F.when(F.col(col) < bounds["lower"], bounds["lower"])
                         .when(F.col(col) > bounds["upper"], bounds["upper"])
                         .otherwise(F.col(col))
                    )
                    
        return df
        
    def _data_validation(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Apply data validation rules"""
        validations = params.get("validations", [])
        
        for validation in validations:
            column = validation.get("column")
            rule_type = validation.get("type")
            
            if rule_type == "regex":
                pattern = validation.get("pattern")
                df = df.filter(F.col(column).rlike(pattern))
                
            elif rule_type == "range":
                min_val = validation.get("min")
                max_val = validation.get("max")
                df = df.filter((F.col(column) >= min_val) & (F.col(column) <= max_val))
                
            elif rule_type == "values":
                allowed_values = validation.get("allowed_values", [])
                df = df.filter(F.col(column).isin(allowed_values))
                
            elif rule_type == "not_null":
                df = df.filter(F.col(column).isNotNull())
                
        return df
        
    def _referential_integrity(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Check referential integrity"""
        reference_df = params.get("reference_df")
        foreign_key = params.get("foreign_key")
        reference_key = params.get("reference_key")
        action = params.get("action", "filter")  # filter, flag, null
        
        if reference_df and foreign_key and reference_key:
            # Get valid reference values
            valid_refs = reference_df.select(reference_key).distinct()
            
            if action == "filter":
                # Keep only records with valid references
                df = df.join(valid_refs, df[foreign_key] == valid_refs[reference_key], "inner")
            elif action == "flag":
                # Add flag for invalid references
                df = df.join(
                    valid_refs,
                    df[foreign_key] == valid_refs[reference_key],
                    "left"
                ).withColumn(
                    f"{foreign_key}_valid",
                    F.col(reference_key).isNotNull()
                ).drop(reference_key)
            elif action == "null":
                # Set invalid references to null
                df = df.join(
                    valid_refs,
                    df[foreign_key] == valid_refs[reference_key],
                    "left"
                ).withColumn(
                    foreign_key,
                    F.when(F.col(reference_key).isNotNull(), F.col(foreign_key)).otherwise(None)
                ).drop(reference_key)
                
        return df


class LayerProcessingPipeline:
    """Main processing pipeline for medallion architecture"""
    
    def __init__(self,
                 spark: SparkSession,
                 medallion: MedallionArchitecture,
                 quality_framework: DataQualityFramework,
                 job_repository: JobRepository):
        self.spark = spark
        self.medallion = medallion
        self.quality_framework = quality_framework
        self.transformation_registry = TransformationRegistry()
        self.job_repository = job_repository
        
    async def start_processing(self, config: ProcessingConfig) -> str:
        """Start a new processing job"""
        job_id = hashlib.md5(
            f"{config.source_layer.value}_{config.target_layer.value}_{config.dataset_name}_{datetime.now()}".encode()
        ).hexdigest()
        
        job = ProcessingJob(
            job_id=job_id,
            config=config,
            status="pending",
            errors=[]
        )
        
        self.job_repository.add_processing_job(job)
        
        # Start processing in background
        asyncio.create_task(self._run_processing(job))
        
        logger.info(f"Started processing job {job_id} for {config.dataset_name}")
        return job_id
        
    async def _run_processing(self, job: ProcessingJob):
        """Run the processing job"""
        try:
            job.status = "running"
            job.started_at = datetime.now(timezone.utc)
            
            if job.config.source_layer == DataLayer.BRONZE and job.config.target_layer == DataLayer.SILVER:
                await self._process_bronze_to_silver(job)
            elif job.config.source_layer == DataLayer.SILVER and job.config.target_layer == DataLayer.GOLD:
                await self._process_silver_to_gold(job)
            else:
                raise ValueError(f"Unsupported layer transition: {job.config.source_layer} -> {job.config.target_layer}")
                
            job.status = "completed"
            
        except Exception as e:
            logger.error(f"Processing job {job.job_id} failed: {e}")
            job.status = "failed"
            job.errors.append(str(e))
        finally:
            job.completed_at = datetime.now(timezone.utc)
            
    async def _process_bronze_to_silver(self, job: ProcessingJob):
        """Process data from Bronze to Silver layer"""
        # Get latest Bronze data
        bronze_paths = self._get_latest_paths(DataLayer.BRONZE, job.config.dataset_name)
        if not bronze_paths:
            raise ValueError(f"No Bronze data found for {job.config.dataset_name}")
            
        job.source_paths = bronze_paths
        
        # Read Bronze data
        df = self._read_and_union_data(bronze_paths)
        initial_count = df.count()
        job.records_processed = initial_count
        
        # Apply transformations
        for transform_config in job.config.transformations:
            transform_name = transform_config.get("name")
            transform_params = transform_config.get("params", {})
            
            transform_func = self.transformation_registry.get(transform_name)
            if transform_func:
                logger.info(f"Applying transformation: {transform_name}")
                df = transform_func(df, transform_params)
            else:
                logger.warning(f"Unknown transformation: {transform_name}")
                
        # Run quality checks
        quality_profile = self.quality_framework.profile_dataset(
            df,
            f"{job.config.dataset_name}_silver",
            rules=None  # Apply all rules
        )
        
        job.quality_score = quality_profile.overall_score
        
        # Check quality threshold
        if quality_profile.overall_score < job.config.quality_threshold:
            raise ValueError(
                f"Data quality score {quality_profile.overall_score:.2f} "
                f"below threshold {job.config.quality_threshold}"
            )
            
        # Add Silver metadata
        df = self._add_processing_metadata(df, job)
        
        # Optimize partitioning if configured
        if job.config.partition_strategy:
            df = self._apply_partitioning(df, job.config.partition_strategy)
            
        # Write to Silver layer
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        silver_path = f"{DataLayer.SILVER.value}/{job.config.dataset_name}/{timestamp}"
        
        self._write_data(df, silver_path, job.config.optimization_config)
        job.target_path = silver_path
        
        logger.info(f"Processed {initial_count} records from Bronze to Silver: {silver_path}")
        
    async def _process_silver_to_gold(self, job: ProcessingJob):
        """Process data from Silver to Gold layer"""
        # Get Silver data based on configuration
        silver_paths = []
        
        if "lookback_days" in job.config.transformations[0].get("params", {}):
            # Get data for specific time range
            lookback_days = job.config.transformations[0]["params"]["lookback_days"]
            silver_paths = self._get_paths_by_date_range(
                DataLayer.SILVER,
                job.config.dataset_name,
                lookback_days
            )
        else:
            # Get latest Silver data
            silver_paths = self._get_latest_paths(DataLayer.SILVER, job.config.dataset_name)
            
        if not silver_paths:
            raise ValueError(f"No Silver data found for {job.config.dataset_name}")
            
        job.source_paths = silver_paths
        
        # Read Silver data
        df = self._read_and_union_data(silver_paths)
        initial_count = df.count()
        job.records_processed = initial_count
        
        # Apply aggregations and business logic
        for transform_config in job.config.transformations:
            transform_name = transform_config.get("name")
            transform_params = transform_config.get("params", {})
            
            transform_func = self.transformation_registry.get(transform_name)
            if transform_func:
                logger.info(f"Applying transformation: {transform_name}")
                df = transform_func(df, transform_params)
            else:
                logger.warning(f"Unknown transformation: {transform_name}")
                
        # Run quality checks
        quality_profile = self.quality_framework.profile_dataset(
            df,
            f"{job.config.dataset_name}_gold"
        )
        
        job.quality_score = quality_profile.overall_score
        
        # Add Gold metadata
        df = self._add_processing_metadata(df, job)
        
        # Optimize for query performance
        if job.config.optimization_config:
            df = self._optimize_for_queries(df, job.config.optimization_config)
            
        # Write to Gold layer
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        gold_path = f"{DataLayer.GOLD.value}/{job.config.dataset_name}/{timestamp}"
        
        self._write_data(df, gold_path, job.config.optimization_config, mode="overwrite")
        job.target_path = gold_path
        
        logger.info(f"Created Gold dataset from {initial_count} Silver records: {gold_path}")
        
    def _get_latest_paths(self, layer: DataLayer, dataset_name: str, limit: int = 1) -> List[str]:
        """Get latest data paths for a dataset"""
        base_path = f"s3a://{self.medallion.bucket_name}/{layer.value}/{dataset_name}"
        
        try:
            # List directories and sort by timestamp
            paths = []
            objects = self.medallion.minio.list_objects(
                self.medallion.bucket_name,
                prefix=f"{layer.value}/{dataset_name}/",
                recursive=False
            )
            
            for obj in objects:
                if obj.is_dir:
                    paths.append(obj.object_name.rstrip("/"))
                    
            # Sort by timestamp (assuming YYYYMMDD_HHMMSS format)
            paths.sort(reverse=True)
            
            return paths[:limit]
            
        except Exception as e:
            logger.error(f"Error listing paths: {e}")
            return []
            
    def _get_paths_by_date_range(self,
                                layer: DataLayer,
                                dataset_name: str,
                                lookback_days: int) -> List[str]:
        """Get data paths within a date range"""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        cutoff_str = cutoff_date.strftime("%Y%m%d")
        
        paths = []
        objects = self.medallion.minio.list_objects(
            self.medallion.bucket_name,
            prefix=f"{layer.value}/{dataset_name}/",
            recursive=False
        )
        
        for obj in objects:
            if obj.is_dir:
                # Extract timestamp from path
                parts = obj.object_name.rstrip("/").split("/")
                if len(parts) >= 3:
                    timestamp_str = parts[-1]
                    if timestamp_str >= cutoff_str:
                        paths.append(obj.object_name.rstrip("/"))
                        
        return sorted(paths)
        
    def _read_and_union_data(self, paths: List[str]) -> DataFrame:
        """Read and union data from multiple paths"""
        dfs = []
        
        for path in paths:
            full_path = f"s3a://{self.medallion.bucket_name}/{path}"
            
            if self.medallion.delta_enabled:
                df = self.spark.read.format("delta").load(full_path)
            else:
                df = self.spark.read.parquet(full_path)
                
            dfs.append(df)
            
        # Union all DataFrames
        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)
            
        return result
        
    def _add_processing_metadata(self, df: DataFrame, job: ProcessingJob) -> DataFrame:
        """Add processing metadata to DataFrame"""
        return df.withColumn("_processing_job_id", F.lit(job.job_id)) \
                 .withColumn("_processing_timestamp", F.lit(datetime.now(timezone.utc).isoformat())) \
                 .withColumn("_quality_score", F.lit(job.quality_score))
                 
    def _apply_partitioning(self, df: DataFrame, strategy: Dict[str, Any]) -> DataFrame:
        """Apply partitioning strategy"""
        method = strategy.get("method", "hash")
        columns = strategy.get("columns", [])
        num_partitions = strategy.get("num_partitions", 200)
        
        if method == "hash" and columns:
            df = df.repartition(num_partitions, *columns)
        elif method == "range" and columns:
            df = df.repartitionByRange(num_partitions, *columns)
        elif method == "coalesce":
            df = df.coalesce(num_partitions)
            
        return df
        
    def _optimize_for_queries(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """Optimize DataFrame for query performance"""
        # Sort by commonly queried columns
        sort_columns = config.get("sort_columns", [])
        if sort_columns:
            df = df.sortWithinPartitions(*sort_columns)
            
        # Coalesce to optimal file size
        target_file_size_mb = config.get("target_file_size_mb", 128)
        
        # Estimate current partition size
        total_size = df.count() * 1024  # Rough estimate
        optimal_partitions = max(1, int(total_size / (target_file_size_mb * 1024 * 1024)))
        
        df = df.coalesce(optimal_partitions)
        
        return df
        
    def _write_data(self,
                   df: DataFrame,
                   path: str,
                   optimization_config: Optional[Dict[str, Any]],
                   mode: str = "append"):
        """Write DataFrame to storage"""
        full_path = f"s3a://{self.medallion.bucket_name}/{path}"
        
        writer = df.write.mode(mode)
        
        # Apply optimization settings
        if optimization_config:
            if "compression" in optimization_config:
                writer = writer.option("compression", optimization_config["compression"])
                
        if self.medallion.delta_enabled:
            writer.format("delta").save(full_path)
            
            # Optimize Delta table if requested
            if optimization_config and optimization_config.get("optimize_delta", False):
                delta_table = DeltaTable.forPath(self.spark, full_path)
                delta_table.optimize().executeCompaction()
        else:
            writer.parquet(full_path)
            
    def get_job_status(self, job_id: str) -> Optional[ProcessingJob]:
        """Get status of a processing job"""
        return self.job_repository.get_processing_job(job_id)
        
    def list_active_jobs(self) -> List[ProcessingJob]:
        """List all active processing jobs"""
        return self.job_repository.list_active_processing_jobs()
        
    def create_processing_schedule(self,
                                 config: ProcessingConfig,
                                 start_time: datetime) -> Dict[str, Any]:
        """Create a processing schedule for automated runs"""
        schedule = {
            "config": config,
            "start_time": start_time.isoformat(),
            "cron_expression": config.schedule,
            "enabled": True,
            "last_run": None,
            "next_run": self._calculate_next_run(config.schedule, start_time)
        }
        
        # In production, this would be persisted to a scheduler
        return schedule
        
    def _calculate_next_run(self, cron_expression: str, after_time: datetime) -> str:
        """Calculate next run time based on cron expression"""
        # Simplified implementation - in production use croniter or similar
        if cron_expression == "0 * * * *":  # Hourly
            next_run = after_time + timedelta(hours=1)
        elif cron_expression == "0 0 * * *":  # Daily
            next_run = after_time + timedelta(days=1)
        else:
            next_run = after_time + timedelta(hours=1)  # Default to hourly
            
        return next_run.isoformat() 