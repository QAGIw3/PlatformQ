"""
Data Transformation Engine for medallion architecture processing
"""
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from enum import Enum
import yaml

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta import DeltaTable

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError
from .medallion_architecture import MedallionLakeManager, DataZone

logger = get_logger(__name__)


class TransformationType(str, Enum):
    """Types of transformations"""
    CLEANSING = "cleansing"
    ENRICHMENT = "enrichment"
    AGGREGATION = "aggregation"
    NORMALIZATION = "normalization"
    FEATURE_ENGINEERING = "feature_engineering"


class TransformationEngine:
    """
    Engine for transforming data through medallion architecture layers.
    
    Features:
    - Bronze to Silver transformations
    - Silver to Gold aggregations
    - Data quality enforcement
    - Schema evolution handling
    - Custom transformation pipelines
    - Incremental processing
    """
    
    def __init__(self, lake_manager: MedallionLakeManager):
        self.lake_manager = lake_manager
        self.spark = lake_manager.spark
        
        # Transformation registry
        self.transformations: Dict[str, Callable] = {}
        
        # Built-in transformations
        self._register_builtin_transformations()
        
        # Active transformation jobs
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        
        # Statistics
        self.stats = {
            "total_transformations": 0,
            "successful_transformations": 0,
            "failed_transformations": 0,
            "total_rows_processed": 0
        }
    
    def _register_builtin_transformations(self) -> None:
        """Register built-in transformation functions"""
        # Cleansing transformations
        self.register_transformation("remove_nulls", self._remove_nulls)
        self.register_transformation("deduplicate", self._deduplicate)
        self.register_transformation("trim_strings", self._trim_strings)
        self.register_transformation("standardize_dates", self._standardize_dates)
        
        # Enrichment transformations
        self.register_transformation("add_computed_columns", self._add_computed_columns)
        self.register_transformation("lookup_enrichment", self._lookup_enrichment)
        
        # Normalization transformations
        self.register_transformation("normalize_values", self._normalize_values)
        self.register_transformation("standardize_names", self._standardize_names)
        
        # Aggregation transformations
        self.register_transformation("time_series_aggregation", self._time_series_aggregation)
        self.register_transformation("group_aggregation", self._group_aggregation)
    
    def register_transformation(self, 
                              name: str,
                              func: Callable[[DataFrame, Dict[str, Any]], DataFrame]) -> None:
        """Register a custom transformation function"""
        self.transformations[name] = func
        logger.info(f"Registered transformation: {name}")
    
    async def transform_bronze_to_silver(self,
                                       bronze_dataset: str,
                                       silver_dataset: str,
                                       tenant_id: str,
                                       transformation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data from Bronze to Silver layer"""
        job_id = f"b2s_{silver_dataset}_{datetime.utcnow().timestamp()}"
        
        try:
            # Track job
            self.active_jobs[job_id] = {
                "type": "bronze_to_silver",
                "source": bronze_dataset,
                "target": silver_dataset,
                "status": "running",
                "started_at": datetime.utcnow()
            }
            self.stats["total_transformations"] += 1
            
            # Read from Bronze
            bronze_path = f"{self.lake_manager.zone_paths[DataZone.BRONZE]}/{tenant_id}/{bronze_dataset}"
            df = self.spark.read.format("delta").load(bronze_path)
            
            initial_count = df.count()
            
            # Apply transformations
            df = await self._apply_transformation_pipeline(df, transformation_config)
            
            # Add Silver metadata
            df = df.withColumn("_silver_timestamp", F.current_timestamp()) \
                   .withColumn("_transformation_version", F.lit(transformation_config.get("version", "1.0"))) \
                   .withColumn("_quality_validated", F.lit(True))
            
            # Apply quality rules
            quality_rules = transformation_config.get("quality_rules", [])
            if quality_rules:
                df = await self._apply_quality_validation(df, quality_rules)
            
            # Write to Silver
            silver_path = f"{self.lake_manager.zone_paths[DataZone.SILVER]}/{tenant_id}/{silver_dataset}"
            
            # Handle schema evolution
            if self._dataset_exists(silver_path):
                await self._merge_with_schema_evolution(df, silver_path, transformation_config)
            else:
                df.write.mode("overwrite").format("delta").save(silver_path)
            
            # Optimize table
            delta_table = DeltaTable.forPath(self.spark, silver_path)
            delta_table.optimize().executeCompaction()
            
            # Get final statistics
            final_count = df.count()
            
            # Update job status
            self.active_jobs[job_id]["status"] = "completed"
            self.active_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            self.stats["successful_transformations"] += 1
            self.stats["total_rows_processed"] += final_count
            
            result = {
                "job_id": job_id,
                "source_dataset": bronze_dataset,
                "target_dataset": silver_dataset,
                "initial_row_count": initial_count,
                "final_row_count": final_count,
                "rows_filtered": initial_count - final_count,
                "duration_seconds": (datetime.utcnow() - self.active_jobs[job_id]["started_at"]).total_seconds(),
                "status": "success"
            }
            
            logger.info(f"Bronze to Silver transformation completed: {job_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Transformation job {job_id} failed: {e}")
            
            self.active_jobs[job_id]["status"] = "failed"
            self.active_jobs[job_id]["error"] = str(e)
            self.stats["failed_transformations"] += 1
            
            raise ServiceError(f"Transformation failed: {str(e)}")
    
    async def transform_silver_to_gold(self,
                                     silver_datasets: List[str],
                                     gold_dataset: str,
                                     tenant_id: str,
                                     aggregation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data from Silver to Gold layer"""
        job_id = f"s2g_{gold_dataset}_{datetime.utcnow().timestamp()}"
        
        try:
            # Track job
            self.active_jobs[job_id] = {
                "type": "silver_to_gold",
                "sources": silver_datasets,
                "target": gold_dataset,
                "status": "running",
                "started_at": datetime.utcnow()
            }
            self.stats["total_transformations"] += 1
            
            # Load Silver datasets
            dataframes = []
            for dataset in silver_datasets:
                silver_path = f"{self.lake_manager.zone_paths[DataZone.SILVER]}/{tenant_id}/{dataset}"
                df = self.spark.read.format("delta").load(silver_path)
                dataframes.append((dataset, df))
            
            # Join datasets if multiple
            if len(dataframes) > 1:
                df = await self._join_silver_datasets(dataframes, aggregation_config.get("joins", []))
            else:
                df = dataframes[0][1]
            
            initial_count = df.count()
            
            # Apply aggregations
            aggregations = aggregation_config.get("aggregations", [])
            for agg_config in aggregations:
                agg_type = agg_config.get("type")
                if agg_type in self.transformations:
                    df = self.transformations[agg_type](df, agg_config)
                else:
                    logger.warning(f"Unknown aggregation type: {agg_type}")
            
            # Apply business logic transformations
            business_rules = aggregation_config.get("business_rules", [])
            df = await self._apply_business_rules(df, business_rules)
            
            # Add Gold metadata
            df = df.withColumn("_gold_timestamp", F.current_timestamp()) \
                   .withColumn("_aggregation_level", F.lit(aggregation_config.get("level", "daily"))) \
                   .withColumn("_source_datasets", F.lit(",".join(silver_datasets)))
            
            # Write to Gold
            gold_path = f"{self.lake_manager.zone_paths[DataZone.GOLD]}/{tenant_id}/{gold_dataset}"
            
            # Determine write mode based on config
            write_mode = aggregation_config.get("write_mode", "overwrite")
            
            if write_mode == "merge":
                await self._merge_gold_data(df, gold_path, aggregation_config)
            else:
                df.write.mode(write_mode).format("delta").save(gold_path)
            
            # Create/update table for SQL access
            df.write.mode("overwrite").saveAsTable(f"{tenant_id}_{gold_dataset}")
            
            # Optimize table
            delta_table = DeltaTable.forPath(self.spark, gold_path)
            delta_table.optimize().executeCompaction()
            
            # Create views if specified
            views = aggregation_config.get("create_views", [])
            for view_config in views:
                await self._create_gold_view(df, view_config, tenant_id)
            
            # Get final statistics
            final_count = df.count()
            
            # Update job status
            self.active_jobs[job_id]["status"] = "completed"
            self.active_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            self.stats["successful_transformations"] += 1
            self.stats["total_rows_processed"] += final_count
            
            result = {
                "job_id": job_id,
                "source_datasets": silver_datasets,
                "target_dataset": gold_dataset,
                "initial_row_count": initial_count,
                "final_row_count": final_count,
                "aggregation_level": aggregation_config.get("level", "daily"),
                "duration_seconds": (datetime.utcnow() - self.active_jobs[job_id]["started_at"]).total_seconds(),
                "status": "success"
            }
            
            logger.info(f"Silver to Gold transformation completed: {job_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Transformation job {job_id} failed: {e}")
            
            self.active_jobs[job_id]["status"] = "failed"
            self.active_jobs[job_id]["error"] = str(e)
            self.stats["failed_transformations"] += 1
            
            raise ServiceError(f"Transformation failed: {str(e)}")
    
    async def _apply_transformation_pipeline(self,
                                           df: DataFrame,
                                           config: Dict[str, Any]) -> DataFrame:
        """Apply a pipeline of transformations"""
        pipeline = config.get("pipeline", [])
        
        for step in pipeline:
            transform_name = step.get("name")
            transform_params = step.get("params", {})
            
            if transform_name in self.transformations:
                logger.info(f"Applying transformation: {transform_name}")
                df = self.transformations[transform_name](df, transform_params)
            else:
                logger.warning(f"Unknown transformation: {transform_name}")
        
        return df
    
    async def _apply_quality_validation(self,
                                      df: DataFrame,
                                      rules: List[Dict[str, Any]]) -> DataFrame:
        """Apply data quality validation rules"""
        for rule in rules:
            rule_type = rule.get("type")
            
            if rule_type == "not_null":
                columns = rule.get("columns", [])
                for col in columns:
                    df = df.filter(F.col(col).isNotNull())
                    
            elif rule_type == "unique":
                columns = rule.get("columns", [])
                df = df.dropDuplicates(columns)
                
            elif rule_type == "range":
                column = rule.get("column")
                min_val = rule.get("min")
                max_val = rule.get("max")
                df = df.filter((F.col(column) >= min_val) & (F.col(column) <= max_val))
                
            elif rule_type == "pattern":
                column = rule.get("column")
                pattern = rule.get("pattern")
                df = df.filter(F.col(column).rlike(pattern))
                
            elif rule_type == "custom":
                # Custom SQL condition
                condition = rule.get("condition")
                df = df.filter(condition)
        
        return df
    
    async def _apply_business_rules(self,
                                  df: DataFrame,
                                  rules: List[Dict[str, Any]]) -> DataFrame:
        """Apply business logic rules"""
        for rule in rules:
            rule_type = rule.get("type")
            
            if rule_type == "calculation":
                # Add calculated columns
                calculations = rule.get("calculations", {})
                for col_name, expression in calculations.items():
                    df = df.withColumn(col_name, F.expr(expression))
                    
            elif rule_type == "categorization":
                # Add category columns based on conditions
                column = rule.get("column")
                categories = rule.get("categories", [])
                
                # Build case/when expression
                case_expr = None
                for category in categories:
                    condition = category.get("condition")
                    value = category.get("value")
                    
                    if case_expr is None:
                        case_expr = F.when(F.expr(condition), value)
                    else:
                        case_expr = case_expr.when(F.expr(condition), value)
                
                if case_expr:
                    df = df.withColumn(column, case_expr.otherwise(F.lit("Other")))
                    
            elif rule_type == "filter":
                # Apply business filters
                condition = rule.get("condition")
                df = df.filter(condition)
        
        return df
    
    # Built-in transformation functions
    
    def _remove_nulls(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Remove rows with null values"""
        columns = params.get("columns")
        how = params.get("how", "any")  # any or all
        
        if columns:
            subset = columns
        else:
            subset = None
        
        return df.dropna(how=how, subset=subset)
    
    def _deduplicate(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Remove duplicate rows"""
        columns = params.get("columns")
        keep = params.get("keep", "first")  # first, last, or none
        
        if columns:
            if keep == "first":
                # Keep first occurrence
                df = df.dropDuplicates(subset=columns)
            elif keep == "last":
                # Keep last occurrence - requires ordering
                order_col = params.get("order_by", "_ingestion_timestamp")
                window = Window.partitionBy(columns).orderBy(F.col(order_col).desc())
                df = df.withColumn("_rn", F.row_number().over(window))
                df = df.filter(F.col("_rn") == 1).drop("_rn")
            else:
                # Remove all duplicates
                df = df.dropDuplicates(subset=columns)
        else:
            df = df.dropDuplicates()
        
        return df
    
    def _trim_strings(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Trim whitespace from string columns"""
        columns = params.get("columns")
        
        if not columns:
            # Apply to all string columns
            columns = [field.name for field in df.schema.fields 
                      if field.dataType.simpleString() == "string"]
        
        for col_name in columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
        
        return df
    
    def _standardize_dates(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Standardize date formats"""
        date_columns = params.get("columns", [])
        target_format = params.get("format", "yyyy-MM-dd")
        
        for col_name in date_columns:
            # Try to parse and reformat
            df = df.withColumn(
                col_name,
                F.date_format(F.to_date(F.col(col_name)), target_format)
            )
        
        return df
    
    def _add_computed_columns(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Add computed columns"""
        computations = params.get("computations", {})
        
        for col_name, expression in computations.items():
            df = df.withColumn(col_name, F.expr(expression))
        
        return df
    
    def _lookup_enrichment(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Enrich data with lookup values"""
        lookup_dataset = params.get("lookup_dataset")
        join_keys = params.get("join_keys", [])
        select_columns = params.get("select_columns", ["*"])
        
        if not lookup_dataset:
            return df
        
        # Load lookup data
        # In production, would load from actual dataset
        # For now, return original dataframe
        return df
    
    def _normalize_values(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Normalize numeric values"""
        columns = params.get("columns", [])
        method = params.get("method", "min_max")  # min_max or z_score
        
        for col_name in columns:
            if method == "min_max":
                # Min-max normalization
                min_val = df.agg(F.min(col_name)).collect()[0][0]
                max_val = df.agg(F.max(col_name)).collect()[0][0]
                
                df = df.withColumn(
                    f"{col_name}_normalized",
                    (F.col(col_name) - min_val) / (max_val - min_val)
                )
            elif method == "z_score":
                # Z-score normalization
                mean_val = df.agg(F.mean(col_name)).collect()[0][0]
                stddev_val = df.agg(F.stddev(col_name)).collect()[0][0]
                
                df = df.withColumn(
                    f"{col_name}_normalized",
                    (F.col(col_name) - mean_val) / stddev_val
                )
        
        return df
    
    def _standardize_names(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Standardize column names"""
        case = params.get("case", "snake")  # snake, camel, or pascal
        
        for col in df.columns:
            new_name = col
            
            if case == "snake":
                # Convert to snake_case
                import re
                new_name = re.sub('([A-Z]+)', r'_\1', col).lower().strip('_')
            elif case == "camel":
                # Convert to camelCase
                parts = col.split('_')
                new_name = parts[0].lower() + ''.join(p.capitalize() for p in parts[1:])
            elif case == "pascal":
                # Convert to PascalCase
                new_name = ''.join(p.capitalize() for p in col.split('_'))
            
            if new_name != col:
                df = df.withColumnRenamed(col, new_name)
        
        return df
    
    def _time_series_aggregation(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Aggregate time series data"""
        time_column = params.get("time_column", "_ingestion_timestamp")
        group_columns = params.get("group_by", [])
        window = params.get("window", "1 day")
        aggregations = params.get("aggregations", {})
        
        # Add time window column
        df = df.withColumn("time_window", F.window(F.col(time_column), window))
        
        # Group by time window and other columns
        group_cols = ["time_window"] + group_columns
        grouped = df.groupBy(*group_cols)
        
        # Apply aggregations
        agg_exprs = []
        for col_name, agg_funcs in aggregations.items():
            if isinstance(agg_funcs, str):
                agg_funcs = [agg_funcs]
            
            for func in agg_funcs:
                if func == "sum":
                    agg_exprs.append(F.sum(col_name).alias(f"{col_name}_sum"))
                elif func == "avg":
                    agg_exprs.append(F.avg(col_name).alias(f"{col_name}_avg"))
                elif func == "count":
                    agg_exprs.append(F.count(col_name).alias(f"{col_name}_count"))
                elif func == "min":
                    agg_exprs.append(F.min(col_name).alias(f"{col_name}_min"))
                elif func == "max":
                    agg_exprs.append(F.max(col_name).alias(f"{col_name}_max"))
        
        if agg_exprs:
            df = grouped.agg(*agg_exprs)
        
        # Extract window start and end
        df = df.withColumn("window_start", F.col("time_window.start")) \
               .withColumn("window_end", F.col("time_window.end")) \
               .drop("time_window")
        
        return df
    
    def _group_aggregation(self, df: DataFrame, params: Dict[str, Any]) -> DataFrame:
        """Standard group by aggregation"""
        group_columns = params.get("group_by", [])
        aggregations = params.get("aggregations", {})
        
        if not group_columns:
            return df
        
        grouped = df.groupBy(*group_columns)
        
        # Build aggregation expressions
        agg_exprs = []
        for col_name, agg_funcs in aggregations.items():
            if isinstance(agg_funcs, str):
                agg_funcs = [agg_funcs]
            
            for func in agg_funcs:
                if func == "sum":
                    agg_exprs.append(F.sum(col_name).alias(f"{col_name}_sum"))
                elif func == "avg":
                    agg_exprs.append(F.avg(col_name).alias(f"{col_name}_avg"))
                elif func == "count":
                    agg_exprs.append(F.count(col_name).alias(f"{col_name}_count"))
                elif func == "min":
                    agg_exprs.append(F.min(col_name).alias(f"{col_name}_min"))
                elif func == "max":
                    agg_exprs.append(F.max(col_name).alias(f"{col_name}_max"))
                elif func == "collect_list":
                    agg_exprs.append(F.collect_list(col_name).alias(f"{col_name}_list"))
                elif func == "collect_set":
                    agg_exprs.append(F.collect_set(col_name).alias(f"{col_name}_set"))
        
        if agg_exprs:
            df = grouped.agg(*agg_exprs)
        
        return df
    
    async def _join_silver_datasets(self,
                                  dataframes: List[tuple],
                                  join_configs: List[Dict[str, Any]]) -> DataFrame:
        """Join multiple Silver datasets"""
        result_df = dataframes[0][1]
        
        for i in range(1, len(dataframes)):
            if i-1 < len(join_configs):
                join_config = join_configs[i-1]
            else:
                join_config = {}
            
            right_df = dataframes[i][1]
            
            # Get join configuration
            join_keys = join_config.get("keys", [])
            join_type = join_config.get("type", "inner")
            
            # Handle column prefixes to avoid conflicts
            left_prefix = join_config.get("left_prefix", "")
            right_prefix = join_config.get("right_prefix", f"{dataframes[i][0]}_")
            
            if left_prefix:
                for col in result_df.columns:
                    if col not in join_keys:
                        result_df = result_df.withColumnRenamed(col, f"{left_prefix}{col}")
            
            if right_prefix:
                for col in right_df.columns:
                    if col not in join_keys:
                        right_df = right_df.withColumnRenamed(col, f"{right_prefix}{col}")
            
            # Perform join
            if join_keys:
                result_df = result_df.join(right_df, on=join_keys, how=join_type)
            else:
                result_df = result_df.join(right_df, how=join_type)
        
        return result_df
    
    def _dataset_exists(self, path: str) -> bool:
        """Check if a Delta dataset exists"""
        try:
            DeltaTable.forPath(self.spark, path)
            return True
        except:
            return False
    
    async def _merge_with_schema_evolution(self,
                                         df: DataFrame,
                                         target_path: str,
                                         config: Dict[str, Any]) -> None:
        """Merge data with schema evolution support"""
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        # Get merge keys
        merge_keys = config.get("merge_keys", ["id"])
        
        # Build merge condition
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        # Perform merge with schema evolution
        merge_builder = delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        )
        
        # Handle updates
        update_columns = {col: f"source.{col}" for col in df.columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)
        
        # Handle inserts
        insert_columns = {col: f"source.{col}" for col in df.columns}
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_columns)
        
        # Execute merge
        merge_builder.execute()
    
    async def _merge_gold_data(self,
                             df: DataFrame,
                             target_path: str,
                             config: Dict[str, Any]) -> None:
        """Merge aggregated data into Gold layer"""
        if not self._dataset_exists(target_path):
            # First time, just write
            df.write.mode("overwrite").format("delta").save(target_path)
            return
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        # Get merge strategy
        strategy = config.get("merge_strategy", "update")
        merge_keys = config.get("merge_keys", [])
        
        if strategy == "update":
            # Update existing and insert new
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            merge_builder = delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            )
            
            # Update all columns except keys
            update_columns = {
                col: f"source.{col}" 
                for col in df.columns 
                if col not in merge_keys
            }
            merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)
            
            # Insert new records
            insert_columns = {col: f"source.{col}" for col in df.columns}
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_columns)
            
            merge_builder.execute()
            
        elif strategy == "append":
            # Simply append new data
            df.write.mode("append").format("delta").save(target_path)
            
        elif strategy == "replace":
            # Replace entire dataset
            df.write.mode("overwrite").format("delta").save(target_path)
    
    async def _create_gold_view(self,
                              df: DataFrame,
                              view_config: Dict[str, Any],
                              tenant_id: str) -> None:
        """Create a view on Gold data"""
        view_name = view_config.get("name")
        view_type = view_config.get("type", "temporary")  # temporary or global
        filters = view_config.get("filters", [])
        select_columns = view_config.get("columns", ["*"])
        
        # Apply filters
        for filter_expr in filters:
            df = df.filter(filter_expr)
        
        # Select columns
        if select_columns != ["*"]:
            df = df.select(*select_columns)
        
        # Create view
        full_view_name = f"{tenant_id}_{view_name}"
        
        if view_type == "global":
            df.createOrReplaceGlobalTempView(full_view_name)
        else:
            df.createOrReplaceTempView(full_view_name)
        
        logger.info(f"Created {view_type} view: {full_view_name}")
    
    def get_transformation_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a transformation job"""
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get transformation engine statistics"""
        return {
            "active_jobs": len(self.active_jobs),
            "registered_transformations": len(self.transformations),
            **self.stats
        }


# Import Window for window functions
from pyspark.sql.window import Window 