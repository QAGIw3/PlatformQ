"""
Observable Data Pipeline Example

Demonstrates integration of unified observability with data pipelines
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from platformq_shared.observability import (
    DataPipelineTracer,
    trace_async,
    SpanType,
    get_observability
)
from ..lake.medallion_architecture import MedallionLakeManager, DataZone

import logging
logger = logging.getLogger(__name__)


class ObservableDataPipeline:
    """
    Example data pipeline with comprehensive observability
    """
    
    def __init__(
        self,
        pipeline_name: str,
        spark: SparkSession,
        lake_manager: MedallionLakeManager
    ):
        self.pipeline_name = pipeline_name
        self.spark = spark
        self.lake_manager = lake_manager
        self.obs = None
        
    async def initialize(self):
        """Initialize observability"""
        self.obs = await get_observability()
        
    @trace_async(
        span_type=SpanType.DATA_PIPELINE,
        track_args=True,
        track_result=True
    )
    async def run(
        self,
        source_date: str,
        target_zone: DataZone = DataZone.SILVER
    ) -> Dict[str, Any]:
        """
        Run the observable pipeline
        
        Args:
            source_date: Date to process (YYYY-MM-DD)
            target_zone: Target data zone
            
        Returns:
            Pipeline execution results
        """
        async with DataPipelineTracer(self.pipeline_name) as tracer:
            # Phase 1: Read raw data
            raw_data = await self._read_raw_data(tracer, source_date)
            
            # Phase 2: Clean and validate
            cleaned_data = await self._clean_data(tracer, raw_data)
            
            # Phase 3: Enrich with reference data
            enriched_data = await self._enrich_data(tracer, cleaned_data)
            
            # Phase 4: Apply business transformations
            transformed_data = await self._transform_data(tracer, enriched_data)
            
            # Phase 5: Write to target zone
            result = await self._write_results(
                tracer,
                transformed_data,
                target_zone,
                source_date
            )
            
            # Track final metrics
            tracer.set_row_count(result["row_count"])
            
            return result
            
    async def _read_raw_data(
        self,
        tracer: DataPipelineTracer,
        source_date: str
    ) -> DataFrame:
        """Read raw data with lineage tracking"""
        source_tables = [
            f"bronze.events_{source_date.replace('-', '')}",
            f"bronze.users_snapshot_{source_date.replace('-', '')}"
        ]
        
        # Read events
        events_df = await tracer.trace_read(
            source_tables[0],
            self._read_iceberg_table,
            source_tables[0]
        )
        
        # Read users
        users_df = await tracer.trace_read(
            source_tables[1],
            self._read_iceberg_table,
            source_tables[1]
        )
        
        # Track columns used
        tracer.add_columns_read([
            "event_id", "user_id", "event_type", "event_timestamp",
            "user_name", "user_email", "user_status"
        ])
        
        # Join data
        return events_df.join(users_df, on="user_id", how="left")
        
    async def _clean_data(
        self,
        tracer: DataPipelineTracer,
        df: DataFrame
    ) -> DataFrame:
        """Clean data with quality tracking"""
        initial_count = df.count()
        
        cleaned = await tracer.trace_transform(
            "data_cleaning",
            self._apply_cleaning_rules,
            df
        )
        
        # Track data quality metrics
        final_count = cleaned.count()
        if self.obs:
            current_span = trace.get_current_span()
            current_span.set_attribute("quality.initial_rows", initial_count)
            current_span.set_attribute("quality.cleaned_rows", final_count)
            current_span.set_attribute("quality.dropped_rows", initial_count - final_count)
            current_span.set_attribute("quality.drop_rate", 
                                     (initial_count - final_count) / initial_count)
            
        return cleaned
        
    async def _enrich_data(
        self,
        tracer: DataPipelineTracer,
        df: DataFrame
    ) -> DataFrame:
        """Enrich with reference data"""
        # Read reference data
        geo_data = await tracer.trace_read(
            "gold.geo_reference",
            self._read_iceberg_table,
            "gold.geo_reference"
        )
        
        enriched = await tracer.trace_transform(
            "geo_enrichment",
            lambda d: d.join(geo_data, on="country_code", how="left"),
            df
        )
        
        # Track new columns
        tracer.add_columns_written([
            "country_name", "region", "timezone"
        ])
        
        return enriched
        
    async def _transform_data(
        self,
        tracer: DataPipelineTracer,
        df: DataFrame
    ) -> DataFrame:
        """Apply business transformations"""
        return await tracer.trace_transform(
            "business_transformations",
            self._apply_business_rules,
            df
        )
        
    async def _write_results(
        self,
        tracer: DataPipelineTracer,
        df: DataFrame,
        target_zone: DataZone,
        source_date: str
    ) -> Dict[str, Any]:
        """Write results with Iceberg tracking"""
        table_name = f"{target_zone.value}.processed_events_{source_date.replace('-', '')}"
        
        # Write data
        await tracer.trace_write(
            table_name,
            self._write_iceberg_table,
            df,
            table_name,
            {"date": source_date}  # partition
        )
        
        # Track Iceberg metadata
        if self.obs:
            iceberg_metadata = await self.obs.track_iceberg_operation(
                table_name,
                "write",
                trace.get_current_span()
            )
            
            return {
                "table_name": table_name,
                "row_count": df.count(),
                "snapshot_id": iceberg_metadata.get("snapshot_id") if iceberg_metadata else None,
                "file_count": iceberg_metadata.get("file_count") if iceberg_metadata else None,
                "total_size_bytes": iceberg_metadata.get("file_size_bytes") if iceberg_metadata else None
            }
            
        return {
            "table_name": table_name,
            "row_count": df.count()
        }
        
    async def _read_iceberg_table(self, table_name: str) -> DataFrame:
        """Read from Iceberg table"""
        return self.spark.table(table_name)
        
    async def _write_iceberg_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_values: Dict[str, str]
    ):
        """Write to Iceberg table"""
        writer = df.writeTo(table_name).using("iceberg")
        
        # Add partitioning
        if partition_values:
            for key, value in partition_values.items():
                writer = writer.partitionedBy(key)
                
        writer.createOrReplace()
        
    async def _apply_cleaning_rules(self, df: DataFrame) -> DataFrame:
        """Apply data cleaning rules"""
        from pyspark.sql.functions import col, isnan, when, trim
        
        # Remove null user_ids
        cleaned = df.filter(col("user_id").isNotNull())
        
        # Trim string fields
        for column in ["user_name", "user_email", "event_type"]:
            cleaned = cleaned.withColumn(column, trim(col(column)))
            
        # Remove invalid timestamps
        cleaned = cleaned.filter(
            (col("event_timestamp") > "2020-01-01") &
            (col("event_timestamp") < "2030-01-01")
        )
        
        return cleaned
        
    async def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business transformation rules"""
        from pyspark.sql.functions import col, when, datediff, current_date
        
        # Calculate user activity level
        df = df.withColumn(
            "activity_level",
            when(datediff(current_date(), col("event_timestamp")) <= 7, "active")
            .when(datediff(current_date(), col("event_timestamp")) <= 30, "moderate")
            .otherwise("inactive")
        )
        
        # Flag high-value events
        df = df.withColumn(
            "is_high_value",
            when(col("event_type").isin(["purchase", "subscription"]), True)
            .otherwise(False)
        )
        
        return df
        
    @trace_async(span_type=SpanType.DATA_PIPELINE)
    async def run_quality_checks(
        self,
        table_name: str,
        expected_schema: List[str]
    ) -> Dict[str, Any]:
        """Run quality checks on output table"""
        df = self.spark.table(table_name)
        
        # Schema validation
        actual_columns = df.columns
        missing_columns = set(expected_schema) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_schema)
        
        # Basic statistics
        row_count = df.count()
        null_counts = {}
        
        for col in expected_schema:
            if col in actual_columns:
                null_count = df.filter(df[col].isNull()).count()
                null_counts[col] = null_count
                
        quality_score = 1.0
        if missing_columns:
            quality_score -= 0.3
        if any(null_count > row_count * 0.1 for null_count in null_counts.values()):
            quality_score -= 0.2
            
        result = {
            "table_name": table_name,
            "row_count": row_count,
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "null_counts": null_counts,
            "quality_score": quality_score
        }
        
        # Track in current span
        current_span = trace.get_current_span()
        if current_span:
            current_span.set_attribute("quality.score", quality_score)
            current_span.set_attribute("quality.row_count", row_count)
            
        return result


# Example usage
async def main():
    """Example pipeline execution"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ObservablePipeline") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .getOrCreate()
    
    # Initialize lake manager
    lake_manager = MedallionLakeManager(
        spark_config={"spark.app.name": "ObservablePipeline"},
        default_table_format="iceberg"
    )
    await lake_manager.initialize()
    
    # Create pipeline
    pipeline = ObservableDataPipeline(
        "daily_user_activity_pipeline",
        spark,
        lake_manager
    )
    await pipeline.initialize()
    
    # Run pipeline
    result = await pipeline.run(
        source_date="2024-01-15",
        target_zone=DataZone.SILVER
    )
    
    logger.info(f"Pipeline completed: {result}")
    
    # Run quality checks
    quality_result = await pipeline.run_quality_checks(
        result["table_name"],
        expected_schema=[
            "event_id", "user_id", "event_type", "event_timestamp",
            "user_name", "user_email", "user_status",
            "country_name", "region", "timezone",
            "activity_level", "is_high_value"
        ]
    )
    
    logger.info(f"Quality check result: {quality_result}")
    
    # Query lineage
    obs = await get_observability()
    lineage = await obs.query_lineage(
        dataset_id=f"daily_user_activity_pipeline_{datetime.now().strftime('%Y%m%d')}",
        limit=10
    )
    
    logger.info(f"Found {len(lineage)} lineage records")
    

if __name__ == "__main__":
    asyncio.run(main()) 