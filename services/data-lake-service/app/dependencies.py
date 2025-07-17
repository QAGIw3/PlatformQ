from pyspark.sql import SparkSession
from minio import Minio
from functools import lru_cache

from .config import settings
from .models.medallion_architecture import MedallionArchitecture
from .ingestion.data_ingestion import DataIngestionPipeline
from .quality.data_quality_framework import DataQualityFramework
from .processing.layer_processing import LayerProcessingPipeline
from .jobs import JobRepository
from platformq.shared.event_publisher import EventPublisher

@lru_cache()
def get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("PlatformQ-DataLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

@lru_cache()
def get_minio_client() -> Minio:
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=False
    )

@lru_cache()
def get_event_publisher() -> EventPublisher:
    return EventPublisher(pulsar_url=settings.PULSAR_URL)

@lru_cache()
def get_job_repository() -> JobRepository:
    return JobRepository()

@lru_cache()
def get_medallion_architecture() -> MedallionArchitecture:
    return MedallionArchitecture(
        spark_session=get_spark_session(),
        minio_client=get_minio_client(),
        event_publisher=get_event_publisher(),
        bucket_name=settings.DATA_LAKE_BUCKET,
        delta_enabled=True
    )

@lru_cache()
def get_quality_framework() -> DataQualityFramework:
    return DataQualityFramework(spark=get_spark_session())

@lru_cache()
def get_ingestion_pipeline() -> DataIngestionPipeline:
    return DataIngestionPipeline(
        spark=get_spark_session(),
        medallion=get_medallion_architecture(),
        job_repository=get_job_repository()
    )

@lru_cache()
def get_processing_pipeline() -> LayerProcessingPipeline:
    return LayerProcessingPipeline(
        spark=get_spark_session(),
        medallion=get_medallion_architecture(),
        quality_framework=get_quality_framework(),
        job_repository=get_job_repository()
    ) 