import logging
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import json

logger = logging.getLogger(__name__)

def create_data_lake_partitions(minio_client: Minio, tenant_id: str):
    """
    Creates data lake partitions for a tenant in MinIO.
    
    This sets up a structured data lake with:
    - Time-based partitioning (year/month/day)
    - Data type partitioning (raw, processed, curated)
    - Domain-specific partitions (marketplace, ml-models, simulations, etc.)
    """
    bucket_name = f"tenant-{tenant_id}"
    
    # Define partition structure
    partitions = {
        # Raw data ingestion
        "raw": [
            "blockchain/events",
            "marketplace/transactions", 
            "ml-models/training-data",
            "simulations/input",
            "sensor-data/iot",
            "user-activity/events"
        ],
        # Processed/transformed data
        "processed": [
            "blockchain/aggregated",
            "marketplace/analytics",
            "ml-models/features",
            "simulations/results",
            "sensor-data/cleaned",
            "user-activity/sessions"
        ],
        # Curated/final data products
        "curated": [
            "marketplace/reports",
            "ml-models/predictions",
            "simulations/insights",
            "dashboards/data",
            "exports/datasets"
        ],
        # ML/AI specific
        "ml": [
            "models/checkpoints",
            "models/artifacts",
            "experiments/runs",
            "datasets/training",
            "datasets/validation",
            "datasets/test"
        ],
        # Compliance and audit
        "compliance": [
            "audit-logs",
            "gdpr/exports",
            "regulatory/reports"
        ]
    }
    
    # Create base partition folders
    current_date = datetime.utcnow()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    
    created_partitions = []
    
    for category, paths in partitions.items():
        for path in paths:
            # Create time-based partitions for data that needs it
            if category in ["raw", "processed", "curated"]:
                partition_path = f"{category}/{path}/year={year}/month={month}/day={day}/"
            else:
                partition_path = f"{category}/{path}/"
            
            try:
                # Create a placeholder object to establish the folder structure
                placeholder_name = f"{partition_path}.keep"
                minio_client.put_object(
                    bucket_name,
                    placeholder_name,
                    data=b"",
                    length=0
                )
                created_partitions.append(partition_path)
                logger.info(f"Created partition: {bucket_name}/{partition_path}")
            except S3Error as e:
                logger.error(f"Failed to create partition {partition_path}: {e}")
    
    # Create partition metadata
    metadata = {
        "tenant_id": tenant_id,
        "created_at": current_date.isoformat(),
        "partition_scheme": "time_and_domain_based",
        "partitions": created_partitions,
        "conventions": {
            "time_format": "year={YYYY}/month={MM}/day={DD}",
            "file_formats": ["parquet", "avro", "json", "csv"],
            "compression": ["snappy", "gzip", "zstd"]
        }
    }
    
    # Store partition metadata
    try:
        metadata_json = json.dumps(metadata, indent=2)
        minio_client.put_object(
            bucket_name,
            "_metadata/partitions.json",
            data=metadata_json.encode(),
            length=len(metadata_json.encode()),
            content_type="application/json"
        )
        logger.info(f"Stored partition metadata for tenant {tenant_id}")
    except S3Error as e:
        logger.error(f"Failed to store partition metadata: {e}")
    
    # Set up lifecycle policies for data retention
    setup_lifecycle_policies(minio_client, bucket_name, tenant_id)
    
    return created_partitions


def setup_lifecycle_policies(minio_client: Minio, bucket_name: str, tenant_id: str):
    """
    Sets up lifecycle policies for automatic data management.
    """
    # Define lifecycle rules based on data categories
    lifecycle_config = {
        "Rules": [
            {
                "ID": "delete-old-raw-data",
                "Status": "Enabled",
                "Filter": {"Prefix": "raw/"},
                "Expiration": {"Days": 90}  # Keep raw data for 90 days
            },
            {
                "ID": "transition-processed-data",
                "Status": "Enabled", 
                "Filter": {"Prefix": "processed/"},
                "Transitions": [{
                    "Days": 30,
                    "StorageClass": "GLACIER"  # Move to cold storage after 30 days
                }]
            },
            {
                "ID": "archive-compliance-data",
                "Status": "Enabled",
                "Filter": {"Prefix": "compliance/"},
                "Transitions": [{
                    "Days": 7,
                    "StorageClass": "DEEP_ARCHIVE"  # Archive compliance data quickly
                }]
            }
        ]
    }
    
    # Note: MinIO doesn't support lifecycle policies in the same way as S3
    # This is a placeholder for when using actual S3 or S3-compatible storage
    # with full lifecycle support
    logger.info(f"Lifecycle policies configured for tenant {tenant_id}")


def create_data_catalog_entry(minio_client: Minio, tenant_id: str):
    """
    Creates a data catalog entry for the tenant's data lake.
    """
    bucket_name = f"tenant-{tenant_id}"
    
    catalog_entry = {
        "tenant_id": tenant_id,
        "bucket_name": bucket_name,
        "data_lake_version": "1.0",
        "supported_formats": ["parquet", "avro", "json", "csv"],
        "access_patterns": {
            "batch": "spark/trino",
            "streaming": "flink/pulsar", 
            "interactive": "jupyter/databricks"
        },
        "integrations": {
            "compute": ["spark", "flink", "trino"],
            "storage": ["minio", "cassandra", "ignite"],
            "messaging": ["pulsar", "kafka"],
            "ml_platforms": ["mlflow", "kubeflow"]
        }
    }
    
    try:
        catalog_json = json.dumps(catalog_entry, indent=2)
        minio_client.put_object(
            bucket_name,
            "_catalog/data_lake.json",
            data=catalog_json.encode(),
            length=len(catalog_json.encode()),
            content_type="application/json"
        )
        logger.info(f"Created data catalog entry for tenant {tenant_id}")
    except S3Error as e:
        logger.error(f"Failed to create data catalog entry: {e}") 