"""
Integration tests for Data Platform Service v2.0
"""

import pytest
import asyncio
from httpx import AsyncClient
from typing import Dict, Any
import json
from datetime import datetime

from data_intelligence_common.core.processing import BatchEngine, StreamEngine, LakehouseFormat


@pytest.fixture
async def client():
    """Create async HTTP client for testing"""
    async with AsyncClient(base_url="http://localhost:8010") as client:
        yield client


@pytest.fixture
def sample_batch_job():
    """Sample batch job request"""
    return {
        "name": "test-batch-job",
        "description": "Integration test batch job",
        "engine": "spark",
        "source_path": "s3://test-bucket/raw/data",
        "target_path": "s3://test-bucket/processed/data",
        "lakehouse_format": "delta",
        "transformations": [
            {
                "type": "filter",
                "condition": "value > 100"
            },
            {
                "type": "aggregate",
                "group_by": ["category"],
                "aggregations": {"total": "sum(value)"}
            }
        ],
        "quality_checks": ["completeness", "uniqueness"],
        "enable_ml_optimization": True
    }


class TestDataPlatformServiceV2:
    """Test suite for Data Platform Service v2.0"""
    
    @pytest.mark.asyncio
    async def test_health_check(self, client: AsyncClient):
        """Test service health endpoint"""
        response = await client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert data["version"] == "2.0.0"
    
    @pytest.mark.asyncio
    async def test_v2_api_info(self, client: AsyncClient):
        """Test v2 API information endpoint"""
        response = await client.get("/api/v2/")
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == "2.0.0"
        assert "multi-engine-batch-processing" in data["features"]
    
    @pytest.mark.asyncio
    async def test_batch_job_submission(self, client: AsyncClient, sample_batch_job: Dict[str, Any]):
        """Test batch job submission with multi-engine support"""
        response = await client.post("/api/v2/batch/jobs", json=sample_batch_job)
        assert response.status_code == 200
        
        data = response.json()
        assert "job_id" in data
        assert data["status"] == "submitted"
        assert data["engine"] == "spark"
        assert "estimated_duration" in data
        assert "resource_allocation" in data
    
    @pytest.mark.asyncio
    async def test_multi_engine_batch_support(self, client: AsyncClient):
        """Test support for multiple batch engines"""
        response = await client.get("/api/v2/batch/engines")
        assert response.status_code == 200
        
        data = response.json()
        engines = [e["name"] for e in data["engines"]]
        assert "spark" in engines
        assert "ray" in engines
        assert "dask" in engines
        assert "pandas" in engines
    
    @pytest.mark.asyncio
    async def test_lakehouse_operations(self, client: AsyncClient):
        """Test lakehouse table operations"""
        # Create table
        create_request = {
            "table_name": "test_table",
            "format": "delta",
            "schema": {
                "id": "string",
                "value": "double",
                "timestamp": "timestamp"
            },
            "partition_by": ["timestamp"],
            "enable_cdc": True
        }
        
        response = await client.post("/api/v1/lake/tables", json=create_request)
        assert response.status_code in [200, 201]
        
        # Write data
        write_request = {
            "data": [
                {"id": "1", "value": 100.5, "timestamp": "2024-01-15T10:00:00Z"},
                {"id": "2", "value": 200.7, "timestamp": "2024-01-15T11:00:00Z"}
            ],
            "mode": "append"
        }
        
        response = await client.post(
            f"/api/v1/lake/tables/test_table/write",
            json=write_request
        )
        assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_stream_processing_pipeline(self, client: AsyncClient):
        """Test stream processing pipeline creation"""
        pipeline_request = {
            "name": "test-stream-pipeline",
            "engine": "flink",
            "source": {
                "type": "pulsar",
                "topics": ["test-events"],
                "subscription": "test-sub"
            },
            "transformations": [
                {
                    "type": "map",
                    "function": "parse_json"
                },
                {
                    "type": "window",
                    "size": "5 minutes",
                    "slide": "1 minute"
                }
            ],
            "sink": {
                "type": "delta",
                "path": "s3://test-bucket/streaming/events",
                "checkpoint_interval": 60000
            }
        }
        
        response = await client.post("/api/v2/stream/pipelines", json=pipeline_request)
        assert response.status_code == 200
        data = response.json()
        assert "pipeline_id" in data
        assert data["status"] == "active"
    
    @pytest.mark.asyncio
    async def test_data_quality_assessment(self, client: AsyncClient):
        """Test data quality assessment with ML"""
        quality_request = {
            "dataset": "s3://test-bucket/processed/data",
            "rules": [
                {
                    "type": "completeness",
                    "columns": ["id", "value"],
                    "threshold": 0.99
                },
                {
                    "type": "uniqueness",
                    "columns": ["id"],
                    "threshold": 1.0
                },
                {
                    "type": "ml_anomaly",
                    "model": "isolation_forest",
                    "contamination": 0.01
                }
            ],
            "enable_remediation": True
        }
        
        response = await client.post("/api/v2/quality/assessments", json=quality_request)
        assert response.status_code == 200
        data = response.json()
        assert "assessment_id" in data
        assert "overall_score" in data
        assert 0 <= data["overall_score"] <= 1
    
    @pytest.mark.asyncio
    async def test_catalog_operations(self, client: AsyncClient):
        """Test unified catalog operations"""
        # Register dataset
        dataset_request = {
            "name": "customer_data",
            "description": "Customer demographic and transaction data",
            "format": "delta",
            "location": "s3://data-lake/customers",
            "schema": {
                "customer_id": "string",
                "name": "string",
                "email": "string",
                "age": "integer",
                "total_spent": "double"
            },
            "tags": ["customer", "pii", "production"],
            "owner": "data-team"
        }
        
        response = await client.post("/api/v2/catalog/datasets", json=dataset_request)
        assert response.status_code in [200, 201]
        data = response.json()
        assert "dataset_id" in data
        
        # Search catalog
        search_response = await client.get("/api/v2/catalog/search?q=customer&tags=pii")
        assert search_response.status_code == 200
        results = search_response.json()
        assert len(results["datasets"]) > 0
    
    @pytest.mark.asyncio
    async def test_ml_integration(self, client: AsyncClient):
        """Test ML pipeline integration"""
        ml_request = {
            "name": "feature-engineering-pipeline",
            "type": "batch",
            "engine": "ray",
            "source_dataset": "s3://data-lake/customers",
            "target_dataset": "s3://features/customers",
            "transformations": [
                {
                    "type": "feature_engineering",
                    "features": [
                        "age_group",
                        "spending_category",
                        "recency_score"
                    ]
                },
                {
                    "type": "normalization",
                    "method": "standard"
                }
            ],
            "ml_optimization": {
                "auto_feature_selection": True,
                "target_column": "churn_probability"
            }
        }
        
        response = await client.post("/api/v2/ml/pipelines", json=ml_request)
        assert response.status_code == 200
        data = response.json()
        assert "pipeline_id" in data
        assert data["engine"] == "ray"
    
    @pytest.mark.asyncio
    async def test_backward_compatibility(self, client: AsyncClient):
        """Test v1 API backward compatibility"""
        # Test legacy ingestion endpoint
        legacy_request = {
            "source": "postgresql",
            "connection": {
                "host": "postgres.example.com",
                "database": "production",
                "table": "users"
            },
            "destination": "s3://raw-data/users",
            "schedule": "0 */6 * * *"
        }
        
        response = await client.post("/api/v1/ingestion/jobs", json=legacy_request)
        assert response.status_code == 200
        
        # Test legacy catalog endpoint
        catalog_response = await client.get("/api/v1/catalog/datasets")
        assert catalog_response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_performance_metrics(self, client: AsyncClient):
        """Test performance metrics endpoint"""
        response = await client.get("/metrics")
        assert response.status_code == 200
        
        metrics = response.text
        assert "processing_records_total" in metrics
        assert "processing_duration_seconds" in metrics
        assert "http_requests_total" in metrics
        assert "http_request_duration_seconds" in metrics


@pytest.mark.integration
class TestEndToEndWorkflow:
    """End-to-end workflow integration tests"""
    
    @pytest.mark.asyncio
    async def test_complete_data_pipeline(self, client: AsyncClient):
        """Test complete data pipeline from ingestion to analytics"""
        # Step 1: Ingest data
        ingest_response = await client.post("/api/v1/ingestion/batch", json={
            "source": "s3://external/raw-data.csv",
            "format": "csv",
            "destination": "raw_zone",
            "schema_inference": True
        })
        assert ingest_response.status_code == 200
        ingest_job_id = ingest_response.json()["job_id"]
        
        # Step 2: Process with batch job
        batch_response = await client.post("/api/v2/batch/jobs", json={
            "name": "etl-pipeline",
            "engine": "spark",
            "source_path": "s3://raw_zone/data",
            "target_path": "s3://processed_zone/data",
            "lakehouse_format": "delta",
            "quality_checks": ["completeness", "consistency"]
        })
        assert batch_response.status_code == 200
        batch_job_id = batch_response.json()["job_id"]
        
        # Step 3: Register in catalog
        catalog_response = await client.post("/api/v2/catalog/datasets", json={
            "name": "processed_customer_data",
            "location": "s3://processed_zone/data",
            "format": "delta",
            "lineage": {
                "upstream": ["raw_zone/data"],
                "jobs": [ingest_job_id, batch_job_id]
            }
        })
        assert catalog_response.status_code in [200, 201]
        
        # Step 4: Create ML features
        ml_response = await client.post("/api/v2/ml/features", json={
            "source_dataset": "processed_customer_data",
            "feature_group": "customer_features",
            "features": ["age_bucket", "total_spent_30d", "purchase_frequency"]
        })
        assert ml_response.status_code == 200
        
        # Verify complete pipeline
        lineage_response = await client.get(
            f"/api/v2/catalog/lineage/processed_customer_data"
        )
        assert lineage_response.status_code == 200
        lineage = lineage_response.json()
        assert len(lineage["upstream"]) > 0
        assert len(lineage["downstream"]) > 0 