"""
Tests for Data Compaction Service
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import json

from app.lake.data_compaction import (
    DataCompactionService,
    CompactionConfig,
    CompactionStrategy,
    CompactionStatus,
    FileFormat,
    FileMetadata,
    CompactionJob
)


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client"""
    client = Mock()
    client.list_objects = Mock(return_value=[])
    client.fget_object = Mock()
    client.fput_object = Mock()
    return client


@pytest.fixture
def mock_consul_client():
    """Mock Consul client"""
    client = AsyncMock()
    client.kv_get = AsyncMock(return_value=None)
    client.kv_put = AsyncMock()
    client.kv_list = AsyncMock(return_value=[])
    client.register_service = AsyncMock()
    client.deregister_service = AsyncMock()
    return client


@pytest.fixture
async def compaction_service(mock_minio_client, mock_consul_client):
    """Create compaction service instance"""
    service = DataCompactionService(
        minio_client=mock_minio_client,
        consul_client=mock_consul_client
    )
    await service.initialize()
    yield service
    await service.shutdown()


class TestDataCompactionService:
    """Test cases for DataCompactionService"""
    
    async def test_initialization(self, compaction_service):
        """Test service initialization"""
        assert compaction_service.config is not None
        assert compaction_service.config.strategy == CompactionStrategy.HYBRID
        assert compaction_service._running is True
        
    async def test_submit_compaction_job(self, compaction_service):
        """Test submitting a compaction job"""
        job_id = await compaction_service.submit_compaction_job(
            dataset="test_dataset",
            source_path="s3://test-bucket/data",
            strategy=CompactionStrategy.SIZE_BASED
        )
        
        assert job_id.startswith("compact-test_dataset-")
        assert len(compaction_service._active_jobs) == 0  # Job is in queue, not active
        
    async def test_job_status(self, compaction_service, mock_consul_client):
        """Test getting job status"""
        # Submit a job
        job_id = await compaction_service.submit_compaction_job(
            dataset="test_dataset",
            source_path="s3://test-bucket/data"
        )
        
        # Mock job data in Consul
        job_data = {
            "job_id": job_id,
            "dataset": "test_dataset",
            "source_path": "s3://test-bucket/data",
            "target_path": "s3://test-bucket/data/compacted",
            "status": CompactionStatus.COMPLETED.value,
            "strategy": CompactionStrategy.HYBRID.value,
            "files_processed": 10,
            "bytes_processed": 1024000,
            "bytes_saved": 204800
        }
        
        mock_consul_client.kv_get.return_value = json.dumps(job_data)
        mock_consul_client.kv_list.return_value = [f"compaction/jobs/test_dataset/{job_id}"]
        
        # Get job status
        job = await compaction_service.get_job_status(job_id)
        
        assert job is not None
        assert job.job_id == job_id
        assert job.status == CompactionStatus.COMPLETED
        
    async def test_analyze_dataset(self, compaction_service, mock_minio_client):
        """Test dataset analysis"""
        # Mock file listing
        from types import SimpleNamespace
        
        mock_files = [
            SimpleNamespace(
                object_name="data/part-001.parquet",
                size=50 * 1024 * 1024,  # 50MB (small file)
                last_modified=datetime.utcnow()
            ),
            SimpleNamespace(
                object_name="data/part-002.parquet",
                size=60 * 1024 * 1024,  # 60MB (small file)
                last_modified=datetime.utcnow()
            ),
            SimpleNamespace(
                object_name="data/part-003.parquet",
                size=200 * 1024 * 1024,  # 200MB (normal file)
                last_modified=datetime.utcnow()
            )
        ]
        
        mock_minio_client.list_objects.return_value = mock_files
        
        # Analyze dataset
        analysis = await compaction_service.analyze_dataset(
            dataset="test_dataset",
            path="s3://test-bucket/data"
        )
        
        assert analysis["dataset"] == "test_dataset"
        assert analysis["total_files"] == 3
        assert analysis["small_files"] == 2  # Files < 128MB
        assert analysis["recommended_strategy"] == CompactionStrategy.SIZE_BASED
        
    async def test_cancel_job(self, compaction_service):
        """Test cancelling a job"""
        # Create a mock active job
        job = CompactionJob(
            job_id="test-job-123",
            dataset="test_dataset",
            source_path="s3://test-bucket/data",
            target_path="s3://test-bucket/data/compacted",
            status=CompactionStatus.RUNNING,
            strategy=CompactionStrategy.HYBRID
        )
        
        compaction_service._active_jobs["test-job-123"] = job
        
        # Cancel the job
        result = await compaction_service.cancel_job("test-job-123")
        
        assert result is True
        assert "test-job-123" not in compaction_service._active_jobs
        
    def test_parse_s3_path(self, compaction_service):
        """Test S3 path parsing"""
        # Test with s3:// prefix
        bucket, key = compaction_service._parse_s3_path("s3://my-bucket/path/to/file.parquet")
        assert bucket == "my-bucket"
        assert key == "path/to/file.parquet"
        
        # Test without prefix
        bucket, key = compaction_service._parse_s3_path("my-bucket/path/to/file.parquet")
        assert bucket == "my-bucket"
        assert key == "path/to/file.parquet"
        
    def test_detect_format(self, compaction_service):
        """Test file format detection"""
        assert compaction_service._detect_format("file.parquet") == FileFormat.PARQUET
        assert compaction_service._detect_format("file.avro") == FileFormat.AVRO
        assert compaction_service._detect_format("file.csv") == FileFormat.CSV
        assert compaction_service._detect_format("file.json") == FileFormat.JSON
        assert compaction_service._detect_format("file.unknown") == FileFormat.PARQUET  # Default
        
    def test_extract_partition_values(self, compaction_service):
        """Test partition value extraction"""
        path = "data/year=2024/month=01/day=15/file.parquet"
        values = compaction_service._extract_partition_values(path)
        
        assert values["year"] == "2024"
        assert values["month"] == "01"
        assert values["day"] == "15"
        
    def test_recommend_strategy(self, compaction_service):
        """Test strategy recommendation"""
        # Small files -> size-based
        small_files = [
            FileMetadata(
                path=f"file{i}.parquet",
                size_bytes=10 * 1024 * 1024,  # 10MB
                created_at=datetime.utcnow(),
                format=FileFormat.PARQUET
            )
            for i in range(10)
        ]
        
        strategy = compaction_service._recommend_strategy(small_files, {})
        assert strategy == CompactionStrategy.SIZE_BASED
        
        # Many files -> file count based
        many_files = [
            FileMetadata(
                path=f"file{i}.parquet",
                size_bytes=100 * 1024 * 1024,  # 100MB
                created_at=datetime.utcnow(),
                format=FileFormat.PARQUET
            )
            for i in range(1500)
        ]
        
        strategy = compaction_service._recommend_strategy(many_files, {})
        assert strategy == CompactionStrategy.FILE_COUNT_BASED


@pytest.mark.asyncio
class TestCompactionWorkflow:
    """Test complete compaction workflow"""
    
    async def test_compaction_workflow(self, compaction_service, mock_minio_client):
        """Test end-to-end compaction workflow"""
        # Submit job
        job_id = await compaction_service.submit_compaction_job(
            dataset="test_dataset",
            source_path="s3://test-bucket/data",
            strategy=CompactionStrategy.SIZE_BASED
        )
        
        assert job_id is not None
        
        # Verify job was stored
        assert compaction_service.consul.kv_put.called
        
        # Check that background workers would process the job
        assert compaction_service._job_queue.qsize() > 0 