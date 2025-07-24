"""
Test base class implementations
"""

import pytest
import asyncio
from typing import Any, Dict

from ..core.algorithms.base_algorithm import BaseAlgorithm, AlgorithmConfig, AlgorithmType
from ..core.engines.base_engine import BaseEngine, EngineConfig, EngineType
from ..base_service.base import DataIntelligenceBaseService, ServiceMetadata


class TestAlgorithm(BaseAlgorithm[Dict[str, Any], Dict[str, Any]]):
    """Test implementation of BaseAlgorithm"""
    
    async def _execute_algorithm(self, input_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Simple test implementation"""
        return {"result": input_data.get("value", 0) * 2}


class TestEngine(BaseEngine[Dict[str, Any], Dict[str, Any]]):
    """Test implementation of BaseEngine"""
    
    async def _process_task(self, task_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Simple test implementation"""
        return {"processed": task_data}


class TestService(DataIntelligenceBaseService):
    """Test implementation of DataIntelligenceBaseService"""
    
    async def initialize_service(self):
        """Test service initialization"""
        await super().initialize_service()
        # Add test-specific initialization
        self.test_initialized = True
        
    async def cleanup_service(self):
        """Test service cleanup"""
        # Add test-specific cleanup
        self.test_cleaned_up = True
        await super().cleanup_service()


@pytest.mark.asyncio
async def test_base_algorithm():
    """Test BaseAlgorithm implementation"""
    config = AlgorithmConfig(
        name="test_algorithm",
        type=AlgorithmType.CUSTOM,
        max_memory_mb=100
    )
    
    algo = TestAlgorithm(config)
    
    # Test validation
    with pytest.raises(ValueError, match="Input data cannot be None"):
        algo.validate_input(None)
        
    # Test valid input
    algo.validate_input({"value": 42})
    
    # Test execution
    result = await algo.execute({"value": 10})
    assert result.status == "completed"
    assert result.result == {"result": 20}


@pytest.mark.asyncio
async def test_base_engine():
    """Test BaseEngine implementation"""
    config = EngineConfig(
        name="test_engine",
        type=EngineType.NATIVE,
        max_workers=2,
        max_queue_size=10
    )
    
    engine = TestEngine(config)
    
    # Test initialization
    await engine.initialize()
    assert engine.status.is_running
    
    # Test task submission
    task_id = await engine.submit_task({"data": "test"})
    assert task_id is not None
    
    # Wait for processing
    result = await engine.get_result(task_id, wait=True)
    assert result is not None
    assert result.result == {"processed": {"data": "test"}}
    
    # Test shutdown
    await engine.shutdown()
    assert not engine.status.is_running


@pytest.mark.asyncio
async def test_base_service():
    """Test DataIntelligenceBaseService implementation"""
    metadata = ServiceMetadata(
        name="test_service",
        version="1.0.0",
        description="Test service",
        capabilities=["test"],
        dependencies=[]
    )
    
    service = TestService(metadata)
    
    # Test initialization
    await service.startup()
    assert service._initialized
    assert hasattr(service, 'test_initialized')
    assert service.test_initialized
    
    # Test cleanup
    await service.shutdown()
    assert service._shutting_down
    assert hasattr(service, 'test_cleaned_up')
    assert service.test_cleaned_up


if __name__ == "__main__":
    # Run tests
    asyncio.run(test_base_algorithm())
    asyncio.run(test_base_engine())
    asyncio.run(test_base_service())
    print("All tests passed!") 