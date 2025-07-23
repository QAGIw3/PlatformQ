"""
NVIDIA Triton client for high-performance model serving
"""
import logging
from typing import Dict, List, Optional, Any, Tuple
import asyncio
import numpy as np
import tritonclient.http as httpclient
import tritonclient.grpc as grpcclient
from tritonclient.utils import InferenceServerException

logger = logging.getLogger(__name__)


class TritonClient:
    """
    Client for NVIDIA Triton Inference Server
    """
    
    def __init__(self,
                 server_url: str,
                 model_repository: str,
                 protocol: str = "http",
                 verbose: bool = False):
        self.server_url = server_url
        self.model_repository = model_repository
        self.protocol = protocol
        self.verbose = verbose
        self.client: Optional[Any] = None
        
    async def initialize(self):
        """Initialize Triton client"""
        try:
            loop = asyncio.get_event_loop()
            
            def _create_client():
                if self.protocol == "http":
                    client = httpclient.InferenceServerClient(
                        url=self.server_url,
                        verbose=self.verbose
                    )
                else:
                    client = grpcclient.InferenceServerClient(
                        url=self.server_url,
                        verbose=self.verbose
                    )
                    
                # Check if server is live
                if not client.is_server_live():
                    raise Exception("Triton server is not live")
                    
                return client
                
            self.client = await loop.run_in_executor(None, _create_client)
            logger.info(f"Triton client initialized: {self.server_url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Triton client: {str(e)}")
            raise
    
    async def load_model(self, model_name: str, config: Optional[str] = None) -> bool:
        """Load a model in Triton"""
        loop = asyncio.get_event_loop()
        
        def _load_model():
            try:
                self.client.load_model(model_name, config=config)
                return True
            except InferenceServerException as e:
                logger.error(f"Failed to load model {model_name}: {str(e)}")
                return False
                
        return await loop.run_in_executor(None, _load_model)
    
    async def unload_model(self, model_name: str) -> bool:
        """Unload a model from Triton"""
        loop = asyncio.get_event_loop()
        
        def _unload_model():
            try:
                self.client.unload_model(model_name)
                return True
            except InferenceServerException as e:
                logger.error(f"Failed to unload model {model_name}: {str(e)}")
                return False
                
        return await loop.run_in_executor(None, _unload_model)
    
    async def get_model_metadata(self, model_name: str, model_version: str = "") -> Dict[str, Any]:
        """Get model metadata"""
        loop = asyncio.get_event_loop()
        
        def _get_metadata():
            metadata = self.client.get_model_metadata(
                model_name=model_name,
                model_version=model_version
            )
            
            return {
                "name": metadata.name,
                "versions": metadata.versions,
                "platform": metadata.platform,
                "inputs": [
                    {
                        "name": inp.name,
                        "datatype": inp.datatype,
                        "shape": inp.shape
                    }
                    for inp in metadata.inputs
                ],
                "outputs": [
                    {
                        "name": out.name,
                        "datatype": out.datatype,
                        "shape": out.shape
                    }
                    for out in metadata.outputs
                ]
            }
            
        return await loop.run_in_executor(None, _get_metadata)
    
    async def infer(self,
                   model_name: str,
                   inputs: Dict[str, np.ndarray],
                   model_version: str = "",
                   request_id: str = "0",
                   outputs: Optional[List[str]] = None) -> Dict[str, np.ndarray]:
        """Run inference on a model"""
        loop = asyncio.get_event_loop()
        
        def _infer():
            # Prepare inputs
            triton_inputs = []
            for name, data in inputs.items():
                if self.protocol == "http":
                    triton_input = httpclient.InferInput(name, data.shape, "FP32")
                else:
                    triton_input = grpcclient.InferInput(name, data.shape, "FP32")
                    
                triton_input.set_data_from_numpy(data.astype(np.float32))
                triton_inputs.append(triton_input)
            
            # Prepare outputs
            triton_outputs = []
            if outputs:
                for name in outputs:
                    if self.protocol == "http":
                        triton_output = httpclient.InferRequestedOutput(name)
                    else:
                        triton_output = grpcclient.InferRequestedOutput(name)
                    triton_outputs.append(triton_output)
            
            # Run inference
            response = self.client.infer(
                model_name=model_name,
                model_version=model_version,
                inputs=triton_inputs,
                outputs=triton_outputs,
                request_id=request_id
            )
            
            # Parse response
            results = {}
            for output in response.get_output():
                results[output['name']] = response.as_numpy(output['name'])
                
            return results
            
        return await loop.run_in_executor(None, _infer)
    
    async def batch_infer(self,
                         model_name: str,
                         batch_inputs: List[Dict[str, np.ndarray]],
                         model_version: str = "") -> List[Dict[str, np.ndarray]]:
        """Run batch inference"""
        results = []
        
        # Process batch in parallel
        tasks = []
        for i, inputs in enumerate(batch_inputs):
            task = self.infer(
                model_name=model_name,
                inputs=inputs,
                model_version=model_version,
                request_id=str(i)
            )
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)
        return results
    
    async def get_model_stats(self, model_name: str, model_version: str = "") -> Dict[str, Any]:
        """Get model statistics"""
        loop = asyncio.get_event_loop()
        
        def _get_stats():
            stats = self.client.get_inference_statistics(
                model_name=model_name,
                model_version=model_version
            )
            
            model_stats = stats.model_stats[0] if stats.model_stats else None
            if not model_stats:
                return {}
                
            return {
                "model_name": model_stats.name,
                "model_version": model_stats.version,
                "inference_count": model_stats.inference_stats.success.count,
                "inference_compute_time_ns": model_stats.inference_stats.success.ns,
                "queue_time_ns": model_stats.inference_stats.queue.ns,
                "cache_hit": model_stats.inference_stats.cache_hit.count,
                "cache_miss": model_stats.inference_stats.cache_miss.count
            }
            
        return await loop.run_in_executor(None, _get_stats)
    
    async def is_model_ready(self, model_name: str, model_version: str = "") -> bool:
        """Check if model is ready for inference"""
        loop = asyncio.get_event_loop()
        
        def _is_ready():
            try:
                return self.client.is_model_ready(
                    model_name=model_name,
                    model_version=model_version
                )
            except:
                return False
                
        return await loop.run_in_executor(None, _is_ready)
    
    async def get_server_metadata(self) -> Dict[str, Any]:
        """Get server metadata"""
        loop = asyncio.get_event_loop()
        
        def _get_server_metadata():
            metadata = self.client.get_server_metadata()
            return {
                "name": metadata.name,
                "version": metadata.version,
                "extensions": metadata.extensions
            }
            
        return await loop.run_in_executor(None, _get_server_metadata)
    
    async def list_models(self) -> List[Dict[str, Any]]:
        """List all loaded models"""
        loop = asyncio.get_event_loop()
        
        def _list_models():
            models = []
            repository_index = self.client.get_model_repository_index()
            
            for model in repository_index.models:
                models.append({
                    "name": model.name,
                    "version": model.version,
                    "state": model.state,
                    "reason": model.reason
                })
                
            return models
            
        return await loop.run_in_executor(None, _list_models)
    
    async def close(self):
        """Close Triton client"""
        # Triton client doesn't need explicit closing
        logger.info("Triton client closed") 