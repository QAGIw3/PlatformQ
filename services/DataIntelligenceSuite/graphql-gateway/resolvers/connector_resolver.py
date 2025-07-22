"""
Connector Resolver

Resolves GraphQL queries and mutations for connectors and processors.
"""

from typing import List, Optional, Dict, Any
import httpx
import logging
from datetime import datetime

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ConnectorResolver:
    """Resolves connector and processor related queries"""
    
    def __init__(self, service_urls: Dict[str, str]):
        self.ingestion_url = service_urls.get("data-ingestion-service", "http://data-ingestion-service:8010")
        self.batch_url = service_urls.get("batch-processing-service", "http://batch-processing-service:8012")
        self.http_client = httpx.AsyncClient(timeout=30.0)
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.http_client.aclose()
    
    # Connector queries
    async def get_connectors(self) -> List[Dict[str, Any]]:
        """Get all active connectors"""
        try:
            response = await self.http_client.get(f"{self.ingestion_url}/api/v1/connectors")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get connectors: {e}")
            return []
    
    async def get_connector(self, connector_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific connector"""
        try:
            response = await self.http_client.get(f"{self.ingestion_url}/api/v1/connectors/{connector_id}/status")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            logger.error(f"Failed to get connector {connector_id}: {e}")
            return None
    
    # Processor queries
    async def get_supported_processors(self) -> List[Dict[str, Any]]:
        """Get all supported file processors"""
        try:
            response = await self.http_client.get(f"{self.batch_url}/api/v1/processors/formats")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get supported processors: {e}")
            return []
    
    async def get_processor_info(self, processor_type: str) -> Optional[Dict[str, Any]]:
        """Get detailed processor information"""
        try:
            response = await self.http_client.get(f"{self.batch_url}/api/v1/processors/{processor_type}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            logger.error(f"Failed to get processor info for {processor_type}: {e}")
            return None
    
    async def get_processing_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get processing job status"""
        try:
            response = await self.http_client.get(f"{self.batch_url}/api/v1/jobs/{job_id}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            logger.error(f"Failed to get processing job {job_id}: {e}")
            return None
    
    async def get_processing_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all processing jobs"""
        try:
            params = {"status": status} if status else {}
            response = await self.http_client.get(f"{self.batch_url}/api/v1/jobs", params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get processing jobs: {e}")
            return []
    
    # Connector mutations
    async def create_connector(self, connector_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new connector"""
        try:
            response = await self.http_client.post(
                f"{self.ingestion_url}/api/v1/connectors",
                json={"connector_id": connector_id, "config": config}
            )
            response.raise_for_status()
            return {"success": True, "message": f"Connector {connector_id} created successfully"}
        except httpx.HTTPStatusError as e:
            return {"success": False, "message": f"Failed to create connector: {e.response.text}"}
        except Exception as e:
            logger.error(f"Failed to create connector {connector_id}: {e}")
            return {"success": False, "message": str(e)}
    
    async def delete_connector(self, connector_id: str) -> Dict[str, Any]:
        """Delete a connector"""
        try:
            response = await self.http_client.delete(f"{self.ingestion_url}/api/v1/connectors/{connector_id}")
            response.raise_for_status()
            return {"success": True, "message": f"Connector {connector_id} deleted successfully"}
        except httpx.HTTPStatusError as e:
            return {"success": False, "message": f"Failed to delete connector: {e.response.text}"}
        except Exception as e:
            logger.error(f"Failed to delete connector {connector_id}: {e}")
            return {"success": False, "message": str(e)}
    
    async def trigger_connector(self, connector_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Trigger a connector"""
        try:
            response = await self.http_client.post(
                f"{self.ingestion_url}/api/v1/connectors/{connector_id}/trigger",
                json=params or {}
            )
            response.raise_for_status()
            result = response.json()
            return {
                "success": True,
                "message": f"Connector {connector_id} triggered successfully",
                "job_id": result.get("job_id")
            }
        except httpx.HTTPStatusError as e:
            return {"success": False, "message": f"Failed to trigger connector: {e.response.text}"}
        except Exception as e:
            logger.error(f"Failed to trigger connector {connector_id}: {e}")
            return {"success": False, "message": str(e)}
    
    # Processor mutations
    async def process_file(self, file_path: str, processor_type: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process a file"""
        try:
            payload = {
                "file_path": file_path,
                "options": options or {}
            }
            if processor_type:
                payload["processor_type"] = processor_type
                
            response = await self.http_client.post(
                f"{self.batch_url}/api/v1/processors/process",
                json=payload
            )
            response.raise_for_status()
            result = response.json()
            
            return {
                "job_id": result.get("job_id"),
                "status": "submitted",
                "processor_type": result.get("processor_type"),
                "created_at": datetime.utcnow().isoformat()
            }
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to process file: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise
    
    async def process_batch(self, file_paths: List[str], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process multiple files in batch"""
        try:
            response = await self.http_client.post(
                f"{self.batch_url}/api/v1/processors/process/batch",
                json={
                    "file_paths": file_paths,
                    "options": options or {}
                }
            )
            response.raise_for_status()
            result = response.json()
            
            return {
                "jobs": result,
                "total_files": len(file_paths),
                "status": "submitted"
            }
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to process batch: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            raise
    
    async def receive_webhook(self, webhook_type: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Receive and process webhook data"""
        try:
            response = await self.http_client.post(
                f"{self.ingestion_url}/api/v1/connectors/webhook/{webhook_type}",
                json=payload,
                headers=headers or {}
            )
            response.raise_for_status()
            return {"success": True, "message": "Webhook processed successfully"}
        except httpx.HTTPStatusError as e:
            return {"success": False, "message": f"Failed to process webhook: {e.response.text}"}
        except Exception as e:
            logger.error(f"Failed to process webhook {webhook_type}: {e}")
            return {"success": False, "message": str(e)} 