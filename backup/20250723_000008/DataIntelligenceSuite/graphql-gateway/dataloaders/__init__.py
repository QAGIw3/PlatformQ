"""
DataLoader Registry

Manages DataLoaders for efficient batching and caching of GraphQL requests.
"""

from typing import Dict, List, Any, Optional
from dataloader import DataLoader
import asyncio

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class DataLoaderRegistry:
    """
    Registry for all DataLoaders used in GraphQL resolvers
    """
    
    def __init__(self, service_resolver):
        self.service_resolver = service_resolver
        self._loaders = {}
        self._initialize_loaders()
    
    def _initialize_loaders(self):
        """Initialize all DataLoaders"""
        # Pipeline loader
        self._loaders['pipeline'] = DataLoader(self._batch_load_pipelines)
        
        # ML Model loader
        self._loaders['model'] = DataLoader(self._batch_load_models)
        
        # Catalog Entity loader
        self._loaders['entity'] = DataLoader(self._batch_load_entities)
        
        # Workflow loader
        self._loaders['workflow'] = DataLoader(self._batch_load_workflows)
        
        # User loader (for owner fields)
        self._loaders['user'] = DataLoader(self._batch_load_users)
        
        # Quality Profile loader
        self._loaders['quality_profile'] = DataLoader(self._batch_load_quality_profiles)
    
    def get_pipeline_loader(self) -> DataLoader:
        """Get pipeline DataLoader"""
        return self._loaders['pipeline']
    
    def get_model_loader(self) -> DataLoader:
        """Get ML model DataLoader"""
        return self._loaders['model']
    
    def get_entity_loader(self) -> DataLoader:
        """Get catalog entity DataLoader"""
        return self._loaders['entity']
    
    def get_workflow_loader(self) -> DataLoader:
        """Get workflow DataLoader"""
        return self._loaders['workflow']
    
    def get_user_loader(self) -> DataLoader:
        """Get user DataLoader"""
        return self._loaders['user']
    
    def get_quality_profile_loader(self) -> DataLoader:
        """Get quality profile DataLoader"""
        return self._loaders['quality_profile']
    
    # Batch loading functions
    async def _batch_load_pipelines(self, pipeline_ids: List[str]) -> List[Optional[Dict]]:
        """Batch load pipelines"""
        try:
            # Make batch request to workflow service
            url = f"{self.service_resolver.service_urls['workflow-engine-service']}/api/v1/workflows/batch"
            response = await self.service_resolver.http_client.post(
                url,
                json={"ids": pipeline_ids}
            )
            
            if response.status_code == 200:
                pipelines = response.json()
                # Create a map for efficient lookup
                pipeline_map = {p['id']: p for p in pipelines}
                # Return in the same order as requested
                return [pipeline_map.get(pid) for pid in pipeline_ids]
            else:
                logger.error(f"Failed to batch load pipelines: {response.status_code}")
                return [None] * len(pipeline_ids)
                
        except Exception as e:
            logger.error(f"Error batch loading pipelines: {e}")
            return [None] * len(pipeline_ids)
    
    async def _batch_load_models(self, model_ids: List[str]) -> List[Optional[Dict]]:
        """Batch load ML models"""
        try:
            url = f"{self.service_resolver.service_urls['mlops-service']}/api/v1/models/batch"
            response = await self.service_resolver.http_client.post(
                url,
                json={"ids": model_ids}
            )
            
            if response.status_code == 200:
                models = response.json()
                model_map = {m['id']: m for m in models}
                return [model_map.get(mid) for mid in model_ids]
            else:
                logger.error(f"Failed to batch load models: {response.status_code}")
                return [None] * len(model_ids)
                
        except Exception as e:
            logger.error(f"Error batch loading models: {e}")
            return [None] * len(model_ids)
    
    async def _batch_load_entities(self, entity_ids: List[str]) -> List[Optional[Dict]]:
        """Batch load catalog entities"""
        try:
            url = f"{self.service_resolver.service_urls['data-catalog-service']}/api/v1/entities/batch"
            response = await self.service_resolver.http_client.post(
                url,
                json={"ids": entity_ids}
            )
            
            if response.status_code == 200:
                entities = response.json()
                entity_map = {e['id']: e for e in entities}
                return [entity_map.get(eid) for eid in entity_ids]
            else:
                logger.error(f"Failed to batch load entities: {response.status_code}")
                return [None] * len(entity_ids)
                
        except Exception as e:
            logger.error(f"Error batch loading entities: {e}")
            return [None] * len(entity_ids)
    
    async def _batch_load_workflows(self, workflow_ids: List[str]) -> List[Optional[Dict]]:
        """Batch load workflows"""
        try:
            url = f"{self.service_resolver.service_urls['workflow-engine-service']}/api/v1/workflows/batch"
            response = await self.service_resolver.http_client.post(
                url,
                json={"ids": workflow_ids}
            )
            
            if response.status_code == 200:
                workflows = response.json()
                workflow_map = {w['id']: w for w in workflows}
                return [workflow_map.get(wid) for wid in workflow_ids]
            else:
                logger.error(f"Failed to batch load workflows: {response.status_code}")
                return [None] * len(workflow_ids)
                
        except Exception as e:
            logger.error(f"Error batch loading workflows: {e}")
            return [None] * len(workflow_ids)
    
    async def _batch_load_users(self, user_ids: List[str]) -> List[Optional[Dict]]:
        """Batch load users (placeholder - would integrate with auth service)"""
        # For now, return simple user objects
        return [{"id": uid, "name": f"User {uid}"} for uid in user_ids]
    
    async def _batch_load_quality_profiles(self, dataset_names: List[str]) -> List[Optional[Dict]]:
        """Batch load quality profiles"""
        try:
            url = f"{self.service_resolver.service_urls['quality-engine-service']}/api/v1/profiles/batch"
            response = await self.service_resolver.http_client.post(
                url,
                json={"datasets": dataset_names}
            )
            
            if response.status_code == 200:
                profiles = response.json()
                profile_map = {p['dataset']: p for p in profiles}
                return [profile_map.get(ds) for ds in dataset_names]
            else:
                logger.error(f"Failed to batch load quality profiles: {response.status_code}")
                return [None] * len(dataset_names)
                
        except Exception as e:
            logger.error(f"Error batch loading quality profiles: {e}")
            return [None] * len(dataset_names)


__all__ = ['DataLoaderRegistry'] 