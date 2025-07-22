"""
Pipeline Repository

Manages pipeline definitions, templates, and metadata storage.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import uuid
from enum import Enum

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class PipelineStatus(Enum):
    """Pipeline status"""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"
    ARCHIVED = "archived"


class PipelineDefinition:
    """Pipeline definition model"""
    
    def __init__(
        self,
        id: str,
        name: str,
        type: str,
        description: str,
        config: Dict[str, Any],
        schedule: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        status: PipelineStatus = PipelineStatus.DRAFT,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.id = id
        self.name = name
        self.type = type
        self.description = description
        self.config = config
        self.schedule = schedule or {}
        self.dependencies = dependencies or []
        self.tags = tags or []
        self.owner = owner
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "config": self.config,
            "schedule": self.schedule,
            "dependencies": self.dependencies,
            "tags": self.tags,
            "owner": self.owner,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineDefinition':
        """Create from dictionary"""
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            description=data.get("description", ""),
            config=data["config"],
            schedule=data.get("schedule"),
            dependencies=data.get("dependencies"),
            tags=data.get("tags"),
            owner=data.get("owner"),
            status=PipelineStatus(data.get("status", "draft")),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else None,
            metadata=data.get("metadata", {})
        )


class PipelineRepository:
    """
    Repository for managing pipeline definitions
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration):
        self.vault_consul = vault_consul
        self.pipelines_cache: Dict[str, PipelineDefinition] = {}
        self.templates_cache: Dict[str, Dict[str, Any]] = {}
        self.last_sync: Optional[datetime] = None
    
    async def initialize(self):
        """Initialize repository"""
        logger.info("initializing_pipeline_repository")
        
        # Load templates
        await self._load_templates()
        
        # Sync pipelines from storage
        await self.sync_pipelines()
        
        logger.info("pipeline_repository_initialized", 
                   pipeline_count=len(self.pipelines_cache),
                   template_count=len(self.templates_cache))
    
    async def _load_templates(self):
        """Load pipeline templates"""
        # Load from Consul or default templates
        try:
            templates = await self.vault_consul.get_config("pipelines/templates", {})
            self.templates_cache = templates
        except Exception:
            # Use default templates
            self._load_default_templates()
    
    def _load_default_templates(self):
        """Load default pipeline templates"""
        self.templates_cache = {
            "bronze_to_silver": {
                "name": "Bronze to Silver Pipeline",
                "type": "transformation",
                "description": "Standard Bronze to Silver transformation pipeline",
                "config": {
                    "source_zone": "bronze",
                    "target_zone": "silver",
                    "steps": [
                        {
                            "type": "quality_check",
                            "config": {"checks": ["null_check", "duplicate_check"]}
                        },
                        {
                            "type": "transform",
                            "config": {
                                "operations": ["clean_data", "normalize", "deduplicate"]
                            }
                        },
                        {
                            "type": "validate",
                            "config": {"schema_validation": True}
                        }
                    ]
                }
            },
            "silver_to_gold": {
                "name": "Silver to Gold Pipeline",
                "type": "transformation",
                "description": "Standard Silver to Gold aggregation pipeline",
                "config": {
                    "source_zone": "silver",
                    "target_zone": "gold",
                    "steps": [
                        {
                            "type": "aggregate",
                            "config": {
                                "operations": ["time_series_agg", "business_metrics"]
                            }
                        },
                        {
                            "type": "optimize",
                            "config": {"partitioning": True, "indexing": True}
                        }
                    ]
                }
            },
            "batch_ingestion": {
                "name": "Batch Ingestion Pipeline",
                "type": "ingestion",
                "description": "Template for batch data ingestion",
                "config": {
                    "mode": "batch",
                    "steps": [
                        {
                            "type": "extract",
                            "config": {"format": "parquet", "compression": "snappy"}
                        },
                        {
                            "type": "validate",
                            "config": {"schema_check": True}
                        },
                        {
                            "type": "load",
                            "config": {"target": "bronze", "write_mode": "append"}
                        }
                    ]
                }
            },
            "streaming_ingestion": {
                "name": "Streaming Ingestion Pipeline",
                "type": "ingestion",
                "description": "Template for streaming data ingestion",
                "config": {
                    "mode": "streaming",
                    "steps": [
                        {
                            "type": "consume",
                            "config": {"buffer_size": 1000, "checkpoint_interval": 60}
                        },
                        {
                            "type": "transform",
                            "config": {"operations": ["parse", "enrich"]}
                        },
                        {
                            "type": "sink",
                            "config": {"target": "bronze", "micro_batch_interval": 30}
                        }
                    ]
                }
            }
        }
    
    async def sync_pipelines(self):
        """Sync pipelines from Consul storage"""
        try:
            # Get all pipeline definitions from Consul
            pipelines_data = await self.vault_consul.get_config("pipelines/definitions", {})
            
            # Clear and reload cache
            self.pipelines_cache.clear()
            
            for pipeline_id, pipeline_data in pipelines_data.items():
                try:
                    pipeline = PipelineDefinition.from_dict(pipeline_data)
                    self.pipelines_cache[pipeline_id] = pipeline
                except Exception as e:
                    logger.error("invalid_pipeline_data", 
                               pipeline_id=pipeline_id, 
                               error=str(e))
            
            self.last_sync = datetime.utcnow()
            
            logger.info("pipelines_synced", count=len(self.pipelines_cache))
            
        except Exception as e:
            logger.error("sync_pipelines_error", error=str(e))
    
    async def get_pipeline(self, pipeline_id: str) -> Optional[PipelineDefinition]:
        """Get a pipeline by ID"""
        return self.pipelines_cache.get(pipeline_id)
    
    async def list_pipelines(
        self,
        status: Optional[PipelineStatus] = None,
        type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None
    ) -> List[PipelineDefinition]:
        """List pipelines with optional filtering"""
        pipelines = list(self.pipelines_cache.values())
        
        # Apply filters
        if status:
            pipelines = [p for p in pipelines if p.status == status]
        
        if type:
            pipelines = [p for p in pipelines if p.type == type]
        
        if tags:
            pipelines = [p for p in pipelines if any(tag in p.tags for tag in tags)]
        
        if owner:
            pipelines = [p for p in pipelines if p.owner == owner]
        
        return pipelines
    
    async def create_pipeline(
        self,
        name: str,
        type: str,
        config: Dict[str, Any],
        description: str = "",
        schedule: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PipelineDefinition:
        """Create a new pipeline"""
        pipeline_id = str(uuid.uuid4())
        
        pipeline = PipelineDefinition(
            id=pipeline_id,
            name=name,
            type=type,
            description=description,
            config=config,
            schedule=schedule,
            dependencies=dependencies,
            tags=tags,
            owner=owner,
            status=PipelineStatus.DRAFT,
            metadata=metadata
        )
        
        # Save to storage
        await self._save_pipeline(pipeline)
        
        # Add to cache
        self.pipelines_cache[pipeline_id] = pipeline
        
        logger.info("pipeline_created", 
                   pipeline_id=pipeline_id,
                   name=name,
                   type=type)
        
        return pipeline
    
    async def update_pipeline(
        self,
        pipeline_id: str,
        updates: Dict[str, Any]
    ) -> Optional[PipelineDefinition]:
        """Update an existing pipeline"""
        pipeline = self.pipelines_cache.get(pipeline_id)
        if not pipeline:
            return None
        
        # Update fields
        for field, value in updates.items():
            if hasattr(pipeline, field):
                setattr(pipeline, field, value)
        
        pipeline.updated_at = datetime.utcnow()
        
        # Save to storage
        await self._save_pipeline(pipeline)
        
        logger.info("pipeline_updated", 
                   pipeline_id=pipeline_id,
                   updates=list(updates.keys()))
        
        return pipeline
    
    async def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline"""
        if pipeline_id not in self.pipelines_cache:
            return False
        
        # Remove from storage
        try:
            key = f"pipelines/definitions/{pipeline_id}"
            await self.vault_consul.consul.kv.delete(key)
        except Exception as e:
            logger.error("delete_pipeline_error", 
                        pipeline_id=pipeline_id, 
                        error=str(e))
            return False
        
        # Remove from cache
        del self.pipelines_cache[pipeline_id]
        
        logger.info("pipeline_deleted", pipeline_id=pipeline_id)
        return True
    
    async def _save_pipeline(self, pipeline: PipelineDefinition):
        """Save pipeline to storage"""
        key = f"pipelines/definitions/{pipeline.id}"
        data = json.dumps(pipeline.to_dict())
        await self.vault_consul.consul.kv.put(key, data)
    
    async def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get a pipeline template"""
        return self.templates_cache.get(template_id)
    
    async def list_templates(self) -> Dict[str, Dict[str, Any]]:
        """List all templates"""
        return self.templates_cache.copy()
    
    async def create_from_template(
        self,
        template_id: str,
        name: str,
        overrides: Optional[Dict[str, Any]] = None,
        owner: Optional[str] = None
    ) -> Optional[PipelineDefinition]:
        """Create a pipeline from a template"""
        template = self.templates_cache.get(template_id)
        if not template:
            return None
        
        # Merge template config with overrides
        config = template["config"].copy()
        if overrides:
            config.update(overrides)
        
        # Create pipeline
        return await self.create_pipeline(
            name=name,
            type=template.get("type", "custom"),
            config=config,
            description=template.get("description", ""),
            tags=[f"template:{template_id}"],
            owner=owner
        )
    
    async def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        pipelines = list(self.pipelines_cache.values())
        
        stats = {
            "total": len(pipelines),
            "by_status": {},
            "by_type": {},
            "with_schedule": 0,
            "with_dependencies": 0
        }
        
        for pipeline in pipelines:
            # Count by status
            status = pipeline.status.value
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            
            # Count by type
            stats["by_type"][pipeline.type] = stats["by_type"].get(pipeline.type, 0) + 1
            
            # Count scheduled
            if pipeline.schedule:
                stats["with_schedule"] += 1
            
            # Count with dependencies
            if pipeline.dependencies:
                stats["with_dependencies"] += 1
        
        return stats 