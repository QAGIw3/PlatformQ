"""
Project Orchestration Module

Handles project-related workflows including:
- Project provisioning across integrated services
- Collaborative ML training orchestration
- Multi-service project coordination
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio

from platformq_shared.event_publisher import EventPublisher
from platformq_shared import ServiceClients

logger = logging.getLogger(__name__)


class ProjectOrchestrator:
    """
    Orchestrates project-related workflows across multiple services.
    
    This replaces the functionality from the deprecated projects-service,
    integrating it into the workflow service for better orchestration.
    """
    
    def __init__(self,
                 event_publisher: EventPublisher,
                 service_clients: ServiceClients):
        self.event_publisher = event_publisher
        self.service_clients = service_clients
        
    async def create_project_workflow(self,
                                    project_name: str,
                                    project_description: str,
                                    tenant_id: str,
                                    user_id: str,
                                    options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Orchestrate project creation across multiple services.
        
        This workflow:
        1. Creates entries in OpenProject (if enabled)
        2. Creates collaboration space in Nextcloud
        3. Sets up Zulip stream for communication
        4. Provisions any required resources
        5. Publishes project created event
        """
        options = options or {}
        results = {
            "project_id": f"proj_{datetime.utcnow().timestamp()}",
            "status": "creating",
            "services": {}
        }
        
        try:
            # Step 1: Create OpenProject entry (if enabled)
            if options.get("enable_openproject", True):
                try:
                    # In production, would call OpenProject API
                    results["services"]["openproject"] = {
                        "status": "created",
                        "project_id": f"op_{results['project_id']}"
                    }
                except Exception as e:
                    logger.error(f"Failed to create OpenProject: {e}")
                    results["services"]["openproject"] = {"status": "failed", "error": str(e)}
                    
            # Step 2: Create Nextcloud collaboration space
            if options.get("enable_nextcloud", True):
                try:
                    # In production, would call Nextcloud API
                    results["services"]["nextcloud"] = {
                        "status": "created",
                        "folder_id": f"nc_{results['project_id']}"
                    }
                except Exception as e:
                    logger.error(f"Failed to create Nextcloud space: {e}")
                    results["services"]["nextcloud"] = {"status": "failed", "error": str(e)}
                    
            # Step 3: Create Zulip stream
            if options.get("enable_zulip", True):
                try:
                    # In production, would call Zulip API
                    results["services"]["zulip"] = {
                        "status": "created",
                        "stream_id": f"zulip_{results['project_id']}"
                    }
                except Exception as e:
                    logger.error(f"Failed to create Zulip stream: {e}")
                    results["services"]["zulip"] = {"status": "failed", "error": str(e)}
                    
            # Step 4: Publish project created event
            await self.event_publisher.publish(
                f"persistent://platformq/{tenant_id}/project-created-events",
                {
                    "project_id": results["project_id"],
                    "project_name": project_name,
                    "project_description": project_description,
                    "tenant_id": tenant_id,
                    "created_by": user_id,
                    "services": results["services"],
                    "created_at": datetime.utcnow().isoformat()
                }
            )
            
            results["status"] = "created"
            logger.info(f"Successfully created project workflow: {results['project_id']}")
            
        except Exception as e:
            logger.error(f"Error in project creation workflow: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            
        return results
        
    async def orchestrate_collaborative_ml_training(self,
                                                  project_id: str,
                                                  model_config: Dict[str, Any],
                                                  tenant_id: str,
                                                  user_id: str) -> Dict[str, Any]:
        """
        Orchestrate collaborative ML training for a project.
        
        This workflow:
        1. Gathers project assets from digital-asset-service
        2. Prepares data for training
        3. Initiates federated learning session
        4. Monitors training progress
        5. Stores results
        """
        training_id = f"ml_training_{datetime.utcnow().timestamp()}"
        
        try:
            # Step 1: Gather project assets
            assets = await self._gather_project_assets(project_id, tenant_id)
            
            # Step 2: Prepare training data
            prepared_data = await self._prepare_training_data(assets, model_config)
            
            # Step 3: Create federated learning session
            fl_session = await self.service_clients.call(
                "unified-ml-platform-service",
                "POST",
                "/api/v1/federated/sessions",
                json={
                    "model_type": model_config.get("model_type", "classification"),
                    "dataset_requirements": prepared_data["requirements"],
                    "privacy_parameters": {
                        "differential_privacy": True,
                        "epsilon": model_config.get("privacy_budget", 1.0)
                    },
                    "training_parameters": {
                        "rounds": model_config.get("num_rounds", 10),
                        "min_participants": model_config.get("min_participants", 2)
                    }
                }
            )
            
            # Step 4: Publish training initiated event
            await self.event_publisher.publish(
                f"persistent://platformq/{tenant_id}/ml-training-initiated-events",
                {
                    "training_id": training_id,
                    "project_id": project_id,
                    "session_id": fl_session.get("session_id"),
                    "initiated_by": user_id,
                    "model_config": model_config,
                    "initiated_at": datetime.utcnow().isoformat()
                }
            )
            
            return {
                "training_id": training_id,
                "session_id": fl_session.get("session_id"),
                "status": "initiated",
                "participants_required": model_config.get("min_participants", 2),
                "participants_joined": 0
            }
            
        except Exception as e:
            logger.error(f"Error in collaborative ML training: {e}")
            return {
                "training_id": training_id,
                "status": "failed",
                "error": str(e)
            }
            
    async def _gather_project_assets(self, project_id: str, tenant_id: str) -> List[Dict[str, Any]]:
        """Gather all assets associated with a project"""
        try:
            response = await self.service_clients.call(
                "digital-asset-service",
                "GET",
                f"/api/v1/digital-assets",
                params={
                    "project_id": project_id,
                    "tenant_id": tenant_id
                }
            )
            return response.get("assets", [])
        except Exception as e:
            logger.error(f"Failed to gather project assets: {e}")
            return []
            
    async def _prepare_training_data(self, 
                                   assets: List[Dict[str, Any]], 
                                   model_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare training data from project assets"""
        # This would implement data preparation logic
        # For now, return a simple structure
        return {
            "requirements": {
                "data_format": model_config.get("data_format", "tabular"),
                "feature_columns": model_config.get("features", []),
                "label_column": model_config.get("label", "target"),
                "min_samples": model_config.get("min_samples", 100)
            },
            "asset_count": len(assets),
            "prepared_at": datetime.utcnow().isoformat()
        }
        
    async def delete_project_workflow(self,
                                    project_id: str,
                                    tenant_id: str,
                                    archive: bool = True) -> Dict[str, Any]:
        """
        Orchestrate project deletion/archival across services.
        
        This workflow:
        1. Archives/deletes resources in each integrated service
        2. Moves data to archive storage if requested
        3. Publishes project deleted event
        """
        results = {
            "project_id": project_id,
            "action": "archive" if archive else "delete",
            "status": "processing",
            "services": {}
        }
        
        try:
            # Archive/delete in each service
            # (In production, would make actual API calls)
            
            # Publish project deleted/archived event
            await self.event_publisher.publish(
                f"persistent://platformq/{tenant_id}/project-deleted-events",
                {
                    "project_id": project_id,
                    "archived": archive,
                    "deleted_at": datetime.utcnow().isoformat()
                }
            )
            
            results["status"] = "completed"
            
        except Exception as e:
            logger.error(f"Error in project deletion workflow: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            
        return results 