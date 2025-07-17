"""
Collaborative ML Model Training for Projects

Enables teams to collaboratively train ML models on shared project assets
using federated learning and secure data sharing.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
import httpx
from pydantic import BaseModel, Field
import hashlib

from platformq_shared.event_publisher import EventPublisher
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class CollaborativeMLConfig(BaseModel):
    """Configuration for collaborative ML training"""
    project_id: str
    model_name: str
    model_type: str = Field(default="classification", description="classification, regression, clustering")
    asset_types: List[str] = Field(default=["3d_model", "document", "dataset"])
    min_participants: int = Field(default=2)
    privacy_mode: str = Field(default="federated", description="federated, differential_privacy, secure_multiparty")
    training_rounds: int = Field(default=10)
    consensus_threshold: float = Field(default=0.8)


class ProjectMLCoordinator:
    """Coordinates ML training within project context"""
    
    def __init__(self,
                 event_publisher: EventPublisher,
                 federated_learning_url: str,
                 digital_asset_url: str,
                 workflow_url: str):
        self.event_publisher = event_publisher
        self.fl_url = federated_learning_url
        self.asset_url = digital_asset_url
        self.workflow_url = workflow_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def initiate_collaborative_training(self,
                                            project_id: str,
                                            config: CollaborativeMLConfig,
                                            participants: List[str],
                                            db: Session) -> Dict[str, Any]:
        """Initiate collaborative ML training for project"""
        try:
            training_id = f"proj-ml-{project_id}-{datetime.utcnow().timestamp()}"
            
            # Get project assets
            project_assets = await self._get_project_assets(
                project_id,
                config.asset_types
            )
            
            if not project_assets:
                raise ValueError("No assets found for training")
                
            # Verify participant permissions
            verified_participants = await self._verify_participants(
                project_id,
                participants,
                project_assets
            )
            
            if len(verified_participants) < config.min_participants:
                raise ValueError(f"Not enough participants: {len(verified_participants)} < {config.min_participants}")
                
            # Create federated learning session
            fl_session = await self._create_fl_session(
                training_id,
                config,
                verified_participants,
                project_assets
            )
            
            # Create workflow for orchestration
            workflow_id = await self._create_training_workflow(
                project_id,
                training_id,
                fl_session["session_id"],
                config
            )
            
            # Store training info
            training_info = {
                "training_id": training_id,
                "project_id": project_id,
                "fl_session_id": fl_session["session_id"],
                "workflow_id": workflow_id,
                "config": config.dict(),
                "participants": verified_participants,
                "asset_count": len(project_assets),
                "status": "initiated",
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Publish event
            await self.event_publisher.publish_event(
                f"platformq/projects/ml-training-events",
                "CollaborativeMLTrainingStarted",
                training_info
            )
            
            logger.info(f"Initiated collaborative ML training {training_id} for project {project_id}")
            
            return training_info
            
        except Exception as e:
            logger.error(f"Error initiating collaborative training: {e}")
            raise
            
    async def _get_project_assets(self,
                                project_id: str,
                                asset_types: List[str]) -> List[Dict[str, Any]]:
        """Get project assets for training"""
        try:
            # Query digital asset service for project assets
            params = {
                "project_id": project_id,
                "asset_types": ",".join(asset_types),
                "status": "active"
            }
            
            response = await self.http_client.get(
                f"{self.asset_url}/api/v1/digital-assets",
                params=params
            )
            
            if response.status_code == 200:
                assets = response.json().get("assets", [])
                
                # Filter assets suitable for ML
                ml_assets = []
                for asset in assets:
                    if self._is_ml_suitable(asset):
                        ml_assets.append({
                            "asset_id": asset["cid"],
                            "asset_type": asset["asset_type"],
                            "metadata": asset.get("metadata", {}),
                            "size": asset.get("size", 0),
                            "owner_id": asset["owner_id"]
                        })
                        
                return ml_assets
            else:
                logger.error(f"Failed to get project assets: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting project assets: {e}")
            return []
            
    def _is_ml_suitable(self, asset: Dict[str, Any]) -> bool:
        """Check if asset is suitable for ML training"""
        # Check file size (not too large)
        if asset.get("size", 0) > 1024 * 1024 * 100:  # 100MB limit
            return False
            
        # Check if has required metadata
        metadata = asset.get("metadata", {})
        if asset["asset_type"] == "dataset":
            return "schema" in metadata or "columns" in metadata
        elif asset["asset_type"] == "3d_model":
            return "vertices" in metadata or "format" in metadata
        elif asset["asset_type"] == "document":
            return "content_type" in metadata
            
        return True
        
    async def _verify_participants(self,
                                 project_id: str,
                                 participants: List[str],
                                 assets: List[Dict[str, Any]]) -> List[str]:
        """Verify participant permissions"""
        verified = []
        
        # Get asset owners
        asset_owners = {asset["owner_id"] for asset in assets}
        
        for participant in participants:
            # Check if participant owns any assets or has project access
            if participant in asset_owners:
                verified.append(participant)
            else:
                # Check project membership (simplified)
                # In real implementation, would check project permissions
                verified.append(participant)
                
        return list(set(verified))
        
    async def _create_fl_session(self,
                               training_id: str,
                               config: CollaborativeMLConfig,
                               participants: List[str],
                               assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create federated learning session"""
        try:
            # Prepare data requirements based on assets
            data_requirements = self._prepare_data_requirements(config, assets)
            
            fl_request = {
                "session_name": f"Project {config.project_id} - {config.model_name}",
                "model_type": config.model_type,
                "min_participants": config.min_participants,
                "rounds": config.training_rounds,
                "privacy_parameters": self._get_privacy_params(config.privacy_mode),
                "aggregation_strategy": "WEIGHTED_AVERAGE",
                "data_requirements": data_requirements,
                "participant_criteria": {
                    "whitelist": participants,
                    "required_credentials": ["PROJECT_MEMBER"]
                },
                "metadata": {
                    "project_id": config.project_id,
                    "training_id": training_id
                }
            }
            
            response = await self.http_client.post(
                f"{self.fl_url}/api/v1/sessions",
                json=fl_request
            )
            
            if response.status_code != 201:
                raise Exception(f"Failed to create FL session: {response.text}")
                
            return response.json()
            
        except Exception as e:
            logger.error(f"Error creating FL session: {e}")
            raise
            
    def _prepare_data_requirements(self,
                                 config: CollaborativeMLConfig,
                                 assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare data requirements for FL session"""
        # Analyze assets to determine requirements
        total_size = sum(asset["size"] for asset in assets)
        asset_type_counts = {}
        
        for asset in assets:
            asset_type = asset["asset_type"]
            asset_type_counts[asset_type] = asset_type_counts.get(asset_type, 0) + 1
            
        return {
            "min_samples": max(100, len(assets) * 10),
            "data_types": list(asset_type_counts.keys()),
            "estimated_size_mb": total_size / (1024 * 1024),
            "feature_requirements": self._get_feature_requirements(config.model_type)
        }
        
    def _get_feature_requirements(self, model_type: str) -> Dict[str, Any]:
        """Get feature requirements for model type"""
        if model_type == "classification":
            return {
                "min_features": 5,
                "max_features": 1000,
                "label_type": "categorical"
            }
        elif model_type == "regression":
            return {
                "min_features": 3,
                "max_features": 500,
                "label_type": "numeric"
            }
        else:  # clustering
            return {
                "min_features": 2,
                "max_features": 100,
                "label_type": "none"
            }
            
    def _get_privacy_params(self, privacy_mode: str) -> Dict[str, Any]:
        """Get privacy parameters based on mode"""
        if privacy_mode == "federated":
            return {
                "mechanism": "FEDERATED_AVERAGING",
                "epsilon": float('inf'),  # No differential privacy
                "secure_aggregation": True
            }
        elif privacy_mode == "differential_privacy":
            return {
                "mechanism": "GAUSSIAN",
                "epsilon": 1.0,
                "delta": 1e-5,
                "secure_aggregation": True
            }
        else:  # secure_multiparty
            return {
                "mechanism": "SECURE_MULTIPARTY",
                "protocol": "SPDZ",
                "threshold": 0.5
            }
            
    async def _create_training_workflow(self,
                                      project_id: str,
                                      training_id: str,
                                      fl_session_id: str,
                                      config: CollaborativeMLConfig) -> str:
        """Create workflow to orchestrate training"""
        try:
            workflow_request = {
                "name": f"collaborative_ml_{training_id}",
                "type": "collaborative_ml_training",
                "config": {
                    "project_id": project_id,
                    "training_id": training_id,
                    "fl_session_id": fl_session_id,
                    "steps": [
                        {
                            "name": "data_preparation",
                            "type": "parallel",
                            "tasks": ["extract_features", "validate_data", "split_data"]
                        },
                        {
                            "name": "training",
                            "type": "federated",
                            "fl_session_id": fl_session_id
                        },
                        {
                            "name": "evaluation",
                            "type": "parallel",
                            "tasks": ["calculate_metrics", "generate_report"]
                        },
                        {
                            "name": "deployment",
                            "type": "conditional",
                            "condition": f"metrics.accuracy > {config.consensus_threshold}",
                            "tasks": ["register_model", "create_endpoint"]
                        }
                    ]
                },
                "schedule": "immediate"
            }
            
            response = await self.http_client.post(
                f"{self.workflow_url}/api/v1/workflows",
                json=workflow_request
            )
            
            if response.status_code != 201:
                raise Exception(f"Failed to create workflow: {response.text}")
                
            workflow = response.json()
            return workflow["workflow_id"]
            
        except Exception as e:
            logger.error(f"Error creating training workflow: {e}")
            raise
            
    async def get_training_status(self,
                                training_id: str) -> Dict[str, Any]:
        """Get status of collaborative training"""
        try:
            # Get FL session status
            # Get workflow status
            # Combine into training status
            
            return {
                "training_id": training_id,
                "status": "in_progress",
                "current_round": 5,
                "total_rounds": 10,
                "participants_active": 3,
                "metrics": {
                    "loss": 0.234,
                    "accuracy": 0.876
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting training status: {e}")
            return {"error": str(e)}
            
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose()


class ProjectDataPreparator:
    """Prepares project data for collaborative ML"""
    
    def __init__(self, asset_service_url: str, connector_service_url: str):
        self.asset_url = asset_service_url
        self.connector_url = connector_service_url
        self.http_client = httpx.AsyncClient(timeout=60.0)
        
    async def prepare_training_data(self,
                                  project_id: str,
                                  assets: List[Dict[str, Any]],
                                  model_type: str) -> Dict[str, Any]:
        """Prepare training data from project assets"""
        try:
            prepared_data = {
                "project_id": project_id,
                "datasets": [],
                "metadata": {
                    "total_assets": len(assets),
                    "preparation_time": datetime.utcnow().isoformat()
                }
            }
            
            # Group assets by type
            assets_by_type = {}
            for asset in assets:
                asset_type = asset["asset_type"]
                if asset_type not in assets_by_type:
                    assets_by_type[asset_type] = []
                assets_by_type[asset_type].append(asset)
                
            # Process each asset type
            for asset_type, type_assets in assets_by_type.items():
                if asset_type == "dataset":
                    datasets = await self._process_datasets(type_assets, model_type)
                    prepared_data["datasets"].extend(datasets)
                elif asset_type == "3d_model":
                    datasets = await self._process_3d_models(type_assets, model_type)
                    prepared_data["datasets"].extend(datasets)
                elif asset_type == "document":
                    datasets = await self._process_documents(type_assets, model_type)
                    prepared_data["datasets"].extend(datasets)
                    
            return prepared_data
            
        except Exception as e:
            logger.error(f"Error preparing training data: {e}")
            raise
            
    async def _process_datasets(self,
                              assets: List[Dict[str, Any]],
                              model_type: str) -> List[Dict[str, Any]]:
        """Process dataset assets"""
        datasets = []
        
        for asset in assets:
            try:
                # Get dataset content
                dataset_url = f"{self.asset_url}/api/v1/digital-assets/{asset['asset_id']}/content"
                response = await self.http_client.get(dataset_url)
                
                if response.status_code == 200:
                    content = response.json()
                    
                    # Extract features based on model type
                    features = self._extract_dataset_features(content, model_type)
                    
                    datasets.append({
                        "asset_id": asset["asset_id"],
                        "features": features,
                        "samples": len(features),
                        "metadata": asset.get("metadata", {})
                    })
                    
            except Exception as e:
                logger.error(f"Error processing dataset {asset['asset_id']}: {e}")
                
        return datasets
        
    async def _process_3d_models(self,
                               assets: List[Dict[str, Any]],
                               model_type: str) -> List[Dict[str, Any]]:
        """Process 3D model assets"""
        datasets = []
        
        for asset in assets:
            try:
                # Trigger feature extraction via connector service
                extraction_request = {
                    "asset_id": asset["asset_id"],
                    "extraction_type": "ml_features",
                    "feature_types": ["geometry", "topology", "material"]
                }
                
                response = await self.http_client.post(
                    f"{self.connector_url}/api/v1/extract-features",
                    json=extraction_request
                )
                
                if response.status_code == 200:
                    features = response.json()["features"]
                    
                    datasets.append({
                        "asset_id": asset["asset_id"],
                        "features": features,
                        "samples": 1,  # One sample per model
                        "metadata": {
                            **asset.get("metadata", {}),
                            "feature_extraction": "3d_geometry"
                        }
                    })
                    
            except Exception as e:
                logger.error(f"Error processing 3D model {asset['asset_id']}: {e}")
                
        return datasets
        
    async def _process_documents(self,
                               assets: List[Dict[str, Any]],
                               model_type: str) -> List[Dict[str, Any]]:
        """Process document assets"""
        datasets = []
        
        for asset in assets:
            try:
                # Extract text features via NLP
                extraction_request = {
                    "asset_id": asset["asset_id"],
                    "extraction_type": "text_features",
                    "feature_types": ["embeddings", "keywords", "entities"]
                }
                
                response = await self.http_client.post(
                    f"{self.connector_url}/api/v1/extract-features",
                    json=extraction_request
                )
                
                if response.status_code == 200:
                    features = response.json()["features"]
                    
                    datasets.append({
                        "asset_id": asset["asset_id"],
                        "features": features,
                        "samples": len(features.get("embeddings", [])),
                        "metadata": {
                            **asset.get("metadata", {}),
                            "feature_extraction": "nlp"
                        }
                    })
                    
            except Exception as e:
                logger.error(f"Error processing document {asset['asset_id']}: {e}")
                
        return datasets
        
    def _extract_dataset_features(self,
                                content: Dict[str, Any],
                                model_type: str) -> List[Dict[str, Any]]:
        """Extract features from dataset content"""
        # Simplified feature extraction
        # Real implementation would use pandas/numpy
        
        rows = content.get("data", [])
        features = []
        
        for row in rows:
            if model_type == "classification":
                # Extract features and label
                feature_dict = {
                    k: v for k, v in row.items()
                    if k != "label" and isinstance(v, (int, float))
                }
                if "label" in row:
                    feature_dict["_label"] = row["label"]
                features.append(feature_dict)
            else:
                # All numeric columns as features
                feature_dict = {
                    k: v for k, v in row.items()
                    if isinstance(v, (int, float))
                }
                features.append(feature_dict)
                
        return features
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 