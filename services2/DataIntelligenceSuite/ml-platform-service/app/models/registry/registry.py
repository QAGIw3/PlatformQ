"""
ML Model Registry
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from data_intelligence_common.core.ml import ModelRegistry

class MLPlatformRegistry(ModelRegistry):
    """Enhanced model registry for ML platform"""
    
    async def register_model(
        self,
        name: str,
        version: str,
        model_path: str,
        metadata: Dict[str, Any],
        tags: Optional[List[str]] = None
    ):
        """Register a new model version"""
        model_info = {
            "name": name,
            "version": version,
            "path": model_path,
            "metadata": metadata,
            "tags": tags or [],
            "registered_at": datetime.utcnow(),
            "status": "pending_validation"
        }
        
        # Validate model
        await self._validate_model(model_path)
        
        # Register
        return await super().register(model_info)
