from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

class MLSessionNotification(BaseModel):
    """Notification about ML training session for simulation"""
    fl_session_id: str = Field(..., description="Federated learning session ID")
    model_type: str = Field(..., description="Type of model being trained")
    status: str = Field(..., description="Status of the ML session")
    metadata: Optional[Dict[str, Any]] = Field(default={}, description="Additional metadata") 