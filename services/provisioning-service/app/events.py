"""Event definitions for the provisioning service"""

from typing import Dict, Any
from platformq_shared.events import BaseEvent


class ResourceAnomalyEvent(BaseEvent):
    """Event for resource anomalies detected in the platform"""
    service_name: str
    namespace: str
    anomaly_type: str
    severity: float
    timestamp: str
    details: Dict[str, Any] 