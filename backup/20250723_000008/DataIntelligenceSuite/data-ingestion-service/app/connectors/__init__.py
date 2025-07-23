"""
Data ingestion connectors
"""

from .base import BaseIngestionConnector
from .crm.suitecrm import SuiteCRMConnector
from .crm.metasfresh import MetasfreshConnector
from .api.openstreetmap import OpenStreetMapConnector
from .webhook.generic import WebhookConnector

# Registry of available connectors
CONNECTOR_REGISTRY = {
    "suitecrm": SuiteCRMConnector,
    "metasfresh": MetasfreshConnector,
    "openstreetmap": OpenStreetMapConnector,
    "webhook": WebhookConnector
}

__all__ = [
    "BaseIngestionConnector",
    "SuiteCRMConnector", 
    "MetasfreshConnector",
    "OpenStreetMapConnector",
    "WebhookConnector",
    "CONNECTOR_REGISTRY"
] 