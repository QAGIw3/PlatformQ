"""
GraphQL Federation for Data Catalog Hub

Provides a federated GraphQL API for unified metadata access across services.
"""

from .schema import schema
from .federation import federation_schema

__all__ = ["schema", "federation_schema"] 