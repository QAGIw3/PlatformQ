"""
Federation components for cross-datasource queries
"""
from .federated_query_engine import FederatedQueryEngine, QueryResult
from .trino_client import TrinoQueryExecutor

__all__ = [
    "FederatedQueryEngine",
    "QueryResult",
    "TrinoQueryExecutor"
]
