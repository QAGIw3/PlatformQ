"""
Real-time Analytics Integrations

Provides integrations with real-time OLAP and analytics systems.
"""

from .pinot_client import PinotClient, PinotConfig, TableSchema, TableConfig, TableType
from .clickhouse_client import (
    ClickHouseClient, ClickHouseConfig, TableDefinition, TableColumn,
    Engine, DataType, QueryResult
)
from .doris_client import (
    DorisClient, DorisConfig, Column, TableDefinition as DorisTableDefinition,
    TableModel, StreamLoadResult
)

__all__ = [
    # Pinot
    "PinotClient",
    "PinotConfig",
    "TableSchema",
    "TableConfig",
    "TableType",
    
    # ClickHouse
    "ClickHouseClient",
    "ClickHouseConfig",
    "TableDefinition",
    "TableColumn",
    "Engine",
    "DataType",
    "QueryResult",
    
    # Doris
    "DorisClient",
    "DorisConfig",
    "Column",
    "DorisTableDefinition",
    "TableModel",
    "StreamLoadResult"
] 