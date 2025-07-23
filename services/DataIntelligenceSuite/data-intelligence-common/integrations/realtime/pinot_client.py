"""
Apache Pinot Client Integration

Provides real-time distributed OLAP datastore capabilities.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
import requests
from pinotdb import connect

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TableType(str, Enum):
    """Pinot table types"""
    OFFLINE = "OFFLINE"
    REALTIME = "REALTIME"
    HYBRID = "HYBRID"


class SegmentAssignmentStrategy(str, Enum):
    """Segment assignment strategies"""
    BALANCED_NUM_SEGMENT = "BalancedNumSegmentAssignmentStrategy"
    REPLICA_GROUP = "ReplicaGroupSegmentAssignmentStrategy"
    PARTITION_AWARE = "PartitionAwareOfflineSegmentAssignmentStrategy"


class StreamType(str, Enum):
    """Supported stream types"""
    KAFKA = "kafka"
    PULSAR = "pulsar"
    KINESIS = "kinesis"


@dataclass
class PinotConfig(ClientConfig):
    """Configuration for Pinot client"""
    # Connection settings
    controller_host: str = "localhost"
    controller_port: int = 9000
    broker_host: str = "localhost"
    broker_port: int = 8099
    
    # Authentication
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Query settings
    query_timeout_ms: int = 30000
    enable_query_options: bool = True
    
    # Table defaults
    default_replication: int = 1
    default_segment_push_type: str = "APPEND"
    
    # Performance
    connection_pool_size: int = 10
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "pinot"


@dataclass
class TableSchema:
    """Pinot table schema"""
    schema_name: str
    dimension_fields: List[Dict[str, str]]
    metric_fields: List[Dict[str, str]]
    time_field: Dict[str, str]
    date_time_fields: Optional[List[Dict[str, str]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Pinot schema format"""
        schema = {
            "schemaName": self.schema_name,
            "dimensionFieldSpecs": self.dimension_fields,
            "metricFieldSpecs": self.metric_fields,
            "timeFieldSpec": self.time_field
        }
        
        if self.date_time_fields:
            schema["dateTimeFieldSpecs"] = self.date_time_fields
        
        return schema


@dataclass
class TableConfig:
    """Pinot table configuration"""
    table_name: str
    table_type: TableType
    
    # Segmentation
    time_column: str
    time_unit: str = "DAYS"
    retention_time_value: int = 30
    retention_time_unit: str = "DAYS"
    segment_push_type: str = "APPEND"
    
    # Replication and assignment
    replication: int = 1
    segment_assignment_strategy: SegmentAssignmentStrategy = SegmentAssignmentStrategy.BALANCED_NUM_SEGMENT
    
    # Indexing
    inverted_index_columns: List[str] = field(default_factory=list)
    sorted_column: Optional[str] = None
    bloom_filter_columns: List[str] = field(default_factory=list)
    
    # Stream ingestion (for realtime tables)
    stream_type: Optional[StreamType] = None
    stream_topic: Optional[str] = None
    stream_bootstrap_servers: Optional[str] = None
    stream_consumer_type: str = "lowlevel"
    
    # Routing
    routing_config: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Pinot table config format"""
        config = {
            "tableName": self.table_name,
            "tableType": self.table_type.value,
            "segmentsConfig": {
                "timeColumnName": self.time_column,
                "timeType": self.time_unit,
                "retentionTimeValue": str(self.retention_time_value),
                "retentionTimeUnit": self.retention_time_unit,
                "segmentPushType": self.segment_push_type,
                "replication": str(self.replication),
                "segmentAssignmentStrategy": self.segment_assignment_strategy.value
            },
            "tableIndexConfig": {
                "loadMode": "MMAP"
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant"
            },
            "metadata": {}
        }
        
        # Add indexing config
        if self.inverted_index_columns:
            config["tableIndexConfig"]["invertedIndexColumns"] = self.inverted_index_columns
        
        if self.sorted_column:
            config["tableIndexConfig"]["sortedColumn"] = [self.sorted_column]
        
        if self.bloom_filter_columns:
            config["tableIndexConfig"]["bloomFilterColumns"] = self.bloom_filter_columns
        
        # Add stream config for realtime tables
        if self.table_type in [TableType.REALTIME, TableType.HYBRID] and self.stream_type:
            config["streamConfigs"] = {
                "streamType": self.stream_type.value,
                f"{self.stream_type.value}.consumer.type": self.stream_consumer_type,
                f"{self.stream_type.value}.topic.name": self.stream_topic,
                f"{self.stream_type.value}.bootstrap.servers": self.stream_bootstrap_servers,
                "realtime.segment.flush.threshold.rows": "10000",
                "realtime.segment.flush.threshold.time": "1h"
            }
        
        # Add routing config
        if self.routing_config:
            config["routing"] = self.routing_config
        
        return config


@dataclass
class QueryResult:
    """Query execution result"""
    rows: List[List[Any]]
    columns: List[str]
    row_count: int
    execution_time_ms: int
    exceptions: List[str] = field(default_factory=list)
    
    def to_dataframe(self):
        """Convert to pandas DataFrame"""
        import pandas as pd
        return pd.DataFrame(self.rows, columns=self.columns)


class PinotClient(BaseServiceClient):
    """
    Apache Pinot client for real-time OLAP.
    
    Features:
    - Real-time and batch ingestion
    - Low latency queries
    - Distributed architecture
    - Column indexing
    - Star-tree indexes
    - Upserts support
    """
    
    def __init__(
        self,
        config: Optional[PinotConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = PinotConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: PinotConfig = config
        self._connection = None
        self._session = None
        
    async def connect(self):
        """Connect to Pinot cluster"""
        await super().connect()
        
        try:
            # Get credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.username = creds.get("username")
                    self.config.password = creds.get("password")
            
            # Create broker connection for queries
            self._connection = connect(
                host=self.config.broker_host,
                port=self.config.broker_port,
                path="/query/sql",
                scheme="http" if not self.config.use_ssl else "https",
                username=self.config.username,
                password=self.config.password,
                verify_ssl=self.config.verify_ssl if self.config.use_ssl else False
            )
            
            # Create session for controller API
            self._session = requests.Session()
            if self.config.username and self.config.password:
                self._session.auth = (self.config.username, self.config.password)
            
            logger.info(f"Connected to Pinot cluster: {self.config.controller_host}:{self.config.controller_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Pinot: {e}")
            raise
    
    async def create_schema(
        self,
        schema: TableSchema
    ) -> bool:
        """
        Create table schema.
        
        Args:
            schema: Table schema definition
            
        Returns:
            Success status
        """
        try:
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/schemas"
            
            response = self._session.post(
                url,
                json=schema.to_dict(),
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"Created schema: {schema.schema_name}")
                return True
            else:
                logger.error(f"Failed to create schema: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    async def create_table(
        self,
        config: TableConfig
    ) -> bool:
        """
        Create table with configuration.
        
        Args:
            config: Table configuration
            
        Returns:
            Success status
        """
        try:
            # Determine endpoint based on table type
            if config.table_type == TableType.OFFLINE:
                endpoint = "tables/offline"
            elif config.table_type == TableType.REALTIME:
                endpoint = "tables/realtime"
            else:
                # For hybrid, create both
                offline_config = config.to_dict()
                offline_config["tableType"] = "OFFLINE"
                realtime_config = config.to_dict()
                realtime_config["tableType"] = "REALTIME"
                
                # Create offline table
                url = f"http://{self.config.controller_host}:{self.config.controller_port}/tables/offline"
                response = self._session.post(
                    url,
                    json=offline_config,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to create offline table: {response.text}")
                    return False
                
                    # Create realtime table
                    endpoint = "tables/realtime"
                    config_dict = realtime_config
                else:
                    config_dict = config.to_dict()
                
                url = f"http://{self.config.controller_host}:{self.config.controller_port}/{endpoint}"
                
                response = self._session.post(
                    url,
                    json=config_dict if config.table_type != TableType.HYBRID else realtime_config,
                    headers={"Content-Type": "application/json"}
                )
            
            if response.status_code == 200:
                logger.info(f"Created table: {config.table_name} ({config.table_type.value})")
                return True
            else:
                logger.error(f"Failed to create table: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    async def query(
        self,
        sql: str,
        options: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """
        Execute SQL query.
        
        Args:
            sql: SQL query
            options: Query options
            
        Returns:
            Query result
        """
        try:
            cursor = self._connection.cursor()
            
            # Set query options
            if self.config.enable_query_options and options:
                for key, value in options.items():
                    cursor.execute(f"SET {key} = {value}")
            
            # Set timeout
            cursor.execute(f"SET timeoutMs = {self.config.query_timeout_ms}")
            
            # Execute query
            start_time = datetime.now()
            cursor.execute(sql)
            
            # Fetch results
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return QueryResult(
                rows=rows,
                columns=columns,
                row_count=len(rows),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return QueryResult(
                rows=[],
                columns=[],
                row_count=0,
                execution_time_ms=0,
                exceptions=[str(e)]
            )
    
    async def ingest_batch(
        self,
        table_name: str,
        data_file_path: str,
        format: str = "csv"
    ) -> bool:
        """
        Ingest batch data from file.
        
        Args:
            table_name: Target table
            data_file_path: Path to data file
            format: File format (csv, json, avro, parquet)
            
        Returns:
            Success status
        """
        try:
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/ingestFromFile"
            
            params = {
                "tableName": table_name,
                "batchConfigMapStr": json.dumps({
                    "inputFormat": format,
                    "recordReader.prop.delimiter": ","
                })
            }
            
            with open(data_file_path, 'rb') as f:
                files = {'file': f}
                response = self._session.post(url, params=params, files=files)
            
            if response.status_code == 200:
                logger.info(f"Ingested batch data to table: {table_name}")
                return True
            else:
                logger.error(f"Failed to ingest batch data: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to ingest batch data: {e}")
            return False
    
    async def get_table_stats(
        self,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Get table statistics.
        
        Args:
            table_name: Table name
            
        Returns:
            Table statistics
        """
        try:
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/tables/{table_name}/stats"
            
            response = self._session.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get table stats: {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}
    
    async def get_table_segments(
        self,
        table_name: str,
        table_type: Optional[TableType] = None
    ) -> List[str]:
        """
        Get table segments.
        
        Args:
            table_name: Table name
            table_type: Table type (offline/realtime)
            
        Returns:
            List of segment names
        """
        try:
            if table_type:
                endpoint = f"tables/{table_name}/segments?type={table_type.value}"
            else:
                endpoint = f"tables/{table_name}/segments"
            
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/{endpoint}"
            
            response = self._session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                segments = []
                
                if "OFFLINE" in data:
                    segments.extend(data["OFFLINE"])
                if "REALTIME" in data:
                    segments.extend(data["REALTIME"])
                
                return segments
            else:
                logger.error(f"Failed to get segments: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to get segments: {e}")
            return []
    
    async def reload_segments(
        self,
        table_name: str,
        segment_names: Optional[List[str]] = None
    ) -> bool:
        """
        Reload table segments.
        
        Args:
            table_name: Table name
            segment_names: Specific segments to reload (all if None)
            
        Returns:
            Success status
        """
        try:
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/tables/{table_name}/segments/reload"
            
            params = {}
            if segment_names:
                params["segmentNames"] = ",".join(segment_names)
            
            response = self._session.post(url, params=params)
            
            if response.status_code == 200:
                logger.info(f"Reloaded segments for table: {table_name}")
                return True
            else:
                logger.error(f"Failed to reload segments: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to reload segments: {e}")
            return False
    
    async def delete_table(
        self,
        table_name: str,
        table_type: Optional[TableType] = None
    ) -> bool:
        """
        Delete table.
        
        Args:
            table_name: Table name
            table_type: Table type to delete (all if None)
            
        Returns:
            Success status
        """
        try:
            if table_type:
                endpoint = f"tables/{table_name}?type={table_type.value}"
            else:
                endpoint = f"tables/{table_name}"
            
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/{endpoint}"
            
            response = self._session.delete(url)
            
            if response.status_code == 200:
                logger.info(f"Deleted table: {table_name}")
                return True
            else:
                logger.error(f"Failed to delete table: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete table: {e}")
            return False
    
    async def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get cluster information.
        
        Returns:
            Cluster information
        """
        try:
            info = {}
            
            # Get controller info
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/cluster/info"
            response = self._session.get(url)
            if response.status_code == 200:
                info["cluster"] = response.json()
            
            # Get instances
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/instances"
            response = self._session.get(url)
            if response.status_code == 200:
                info["instances"] = response.json()
            
            # Get tenants
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/tenants"
            response = self._session.get(url)
            if response.status_code == 200:
                info["tenants"] = response.json()
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get cluster info: {e}")
            return {}
    
    async def create_star_tree_index(
        self,
        table_name: str,
        dimension_columns: List[str],
        metric_columns: List[str],
        function_column_pairs: List[Tuple[str, str]],
        max_leaf_records: int = 10000
    ) -> bool:
        """
        Create star-tree index for fast aggregations.
        
        Args:
            table_name: Table name
            dimension_columns: Dimension columns
            metric_columns: Metric columns
            function_column_pairs: List of (function, column) pairs
            max_leaf_records: Maximum records per leaf
            
        Returns:
            Success status
        """
        try:
            # Get current table config
            url = f"http://{self.config.controller_host}:{self.config.controller_port}/tables/{table_name}"
            response = self._session.get(url)
            
            if response.status_code != 200:
                logger.error(f"Failed to get table config: {response.text}")
                return False
            
            table_config = response.json()
            
            # Add star-tree index config
            star_tree_config = {
                "dimensionsSplitOrder": dimension_columns,
                "functionColumnPairs": [f"{func}__{col}" for func, col in function_column_pairs],
                "maxLeafRecords": max_leaf_records
            }
            
            if "tableIndexConfig" not in table_config:
                table_config["tableIndexConfig"] = {}
            
            table_config["tableIndexConfig"]["starTreeIndexConfigs"] = [star_tree_config]
            
            # Update table config
            response = self._session.put(
                url,
                json=table_config,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"Created star-tree index for table: {table_name}")
                return True
            else:
                logger.error(f"Failed to create star-tree index: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to create star-tree index: {e}")
            return False
    
    async def close(self):
        """Close Pinot connections"""
        if self._connection:
            self._connection.close()
        
        if self._session:
            self._session.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Pinot specific configuration"""
        return {
            "controller_host": self.config.controller_host,
            "controller_port": self.config.controller_port,
            "broker_host": self.config.broker_host,
            "broker_port": self.config.broker_port,
            "query_timeout_ms": self.config.query_timeout_ms,
            "default_replication": self.config.default_replication
        } 