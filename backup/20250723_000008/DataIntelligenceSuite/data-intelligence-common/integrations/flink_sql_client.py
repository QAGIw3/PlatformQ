"""
Apache Flink SQL Client Integration

Provides streaming SQL capabilities with unified batch and stream processing.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
import asyncio
import aiohttp
from py4j.java_gateway import JavaGateway, GatewayParameters

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ExecutionMode(str, Enum):
    """Flink SQL execution modes"""
    STREAMING = "streaming"
    BATCH = "batch"


class ResultMode(str, Enum):
    """Result retrieval modes"""
    TABLE = "table"
    CHANGELOG = "changelog"
    TABLEAU = "tableau"


class SavepointRestoreMode(str, Enum):
    """Savepoint restore modes"""
    NO_CLAIM = "NO_CLAIM"
    CLAIM = "CLAIM"
    LEGACY = "LEGACY"


class CatalogType(str, Enum):
    """Catalog types"""
    HIVE = "hive"
    JDBC = "jdbc"
    ICEBERG = "iceberg"
    PULSAR = "pulsar"
    KAFKA = "kafka"


class WatermarkStrategy(str, Enum):
    """Watermark strategies"""
    BOUNDED_OUT_OF_ORDERNESS = "bounded-out-of-orderness"
    ASCENDING = "ascending"
    NO_WATERMARKS = "no-watermarks"


@dataclass
class FlinkSQLConfig(ClientConfig):
    """Configuration for Flink SQL client"""
    # Gateway settings
    gateway_host: str = "localhost"
    gateway_port: int = 8083
    rest_port: int = 8081
    
    # Execution settings
    default_mode: ExecutionMode = ExecutionMode.STREAMING
    default_parallelism: int = 1
    checkpoint_interval_ms: int = 60000
    
    # State backend
    state_backend: str = "rocksdb"
    state_backend_path: str = "file:///tmp/flink-state"
    
    # Catalog settings
    default_catalog: str = "default_catalog"
    default_database: str = "default_database"
    
    # Table settings
    table_exec_resource_default_parallelism: int = 1
    table_exec_sink_not_null_enforcer: str = "error"
    
    # SQL settings
    sql_dialect: str = "default"  # or "hive"
    table_sql_dialect: str = "default"
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "flink-sql"


@dataclass
class TableSchema:
    """Table schema definition"""
    columns: List[Dict[str, str]]  # name, type, comment
    watermark: Optional[Dict[str, str]] = None  # column, strategy, delay
    primary_key: Optional[List[str]] = None
    partitioned_by: Optional[List[str]] = None
    
    def to_ddl(self) -> str:
        """Convert to DDL string"""
        # Columns
        column_defs = []
        for col in self.columns:
            col_def = f"`{col['name']}` {col['type']}"
            if col.get('comment'):
                col_def += f" COMMENT '{col['comment']}'"
            column_defs.append(col_def)
        
        # Watermark
        if self.watermark:
            wm = self.watermark
            watermark_def = f"WATERMARK FOR `{wm['column']}` AS `{wm['column']}` - INTERVAL '{wm['delay']}' SECOND"
            column_defs.append(watermark_def)
        
        # Primary key
        if self.primary_key:
            pk_def = f"PRIMARY KEY ({', '.join(f'`{k}`' for k in self.primary_key)}) NOT ENFORCED"
            column_defs.append(pk_def)
        
        ddl = f"(\n  {',\\n  '.join(column_defs)}\n)"
        
        # Partitions
        if self.partitioned_by:
            ddl += f" PARTITIONED BY ({', '.join(f'`{p}`' for p in self.partitioned_by)})"
        
        return ddl


@dataclass
class TableDescriptor:
    """Table descriptor for dynamic tables"""
    name: str
    schema: TableSchema
    connector: str
    properties: Dict[str, str]
    
    def to_ddl(self) -> str:
        """Convert to CREATE TABLE DDL"""
        ddl = f"CREATE TABLE `{self.name}` {self.schema.to_ddl()}"
        
        # WITH clause
        with_items = [f"'connector' = '{self.connector}'"]
        for key, value in self.properties.items():
            with_items.append(f"'{key}' = '{value}'")
        
        ddl += f" WITH (\n  {',\\n  '.join(with_items)}\n)"
        
        return ddl


@dataclass
class QueryResult:
    """SQL query result"""
    columns: List[str]
    types: List[str]
    data: List[List[Any]]
    job_id: Optional[str] = None
    is_streaming: bool = False
    
    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries"""
        return [dict(zip(self.columns, row)) for row in self.data]


@dataclass
class JobStatus:
    """Flink job status"""
    job_id: str
    name: str
    state: str
    start_time: datetime
    duration: int
    vertices: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def is_running(self) -> bool:
        return self.state in ["RUNNING", "CREATED", "DEPLOYING"]
    
    @property
    def is_finished(self) -> bool:
        return self.state in ["FINISHED", "CANCELED", "FAILED"]


class FlinkSQLClient(BaseServiceClient):
    """
    Apache Flink SQL client for streaming SQL.
    
    Features:
    - Unified batch and streaming SQL
    - Dynamic tables and continuous queries
    - Exactly-once processing
    - Event time semantics
    - State management
    - Multiple catalog support
    """
    
    def __init__(
        self,
        config: Optional[FlinkSQLConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = FlinkSQLConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: FlinkSQLConfig = config
        self._gateway: Optional[JavaGateway] = None
        self._table_env = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._catalogs: Dict[str, Any] = {}
        
    async def connect(self):
        """Connect to Flink SQL Gateway"""
        await super().connect()
        
        try:
            # Create HTTP session for REST API
            self._session = aiohttp.ClientSession()
            
            # Connect to Py4J gateway
            gateway_params = GatewayParameters(
                host=self.config.gateway_host,
                port=self.config.gateway_port,
                auto_convert=True
            )
            self._gateway = JavaGateway(gateway_parameters=gateway_params)
            
            # Get table environment
            if self.config.default_mode == ExecutionMode.STREAMING:
                self._table_env = self._gateway.entry_point.getStreamTableEnvironment()
            else:
                self._table_env = self._gateway.entry_point.getBatchTableEnvironment()
            
            # Configure environment
            await self._configure_environment()
            
            logger.info(f"Connected to Flink SQL Gateway: {self.config.gateway_host}:{self.config.gateway_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Flink SQL: {e}")
            raise
    
    async def _configure_environment(self):
        """Configure Flink environment"""
        config = self._table_env.getConfig()
        
        # Set execution mode
        if self.config.default_mode == ExecutionMode.STREAMING:
            config.getConfiguration().setString(
                "execution.runtime-mode", "streaming"
            )
        else:
            config.getConfiguration().setString(
                "execution.runtime-mode", "batch"
            )
        
        # Set parallelism
        config.getConfiguration().setInteger(
            "parallelism.default", self.config.default_parallelism
        )
        
        # Set checkpoint interval
        config.getConfiguration().setLong(
            "execution.checkpointing.interval", self.config.checkpoint_interval_ms
        )
        
        # Set state backend
        config.getConfiguration().setString(
            "state.backend", self.config.state_backend
        )
        config.getConfiguration().setString(
            "state.checkpoints.dir", f"{self.config.state_backend_path}/checkpoints"
        )
        config.getConfiguration().setString(
            "state.savepoints.dir", f"{self.config.state_backend_path}/savepoints"
        )
        
        # Set SQL dialect
        config.setSqlDialect(self.config.sql_dialect)
    
    async def execute_sql(
        self,
        sql: str,
        mode: Optional[ExecutionMode] = None,
        result_mode: ResultMode = ResultMode.TABLE
    ) -> Union[QueryResult, str]:
        """
        Execute SQL statement.
        
        Args:
            sql: SQL statement
            mode: Execution mode (overrides default)
            result_mode: Result retrieval mode
            
        Returns:
            Query result or job ID for async operations
        """
        try:
            # Execute SQL
            table_result = self._table_env.executeSql(sql)
            
            # Get job ID if available
            job_id = None
            job_client = table_result.getJobClient()
            if job_client.isPresent():
                job_id = str(job_client.get().getJobID())
            
            # For DDL operations, return job ID
            if self._is_ddl(sql):
                return job_id or "DDL_EXECUTED"
            
            # For DML/DQL operations, collect results
            if result_mode == ResultMode.TABLE:
                # Collect results (blocking for batch, limited for streaming)
                result_kind = table_result.getResultKind()
                
                if result_kind.name() == "SUCCESS_WITH_CONTENT":
                    # Collect results
                    schema = table_result.getTableSchema()
                    columns = [str(f.getName()) for f in schema.getFields()]
                    types = [str(f.getDataType()) for f in schema.getFields()]
                    
                    # Collect data
                    data = []
                    with table_result.collect() as results:
                        for row in results:
                            data.append([self._convert_value(v) for v in row])
                            # Limit streaming results
                            if mode == ExecutionMode.STREAMING and len(data) >= 1000:
                                break
                    
                    return QueryResult(
                        columns=columns,
                        types=types,
                        data=data,
                        job_id=job_id,
                        is_streaming=mode == ExecutionMode.STREAMING
                    )
                else:
                    # No content (e.g., INSERT)
                    return job_id or "SUCCESS"
            
            else:
                # Return job ID for async processing
                return job_id or "SUBMITTED"
                
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            raise
    
    def _is_ddl(self, sql: str) -> bool:
        """Check if SQL is DDL statement"""
        sql_upper = sql.strip().upper()
        ddl_keywords = ["CREATE", "ALTER", "DROP", "TRUNCATE"]
        return any(sql_upper.startswith(kw) for kw in ddl_keywords)
    
    def _convert_value(self, value) -> Any:
        """Convert Java value to Python"""
        if hasattr(value, 'toString'):
            return str(value.toString())
        return value
    
    async def create_table(
        self,
        table: TableDescriptor
    ) -> bool:
        """
        Create table.
        
        Args:
            table: Table descriptor
            
        Returns:
            Success status
        """
        try:
            ddl = table.to_ddl()
            result = await self.execute_sql(ddl)
            logger.info(f"Created table: {table.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    async def create_kafka_table(
        self,
        table_name: str,
        topic: str,
        schema: TableSchema,
        bootstrap_servers: str,
        format: str = "json",
        scan_startup_mode: str = "latest-offset",
        properties: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create Kafka source/sink table.
        
        Args:
            table_name: Table name
            topic: Kafka topic
            schema: Table schema
            bootstrap_servers: Kafka brokers
            format: Data format (json, avro, csv)
            scan_startup_mode: Scan mode
            properties: Additional properties
            
        Returns:
            Success status
        """
        try:
            props = {
                "topic": topic,
                "properties.bootstrap.servers": bootstrap_servers,
                "format": format,
                "scan.startup.mode": scan_startup_mode
            }
            
            if properties:
                props.update(properties)
            
            table = TableDescriptor(
                name=table_name,
                schema=schema,
                connector="kafka",
                properties=props
            )
            
            return await self.create_table(table)
            
        except Exception as e:
            logger.error(f"Failed to create Kafka table: {e}")
            return False
    
    async def create_iceberg_table(
        self,
        table_name: str,
        schema: TableSchema,
        warehouse_path: str,
        catalog_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> bool:
        """
        Create Iceberg table.
        
        Args:
            table_name: Table name
            schema: Table schema
            warehouse_path: Iceberg warehouse path
            catalog_name: Catalog name
            database_name: Database name
            
        Returns:
            Success status
        """
        try:
            # Use fully qualified name if catalog/database provided
            if catalog_name and database_name:
                full_name = f"`{catalog_name}`.`{database_name}`.`{table_name}`"
            else:
                full_name = f"`{table_name}`"
            
            props = {
                "warehouse": warehouse_path,
                "catalog-type": "hadoop"
            }
            
            table = TableDescriptor(
                name=full_name,
                schema=schema,
                connector="iceberg",
                properties=props
            )
            
            return await self.create_table(table)
            
        except Exception as e:
            logger.error(f"Failed to create Iceberg table: {e}")
            return False
    
    async def create_view(
        self,
        view_name: str,
        query: str,
        temporary: bool = False
    ) -> bool:
        """
        Create view.
        
        Args:
            view_name: View name
            query: SELECT query
            temporary: Create temporary view
            
        Returns:
            Success status
        """
        try:
            temp = "TEMPORARY" if temporary else ""
            sql = f"CREATE {temp} VIEW `{view_name}` AS {query}"
            
            await self.execute_sql(sql)
            logger.info(f"Created view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create view: {e}")
            return False
    
    async def insert_into(
        self,
        target_table: str,
        source_query: str,
        mode: ExecutionMode = ExecutionMode.STREAMING
    ) -> str:
        """
        Insert data into table.
        
        Args:
            target_table: Target table
            source_query: Source SELECT query
            mode: Execution mode
            
        Returns:
            Job ID
        """
        try:
            sql = f"INSERT INTO `{target_table}` {source_query}"
            job_id = await self.execute_sql(sql, mode=mode)
            
            logger.info(f"Started insert job: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to insert data: {e}")
            raise
    
    async def create_continuous_query(
        self,
        name: str,
        query: str,
        sink_table: str,
        checkpoint_interval_ms: Optional[int] = None
    ) -> str:
        """
        Create continuous query.
        
        Args:
            name: Query name
            query: SELECT query
            sink_table: Sink table
            checkpoint_interval_ms: Checkpoint interval
            
        Returns:
            Job ID
        """
        try:
            # Set checkpoint interval if specified
            if checkpoint_interval_ms:
                config_sql = f"SET 'execution.checkpointing.interval' = '{checkpoint_interval_ms}ms'"
                await self.execute_sql(config_sql)
            
            # Create continuous query
            sql = f"""
            INSERT INTO `{sink_table}`
            {query}
            """
            
            job_id = await self.execute_sql(sql, mode=ExecutionMode.STREAMING)
            
            logger.info(f"Created continuous query '{name}': {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create continuous query: {e}")
            raise
    
    async def execute_statement_set(
        self,
        statements: List[str]
    ) -> str:
        """
        Execute statement set (multiple INSERTs).
        
        Args:
            statements: List of INSERT statements
            
        Returns:
            Job ID
        """
        try:
            # Create statement set
            statement_set = self._table_env.createStatementSet()
            
            for stmt in statements:
                statement_set.addInsertSql(stmt)
            
            # Execute
            table_result = statement_set.execute()
            
            # Get job ID
            job_client = table_result.getJobClient()
            if job_client.isPresent():
                job_id = str(job_client.get().getJobID())
                logger.info(f"Executed statement set: {job_id}")
                return job_id
            
            return "STATEMENT_SET_EXECUTED"
            
        except Exception as e:
            logger.error(f"Failed to execute statement set: {e}")
            raise
    
    async def get_job_status(
        self,
        job_id: str
    ) -> JobStatus:
        """
        Get job status.
        
        Args:
            job_id: Flink job ID
            
        Returns:
            Job status
        """
        try:
            url = f"http://{self.config.gateway_host}:{self.config.rest_port}/jobs/{job_id}"
            
            async with self._session.get(url) as response:
                data = await response.json()
                
                return JobStatus(
                    job_id=data["jid"],
                    name=data["name"],
                    state=data["state"],
                    start_time=datetime.fromtimestamp(data["start-time"] / 1000),
                    duration=data["duration"],
                    vertices=data.get("vertices", [])
                )
                
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            raise
    
    async def cancel_job(
        self,
        job_id: str,
        savepoint_path: Optional[str] = None
    ) -> bool:
        """
        Cancel job.
        
        Args:
            job_id: Job ID
            savepoint_path: Path to save savepoint
            
        Returns:
            Success status
        """
        try:
            url = f"http://{self.config.gateway_host}:{self.config.rest_port}/jobs/{job_id}"
            
            params = {}
            if savepoint_path:
                params["mode"] = "cancel"
                params["targetDirectory"] = savepoint_path
            
            async with self._session.patch(url, params=params) as response:
                if response.status == 202:
                    logger.info(f"Cancelled job: {job_id}")
                    return True
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to cancel job: {e}")
            return False
    
    async def create_savepoint(
        self,
        job_id: str,
        savepoint_path: Optional[str] = None
    ) -> str:
        """
        Create savepoint.
        
        Args:
            job_id: Job ID
            savepoint_path: Savepoint directory
            
        Returns:
            Savepoint path
        """
        try:
            url = f"http://{self.config.gateway_host}:{self.config.rest_port}/jobs/{job_id}/savepoints"
            
            data = {}
            if savepoint_path:
                data["target-directory"] = savepoint_path
            
            async with self._session.post(url, json=data) as response:
                result = await response.json()
                trigger_id = result["request-id"]
            
            # Poll for completion
            status_url = f"{url}/{trigger_id}"
            while True:
                async with self._session.get(status_url) as response:
                    status = await response.json()
                    
                    if status["status"]["id"] == "COMPLETED":
                        savepoint = status["operation"]["location"]
                        logger.info(f"Created savepoint: {savepoint}")
                        return savepoint
                    elif status["status"]["id"] == "FAILED":
                        raise Exception(f"Savepoint failed: {status.get('operation', {}).get('failure-cause')}")
                
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Failed to create savepoint: {e}")
            raise
    
    async def restore_from_savepoint(
        self,
        sql: str,
        savepoint_path: str,
        allow_non_restored_state: bool = False
    ) -> str:
        """
        Restore job from savepoint.
        
        Args:
            sql: SQL statement to execute
            savepoint_path: Savepoint path
            allow_non_restored_state: Allow non-restored state
            
        Returns:
            Job ID
        """
        try:
            # Set savepoint restore options
            config = self._table_env.getConfig().getConfiguration()
            config.setString("execution.savepoint.path", savepoint_path)
            config.setBoolean(
                "execution.savepoint.ignore-unclaimed-state",
                allow_non_restored_state
            )
            
            # Execute SQL
            job_id = await self.execute_sql(sql, mode=ExecutionMode.STREAMING)
            
            logger.info(f"Restored job from savepoint: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to restore from savepoint: {e}")
            raise
    
    async def register_catalog(
        self,
        catalog_name: str,
        catalog_type: CatalogType,
        properties: Dict[str, str]
    ) -> bool:
        """
        Register catalog.
        
        Args:
            catalog_name: Catalog name
            catalog_type: Catalog type
            properties: Catalog properties
            
        Returns:
            Success status
        """
        try:
            # Build CREATE CATALOG statement
            props_str = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
            sql = f"""
            CREATE CATALOG `{catalog_name}` WITH (
                'type' = '{catalog_type.value}',
                {props_str}
            )
            """
            
            await self.execute_sql(sql)
            
            # Store catalog info
            self._catalogs[catalog_name] = {
                "type": catalog_type,
                "properties": properties
            }
            
            logger.info(f"Registered catalog: {catalog_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register catalog: {e}")
            return False
    
    async def use_catalog(
        self,
        catalog_name: str
    ) -> bool:
        """
        Use catalog.
        
        Args:
            catalog_name: Catalog name
            
        Returns:
            Success status
        """
        try:
            sql = f"USE CATALOG `{catalog_name}`"
            await self.execute_sql(sql)
            
            logger.info(f"Using catalog: {catalog_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to use catalog: {e}")
            return False
    
    async def show_tables(
        self,
        database: Optional[str] = None
    ) -> List[str]:
        """
        Show tables.
        
        Args:
            database: Database name
            
        Returns:
            List of table names
        """
        try:
            if database:
                sql = f"SHOW TABLES FROM `{database}`"
            else:
                sql = "SHOW TABLES"
            
            result = await self.execute_sql(sql)
            
            if isinstance(result, QueryResult):
                return [row[0] for row in result.data]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Failed to show tables: {e}")
            return []
    
    async def describe_table(
        self,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Describe table.
        
        Args:
            table_name: Table name
            
        Returns:
            Table description
        """
        try:
            sql = f"DESCRIBE `{table_name}`"
            result = await self.execute_sql(sql)
            
            if isinstance(result, QueryResult):
                columns = []
                for row in result.data:
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "true" if len(row) > 2 else True,
                        "key": row[3] if len(row) > 3 else None,
                        "computed": row[4] if len(row) > 4 else None,
                        "watermark": row[5] if len(row) > 5 else None
                    })
                
                return {
                    "table_name": table_name,
                    "columns": columns
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            return {}
    
    async def close(self):
        """Close Flink SQL connections"""
        if self._gateway:
            self._gateway.shutdown()
        
        if self._session:
            await self._session.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Flink SQL specific configuration"""
        return {
            "gateway_host": self.config.gateway_host,
            "gateway_port": self.config.gateway_port,
            "default_mode": self.config.default_mode.value,
            "default_parallelism": self.config.default_parallelism,
            "state_backend": self.config.state_backend,
            "sql_dialect": self.config.sql_dialect
        } 