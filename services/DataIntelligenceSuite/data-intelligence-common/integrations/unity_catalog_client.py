"""
Databricks Unity Catalog Client Integration

Provides unified governance for data and AI assets across clouds.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
import requests
from urllib.parse import urljoin

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class SecurableType(str, Enum):
    """Unity Catalog securable types"""
    CATALOG = "CATALOG"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    VIEW = "VIEW"
    VOLUME = "VOLUME"
    FUNCTION = "FUNCTION"
    MODEL = "MODEL"
    SHARE = "SHARE"
    RECIPIENT = "RECIPIENT"


class TableType(str, Enum):
    """Table types"""
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"


class DataType(str, Enum):
    """Common data types"""
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    STRING = "STRING"
    BINARY = "BINARY"
    DECIMAL = "DECIMAL"
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"


class PrivilegeType(str, Enum):
    """Privilege types"""
    ALL_PRIVILEGES = "ALL_PRIVILEGES"
    CREATE = "CREATE"
    USAGE = "USAGE"
    SELECT = "SELECT"
    MODIFY = "MODIFY"
    READ_METADATA = "READ_METADATA"
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_FUNCTION = "CREATE_FUNCTION"
    CREATE_MODEL = "CREATE_MODEL"


class ModelVersionStatus(str, Enum):
    """Model version status"""
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    READY = "READY"
    FAILED_REGISTRATION = "FAILED_REGISTRATION"


@dataclass
class UnityCatalogConfig(ClientConfig):
    """Configuration for Unity Catalog client"""
    # Workspace settings
    workspace_url: str = "https://myworkspace.databricks.com"
    
    # Authentication
    token: Optional[str] = None
    service_principal_id: Optional[str] = None
    service_principal_secret: Optional[str] = None
    
    # API settings
    api_version: str = "2.1"
    page_size: int = 100
    
    # Default catalog
    default_catalog: str = "main"
    
    # Features
    enable_lineage: bool = True
    enable_audit: bool = True
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "unity-catalog"


@dataclass
class CatalogInfo:
    """Catalog information"""
    name: str
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class SchemaInfo:
    """Schema information"""
    catalog_name: str
    name: str
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.name}"


@dataclass
class ColumnInfo:
    """Column information"""
    name: str
    type_name: str
    type_text: str
    position: int
    nullable: bool = True
    comment: Optional[str] = None
    partition_index: Optional[int] = None


@dataclass
class TableInfo:
    """Table information"""
    catalog_name: str
    schema_name: str
    name: str
    table_type: TableType
    columns: List[ColumnInfo]
    storage_location: Optional[str] = None
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.name}"


@dataclass
class ModelInfo:
    """ML model information"""
    catalog_name: str
    schema_name: str
    name: str
    comment: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.name}"


@dataclass
class ModelVersionInfo:
    """Model version information"""
    model_name: str
    version: int
    source: str
    run_id: Optional[str] = None
    status: ModelVersionStatus = ModelVersionStatus.PENDING_REGISTRATION
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class LineageInfo:
    """Lineage information"""
    table_name: str
    upstream_tables: List[str] = field(default_factory=list)
    downstream_tables: List[str] = field(default_factory=list)
    notebooks: List[str] = field(default_factory=list)
    jobs: List[str] = field(default_factory=list)


@dataclass
class GrantInfo:
    """Grant information"""
    principal: str
    privileges: List[PrivilegeType]
    securable_type: SecurableType
    securable_name: str


class UnityCatalogClient(BaseServiceClient):
    """
    Databricks Unity Catalog client for unified data governance.
    
    Features:
    - Multi-cloud data governance
    - Fine-grained access control
    - Data lineage tracking
    - ML model registry
    - Data sharing
    - Audit logging
    """
    
    def __init__(
        self,
        config: Optional[UnityCatalogConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = UnityCatalogConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: UnityCatalogConfig = config
        self._session: Optional[requests.Session] = None
        
    async def connect(self):
        """Connect to Unity Catalog"""
        await super().connect()
        
        try:
            # Get credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.token = creds.get("token")
                    self.config.service_principal_id = creds.get("service_principal_id")
                    self.config.service_principal_secret = creds.get("service_principal_secret")
            
            # Create session
            self._session = requests.Session()
            
            # Set authentication
            if self.config.token:
                self._session.headers["Authorization"] = f"Bearer {self.config.token}"
            elif self.config.service_principal_id and self.config.service_principal_secret:
                # OAuth flow for service principal
                await self._authenticate_service_principal()
            
            # Set common headers
            self._session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json"
            })
            
            # Test connection
            await self.list_catalogs(limit=1)
            
            logger.info(f"Connected to Unity Catalog: {self.config.workspace_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Unity Catalog: {e}")
            raise
    
    async def _authenticate_service_principal(self):
        """Authenticate using service principal"""
        # Implementation depends on Databricks OAuth setup
        pass
    
    def _get_api_url(self, endpoint: str) -> str:
        """Get full API URL"""
        base_url = f"{self.config.workspace_url}/api/{self.config.api_version}/unity-catalog"
        return urljoin(base_url, endpoint)
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make API request"""
        url = self._get_api_url(endpoint)
        
        response = self._session.request(
            method=method,
            url=url,
            json=data,
            params=params,
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json() if response.content else {}
    
    # Catalog Management
    
    async def create_catalog(
        self,
        name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> CatalogInfo:
        """
        Create catalog.
        
        Args:
            name: Catalog name
            comment: Catalog comment
            properties: Catalog properties
            
        Returns:
            Catalog information
        """
        try:
            data = {
                "name": name,
                "comment": comment,
                "properties": properties or {}
            }
            
            result = await self._request("POST", "catalogs", data=data)
            
            return CatalogInfo(
                name=result["name"],
                comment=result.get("comment"),
                properties=result.get("properties", {}),
                owner=result.get("owner"),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to create catalog: {e}")
            raise
    
    async def list_catalogs(
        self,
        limit: Optional[int] = None
    ) -> List[CatalogInfo]:
        """
        List catalogs.
        
        Args:
            limit: Maximum number of catalogs
            
        Returns:
            List of catalogs
        """
        try:
            params = {}
            if limit:
                params["max_results"] = limit
            
            result = await self._request("GET", "catalogs", params=params)
            
            catalogs = []
            for cat in result.get("catalogs", []):
                catalogs.append(CatalogInfo(
                    name=cat["name"],
                    comment=cat.get("comment"),
                    properties=cat.get("properties", {}),
                    owner=cat.get("owner"),
                    created_at=datetime.fromtimestamp(cat["created_at"] / 1000) if "created_at" in cat else None,
                    updated_at=datetime.fromtimestamp(cat["updated_at"] / 1000) if "updated_at" in cat else None
                ))
            
            return catalogs
            
        except Exception as e:
            logger.error(f"Failed to list catalogs: {e}")
            return []
    
    # Schema Management
    
    async def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> SchemaInfo:
        """
        Create schema.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            comment: Schema comment
            properties: Schema properties
            
        Returns:
            Schema information
        """
        try:
            data = {
                "catalog_name": catalog_name,
                "name": schema_name,
                "comment": comment,
                "properties": properties or {}
            }
            
            result = await self._request("POST", "schemas", data=data)
            
            return SchemaInfo(
                catalog_name=result["catalog_name"],
                name=result["name"],
                comment=result.get("comment"),
                properties=result.get("properties", {}),
                owner=result.get("owner"),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    async def list_schemas(
        self,
        catalog_name: str,
        limit: Optional[int] = None
    ) -> List[SchemaInfo]:
        """
        List schemas in catalog.
        
        Args:
            catalog_name: Catalog name
            limit: Maximum number of schemas
            
        Returns:
            List of schemas
        """
        try:
            params = {"catalog_name": catalog_name}
            if limit:
                params["max_results"] = limit
            
            result = await self._request("GET", "schemas", params=params)
            
            schemas = []
            for sch in result.get("schemas", []):
                schemas.append(SchemaInfo(
                    catalog_name=sch["catalog_name"],
                    name=sch["name"],
                    comment=sch.get("comment"),
                    properties=sch.get("properties", {}),
                    owner=sch.get("owner"),
                    created_at=datetime.fromtimestamp(sch["created_at"] / 1000) if "created_at" in sch else None,
                    updated_at=datetime.fromtimestamp(sch["updated_at"] / 1000) if "updated_at" in sch else None
                ))
            
            return schemas
            
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            return []
    
    # Table Management
    
    async def create_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_type: TableType = TableType.MANAGED,
        storage_location: Optional[str] = None,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> TableInfo:
        """
        Create table.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            table_name: Table name
            columns: Column definitions
            table_type: Table type
            storage_location: Storage location for external tables
            comment: Table comment
            properties: Table properties
            
        Returns:
            Table information
        """
        try:
            data = {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "name": table_name,
                "table_type": table_type.value,
                "columns": columns,
                "comment": comment,
                "properties": properties or {}
            }
            
            if storage_location:
                data["storage_location"] = storage_location
            
            result = await self._request("POST", "tables", data=data)
            
            column_infos = []
            for i, col in enumerate(result.get("columns", [])):
                column_infos.append(ColumnInfo(
                    name=col["name"],
                    type_name=col["type_name"],
                    type_text=col["type_text"],
                    position=col.get("position", i),
                    nullable=col.get("nullable", True),
                    comment=col.get("comment"),
                    partition_index=col.get("partition_index")
                ))
            
            return TableInfo(
                catalog_name=result["catalog_name"],
                schema_name=result["schema_name"],
                name=result["name"],
                table_type=TableType(result["table_type"]),
                columns=column_infos,
                storage_location=result.get("storage_location"),
                comment=result.get("comment"),
                properties=result.get("properties", {}),
                owner=result.get("owner"),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    async def get_table(
        self,
        full_table_name: str
    ) -> TableInfo:
        """
        Get table information.
        
        Args:
            full_table_name: Full table name (catalog.schema.table)
            
        Returns:
            Table information
        """
        try:
            result = await self._request("GET", f"tables/{full_table_name}")
            
            column_infos = []
            for i, col in enumerate(result.get("columns", [])):
                column_infos.append(ColumnInfo(
                    name=col["name"],
                    type_name=col["type_name"],
                    type_text=col["type_text"],
                    position=col.get("position", i),
                    nullable=col.get("nullable", True),
                    comment=col.get("comment"),
                    partition_index=col.get("partition_index")
                ))
            
            return TableInfo(
                catalog_name=result["catalog_name"],
                schema_name=result["schema_name"],
                name=result["name"],
                table_type=TableType(result["table_type"]),
                columns=column_infos,
                storage_location=result.get("storage_location"),
                comment=result.get("comment"),
                properties=result.get("properties", {}),
                owner=result.get("owner"),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to get table: {e}")
            raise
    
    async def list_tables(
        self,
        catalog_name: str,
        schema_name: str,
        limit: Optional[int] = None
    ) -> List[TableInfo]:
        """
        List tables in schema.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            limit: Maximum number of tables
            
        Returns:
            List of tables
        """
        try:
            params = {
                "catalog_name": catalog_name,
                "schema_name": schema_name
            }
            if limit:
                params["max_results"] = limit
            
            result = await self._request("GET", "tables", params=params)
            
            tables = []
            for tbl in result.get("tables", []):
                # Get full table info
                full_name = f"{tbl['catalog_name']}.{tbl['schema_name']}.{tbl['name']}"
                table_info = await self.get_table(full_name)
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    # ML Model Registry
    
    async def create_model(
        self,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        comment: Optional[str] = None
    ) -> ModelInfo:
        """
        Create ML model.
        
        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            model_name: Model name
            comment: Model comment
            
        Returns:
            Model information
        """
        try:
            data = {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "name": model_name,
                "comment": comment
            }
            
            result = await self._request("POST", "models", data=data)
            
            return ModelInfo(
                catalog_name=result["catalog_name"],
                schema_name=result["schema_name"],
                name=result["name"],
                comment=result.get("comment"),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to create model: {e}")
            raise
    
    async def create_model_version(
        self,
        full_model_name: str,
        source: str,
        run_id: Optional[str] = None,
        comment: Optional[str] = None
    ) -> ModelVersionInfo:
        """
        Create model version.
        
        Args:
            full_model_name: Full model name (catalog.schema.model)
            source: Model source URI
            run_id: MLflow run ID
            comment: Version comment
            
        Returns:
            Model version information
        """
        try:
            data = {
                "model_name": full_model_name,
                "source": source,
                "run_id": run_id,
                "comment": comment
            }
            
            result = await self._request("POST", f"models/{full_model_name}/versions", data=data)
            
            return ModelVersionInfo(
                model_name=result["model_name"],
                version=result["version"],
                source=result["source"],
                run_id=result.get("run_id"),
                status=ModelVersionStatus(result["status"]),
                created_at=datetime.fromtimestamp(result["created_at"] / 1000) if "created_at" in result else None,
                updated_at=datetime.fromtimestamp(result["updated_at"] / 1000) if "updated_at" in result else None
            )
            
        except Exception as e:
            logger.error(f"Failed to create model version: {e}")
            raise
    
    # Access Control
    
    async def grant_permissions(
        self,
        securable_type: SecurableType,
        securable_name: str,
        principal: str,
        privileges: List[PrivilegeType]
    ) -> bool:
        """
        Grant permissions on securable.
        
        Args:
            securable_type: Type of securable
            securable_name: Name of securable
            principal: Principal (user/group/service principal)
            privileges: List of privileges to grant
            
        Returns:
            Success status
        """
        try:
            data = {
                "securable_type": securable_type.value,
                "full_name": securable_name,
                "principal": principal,
                "privileges": [p.value for p in privileges]
            }
            
            await self._request("POST", "permissions/grant", data=data)
            
            logger.info(f"Granted {privileges} on {securable_name} to {principal}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to grant permissions: {e}")
            return False
    
    async def revoke_permissions(
        self,
        securable_type: SecurableType,
        securable_name: str,
        principal: str,
        privileges: List[PrivilegeType]
    ) -> bool:
        """
        Revoke permissions on securable.
        
        Args:
            securable_type: Type of securable
            securable_name: Name of securable
            principal: Principal (user/group/service principal)
            privileges: List of privileges to revoke
            
        Returns:
            Success status
        """
        try:
            data = {
                "securable_type": securable_type.value,
                "full_name": securable_name,
                "principal": principal,
                "privileges": [p.value for p in privileges]
            }
            
            await self._request("POST", "permissions/revoke", data=data)
            
            logger.info(f"Revoked {privileges} on {securable_name} from {principal}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to revoke permissions: {e}")
            return False
    
    async def list_permissions(
        self,
        securable_type: SecurableType,
        securable_name: str
    ) -> List[GrantInfo]:
        """
        List permissions on securable.
        
        Args:
            securable_type: Type of securable
            securable_name: Name of securable
            
        Returns:
            List of grants
        """
        try:
            params = {
                "securable_type": securable_type.value,
                "full_name": securable_name
            }
            
            result = await self._request("GET", "permissions", params=params)
            
            grants = []
            for grant in result.get("privilege_assignments", []):
                grants.append(GrantInfo(
                    principal=grant["principal"],
                    privileges=[PrivilegeType(p) for p in grant["privileges"]],
                    securable_type=securable_type,
                    securable_name=securable_name
                ))
            
            return grants
            
        except Exception as e:
            logger.error(f"Failed to list permissions: {e}")
            return []
    
    # Lineage
    
    async def get_table_lineage(
        self,
        table_name: str,
        include_notebooks: bool = True,
        include_jobs: bool = True
    ) -> LineageInfo:
        """
        Get table lineage.
        
        Args:
            table_name: Full table name
            include_notebooks: Include notebook references
            include_jobs: Include job references
            
        Returns:
            Lineage information
        """
        try:
            params = {
                "table_name": table_name,
                "include_notebooks": include_notebooks,
                "include_jobs": include_jobs
            }
            
            result = await self._request("GET", f"lineage/tables/{table_name}", params=params)
            
            return LineageInfo(
                table_name=table_name,
                upstream_tables=result.get("upstream_tables", []),
                downstream_tables=result.get("downstream_tables", []),
                notebooks=result.get("notebooks", []),
                jobs=result.get("jobs", [])
            )
            
        except Exception as e:
            logger.error(f"Failed to get table lineage: {e}")
            return LineageInfo(table_name=table_name)
    
    # Data Sharing
    
    async def create_share(
        self,
        share_name: str,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create data share.
        
        Args:
            share_name: Share name
            comment: Share comment
            
        Returns:
            Share information
        """
        try:
            data = {
                "name": share_name,
                "comment": comment
            }
            
            result = await self._request("POST", "shares", data=data)
            return result
            
        except Exception as e:
            logger.error(f"Failed to create share: {e}")
            raise
    
    async def add_table_to_share(
        self,
        share_name: str,
        table_name: str,
        alias: Optional[str] = None
    ) -> bool:
        """
        Add table to share.
        
        Args:
            share_name: Share name
            table_name: Full table name
            alias: Table alias in share
            
        Returns:
            Success status
        """
        try:
            data = {
                "name": table_name,
                "alias": alias
            }
            
            await self._request("POST", f"shares/{share_name}/tables", data=data)
            
            logger.info(f"Added table {table_name} to share {share_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add table to share: {e}")
            return False
    
    async def create_recipient(
        self,
        recipient_name: str,
        comment: Optional[str] = None,
        sharing_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create share recipient.
        
        Args:
            recipient_name: Recipient name
            comment: Recipient comment
            sharing_code: Sharing activation code
            
        Returns:
            Recipient information
        """
        try:
            data = {
                "name": recipient_name,
                "comment": comment,
                "sharing_code": sharing_code
            }
            
            result = await self._request("POST", "recipients", data=data)
            return result
            
        except Exception as e:
            logger.error(f"Failed to create recipient: {e}")
            raise
    
    async def grant_share_to_recipient(
        self,
        share_name: str,
        recipient_name: str
    ) -> bool:
        """
        Grant share to recipient.
        
        Args:
            share_name: Share name
            recipient_name: Recipient name
            
        Returns:
            Success status
        """
        try:
            data = {
                "share_name": share_name
            }
            
            await self._request("POST", f"recipients/{recipient_name}/shares", data=data)
            
            logger.info(f"Granted share {share_name} to recipient {recipient_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to grant share to recipient: {e}")
            return False
    
    async def close(self):
        """Close Unity Catalog connection"""
        if self._session:
            self._session.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Unity Catalog specific configuration"""
        return {
            "workspace_url": self.config.workspace_url,
            "api_version": self.config.api_version,
            "default_catalog": self.config.default_catalog,
            "enable_lineage": self.config.enable_lineage,
            "enable_audit": self.config.enable_audit
        } 