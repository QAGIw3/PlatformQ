"""
Soda Core Client Integration

Provides data quality monitoring and alerting using Soda Core.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import yaml
import json
from pathlib import Path
from soda.scan import Scan
from soda.sodacl.check import Check
from soda.sodacl.check_outcome import CheckOutcome

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class DataSourceType(str, Enum):
    """Supported data source types"""
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    SPARK = "spark"
    DUCKDB = "duckdb"
    TRINO = "trino"
    DATABRICKS = "databricks"


class CheckSeverity(str, Enum):
    """Check severity levels"""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class SodaConfig(ClientConfig):
    """Configuration for Soda Core client"""
    # Data source configuration
    data_source_name: str = "default"
    data_source_type: DataSourceType = DataSourceType.POSTGRES
    connection_config: Dict[str, Any] = field(default_factory=dict)
    
    # Soda configuration
    soda_cloud_enabled: bool = False
    soda_cloud_api_key: Optional[str] = None
    soda_cloud_api_secret: Optional[str] = None
    soda_cloud_host: str = "https://cloud.soda.io"
    
    # Check configuration
    default_severity: CheckSeverity = CheckSeverity.ERROR
    fail_on_warning: bool = False
    
    # Scan configuration
    scan_definition_path: Optional[str] = None
    checks_path: Optional[str] = None
    
    # Performance
    sample_size: Optional[int] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "soda-core"


@dataclass
class ScanResult:
    """Scan execution result"""
    scan_id: str
    passed: bool
    check_results: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    errors: List[str]
    warnings: List[str]
    duration_seconds: float
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "scan_id": self.scan_id,
            "passed": self.passed,
            "check_results": self.check_results,
            "metrics": self.metrics,
            "errors": self.errors,
            "warnings": self.warnings,
            "duration_seconds": self.duration_seconds,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class DataQualityCheck:
    """Data quality check definition"""
    name: str
    table: str
    check_type: str
    condition: str
    severity: CheckSeverity = CheckSeverity.ERROR
    filter: Optional[str] = None
    group_by: Optional[List[str]] = None
    
    def to_sodacl(self) -> str:
        """Convert to SodaCL format"""
        check_str = f"checks for {self.table}:\n"
        
        # Add filter if specified
        if self.filter:
            check_str += f"  filter {self.table} [{self.filter}]:\n"
            indent = "    "
        else:
            indent = "  "
        
        # Add check
        check_str += f"{indent}- {self.name}:\n"
        check_str += f"{indent}    {self.check_type}: {self.condition}\n"
        
        # Add severity
        if self.severity != CheckSeverity.ERROR:
            check_str += f"{indent}    severity: {self.severity.value}\n"
        
        return check_str


class SodaCoreClient(BaseServiceClient):
    """
    Soda Core client for data quality monitoring.
    
    Features:
    - Data quality checks with SodaCL
    - Multiple data source support
    - Soda Cloud integration
    - Metric collection
    - Alerting and notifications
    - Historical tracking
    """
    
    def __init__(
        self,
        config: Optional[SodaConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = SodaConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: SodaConfig = config
        self._scan: Optional[Scan] = None
        
    async def connect(self):
        """Connect to data source"""
        await super().connect()
        
        try:
            # Get credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.connection_config.update(creds)
            
            # Get Soda Cloud credentials from Vault
            if self.config.soda_cloud_enabled and not self.config.soda_cloud_api_key:
                cloud_creds = await self._get_soda_cloud_credentials()
                if cloud_creds:
                    self.config.soda_cloud_api_key = cloud_creds.get("api_key")
                    self.config.soda_cloud_api_secret = cloud_creds.get("api_secret")
            
            logger.info(f"Configured Soda Core for {self.config.data_source_type.value}")
            
        except Exception as e:
            logger.error(f"Failed to configure Soda Core: {e}")
            raise
    
    async def run_scan(
        self,
        checks: Union[List[DataQualityCheck], str, Path],
        variables: Optional[Dict[str, Any]] = None,
        data_source_name: Optional[str] = None
    ) -> ScanResult:
        """
        Run a data quality scan.
        
        Args:
            checks: List of checks or path to checks file
            variables: Variables to use in checks
            data_source_name: Override default data source
            
        Returns:
            Scan result
        """
        try:
            start_time = datetime.now()
            
            # Create scan
            scan = Scan()
            scan.set_data_source_name(data_source_name or self.config.data_source_name)
            
            # Configure data source
            self._configure_data_source(scan)
            
            # Configure Soda Cloud if enabled
            if self.config.soda_cloud_enabled:
                self._configure_soda_cloud(scan)
            
            # Add checks
            if isinstance(checks, (str, Path)):
                # Load checks from file
                scan.add_sodacl_yaml_file(str(checks))
            else:
                # Convert checks to SodaCL
                sodacl_content = self._checks_to_sodacl(checks)
                scan.add_sodacl_yaml_str(sodacl_content)
            
            # Add variables
            if variables:
                for key, value in variables.items():
                    scan.add_variable(key, value)
            
            # Execute scan
            scan.execute()
            
            # Process results
            check_results = []
            passed = True
            
            for check in scan.get_checks():
                result = {
                    "name": check.name,
                    "table": check.table_name,
                    "outcome": check.outcome.name,
                    "passed": check.outcome == CheckOutcome.PASS,
                    "metrics": check.metrics if hasattr(check, 'metrics') else {},
                    "diagnostics": check.get_diagnostics() if hasattr(check, 'get_diagnostics') else {}
                }
                
                check_results.append(result)
                
                if not result["passed"]:
                    if check.severity == "error" or (
                        check.severity == "warning" and self.config.fail_on_warning
                    ):
                        passed = False
            
            # Get scan metrics
            metrics = scan.get_scan_results() if hasattr(scan, 'get_scan_results') else {}
            
            # Get errors and warnings
            errors = scan.get_error_logs() if hasattr(scan, 'get_error_logs') else []
            warnings = scan.get_warning_logs() if hasattr(scan, 'get_warning_logs') else []
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return ScanResult(
                scan_id=scan.scan_definition_name if hasattr(scan, 'scan_definition_name') else str(uuid.uuid4()),
                passed=passed,
                check_results=check_results,
                metrics=metrics,
                errors=errors,
                warnings=warnings,
                duration_seconds=duration
            )
            
        except Exception as e:
            logger.error(f"Failed to run scan: {e}")
            raise
    
    async def profile_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Profile a table to understand data characteristics.
        
        Args:
            table_name: Table to profile
            columns: Specific columns to profile
            sample_size: Number of rows to sample
            
        Returns:
            Table profile
        """
        try:
            # Create profiling checks
            checks = []
            
            # Row count
            checks.append(DataQualityCheck(
                name="row_count",
                table=table_name,
                check_type="row_count",
                condition="> 0"
            ))
            
            # Schema checks
            if columns:
                for column in columns:
                    # Completeness
                    checks.append(DataQualityCheck(
                        name=f"{column}_completeness",
                        table=table_name,
                        check_type=f"missing_percent({column})",
                        condition="< 100"
                    ))
                    
                    # Uniqueness
                    checks.append(DataQualityCheck(
                        name=f"{column}_uniqueness",
                        table=table_name,
                        check_type=f"duplicate_percent({column})",
                        condition="< 100"
                    ))
            
            # Run profiling scan
            variables = {}
            if sample_size:
                variables["sample_size"] = sample_size
            
            result = await self.run_scan(checks, variables)
            
            # Extract profile from results
            profile = {
                "table": table_name,
                "metrics": {},
                "columns": {}
            }
            
            for check_result in result.check_results:
                if check_result["name"] == "row_count":
                    profile["metrics"]["row_count"] = check_result["metrics"].get("row_count", 0)
                else:
                    # Extract column profiles
                    for column in columns or []:
                        if column in check_result["name"]:
                            if column not in profile["columns"]:
                                profile["columns"][column] = {}
                            
                            if "completeness" in check_result["name"]:
                                profile["columns"][column]["missing_percent"] = check_result["metrics"].get("missing_percent", 0)
                            elif "uniqueness" in check_result["name"]:
                                profile["columns"][column]["duplicate_percent"] = check_result["metrics"].get("duplicate_percent", 0)
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to profile table: {e}")
            raise
    
    async def create_anomaly_detection_checks(
        self,
        table_name: str,
        metrics: List[str],
        lookback_days: int = 7,
        threshold_stddev: float = 3.0
    ) -> List[DataQualityCheck]:
        """
        Create anomaly detection checks based on historical data.
        
        Args:
            table_name: Table to monitor
            metrics: Metrics to check for anomalies
            lookback_days: Days of history to consider
            threshold_stddev: Standard deviations for anomaly threshold
            
        Returns:
            List of anomaly detection checks
        """
        checks = []
        
        for metric in metrics:
            # Row count anomaly
            if metric == "row_count":
                checks.append(DataQualityCheck(
                    name=f"row_count_anomaly",
                    table=table_name,
                    check_type="anomaly_detection",
                    condition=f"row_count between historic_min and historic_max",
                    severity=CheckSeverity.WARNING
                ))
            
            # Freshness anomaly
            elif metric == "freshness":
                checks.append(DataQualityCheck(
                    name=f"freshness_anomaly",
                    table=table_name,
                    check_type="freshness",
                    condition=f"< {lookback_days}d",
                    severity=CheckSeverity.WARNING
                ))
            
            # Column-specific anomalies
            else:
                checks.append(DataQualityCheck(
                    name=f"{metric}_anomaly",
                    table=table_name,
                    check_type=f"anomaly_detection({metric})",
                    condition=f"between historic_avg - {threshold_stddev} * historic_stddev and historic_avg + {threshold_stddev} * historic_stddev",
                    severity=CheckSeverity.WARNING
                ))
        
        return checks
    
    async def validate_schema(
        self,
        table_name: str,
        expected_schema: Dict[str, str]
    ) -> ScanResult:
        """
        Validate table schema against expected schema.
        
        Args:
            table_name: Table to validate
            expected_schema: Expected column names and types
            
        Returns:
            Validation result
        """
        checks = []
        
        # Check for expected columns
        for column_name, column_type in expected_schema.items():
            checks.append(DataQualityCheck(
                name=f"schema_check_{column_name}",
                table=table_name,
                check_type="schema",
                condition=f"'{column_name}' in columns"
            ))
            
            # Type check if supported
            if column_type.lower() in ["string", "integer", "float", "boolean", "date", "timestamp"]:
                checks.append(DataQualityCheck(
                    name=f"type_check_{column_name}",
                    table=table_name,
                    check_type=f"column_type({column_name})",
                    condition=f"= '{column_type}'"
                ))
        
        return await self.run_scan(checks)
    
    async def create_data_contract(
        self,
        table_name: str,
        contract_definition: Dict[str, Any]
    ) -> List[DataQualityCheck]:
        """
        Create data quality checks from a data contract.
        
        Args:
            table_name: Table name
            contract_definition: Data contract definition
            
        Returns:
            List of checks implementing the contract
        """
        checks = []
        
        # Schema checks
        if "schema" in contract_definition:
            for column, constraints in contract_definition["schema"].items():
                # Required columns
                if constraints.get("required", False):
                    checks.append(DataQualityCheck(
                        name=f"{column}_required",
                        table=table_name,
                        check_type=f"missing_count({column})",
                        condition="= 0",
                        severity=CheckSeverity.CRITICAL
                    ))
                
                # Data type
                if "type" in constraints:
                    checks.append(DataQualityCheck(
                        name=f"{column}_type",
                        table=table_name,
                        check_type=f"invalid_percent({column})",
                        condition="= 0",
                        severity=CheckSeverity.ERROR
                    ))
                
                # Value constraints
                if "min" in constraints:
                    checks.append(DataQualityCheck(
                        name=f"{column}_min_value",
                        table=table_name,
                        check_type=f"min({column})",
                        condition=f">= {constraints['min']}",
                        severity=CheckSeverity.ERROR
                    ))
                
                if "max" in constraints:
                    checks.append(DataQualityCheck(
                        name=f"{column}_max_value",
                        table=table_name,
                        check_type=f"max({column})",
                        condition=f"<= {constraints['max']}",
                        severity=CheckSeverity.ERROR
                    ))
                
                if "values" in constraints:
                    values_str = ", ".join([f"'{v}'" for v in constraints["values"]])
                    checks.append(DataQualityCheck(
                        name=f"{column}_valid_values",
                        table=table_name,
                        check_type=f"invalid_count({column})",
                        condition=f"= 0",
                        filter=f"{column} not in ({values_str})",
                        severity=CheckSeverity.ERROR
                    ))
        
        # Quality SLAs
        if "quality_slas" in contract_definition:
            slas = contract_definition["quality_slas"]
            
            if "completeness" in slas:
                checks.append(DataQualityCheck(
                    name="completeness_sla",
                    table=table_name,
                    check_type="missing_percent",
                    condition=f"< {100 - slas['completeness']}",
                    severity=CheckSeverity.ERROR
                ))
            
            if "freshness_hours" in slas:
                checks.append(DataQualityCheck(
                    name="freshness_sla",
                    table=table_name,
                    check_type="freshness",
                    condition=f"< {slas['freshness_hours']}h",
                    severity=CheckSeverity.CRITICAL
                ))
            
            if "uniqueness" in slas:
                for column in slas["uniqueness"]:
                    checks.append(DataQualityCheck(
                        name=f"{column}_uniqueness_sla",
                        table=table_name,
                        check_type=f"duplicate_count({column})",
                        condition="= 0",
                        severity=CheckSeverity.ERROR
                    ))
        
        return checks
    
    def _configure_data_source(self, scan: Scan):
        """Configure data source for scan"""
        config_dict = {
            "data_source_name": self.config.data_source_name,
            "type": self.config.data_source_type.value,
            "connection": self.config.connection_config
        }
        
        # Add sample size if configured
        if self.config.sample_size:
            config_dict["sample_method"] = "TABLESAMPLE"
            config_dict["sample_percentage"] = min(
                100,
                (self.config.sample_size / 1000000) * 100  # Assume 1M rows
            )
        
        scan.add_configuration_yaml_str(yaml.dump(config_dict))
    
    def _configure_soda_cloud(self, scan: Scan):
        """Configure Soda Cloud integration"""
        if self.config.soda_cloud_api_key and self.config.soda_cloud_api_secret:
            scan.set_soda_cloud_config({
                "api_key": self.config.soda_cloud_api_key,
                "api_secret": self.config.soda_cloud_api_secret,
                "host": self.config.soda_cloud_host
            })
    
    def _checks_to_sodacl(self, checks: List[DataQualityCheck]) -> str:
        """Convert checks to SodaCL YAML format"""
        sodacl_parts = []
        
        # Group checks by table
        checks_by_table = {}
        for check in checks:
            if check.table not in checks_by_table:
                checks_by_table[check.table] = []
            checks_by_table[check.table].append(check)
        
        # Generate SodaCL for each table
        for table, table_checks in checks_by_table.items():
            sodacl_parts.append(f"checks for {table}:")
            
            for check in table_checks:
                # Add filter if specified
                if check.filter:
                    sodacl_parts.append(f"  filter {table} [{check.filter}]:")
                    indent = "    "
                else:
                    indent = "  "
                
                # Add check
                sodacl_parts.append(f"{indent}- {check.name}:")
                sodacl_parts.append(f"{indent}    {check.check_type}: {check.condition}")
                
                # Add severity if not default
                if check.severity != self.config.default_severity:
                    sodacl_parts.append(f"{indent}    severity: {check.severity.value}")
                
                # Add group by if specified
                if check.group_by:
                    sodacl_parts.append(f"{indent}    group by: [{', '.join(check.group_by)}]")
        
        return "\n".join(sodacl_parts)
    
    async def _get_soda_cloud_credentials(self) -> Optional[Dict[str, str]]:
        """Get Soda Cloud credentials from Vault"""
        if self._vault_client:
            try:
                secret = await self._vault_client.read_secret(
                    f"soda-cloud/{self.config.data_source_name}"
                )
                return secret.get("data", {})
            except Exception as e:
                logger.warning(f"Failed to get Soda Cloud credentials from Vault: {e}")
        return None
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Soda Core specific configuration"""
        return {
            "data_source_name": self.config.data_source_name,
            "data_source_type": self.config.data_source_type.value,
            "soda_cloud_enabled": self.config.soda_cloud_enabled,
            "default_severity": self.config.default_severity.value,
            "fail_on_warning": self.config.fail_on_warning
        } 