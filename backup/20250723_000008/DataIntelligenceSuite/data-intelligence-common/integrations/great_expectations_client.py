"""
Great Expectations Client Integration

Provides comprehensive data quality validation and profiling using Great Expectations.
"""

import json
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.datasource import Datasource
from great_expectations.validator.validator import Validator

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ValidationSeverity(str, Enum):
    """Validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ExpectationType(str, Enum):
    """Common expectation types"""
    # Column existence
    COLUMN_EXISTS = "expect_column_to_exist"
    COLUMNS_MATCH = "expect_table_columns_to_match_ordered_list"
    
    # Nullability
    COLUMN_NOT_NULL = "expect_column_values_to_not_be_null"
    COLUMN_NULL_FRACTION = "expect_column_values_to_be_null"
    
    # Value constraints
    COLUMN_IN_SET = "expect_column_values_to_be_in_set"
    COLUMN_BETWEEN = "expect_column_values_to_be_between"
    COLUMN_REGEX = "expect_column_values_to_match_regex"
    
    # Statistical
    COLUMN_MEAN_BETWEEN = "expect_column_mean_to_be_between"
    COLUMN_STDEV_BETWEEN = "expect_column_stdev_to_be_between"
    COLUMN_QUANTILE_BETWEEN = "expect_column_quantile_values_to_be_between"
    
    # Uniqueness
    COLUMN_UNIQUE = "expect_column_values_to_be_unique"
    COLUMN_UNIQUE_FRACTION = "expect_column_proportion_of_unique_values_to_be_between"
    
    # Table level
    ROW_COUNT_BETWEEN = "expect_table_row_count_to_be_between"
    COLUMN_COUNT_EQUAL = "expect_table_column_count_to_equal"


@dataclass
class GreatExpectationsConfig(ClientConfig):
    """Configuration for Great Expectations client"""
    context_root_dir: str = "/tmp/great_expectations"
    
    # Data source configuration
    datasource_name: str = "platformq_datasource"
    
    # Validation settings
    enable_profiling: bool = True
    profile_sample_size: Optional[int] = 10000
    
    # Storage backends (can be S3, GCS, local)
    expectations_store_type: str = "filesystem"
    validations_store_type: str = "filesystem"
    checkpoint_store_type: str = "filesystem"
    
    # S3/MinIO configuration for stores
    expectations_store_s3_bucket: Optional[str] = None
    validations_store_s3_bucket: Optional[str] = None
    s3_endpoint_url: Optional[str] = None
    
    # Slack/email notifications
    enable_notifications: bool = True
    slack_webhook_url: Optional[str] = None
    email_smtp_host: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "great-expectations"


@dataclass
class ValidationRule:
    """Data validation rule"""
    expectation_type: ExpectationType
    kwargs: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    severity: ValidationSeverity = ValidationSeverity.ERROR
    
    def to_expectation_config(self) -> ExpectationConfiguration:
        """Convert to Great Expectations configuration"""
        meta = {
            **self.meta,
            "severity": self.severity.value
        }
        
        return ExpectationConfiguration(
            expectation_type=self.expectation_type.value,
            kwargs=self.kwargs,
            meta=meta
        )


@dataclass
class ValidationResult:
    """Validation result summary"""
    success: bool
    total_expectations: int
    successful_expectations: int
    failed_expectations: int
    validation_time: datetime
    
    # Detailed results
    results: List[Dict[str, Any]] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    # Data docs
    data_docs_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "success": self.success,
            "total_expectations": self.total_expectations,
            "successful_expectations": self.successful_expectations,
            "failed_expectations": self.failed_expectations,
            "validation_time": self.validation_time.isoformat(),
            "results": self.results,
            "statistics": self.statistics,
            "data_docs_url": self.data_docs_url
        }


class GreatExpectationsClient(BaseServiceClient):
    """
    Great Expectations client for data quality validation.
    
    Features:
    - Automated data profiling
    - Expectation suite management
    - Data validation with detailed results
    - Integration with various data sources
    - Data documentation generation
    - Alerting and notifications
    """
    
    def __init__(
        self,
        config: Optional[GreatExpectationsConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = GreatExpectationsConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: GreatExpectationsConfig = config
        self._context: Optional[BaseDataContext] = None
        
    async def connect(self):
        """Initialize Great Expectations context"""
        await super().connect()
        
        try:
            # Build data context configuration
            context_config = DataContextConfig(
                config_version=3,
                datasources={},
                stores={
                    "expectations_store": self._get_store_config("expectations"),
                    "validations_store": self._get_store_config("validations"),
                    "checkpoint_store": self._get_store_config("checkpoint")
                },
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                checkpoint_store_name="checkpoint_store",
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.config.context_root_dir}/uncommitted/data_docs/local_site/"
                        }
                    }
                }
            )
            
            # Create context
            self._context = BaseDataContext(project_config=context_config)
            
            logger.info("Initialized Great Expectations context")
            
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations: {e}")
            raise
    
    def _get_store_config(self, store_type: str) -> Dict[str, Any]:
        """Get store configuration based on type"""
        if self.config.expectations_store_type == "s3":
            bucket = getattr(self.config, f"{store_type}_store_s3_bucket", "ge-store")
            return {
                "class_name": "ExpectationsStore" if store_type == "expectations" else "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": f"{store_type}/",
                    "endpoint_url": self.config.s3_endpoint_url
                }
            }
        else:
            return {
                "class_name": "ExpectationsStore" if store_type == "expectations" else "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": f"{self.config.context_root_dir}/{store_type}/"
                }
            }
    
    async def create_datasource(
        self,
        name: str,
        source_type: str = "pandas",
        connection_string: Optional[str] = None
    ) -> Datasource:
        """
        Create a data source.
        
        Args:
            name: Datasource name
            source_type: Type of datasource (pandas, spark, sql)
            connection_string: Connection string for SQL sources
            
        Returns:
            Created datasource
        """
        try:
            if source_type == "pandas":
                datasource_config = {
                    "name": name,
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "PandasExecutionEngine"
                    },
                    "data_connectors": {
                        "runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            elif source_type == "spark":
                datasource_config = {
                    "name": name,
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine"
                    },
                    "data_connectors": {
                        "runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            elif source_type == "sql":
                # Get credentials from Vault if configured
                if self.config.use_vault_credentials and connection_string is None:
                    creds = await self._get_credentials()
                    if creds:
                        connection_string = creds.get("connection_string")
                
                datasource_config = {
                    "name": name,
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "connection_string": connection_string
                    },
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            datasource = self._context.add_datasource(**datasource_config)
            logger.info(f"Created datasource: {name}")
            return datasource
            
        except Exception as e:
            logger.error(f"Failed to create datasource: {e}")
            raise
    
    async def profile_data(
        self,
        data: Union[pd.DataFrame, str],
        datasource_name: Optional[str] = None,
        suite_name: str = "profiling_suite"
    ) -> ExpectationSuite:
        """
        Profile data and generate expectation suite.
        
        Args:
            data: DataFrame or table name to profile
            datasource_name: Datasource to use
            suite_name: Name for expectation suite
            
        Returns:
            Generated expectation suite
        """
        try:
            # Get or create datasource
            if not datasource_name:
                datasource_name = self.config.datasource_name
                if datasource_name not in self._context.list_datasources():
                    await self.create_datasource(datasource_name)
            
            # Create batch request
            if isinstance(data, pd.DataFrame):
                batch_request = RuntimeBatchRequest(
                    datasource_name=datasource_name,
                    data_connector_name="runtime_data_connector",
                    data_asset_name="profiling_data",
                    runtime_parameters={"batch_data": data},
                    batch_identifiers={"run_id": f"profiling_{datetime.now().isoformat()}"}
                )
            else:
                # Assume it's a table name
                batch_request = RuntimeBatchRequest(
                    datasource_name=datasource_name,
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name=data,
                    batch_identifiers={"run_id": f"profiling_{datetime.now().isoformat()}"}
                )
            
            # Create expectation suite
            suite = self._context.create_expectation_suite(
                expectation_suite_name=suite_name,
                overwrite_existing=True
            )
            
            # Get validator
            validator = self._context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            # Profile data
            if self.config.enable_profiling:
                profiler = validator.profile(
                    profiler_configuration={
                        "ignored_columns": [],
                        "value_set_threshold": "MANY",
                        "semantic_types_dict": {}
                    }
                )
                
                # Add profiler expectations to suite
                suite.add_expectation_configurations(
                    profiler.expectation_configurations
                )
            
            # Save suite
            self._context.save_expectation_suite(suite)
            
            logger.info(f"Profiled data and created suite: {suite_name}")
            return suite
            
        except Exception as e:
            logger.error(f"Failed to profile data: {e}")
            raise
    
    async def create_expectation_suite(
        self,
        suite_name: str,
        rules: List[ValidationRule]
    ) -> ExpectationSuite:
        """
        Create expectation suite from rules.
        
        Args:
            suite_name: Suite name
            rules: Validation rules
            
        Returns:
            Created expectation suite
        """
        try:
            # Create suite
            suite = self._context.create_expectation_suite(
                expectation_suite_name=suite_name,
                overwrite_existing=True
            )
            
            # Add expectations
            for rule in rules:
                suite.add_expectation(rule.to_expectation_config())
            
            # Save suite
            self._context.save_expectation_suite(suite)
            
            logger.info(f"Created expectation suite: {suite_name} with {len(rules)} rules")
            return suite
            
        except Exception as e:
            logger.error(f"Failed to create expectation suite: {e}")
            raise
    
    async def validate_data(
        self,
        data: Union[pd.DataFrame, str],
        suite_name: str,
        datasource_name: Optional[str] = None,
        run_name: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate data against expectation suite.
        
        Args:
            data: DataFrame or table name to validate
            suite_name: Expectation suite name
            datasource_name: Datasource to use
            run_name: Validation run name
            
        Returns:
            Validation results
        """
        try:
            # Get or create datasource
            if not datasource_name:
                datasource_name = self.config.datasource_name
                if datasource_name not in self._context.list_datasources():
                    await self.create_datasource(datasource_name)
            
            # Create batch request
            run_id = run_name or f"validation_{datetime.now().isoformat()}"
            
            if isinstance(data, pd.DataFrame):
                batch_request = RuntimeBatchRequest(
                    datasource_name=datasource_name,
                    data_connector_name="runtime_data_connector",
                    data_asset_name="validation_data",
                    runtime_parameters={"batch_data": data},
                    batch_identifiers={"run_id": run_id}
                )
            else:
                batch_request = RuntimeBatchRequest(
                    datasource_name=datasource_name,
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name=data,
                    batch_identifiers={"run_id": run_id}
                )
            
            # Create checkpoint
            checkpoint_config = {
                "name": f"checkpoint_{run_id}",
                "config_version": 1,
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": suite_name
                    }
                ]
            }
            
            checkpoint = SimpleCheckpoint(**checkpoint_config, data_context=self._context)
            
            # Run validation
            checkpoint_result = checkpoint.run()
            
            # Parse results
            validation_results = list(checkpoint_result.run_results.values())[0]["validation_result"]
            
            # Build result summary
            result = ValidationResult(
                success=validation_results["success"],
                total_expectations=validation_results["statistics"]["evaluated_expectations"],
                successful_expectations=validation_results["statistics"]["successful_expectations"],
                failed_expectations=validation_results["statistics"]["unsuccessful_expectations"],
                validation_time=datetime.now(),
                results=self._parse_validation_results(validation_results),
                statistics=validation_results["statistics"]
            )
            
            # Generate data docs
            if self.config.enable_notifications:
                self._context.build_data_docs()
                result.data_docs_url = self._context.get_docs_sites_urls()[0]
            
            logger.info(f"Validation completed: {result.success}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to validate data: {e}")
            raise
    
    def _parse_validation_results(self, validation_results: Dict) -> List[Dict[str, Any]]:
        """Parse validation results for summary"""
        parsed_results = []
        
        for result in validation_results["results"]:
            expectation_config = result["expectation_config"]
            
            parsed_results.append({
                "expectation_type": expectation_config["expectation_type"],
                "success": result["success"],
                "kwargs": expectation_config["kwargs"],
                "result": result.get("result", {}),
                "exception_info": result.get("exception_info"),
                "meta": expectation_config.get("meta", {})
            })
        
        return parsed_results
    
    async def get_expectation_suites(self) -> List[str]:
        """Get list of expectation suites"""
        return self._context.list_expectation_suite_names()
    
    async def get_validation_history(
        self,
        suite_name: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get validation history.
        
        Args:
            suite_name: Filter by suite name
            limit: Maximum results
            
        Returns:
            Validation history
        """
        try:
            # Get validation results from store
            validations = []
            
            # Retrieve validation history from store
            if self._context:
                # Get all validation results for the suite
                validation_store = self._context.stores.validations_store
                
                # Get all validation result identifiers
                validation_ids = validation_store.list_keys()
                
                # Filter by suite name if provided
                for validation_id in validation_ids:
                    if suite_name and validation_id.expectation_suite_identifier.expectation_suite_name != suite_name:
                        continue
                    
                    try:
                        # Get validation result
                        validation_result = validation_store.get(validation_id)
                        
                        # Convert to our format
                        result = ValidationResult(
                            success=validation_result.success,
                            expectation_suite_name=validation_id.expectation_suite_identifier.expectation_suite_name,
                            run_id=validation_id.run_id.run_name,
                            batch_id=validation_id.batch_identifier,
                            statistics={
                                "evaluated_expectations": validation_result.statistics.get("evaluated_expectations", 0),
                                "successful_expectations": validation_result.statistics.get("successful_expectations", 0),
                                "unsuccessful_expectations": validation_result.statistics.get("unsuccessful_expectations", 0),
                                "success_percent": validation_result.statistics.get("success_percent", 0)
                            },
                            validation_time=validation_result.meta.get("run_time", datetime.now()),
                            batch_kwargs=validation_result.meta.get("batch_kwargs", {}),
                            expectation_results=[
                                {
                                    "expectation_type": exp_result.expectation_config.expectation_type,
                                    "success": exp_result.success,
                                    "kwargs": exp_result.expectation_config.kwargs,
                                    "result": exp_result.result
                                }
                                for exp_result in validation_result.results
                            ]
                        )
                        
                        validations.append(result)
                        
                    except Exception as e:
                        logger.warning(f"Failed to retrieve validation {validation_id}: {e}")
                        continue
                
                # Sort by validation time
                validations.sort(key=lambda x: x.validation_time, reverse=True)
            
            return validations[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get validation history: {e}")
            raise
    
    async def create_data_quality_report(
        self,
        suite_names: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Create data quality report across multiple suites.
        
        Args:
            suite_names: Expectation suites to include
            start_date: Report start date
            end_date: Report end date
            
        Returns:
            Data quality report
        """
        try:
            report = {
                "report_date": datetime.now().isoformat(),
                "suite_summaries": {},
                "overall_statistics": {
                    "total_validations": 0,
                    "successful_validations": 0,
                    "failed_validations": 0,
                    "success_rate": 0.0
                }
            }
            
            # Generate comprehensive quality report
            if self._context:
                # Get all validation results within time range
                validation_store = self._context.stores.validations_store
                validation_ids = validation_store.list_keys()
                
                total_validations = 0
                successful_validations = 0
                failed_validations = 0
                expectation_results = []
                suite_results = {}
                
                for validation_id in validation_ids:
                    try:
                        # Get validation result
                        validation_result = validation_store.get(validation_id)
                        
                        # Check if within time range
                        run_time = validation_result.meta.get("run_time", datetime.now())
                        if isinstance(run_time, str):
                            run_time = datetime.fromisoformat(run_time)
                        
                        if start_date and run_time < start_date:
                            continue
                        if end_date and run_time > end_date:
                            continue
                        
                        # Update counters
                        total_validations += 1
                        if validation_result.success:
                            successful_validations += 1
                        else:
                            failed_validations += 1
                        
                        # Track suite-level results
                        suite_name = validation_id.expectation_suite_identifier.expectation_suite_name
                        if suite_name not in suite_results:
                            suite_results[suite_name] = {
                                "total": 0,
                                "successful": 0,
                                "failed": 0,
                                "expectations": {}
                            }
                        
                        suite_results[suite_name]["total"] += 1
                        if validation_result.success:
                            suite_results[suite_name]["successful"] += 1
                        else:
                            suite_results[suite_name]["failed"] += 1
                        
                        # Track expectation-level results
                        for exp_result in validation_result.results:
                            exp_type = exp_result.expectation_config.expectation_type
                            
                            if exp_type not in suite_results[suite_name]["expectations"]:
                                suite_results[suite_name]["expectations"][exp_type] = {
                                    "total": 0,
                                    "successful": 0,
                                    "failed": 0
                                }
                            
                            suite_results[suite_name]["expectations"][exp_type]["total"] += 1
                            if exp_result.success:
                                suite_results[suite_name]["expectations"][exp_type]["successful"] += 1
                            else:
                                suite_results[suite_name]["expectations"][exp_type]["failed"] += 1
                                
                    except Exception as e:
                        logger.warning(f"Failed to process validation {validation_id}: {e}")
                        continue
                
                # Calculate metrics
                success_rate = (successful_validations / total_validations * 100) if total_validations > 0 else 0
                
                # Find most common failures
                failure_patterns = {}
                for suite_name, suite_data in suite_results.items():
                    for exp_type, exp_data in suite_data["expectations"].items():
                        if exp_data["failed"] > 0:
                            failure_key = f"{suite_name}::{exp_type}"
                            failure_patterns[failure_key] = exp_data["failed"]
                
                # Sort failures by frequency
                top_failures = sorted(failure_patterns.items(), key=lambda x: x[1], reverse=True)[:10]
                
                # Update report
                report["summary"]["total_validations"] = total_validations
                report["summary"]["successful_validations"] = successful_validations
                report["summary"]["failed_validations"] = failed_validations
                report["summary"]["success_rate"] = round(success_rate, 2)
                
                report["suite_results"] = suite_results
                report["top_failures"] = [
                    {
                        "pattern": pattern,
                        "count": count,
                        "suite": pattern.split("::")[0],
                        "expectation": pattern.split("::")[1]
                    }
                    for pattern, count in top_failures
                ]
                
                # Add time-based analysis
                report["time_analysis"] = {
                    "period": f"{start_date or 'beginning'} to {end_date or 'now'}",
                    "daily_average": total_validations / max(1, (end_date - start_date).days if start_date and end_date else 30)
                }
                
                # Generate recommendations
                report["recommendations"] = []
                
                if success_rate < 90:
                    report["recommendations"].append({
                        "severity": "high",
                        "message": f"Overall success rate is {success_rate:.1f}%, below recommended 90%",
                        "action": "Review and fix failing expectations"
                    })
                
                for pattern, count in top_failures[:3]:
                    suite, expectation = pattern.split("::")
                    report["recommendations"].append({
                        "severity": "medium",
                        "message": f"{expectation} in {suite} failed {count} times",
                        "action": f"Investigate root cause of {expectation} failures"
                    })
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to create data quality report: {e}")
            raise
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Great Expectations specific configuration"""
        return {
            "context_root_dir": self.config.context_root_dir,
            "datasource_name": self.config.datasource_name,
            "enable_profiling": self.config.enable_profiling,
            "expectations_store_type": self.config.expectations_store_type,
            "validations_store_type": self.config.validations_store_type
        } 