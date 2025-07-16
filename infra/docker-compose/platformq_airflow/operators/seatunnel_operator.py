"""
SeaTunnel Airflow Operator

Custom operator for interacting with the SeaTunnel service from Airflow DAGs.
"""

from typing import Dict, Any, Optional, List
from datetime import timedelta

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
import json


class SeaTunnelOperator(BaseOperator):
    """
    Operator to create and manage SeaTunnel pipelines
    
    :param operation: Operation to perform (create, run, generate, analyze)
    :param job_id: Job ID for operations on existing jobs
    :param job_config: Configuration for creating new jobs
    :param asset_metadata: Metadata for pipeline generation
    :param http_conn_id: HTTP connection ID for SeaTunnel service
    :param tenant_id: Tenant ID for multi-tenancy
    :param wait_for_completion: Whether to wait for job completion
    :param poke_interval: Time in seconds between status checks
    :param timeout: Maximum time to wait for completion
    """
    
    template_fields = ['job_config', 'asset_metadata', 'job_id']
    ui_color = '#00CED1'
    
    @apply_defaults
    def __init__(
        self,
        operation: str,
        job_id: Optional[str] = None,
        job_config: Optional[Dict[str, Any]] = None,
        asset_metadata: Optional[Dict[str, Any]] = None,
        http_conn_id: str = 'seatunnel_service',
        tenant_id: Optional[str] = None,
        wait_for_completion: bool = False,
        poke_interval: int = 30,
        timeout: Optional[timedelta] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.operation = operation
        self.job_id = job_id
        self.job_config = job_config
        self.asset_metadata = asset_metadata
        self.http_conn_id = http_conn_id
        self.tenant_id = tenant_id
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self.timeout = timeout
    
    def execute(self, context):
        """Execute the SeaTunnel operation"""
        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if self.tenant_id:
            headers['X-Tenant-ID'] = self.tenant_id
        
        if self.operation == 'create':
            return self._create_job(http, headers, context)
        elif self.operation == 'run':
            return self._run_job(http, headers, context)
        elif self.operation == 'generate':
            return self._generate_pipeline(http, headers, context)
        elif self.operation == 'analyze':
            return self._analyze_source(http, headers, context)
        else:
            raise AirflowException(f"Unknown operation: {self.operation}")
    
    def _create_job(self, http, headers, context):
        """Create a new SeaTunnel job"""
        if not self.job_config:
            raise AirflowException("job_config is required for create operation")
        
        self.log.info(f"Creating SeaTunnel job with config: {self.job_config}")
        
        response = http.run(
            endpoint='/api/v1/jobs',
            data=json.dumps(self.job_config),
            headers=headers
        )
        
        job_info = json.loads(response.text)
        job_id = job_info['id']
        
        self.log.info(f"Created SeaTunnel job: {job_id}")
        
        if self.wait_for_completion:
            return self._wait_for_job(http, headers, job_id)
        
        return job_info
    
    def _run_job(self, http, headers, context):
        """Run an existing SeaTunnel job"""
        if not self.job_id:
            raise AirflowException("job_id is required for run operation")
        
        self.log.info(f"Running SeaTunnel job: {self.job_id}")
        
        response = http.run(
            endpoint=f'/api/v1/jobs/{self.job_id}/run',
            data='{}',
            headers=headers
        )
        
        run_info = json.loads(response.text)
        
        if self.wait_for_completion:
            return self._wait_for_job(http, headers, self.job_id)
        
        return run_info
    
    def _generate_pipeline(self, http, headers, context):
        """Generate a pipeline from asset metadata"""
        if not self.asset_metadata:
            raise AirflowException("asset_metadata is required for generate operation")
        
        self.log.info(f"Generating pipeline for asset: {self.asset_metadata.get('asset_id')}")
        
        # Determine if auto-deployment is requested
        auto_deploy = self.asset_metadata.pop('auto_deploy', False)
        
        response = http.run(
            endpoint=f'/api/v1/pipelines/generate?auto_deploy={auto_deploy}',
            data=json.dumps(self.asset_metadata),
            headers=headers
        )
        
        pipeline_info = json.loads(response.text)
        
        self.log.info(f"Generated pipeline with pattern: {pipeline_info['pattern']}")
        
        if auto_deploy and pipeline_info.get('job_id'):
            if self.wait_for_completion:
                return self._wait_for_job(http, headers, pipeline_info['job_id'])
        
        return pipeline_info
    
    def _analyze_source(self, http, headers, context):
        """Analyze a data source"""
        if not self.asset_metadata or 'source_uri' not in self.asset_metadata:
            raise AirflowException("asset_metadata with source_uri is required for analyze operation")
        
        source_uri = self.asset_metadata['source_uri']
        source_type = self.asset_metadata.get('source_type', 'file')
        sample_size = self.asset_metadata.get('sample_size', 1000)
        
        self.log.info(f"Analyzing source: {source_uri}")
        
        # Use GET for analyze endpoint
        http_get = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        
        response = http_get.run(
            endpoint=f'/api/v1/pipelines/analyze?source_uri={source_uri}&source_type={source_type}&sample_size={sample_size}',
            headers=headers
        )
        
        analysis = json.loads(response.text)
        
        self.log.info(f"Analysis complete. Detected format: {analysis.get('source_analysis', {}).get('detected_format')}")
        
        return analysis
    
    def _wait_for_job(self, http, headers, job_id):
        """Wait for job completion"""
        import time
        from datetime import datetime
        
        start_time = datetime.now()
        http_get = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        
        while True:
            response = http_get.run(
                endpoint=f'/api/v1/jobs/{job_id}',
                headers=headers
            )
            
            job_status = json.loads(response.text)
            status = job_status.get('status')
            
            self.log.info(f"Job {job_id} status: {status}")
            
            if status in ['completed', 'failed', 'cancelled']:
                if status == 'failed':
                    raise AirflowException(f"SeaTunnel job failed: {job_status.get('error_message')}")
                return job_status
            
            # Check timeout
            if self.timeout and (datetime.now() - start_time) > self.timeout:
                raise AirflowException(f"Timeout waiting for job {job_id}")
            
            time.sleep(self.poke_interval)


class SeaTunnelMonitoringOperator(BaseOperator):
    """
    Operator to monitor SeaTunnel pipeline metrics
    
    :param job_id: Job ID to monitor
    :param metric_checks: List of metric checks to perform
    :param http_conn_id: HTTP connection ID for SeaTunnel service
    :param time_range_hours: Time range for metrics in hours
    """
    
    template_fields = ['job_id', 'metric_checks']
    ui_color = '#FF6347'
    
    @apply_defaults
    def __init__(
        self,
        job_id: str,
        metric_checks: List[Dict[str, Any]],
        http_conn_id: str = 'seatunnel_service',
        time_range_hours: int = 1,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_id = job_id
        self.metric_checks = metric_checks
        self.http_conn_id = http_conn_id
        self.time_range_hours = time_range_hours
    
    def execute(self, context):
        """Check metrics against thresholds"""
        http = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        
        # Get metrics
        response = http.run(
            endpoint=f'/api/v1/jobs/{self.job_id}/metrics?time_range_hours={self.time_range_hours}'
        )
        
        metrics_data = json.loads(response.text)
        metrics = metrics_data.get('metrics', {})
        
        self.log.info(f"Retrieved metrics for job {self.job_id}: {list(metrics.keys())}")
        
        # Check each metric
        failures = []
        for check in self.metric_checks:
            metric_name = check['metric']
            threshold = check['threshold']
            operator = check.get('operator', '>')
            
            if metric_name not in metrics:
                self.log.warning(f"Metric {metric_name} not found")
                continue
            
            # Get the current value
            metric_value = metrics[metric_name].get('current', 0)
            
            # Perform check
            check_passed = self._check_metric(metric_value, threshold, operator)
            
            if not check_passed:
                failure_msg = f"Metric {metric_name} failed: {metric_value} {operator} {threshold}"
                failures.append(failure_msg)
                self.log.error(failure_msg)
            else:
                self.log.info(f"Metric {metric_name} passed: {metric_value} {operator} {threshold}")
        
        if failures:
            raise AirflowException(f"Metric checks failed: {'; '.join(failures)}")
        
        return metrics_data
    
    def _check_metric(self, value: float, threshold: float, operator: str) -> bool:
        """Check if metric meets threshold"""
        if operator == '>':
            return value <= threshold
        elif operator == '<':
            return value >= threshold
        elif operator == '>=':
            return value < threshold
        elif operator == '<=':
            return value > threshold
        elif operator == '==':
            return value == threshold
        else:
            raise AirflowException(f"Unknown operator: {operator}")


class SeaTunnelBatchOperator(BaseOperator):
    """
    Operator to process multiple assets in batch
    
    :param assets: List of assets to process
    :param pipeline_template: Optional template to apply to all assets
    :param http_conn_id: HTTP connection ID for SeaTunnel service
    :param max_parallel: Maximum parallel pipelines
    """
    
    template_fields = ['assets', 'pipeline_template']
    ui_color = '#9370DB'
    
    @apply_defaults
    def __init__(
        self,
        assets: List[Dict[str, Any]],
        pipeline_template: Optional[Dict[str, Any]] = None,
        http_conn_id: str = 'seatunnel_service',
        max_parallel: int = 5,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.assets = assets
        self.pipeline_template = pipeline_template
        self.http_conn_id = http_conn_id
        self.max_parallel = max_parallel
    
    def execute(self, context):
        """Generate pipelines for multiple assets"""
        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        self.log.info(f"Processing batch of {len(self.assets)} assets")
        
        # Prepare batch request
        batch_request = {
            'assets': self.assets
        }
        
        if self.pipeline_template:
            batch_request['pipeline_template'] = self.pipeline_template
        
        # Submit batch
        response = http.run(
            endpoint='/api/v1/pipelines/batch/generate',
            data=json.dumps(batch_request),
            headers=headers
        )
        
        results = json.loads(response.text)
        
        self.log.info(f"Batch processing complete: {results['successful']} successful, {results['failed']} failed")
        
        # Check for failures
        if results['failed'] > 0:
            failed_assets = [r for r in results['results'] if r['status'] == 'failed']
            self.log.warning(f"Failed assets: {failed_assets}")
        
        return results 