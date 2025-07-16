"""
PlatformQ Spark Operators for Apache Airflow

This module provides custom Airflow operators for submitting and managing
Spark jobs within the PlatformQ ecosystem.
"""

import os
import json
import time
from typing import Dict, Any, Optional, List
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.hooks.http import HttpHook
import requests


class SparkPlatformQOperator(SparkSubmitOperator):
    """
    Extended SparkSubmitOperator with PlatformQ-specific features
    
    This operator adds multi-tenant support, automatic S3 configuration,
    and integration with PlatformQ's monitoring stack.
    """
    
    template_fields = SparkSubmitOperator.template_fields + ('tenant_id', 'processing_config')
    ui_color = '#FD6A02'
    
    @apply_defaults
    def __init__(
        self,
        tenant_id: str,
        processing_config: Optional[Dict[str, Any]] = None,
        spark_master: Optional[str] = None,
        *args,
        **kwargs
    ):
        # Set default Spark configuration
        self.tenant_id = tenant_id
        self.processing_config = processing_config or {}
        
        # Default to PlatformQ Spark master
        if not spark_master:
            spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark_master:7077')
        
        # Set default configuration
        default_conf = {
            'spark.platformq.tenant.id': tenant_id,
            'spark.hadoop.fs.s3a.endpoint': os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            'spark.hadoop.fs.s3a.access.key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'spark.hadoop.fs.s3a.secret.key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
        }
        
        # Merge with user-provided conf
        if 'conf' in kwargs:
            kwargs['conf'].update(default_conf)
        else:
            kwargs['conf'] = default_conf
        
        # Call parent constructor
        super().__init__(conn_id='spark_default', *args, **kwargs)
        
        # Override master URL
        self._conf['spark.master'] = spark_master
    
    def execute(self, context):
        """Execute the Spark job with PlatformQ tracking"""
        # Log job start
        self.log.info(f"Starting Spark job for tenant {self.tenant_id}")
        self.log.info(f"Processing config: {json.dumps(self.processing_config, indent=2)}")
        
        # Record start time
        start_time = datetime.utcnow()
        
        try:
            # Execute Spark job
            result = super().execute(context)
            
            # Record success metrics
            self._record_job_metrics(
                context=context,
                status='success',
                start_time=start_time,
                end_time=datetime.utcnow()
            )
            
            return result
            
        except Exception as e:
            # Record failure metrics
            self._record_job_metrics(
                context=context,
                status='failed',
                start_time=start_time,
                end_time=datetime.utcnow(),
                error=str(e)
            )
            raise
    
    def _record_job_metrics(self, context, status, start_time, end_time, error=None):
        """Record job metrics for monitoring"""
        metrics = {
            'tenant_id': self.tenant_id,
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'run_id': context['run_id'],
            'status': status,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds(),
            'application': self._application,
            'error': error
        }
        
        # Log metrics
        self.log.info(f"Job metrics: {json.dumps(metrics, indent=2)}")
        
        # TODO: Send to Prometheus pushgateway or other metrics store


class SparkProcessorOperator(BaseOperator):
    """
    Operator for running PlatformQ distributed processors on Spark
    
    This operator simplifies running distributed file processors
    (Blender, FreeCAD, etc.) on Spark.
    """
    
    template_fields = ['asset_id', 'asset_uri', 'processing_config']
    ui_color = '#FF7143'
    
    @apply_defaults
    def __init__(
        self,
        processor_type: str,
        asset_id: str,
        asset_uri: str,
        tenant_id: str,
        processing_config: Optional[Dict[str, Any]] = None,
        spark_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.processor_type = processor_type
        self.asset_id = asset_id
        self.asset_uri = asset_uri
        self.tenant_id = tenant_id
        self.processing_config = processing_config or {}
        self.spark_config = spark_config or {}
    
    def execute(self, context):
        """Submit processor job to Spark"""
        self.log.info(f"Submitting {self.processor_type} job for asset {self.asset_id}")
        
        # Map processor type to Spark application
        processor_map = {
            'blender': '/opt/spark-jobs/blender_distributed.py',
            'freecad': '/opt/spark-jobs/freecad_distributed.py',
            'openfoam': '/opt/spark-jobs/openfoam_distributed.py',
            'gimp': '/opt/spark-jobs/gimp_distributed.py'
        }
        
        if self.processor_type not in processor_map:
            raise AirflowException(f"Unknown processor type: {self.processor_type}")
        
        application = processor_map[self.processor_type]
        
        # Prepare processing config
        config = self.processing_config.copy()
        config['asset_id'] = self.asset_id
        
        # Prepare application arguments
        application_args = [
            self.tenant_id,
            self.asset_uri,
            json.dumps(config)
        ]
        
        # Create SparkPlatformQOperator
        spark_op = SparkPlatformQOperator(
            task_id=f"spark_{self.processor_type}_{self.asset_id}",
            tenant_id=self.tenant_id,
            application=application,
            application_args=application_args,
            processing_config=config,
            **self.spark_config
        )
        
        # Execute
        result = spark_op.execute(context)
        
        # Store result in XCom
        context['ti'].xcom_push(key='processing_result', value=result)
        
        return result


class SparkGraphXOperator(BaseOperator):
    """
    Operator for running GraphX analytics jobs on PlatformQ data
    """
    
    template_fields = ['graph_algorithm', 'parameters']
    ui_color = '#3E8DDD'
    
    @apply_defaults
    def __init__(
        self,
        graph_algorithm: str,
        tenant_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        output_table: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.graph_algorithm = graph_algorithm
        self.tenant_id = tenant_id
        self.parameters = parameters or {}
        self.output_table = output_table
    
    def execute(self, context):
        """Execute GraphX algorithm"""
        self.log.info(f"Running GraphX algorithm: {self.graph_algorithm}")
        
        # GraphX job configuration
        spark_conf = {
            'spark.jars': '/opt/spark/jars/janusgraph-spark.jar',
            'spark.executor.memory': '8g',
            'spark.executor.cores': '4'
        }
        
        # Prepare application based on algorithm
        algorithms = {
            'pagerank': '/opt/spark-jobs/graphx/pagerank.py',
            'connected_components': '/opt/spark-jobs/graphx/connected_components.py',
            'triangle_count': '/opt/spark-jobs/graphx/triangle_count.py',
            'community_detection': '/opt/spark-jobs/graphx/community_detection.py'
        }
        
        if self.graph_algorithm not in algorithms:
            raise AirflowException(f"Unknown graph algorithm: {self.graph_algorithm}")
        
        application = algorithms[self.graph_algorithm]
        
        # Prepare arguments
        args = [
            self.tenant_id,
            json.dumps(self.parameters)
        ]
        
        if self.output_table:
            args.append(self.output_table)
        
        # Create and execute Spark job
        spark_op = SparkPlatformQOperator(
            task_id=f"graphx_{self.graph_algorithm}",
            tenant_id=self.tenant_id,
            application=application,
            application_args=args,
            conf=spark_conf
        )
        
        result = spark_op.execute(context)
        
        # Parse and return results
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except:
                pass
        
        context['ti'].xcom_push(key='graph_result', value=result)
        
        return result


class SparkMLOperator(BaseOperator):
    """
    Operator for running Spark MLlib pipelines
    """
    
    template_fields = ['model_type', 'training_data', 'parameters']
    ui_color = '#8B4CDD'
    
    @apply_defaults
    def __init__(
        self,
        model_type: str,
        tenant_id: str,
        training_data: str,
        parameters: Optional[Dict[str, Any]] = None,
        model_name: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.model_type = model_type
        self.tenant_id = tenant_id
        self.training_data = training_data
        self.parameters = parameters or {}
        self.model_name = model_name or f"{model_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    def execute(self, context):
        """Train and save ML model"""
        self.log.info(f"Training {self.model_type} model: {self.model_name}")
        
        # ML job configuration
        spark_conf = {
            'spark.executor.memory': '8g',
            'spark.executor.cores': '4',
            'spark.sql.shuffle.partitions': '200'
        }
        
        # Map model types to applications
        model_apps = {
            'asset_classifier': '/opt/spark-jobs/ml/asset_classifier.py',
            'anomaly_detector': '/opt/spark-jobs/ml/anomaly_detector.py',
            'recommendation': '/opt/spark-jobs/ml/recommendation_engine.py',
            'forecasting': '/opt/spark-jobs/ml/time_series_forecast.py'
        }
        
        if self.model_type not in model_apps:
            raise AirflowException(f"Unknown model type: {self.model_type}")
        
        application = model_apps[self.model_type]
        
        # Prepare arguments
        args = [
            self.tenant_id,
            self.training_data,
            self.model_name,
            json.dumps(self.parameters)
        ]
        
        # Create and execute Spark job
        spark_op = SparkPlatformQOperator(
            task_id=f"ml_{self.model_type}",
            tenant_id=self.tenant_id,
            application=application,
            application_args=args,
            conf=spark_conf
        )
        
        result = spark_op.execute(context)
        
        # Store model info in XCom
        model_info = {
            'model_name': self.model_name,
            'model_type': self.model_type,
            'training_completed': datetime.utcnow().isoformat(),
            'result': result
        }
        
        context['ti'].xcom_push(key='model_info', value=model_info)
        
        return model_info 