"""
Base class for distributed processors using Apache Spark

This module provides the foundation for distributed processing of
digital assets using Spark's parallel computing capabilities.
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType
import boto3

logger = logging.getLogger(__name__)


class ProcessorBase(ABC):
    """Base class for distributed processors"""
    
    def __init__(self, app_name: str, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the processor
        
        :param app_name: Spark application name
        :param tenant_id: Tenant ID for multi-tenant isolation
        :param config: Additional configuration
        """
        self.app_name = app_name
        self.tenant_id = tenant_id
        self.config = config or {}
        self._spark = None
        
    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session"""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark
    
    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with PlatformQ configuration"""
        conf = SparkConf()
        
        # Set application name with tenant context
        conf.setAppName(f"{self.app_name}-{self.tenant_id}")
        
        # Multi-tenant isolation
        conf.set("spark.platformq.tenant.id", self.tenant_id)
        
        # Apply custom configuration
        for key, value in self.config.items():
            if key.startswith("spark."):
                conf.set(key, value)
        
        # Create session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(self.config.get("log_level", "WARN"))
        
        return spark
    
    @abstractmethod
    def process(self, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an asset using distributed computing
        
        :param asset_uri: URI to the asset (S3/MinIO path)
        :param processing_config: Processing configuration
        :return: Processing results with metadata
        """
        pass
    
    @abstractmethod
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """
        Validate processing configuration
        
        :param processing_config: Configuration to validate
        :return: True if valid, raises exception otherwise
        """
        pass
    
    def split_work(self, asset_uri: str, processing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Split work into chunks for parallel processing
        
        :param asset_uri: URI to the asset
        :param processing_config: Processing configuration
        :return: List of work chunks
        """
        # Default implementation - override in subclasses
        return [{"asset_uri": asset_uri, "chunk_id": 0, "config": processing_config}]
    
    def process_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single chunk of work
        
        :param chunk: Work chunk
        :return: Processing result for the chunk
        """
        # Default implementation - must be overridden
        raise NotImplementedError("Subclasses must implement process_chunk")
    
    def merge_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge results from all chunks
        
        :param results: List of chunk results
        :return: Merged result
        """
        # Default implementation - combine all results
        merged = {
            "chunks_processed": len(results),
            "processing_time": sum(r.get("processing_time", 0) for r in results),
            "metadata": {}
        }
        
        # Merge metadata from all chunks
        for result in results:
            if "metadata" in result:
                merged["metadata"].update(result["metadata"])
        
        return merged
    
    def run(self, asset_id: str, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point for distributed processing
        
        :param asset_id: ID of the asset being processed
        :param asset_uri: URI to the asset
        :param processing_config: Processing configuration
        :return: Processing results
        """
        start_time = datetime.utcnow()
        
        try:
            # Validate configuration
            self.validate_config(processing_config)
            
            # Log start
            logger.info(f"Starting distributed processing for asset {asset_id}")
            
            # Split work into chunks
            chunks = self.split_work(asset_uri, processing_config)
            logger.info(f"Split work into {len(chunks)} chunks")
            
            # Create RDD from chunks
            chunks_rdd = self.spark.sparkContext.parallelize(chunks)
            
            # Process chunks in parallel
            results_rdd = chunks_rdd.map(self.process_chunk)
            
            # Collect results
            results = results_rdd.collect()
            
            # Merge results
            final_result = self.merge_results(results)
            
            # Add metadata
            final_result["asset_id"] = asset_id
            final_result["processor"] = self.app_name
            final_result["tenant_id"] = self.tenant_id
            final_result["start_time"] = start_time.isoformat()
            final_result["end_time"] = datetime.utcnow().isoformat()
            final_result["total_processing_time"] = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(f"Completed processing for asset {asset_id}")
            
            return final_result
            
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {str(e)}")
            return {
                "asset_id": asset_id,
                "processor": self.app_name,
                "status": "error",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.utcnow().isoformat()
            }
        finally:
            # Clean up
            if self._spark:
                self._spark.stop()
                self._spark = None
    
    def download_from_s3(self, s3_uri: str, local_path: str) -> str:
        """
        Download file from S3/MinIO to local filesystem
        
        :param s3_uri: S3 URI (s3://bucket/key)
        :param local_path: Local path to save file
        :return: Local file path
        """
        # Parse S3 URI
        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]
        
        # Get S3 client configuration from Spark
        endpoint_url = self.spark.conf.get("spark.hadoop.fs.s3a.endpoint")
        access_key = self.spark.conf.get("spark.hadoop.fs.s3a.access.key")
        secret_key = self.spark.conf.get("spark.hadoop.fs.s3a.secret.key")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        # Download file
        s3_client.download_file(bucket, key, local_path)
        
        return local_path
    
    def upload_to_s3(self, local_path: str, s3_uri: str) -> str:
        """
        Upload file from local filesystem to S3/MinIO
        
        :param local_path: Local file path
        :param s3_uri: S3 URI (s3://bucket/key)
        :return: S3 URI
        """
        # Parse S3 URI
        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]
        
        # Get S3 client configuration from Spark
        endpoint_url = self.spark.conf.get("spark.hadoop.fs.s3a.endpoint")
        access_key = self.spark.conf.get("spark.hadoop.fs.s3a.access.key")
        secret_key = self.spark.conf.get("spark.hadoop.fs.s3a.secret.key")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        # Upload file
        s3_client.upload_file(local_path, bucket, key)
        
        return s3_uri
    
    def broadcast_data(self, data: Any) -> Any:
        """
        Broadcast data to all executors
        
        :param data: Data to broadcast
        :return: Broadcast variable
        """
        return self.spark.sparkContext.broadcast(data)
    
    def accumulator(self, initial_value: Any) -> Any:
        """
        Create an accumulator for collecting metrics
        
        :param initial_value: Initial value
        :return: Accumulator
        """
        return self.spark.sparkContext.accumulator(initial_value) 