"""
Processing VC Helper Module

Provides utilities for processing services to request VCs for completed jobs.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pulsar.schema import Record, String, Long
from .event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class AssetProcessingCredentialRequested(Record):
    tenant_id = String()
    parent_asset_id = String()
    parent_asset_vc_id = String(required=False)
    output_asset_id = String()
    output_asset_hash = String()
    processing_job_id = String()
    processor_did = String()
    algorithm = String()
    parameters = String()  # JSON string
    processing_timestamp = Long()
    processing_duration_ms = Long(required=False)


class ProcessingVCHelper:
    """Helper class for processing services to request VCs"""
    
    def __init__(self, event_publisher: EventPublisher):
        self.publisher = event_publisher
        
    def request_processing_vc(
        self,
        tenant_id: str,
        parent_asset_id: str,
        output_asset_id: str,
        output_asset_hash: str,
        processing_job_id: str,
        processor_did: str,
        algorithm: str,
        parameters: Dict[str, Any],
        processing_duration_ms: Optional[int] = None,
        parent_asset_vc_id: Optional[str] = None
    ) -> bool:
        """
        Request a processing VC for a completed job
        
        Args:
            tenant_id: Tenant ID
            parent_asset_id: ID of the input asset
            output_asset_id: ID of the output asset
            output_asset_hash: Hash of the output asset
            processing_job_id: ID of the processing job
            processor_did: DID of the processor/service
            algorithm: Algorithm or process used
            parameters: Processing parameters
            processing_duration_ms: Processing duration in milliseconds
            parent_asset_vc_id: VC ID of the parent asset (if known)
            
        Returns:
            bool: True if request was published successfully
        """
        try:
            event = AssetProcessingCredentialRequested(
                tenant_id=tenant_id,
                parent_asset_id=parent_asset_id,
                parent_asset_vc_id=parent_asset_vc_id,
                output_asset_id=output_asset_id,
                output_asset_hash=output_asset_hash,
                processing_job_id=processing_job_id,
                processor_did=processor_did,
                algorithm=algorithm,
                parameters=json.dumps(parameters),
                processing_timestamp=int(datetime.utcnow().timestamp() * 1000),
                processing_duration_ms=processing_duration_ms
            )
            
            self.publisher.publish(
                topic_base='asset-processing-credential-requests',
                tenant_id=tenant_id,
                schema_class=AssetProcessingCredentialRequested,
                data=event
            )
            
            logger.info(f"Requested processing VC for job {processing_job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to request processing VC: {e}")
            return False
    
    @staticmethod
    def generate_processor_did(service_name: str, tenant_id: str) -> str:
        """Generate a DID for a processing service"""
        return f"did:platformq:{tenant_id}:service:{service_name}"
    
    @staticmethod
    def compute_output_hash(output_data: bytes) -> str:
        """Compute hash of output data"""
        import hashlib
        return hashlib.sha256(output_data).hexdigest()


# Convenience functions for processing services
def request_processing_vc_for_spark_job(
    publisher: EventPublisher,
    tenant_id: str,
    job_id: str,
    input_asset_id: str,
    output_asset_id: str,
    output_data: bytes,
    algorithm: str,
    spark_config: Dict[str, Any]
) -> bool:
    """Request a VC for a completed Spark job"""
    
    helper = ProcessingVCHelper(publisher)
    
    return helper.request_processing_vc(
        tenant_id=tenant_id,
        parent_asset_id=input_asset_id,
        output_asset_id=output_asset_id,
        output_asset_hash=helper.compute_output_hash(output_data),
        processing_job_id=job_id,
        processor_did=helper.generate_processor_did("spark-processor", tenant_id),
        algorithm=algorithm,
        parameters=spark_config
    )


def request_processing_vc_for_flink_job(
    publisher: EventPublisher,
    tenant_id: str,
    job_id: str,
    input_asset_id: str,
    output_asset_id: str,
    output_data: bytes,
    algorithm: str,
    flink_config: Dict[str, Any],
    processing_duration_ms: int
) -> bool:
    """Request a VC for a completed Flink job"""
    
    helper = ProcessingVCHelper(publisher)
    
    return helper.request_processing_vc(
        tenant_id=tenant_id,
        parent_asset_id=input_asset_id,
        output_asset_id=output_asset_id,
        output_asset_hash=helper.compute_output_hash(output_data),
        processing_job_id=job_id,
        processor_did=helper.generate_processor_did("flink-processor", tenant_id),
        algorithm=algorithm,
        parameters=flink_config,
        processing_duration_ms=processing_duration_ms
    ) 