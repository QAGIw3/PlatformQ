"""
WASM Function Operator for Apache Airflow

This operator triggers WASM function execution by publishing events
to the functions-service via Pulsar.
"""

import os
import time
from typing import Dict, Any, Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pulsar
from pulsar.schema import AvroSchema


# Define the ExecuteWasmFunction schema
class ExecuteWasmFunction:
    """Avro schema for WASM function execution requests"""
    def __init__(self, tenant_id: str, asset_id: str, asset_uri: str, wasm_module_id: str):
        self.tenant_id = tenant_id
        self.asset_id = asset_id
        self.asset_uri = asset_uri
        self.wasm_module_id = wasm_module_id


class WASMFunctionOperator(BaseOperator):
    """
    Triggers WASM function execution for asset processing
    
    :param wasm_module_id: ID of the WASM module to execute
    :param asset_id: ID of the asset to process
    :param asset_uri: URI to the asset data
    :param tenant_id: Tenant ID for multi-tenant isolation
    :param pulsar_url: Pulsar service URL
    :param wait_for_completion: Whether to wait for function completion
    :param timeout: Timeout in seconds when waiting for completion
    """
    
    template_fields = ['wasm_module_id', 'asset_id', 'asset_uri', 'tenant_id']
    ui_color = '#AA96DA'
    
    @apply_defaults
    def __init__(
        self,
        wasm_module_id: str,
        asset_id: str,
        asset_uri: str,
        tenant_id: Optional[str] = None,
        pulsar_url: Optional[str] = None,
        wait_for_completion: bool = True,
        timeout: int = 300,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.wasm_module_id = wasm_module_id
        self.asset_id = asset_id
        self.asset_uri = asset_uri
        self.tenant_id = tenant_id or os.getenv('PLATFORMQ_TENANT_ID', '00000000-0000-0000-0000-000000000000')
        self.pulsar_url = pulsar_url or os.getenv('PLATFORMQ_PULSAR_URL', 'pulsar://pulsar:6650')
        self.wait_for_completion = wait_for_completion
        self.timeout = timeout
    
    def execute(self, context) -> Dict[str, Any]:
        """Execute the operator to trigger WASM function"""
        self.log.info(f"Triggering WASM function {self.wasm_module_id} for asset {self.asset_id}")
        
        # Create Pulsar client
        client = pulsar.Client(self.pulsar_url)
        
        try:
            # Create the event
            event = ExecuteWasmFunction(
                tenant_id=self.tenant_id,
                asset_id=self.asset_id,
                asset_uri=self.asset_uri,
                wasm_module_id=self.wasm_module_id
            )
            
            # Publish to the WASM execution topic
            topic = f"persistent://platformq/{self.tenant_id}/wasm-function-execution-requests"
            
            producer = client.create_producer(
                topic=topic,
                schema=AvroSchema(ExecuteWasmFunction)
            )
            
            message_id = producer.send(event)
            self.log.info(f"Published WASM execution request with message ID: {message_id}")
            
            producer.close()
            
            result = {
                'message_id': str(message_id),
                'asset_id': self.asset_id,
                'wasm_module_id': self.wasm_module_id,
                'status': 'triggered'
            }
            
            if self.wait_for_completion:
                # Set up consumer for completion events
                completion_topic = f"persistent://platformq/{self.tenant_id}/function-execution-completed-events"
                subscription = f"airflow-wasm-completion-{self.asset_id}"
                
                # Note: In production, we'd need the actual FunctionExecutionCompleted schema
                # For now, we'll use a generic consumer
                consumer = client.subscribe(
                    topic=completion_topic,
                    subscription_name=subscription,
                    consumer_type=pulsar.ConsumerType.Shared
                )
                
                start_time = time.time()
                
                while time.time() - start_time < self.timeout:
                    try:
                        msg = consumer.receive(timeout_millis=1000)
                        
                        # Check if this is our completion event
                        # In production, we'd parse the event properly
                        event_data = msg.data()
                        
                        # Simple check - in production, parse the Avro message
                        if self.asset_id.encode() in event_data:
                            self.log.info(f"Received completion event for asset {self.asset_id}")
                            consumer.acknowledge(msg)
                            
                            result['status'] = 'completed'
                            break
                            
                        # Not our event, negative acknowledge to requeue
                        consumer.negative_acknowledge(msg)
                        
                    except Exception as e:
                        if "Timeout" not in str(e):
                            raise
                
                consumer.close()
                
                if result['status'] != 'completed':
                    self.log.warning(f"Timed out waiting for WASM function completion after {self.timeout} seconds")
                    result['status'] = 'timeout'
            
            # Store result in XCom
            context['ti'].xcom_push(key='wasm_result', value=result)
            
            return result
            
        except Exception as e:
            self.log.error(f"Failed to trigger WASM function: {str(e)}")
            raise
        finally:
            client.close()
    
    def on_kill(self):
        """Cleanup when task is killed"""
        self.log.info("Task killed, cleaning up Pulsar connections") 