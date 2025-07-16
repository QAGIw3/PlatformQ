"""
Neuromorphic Event Processing Service

Ultra-low latency anomaly detection using spiking neural networks.
"""

import asyncio
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import numpy as np

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import pulsar
from pydantic import BaseModel

# SNN Libraries (will use Brian2 as primary)
import brian2 as b2
from brian2 import *

# Platform shared libraries
from platformq.shared.base_service import BaseService
from platformq.shared.pulsar_client import PulsarClient
from platformq.shared.ignite_utils import IgniteCache
from platformq.shared.logging_config import get_logger
from platformq.shared.monitoring import track_processing_time

# Local modules
from .models.snn_core import SpikingNeuralNetwork
from .models.event_encoder import EventEncoder
from .models.anomaly_detector import AnomalyDetector
from .models.workflow_anomaly_detector import WorkflowAnomalyDetector
from .core.config import settings

logger = get_logger(__name__)

# --- New Additions for Optimization ---
class OptimizationRequest(BaseModel):
    problem_type: str
    problem_data: Dict[str, Any]
    solver_params: Optional[Dict[str, Any]] = None

class OptimizationResponse(BaseModel):
    status: str
    solution: Optional[Dict[str, Any]] = None
    objective_value: Optional[float] = None
    error: Optional[str] = None

def qubo_to_snn_params(qubo_matrix: np.ndarray):
    """
    Converts a QUBO matrix into parameters suitable for an SNN.
    This is a conceptual mapping. A more rigorous approach would be needed for
    a production system.

    We map the QUBO to a Hopfield-like network using spiking neurons.
    - Diagonal elements (linear terms) map to neuron excitability (current injection).
    - Off-diagonal elements (quadratic terms) map to synaptic weights.
    """
    num_vars = qubo_matrix.shape[0]
    
    # Negative weights for inhibitory connections, positive for excitatory
    synaptic_weights = -qubo_matrix 
    
    # Linear terms influence the baseline firing rate
    neuron_currents = -np.diag(qubo_matrix)
    
    return num_vars, synaptic_weights, neuron_currents

# --- End of New Additions ---


class NeuromorphicService(BaseService):
    """
    Neuromorphic Event Processing Service using Spiking Neural Networks
    """
    
    def __init__(self):
        super().__init__("neuromorphic-service", "Neuromorphic Event Processing")
        self.config = settings
        self.snn = None
        self.encoder = None
        self.detector = None
        self.workflow_detector = None  # New workflow-specific detector
        self.event_cache = None
        self.pattern_memory = None
        self.workflow_patterns = None # New workflow pattern memory
        self.workflow_optimization_client = None  # Client for quantum optimization
        
    async def startup(self):
        """Initialize service components"""
        await super().startup()
        
        # Initialize SNN components
        self.encoder = EventEncoder(
            encoding_type=self.config.encoding_type,
            num_neurons=self.config.input_neurons
        )
        
        self.snn = SpikingNeuralNetwork(
            input_size=self.config.input_neurons,
            reservoir_size=self.config.reservoir_neurons,
            output_size=self.config.output_neurons,
            connectivity=self.config.connectivity,
            spectral_radius=self.config.spectral_radius
        )
        
        self.detector = AnomalyDetector(
            threshold=self.config.anomaly_threshold,
            window_size=self.config.detection_window
        )
        
        # Initialize workflow anomaly detector
        self.workflow_detector = WorkflowAnomalyDetector(
            threshold=self.config.workflow_anomaly_threshold,
            window_size=self.config.detection_window,
            learning_rate=self.config.learning_rate,
            historical_buffer_size=self.config.workflow_history_buffer_size
        )
        
        # Initialize caches
        self.event_cache = IgniteCache("neuromorphic_events")
        self.pattern_memory = IgniteCache("anomaly_patterns")
        self.workflow_patterns = IgniteCache("workflow_patterns")
        
        # Initialize quantum optimization client
        self.workflow_optimization_client = WorkflowOptimizationClient(
            service_url=self.config.quantum_optimization_url
        )
        
        # Start Pulsar consumers
        await self._start_event_consumers()
        
        logger.info("Neuromorphic service initialized successfully")
        
    async def solve_optimization_problem(self, request: OptimizationRequest) -> Dict:
        """
        Solves an optimization problem using a configured SNN.
        """
        if request.problem_type.lower() != 'qubo':
            raise NotImplementedError("Currently, only QUBO problems are supported.")

        qubo_matrix = np.array(request.problem_data.get("qubo_matrix"))
        if qubo_matrix is None:
            raise ValueError("QUBO matrix not found in problem_data.")

        num_vars, weights, currents = qubo_to_snn_params(qubo_matrix)

        # Configure a dedicated SNN for this optimization task
        # This could be cached or managed in a pool in a real system
        solver_snn = SpikingNeuralNetwork(
            input_size=num_vars,
            reservoir_size=num_vars, # For Hopfield-like nets, reservoir can be same as input
            output_size=num_vars,
            connectivity=0.9, # High connectivity for optimization
            spectral_radius=1.1 # Push network to be more dynamic
        )
        solver_snn.configure_for_optimization(weights, currents)
        
        # Run the network until it settles into a stable state (a solution)
        runtime = request.solver_params.get("runtime_ms", 500)
        solution_spikes = solver_snn.process(None, duration=runtime * b2.ms)

        # Decode the final state of the network to a binary solution vector
        # A simple decoding: if a neuron is firing above a threshold, it's a '1'.
        firing_rates = solver_snn.get_firing_rates()
        solution_vector = (firing_rates > np.mean(firing_rates)).astype(int)

        # Calculate the objective value of this solution
        objective_value = solution_vector @ qubo_matrix @ solution_vector

        return {
            "status": "SUCCESS",
            "solution": {f"x_{i}": int(v) for i, v in enumerate(solution_vector)},
            "objective_value": objective_value
        }
        
    async def _start_event_consumers(self):
        """Start consuming events from Pulsar"""
        topics = self.config.get_pulsar_topics_list()
        
        # Add workflow-specific topics
        workflow_topics = self.config.get_workflow_topics_list()
        
        for topic in topics:
            asyncio.create_task(self._consume_events(topic))
            
        for topic in workflow_topics:
            asyncio.create_task(self._consume_workflow_events(topic))
            
    async def _consume_events(self, topic: str):
        """Consume and process events from a specific topic"""
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=f"neuromorphic-{topic.split('/')[-1]}",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=100)
                event_data = json.loads(msg.data())
                
                # Process through SNN
                anomaly = await self._process_event(event_data)
                
                if anomaly:
                    await self._publish_anomaly(anomaly)
                    
                consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing event: {e}")
                    
    async def _consume_workflow_events(self, topic: str):
        """Consume and process workflow events"""
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=f"neuromorphic-workflow-{topic.split('/')[-1]}",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=100)
                event_data = json.loads(msg.data())
                
                # Process through workflow-specific detector
                anomaly = await self._process_workflow_event(event_data)
                
                if anomaly:
                    await self._handle_workflow_anomaly(anomaly)
                    
                consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing workflow event: {e}")
                    
    @track_processing_time("neuromorphic_processing")
    async def _process_event(self, event: Dict) -> Optional[Dict]:
        """Process event through the spiking neural network"""
        
        # Encode event into spike trains
        spike_train = self.encoder.encode(event)
        
        # Process through SNN
        output_spikes = self.snn.process(spike_train)
        
        # Detect anomalies
        anomaly = self.detector.detect(output_spikes, event)
        
        # Cache recent patterns
        if anomaly:
            pattern_key = f"pattern:{anomaly['anomaly_type']}:{datetime.utcnow().isoformat()}"
            await self.pattern_memory.put(pattern_key, {
                "spike_pattern": output_spikes.tolist(),
                "event": event,
                "anomaly": anomaly
            })
            
        return anomaly
        
    @track_processing_time("neuromorphic_workflow_processing")
    async def _process_workflow_event(self, event: Dict) -> Optional[Dict]:
        """Process workflow event through specialized SNN"""
        
        # Detect anomalies using workflow-specific detector
        anomaly = self.workflow_detector.detect_workflow_anomaly(event)
        
        if anomaly:
            # Enrich anomaly with additional context
            anomaly['tenant_id'] = event.get('tenant_id')
            anomaly['workflow_name'] = event.get('workflow_name')
            
            # Cache the anomaly pattern
            pattern_key = f"workflow_pattern:{anomaly['anomaly_type']}:{anomaly['workflow_id']}:{datetime.utcnow().isoformat()}"
            await self.workflow_patterns.put(pattern_key, {
                "anomaly": anomaly,
                "event": event,
                "timestamp": datetime.utcnow().isoformat()
            })
            
        return anomaly
        
    async def _publish_anomaly(self, anomaly: Dict):
        """Publish detected anomaly to Pulsar"""
        await self.pulsar_producer.send_async(
            self.config.anomaly_topic,
            json.dumps(anomaly).encode('utf-8')
        )
        
        # Also cache for fast retrieval
        await self.event_cache.put(
            f"anomaly:{anomaly['anomaly_id']}", 
            anomaly,
            ttl=3600  # 1 hour TTL
        )

    async def _handle_workflow_anomaly(self, anomaly: Dict):
        """Handle detected workflow anomaly"""
        logger.info(f"Workflow anomaly detected: {anomaly['anomaly_type']} for workflow {anomaly['workflow_id']}")
        
        # Publish anomaly event
        await self._publish_workflow_anomaly(anomaly)
        
        # Determine if optimization should be triggered
        if self._should_trigger_optimization(anomaly):
            await self._trigger_workflow_optimization(anomaly)
            
    async def _publish_workflow_anomaly(self, anomaly: Dict):
        """Publish workflow anomaly to Pulsar"""
        await self.pulsar_producer.send_async(
            self.config.workflow_anomaly_topic,
            json.dumps(anomaly).encode('utf-8')
        )
        
        # Also cache for fast retrieval
        await self.event_cache.put(
            f"workflow_anomaly:{anomaly['anomaly_id']}", 
            anomaly,
            ttl=3600  # 1 hour TTL
        )
        
    def _should_trigger_optimization(self, anomaly: Dict) -> bool:
        """Determine if anomaly warrants automatic optimization"""
        # High severity anomalies
        if anomaly['severity'] > self.config.workflow_anomaly_severity_threshold:
            return True
            
        # Specific anomaly types that benefit from optimization
        optimization_worthy_types = [
            'performance_degradation',
            'resource_contention',
            'excessive_retries',
            'scheduling_inefficiency'
        ]
        
        if anomaly['anomaly_type'] in optimization_worthy_types:
            return True
            
        # Multiple failures
        if anomaly.get('metrics', {}).get('failed_tasks', 0) > 5:
            return True
            
        return False
        
    async def _trigger_workflow_optimization(self, anomaly: Dict):
        """Trigger optimization via quantum-optimization-service"""
        logger.info(f"Triggering workflow optimization for anomaly {anomaly['anomaly_id']}")
        
        # Prepare optimization request
        optimization_request = {
            "request_id": f"opt_{anomaly['anomaly_id']}",
            "workflow_id": anomaly['workflow_id'],
            "tenant_id": anomaly['tenant_id'],
            "anomaly_id": anomaly['anomaly_id'],
            "optimization_type": self._determine_optimization_type(anomaly),
            "optimization_goals": self._determine_optimization_goals(anomaly),
            "constraints": self._extract_constraints(anomaly),
            "historical_data": await self._get_historical_data(anomaly['workflow_id']),
            "requested_at": int(datetime.utcnow().timestamp() * 1000),
            "requested_by": "neuromorphic-anomaly-detector",
            "priority": self._determine_priority(anomaly)
        }
        
        # Publish optimization request
        await self.pulsar_producer.send_async(
            self.config.workflow_optimization_topic,
            json.dumps(optimization_request).encode('utf-8')
        )
        
        # Also send directly to quantum optimization service
        try:
            result = await self.workflow_optimization_client.optimize_workflow(
                anomaly=anomaly,
                optimization_request=optimization_request
            )
            logger.info(f"Optimization request submitted: {result}")
        except Exception as e:
            logger.error(f"Failed to submit optimization request: {e}")
            
        # Update anomaly record
        anomaly['auto_optimization_triggered'] = True
        anomaly['optimization_request_id'] = optimization_request['request_id']
        
    def _determine_optimization_type(self, anomaly: Dict) -> str:
        """Determine the type of optimization needed"""
        anomaly_type = anomaly['anomaly_type']
        
        mapping = {
            'performance_degradation': 'task_scheduling',
            'resource_contention': 'resource_allocation',
            'excessive_retries': 'retry_strategy',
            'failure_pattern': 'error_handling',
            'scheduling_inefficiency': 'parallelism'
        }
        
        return mapping.get(anomaly_type, 'general_optimization')
        
    def _determine_optimization_goals(self, anomaly: Dict) -> List[str]:
        """Determine optimization goals based on anomaly"""
        goals = []
        
        if anomaly['anomaly_type'] == 'performance_degradation':
            goals.extend(['minimize_duration', 'optimize_parallelism'])
        elif anomaly['anomaly_type'] == 'failure_pattern':
            goals.extend(['reduce_failures', 'improve_reliability'])
        elif anomaly['anomaly_type'] == 'resource_contention':
            goals.extend(['optimize_resources', 'reduce_wait_time'])
            
        # Add general goals
        if anomaly['severity'] > 0.8:
            goals.append('critical_performance_improvement')
            
        return goals
        
    def _extract_constraints(self, anomaly: Dict) -> Dict[str, str]:
        """Extract optimization constraints from anomaly context"""
        constraints = {}
        
        # Resource constraints
        metrics = anomaly.get('metrics', {})
        if metrics.get('cpu_usage'):
            constraints['max_cpu_cores'] = "16"  # Example constraint
        if metrics.get('memory_usage'):
            constraints['max_memory_gb'] = "32"  # Example constraint
            
        # Time constraints
        constraints['max_duration_minutes'] = "60"
        
        # Reliability constraints
        constraints['max_failure_rate'] = "0.05"
        
        return constraints
        
    async def _get_historical_data(self, workflow_id: str) -> Dict[str, Any]:
        """Get historical performance data for workflow"""
        # Retrieve from workflow history
        history = self.workflow_detector.workflow_history.get(workflow_id, [])
        
        if not history:
            return {
                "avg_duration_ms": 0,
                "failure_rate": 0,
                "resource_usage_patterns": {}
            }
            
        # Calculate statistics
        durations = [h.execution_duration_ms for h in history]
        failures = [h.failed_tasks / max(h.task_count, 1) for h in history]
        
        return {
            "avg_duration_ms": np.mean(durations),
            "failure_rate": np.mean(failures),
            "resource_usage_patterns": {
                "avg_cpu": np.mean([h.cpu_usage for h in history if h.cpu_usage]),
                "avg_memory": np.mean([h.memory_usage for h in history if h.memory_usage]),
                "avg_parallelism": np.mean([h.parallelism_degree for h in history])
            }
        }
        
    def _determine_priority(self, anomaly: Dict) -> str:
        """Determine optimization priority"""
        severity = anomaly['severity']
        
        if severity > 0.9:
            return "critical"
        elif severity > 0.7:
            return "high"
        elif severity > 0.5:
            return "normal"
        else:
            return "low"


class WorkflowOptimizationClient:
    """Client for interacting with quantum-optimization-service"""
    
    def __init__(self, service_url: str):
        self.service_url = service_url
        
    async def optimize_workflow(self, anomaly: Dict, optimization_request: Dict) -> Dict:
        """Submit workflow optimization request to quantum service"""
        import httpx
        
        # Prepare quantum optimization problem
        problem_data = self._prepare_optimization_problem(anomaly, optimization_request)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.service_url}/api/v1/optimize",
                json={
                    "problem_type": "workflow_optimization",
                    "problem_data": problem_data,
                    "algorithm": "hybrid",  # Use hybrid quantum-classical
                    "max_time": 300,
                    "quality_target": 0.95
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Optimization request failed: {response.text}")
                
    def _prepare_optimization_problem(self, anomaly: Dict, request: Dict) -> Dict:
        """Prepare optimization problem for quantum solver"""
        # Convert workflow optimization to QUBO or other quantum-compatible format
        
        optimization_type = request['optimization_type']
        
        if optimization_type == 'task_scheduling':
            return self._prepare_scheduling_problem(anomaly, request)
        elif optimization_type == 'resource_allocation':
            return self._prepare_resource_problem(anomaly, request)
        elif optimization_type == 'parallelism':
            return self._prepare_parallelism_problem(anomaly, request)
        else:
            return self._prepare_general_problem(anomaly, request)
            
    def _prepare_scheduling_problem(self, anomaly: Dict, request: Dict) -> Dict:
        """Prepare task scheduling optimization problem"""
        metrics = anomaly.get('metrics', {})
        
        return {
            "problem_type": "task_scheduling",
            "num_tasks": metrics.get('task_count', 10),
            "dependencies": [],  # Would need actual dependency graph
            "resource_constraints": request.get('constraints', {}),
            "objective": "minimize_makespan",
            "current_duration": metrics.get('duration_ms', 0)
        }
        
    def _prepare_resource_problem(self, anomaly: Dict, request: Dict) -> Dict:
        """Prepare resource allocation problem"""
        return {
            "problem_type": "resource_allocation",
            "resources": {
                "cpu": 16,
                "memory": 32,
                "network": 1000
            },
            "tasks": [],  # Would need actual task requirements
            "objective": "minimize_cost",
            "constraints": request.get('constraints', {})
        }
        
    def _prepare_parallelism_problem(self, anomaly: Dict, request: Dict) -> Dict:
        """Prepare parallelism optimization problem"""
        metrics = anomaly.get('metrics', {})
        
        return {
            "problem_type": "graph_partitioning",
            "num_nodes": metrics.get('task_count', 10),
            "edges": [],  # Would need actual dependency edges
            "num_partitions": 4,  # Target parallelism
            "objective": "minimize_communication",
            "balance_constraint": 0.1  # Max 10% imbalance
        }
        
    def _prepare_general_problem(self, anomaly: Dict, request: Dict) -> Dict:
        """Prepare general optimization problem"""
        return {
            "problem_type": "general_optimization",
            "variables": 10,
            "objective_function": "quadratic",
            "constraints": request.get('constraints', {}),
            "anomaly_context": anomaly
        }


# Initialize FastAPI app
service = NeuromorphicService()
app = service.app

# API Models
class ModelCreateRequest(BaseModel):
    name: str
    architecture: Dict
    training_config: Dict

class AnomalyResponse(BaseModel):
    anomaly_id: str
    detected_at: str
    anomaly_type: str
    severity: float
    contributing_events: List[str]
    recommended_actions: List[str]

# --- New Endpoint ---
@app.post("/api/v1/solve", response_model=OptimizationResponse)
async def solve_problem(request: OptimizationRequest):
    """
    Endpoint to solve optimization problems using a neuromorphic approach.
    """
    try:
        result = await service.solve_optimization_problem(request)
        return OptimizationResponse(**result)
    except Exception as e:
        logger.error(f"Error during neuromorphic optimization: {e}", exc_info=True)
        return OptimizationResponse(status="FAILED", error=str(e))

# REST API Endpoints
@app.post("/api/v1/models", response_model=Dict)
async def create_model(request: ModelCreateRequest):
    """Create a new SNN model configuration"""
    model_id = f"model_{datetime.utcnow().timestamp()}"
    
    # Store model configuration
    await service.pattern_memory.put(f"model:{model_id}", {
        "id": model_id,
        "name": request.name,
        "architecture": request.architecture,
        "training_config": request.training_config,
        "created_at": datetime.utcnow().isoformat()
    })
    
    return {"model_id": model_id, "status": "created"}

@app.get("/api/v1/anomalies", response_model=List[AnomalyResponse])
async def get_anomalies(
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    anomaly_type: Optional[str] = None,
    limit: int = 100
):
    """Retrieve detected anomalies"""
    # Query from cache/storage
    anomalies = []
    
    # TODO: Implement proper querying from Ignite/Cassandra
    # For now, return cached anomalies
    
    return anomalies

@app.get("/api/v1/anomalies/{anomaly_id}", response_model=AnomalyResponse)
async def get_anomaly_detail(anomaly_id: str):
    """Get detailed information about a specific anomaly"""
    anomaly = await service.event_cache.get(f"anomaly:{anomaly_id}")
    
    if not anomaly:
        return JSONResponse(status_code=404, content={"error": "Anomaly not found"})
        
    return AnomalyResponse(**anomaly)

@app.get("/api/v1/metrics")
async def get_metrics():
    """Get service performance metrics"""
    return {
        "events_processed": service.snn.events_processed if service.snn else 0,
        "anomalies_detected": service.detector.total_anomalies if service.detector else 0,
        "average_latency_ms": 0.5,  # Placeholder
        "spike_rate_hz": service.snn.get_spike_rate() if service.snn else 0,
        "memory_usage_mb": 256,  # Placeholder
        "models_deployed": 1
    }

# WebSocket endpoints
@app.websocket("/ws/anomalies/{stream_id}")
async def anomaly_stream(websocket: WebSocket, stream_id: str):
    """Real-time anomaly event stream"""
    await websocket.accept()
    
    # Subscribe to anomaly events for this stream
    consumer = service.pulsar_client.subscribe(
        f"persistent://public/default/anomaly-events-{stream_id}",
        subscription_name=f"ws-{stream_id}-{datetime.utcnow().timestamp()}"
    )
    
    try:
        while True:
            msg = consumer.receive(timeout_millis=1000)
            anomaly = json.loads(msg.data())
            await websocket.send_json(anomaly)
            consumer.acknowledge(msg)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for stream {stream_id}")
    finally:
        consumer.close()

@app.websocket("/ws/spikes/{model_id}")
async def spike_visualization(websocket: WebSocket, model_id: str):
    """Real-time spike train visualization"""
    await websocket.accept()
    
    try:
        while True:
            # Send spike data for visualization
            if service.snn:
                spike_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "spike_trains": service.snn.get_recent_spikes(window_ms=100),
                    "firing_rates": service.snn.get_firing_rates()
                }
                await websocket.send_json(spike_data)
                
            await asyncio.sleep(0.1)  # 100ms updates
            
    except WebSocketDisconnect:
        logger.info(f"Spike visualization disconnected for model {model_id}")

# Health check
@app.get("/health")
async def health_check():
    """Service health check"""
    return {
        "status": "healthy",
        "service": "neuromorphic-service",
        "snn_initialized": service.snn is not None,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 