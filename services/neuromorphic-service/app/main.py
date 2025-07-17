"""
Neuromorphic Event Processing Service

Ultra-low latency anomaly detection using spiking neural networks.
"""

import asyncio
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import numpy as np

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pulsar
from pydantic import BaseModel
import logging

# Platform shared libraries
from platformq_shared.pulsar_client import get_pulsar_client
from platformq_shared.ignite_utils import get_ignite_client

# Local modules
from .models.spiking_neural_network import SpikingNeuralNetwork, SpikeTrain
from .models.event_processor import NeuromorphicEventProcessor, Event, AnomalyDetection
from .models.brain_inspired_optimizer import (
    BrainInspiredOptimizer, 
    OptimizationProblem, 
    OptimizationResult
)

logger = logging.getLogger(__name__)

# --- Pydantic Models ---
class OptimizationRequest(BaseModel):
    problem_type: str
    problem_data: Dict[str, Any]
    solver_params: Optional[Dict[str, Any]] = None

class OptimizationResponse(BaseModel):
    status: str
    solution: Optional[Dict[str, Any]] = None
    objective_value: Optional[float] = None
    error: Optional[str] = None

class EventData(BaseModel):
    event_id: str
    event_type: str
    timestamp: str
    source: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = {}

class AnomalyResponse(BaseModel):
    is_anomaly: bool
    confidence: float
    anomaly_type: Optional[str]
    contributing_factors: List[str]
    recommended_actions: List[str]
    processing_time_ms: float

class TrainingRequest(BaseModel):
    model_type: str  # "anomaly", "pattern", "sequence"
    training_data: List[Dict[str, Any]]
    epochs: int = 10
    learning_rate: float = 0.01

# Initialize FastAPI app
app = FastAPI(
    title="Neuromorphic Processing Service",
    description="Ultra-low latency event processing and optimization using spiking neural networks",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
event_processor = None
brain_optimizer = None
pulsar_client = None
ignite_client = None

# WebSocket connections
active_connections: Dict[str, WebSocket] = {}


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global event_processor, brain_optimizer, pulsar_client, ignite_client
    
    # Initialize neuromorphic components
    event_processor = NeuromorphicEventProcessor(
        feature_dim=100,
        hidden_layers=[200, 100, 50],
        anomaly_threshold=0.8,
        learning_enabled=True
    )
    
    brain_optimizer = BrainInspiredOptimizer(
        neuron_count=1000,
        connectivity=0.1,
        learning_rate=0.01
    )
    
    # Initialize Pulsar client
    try:
        pulsar_client = await get_pulsar_client()
        logger.info("Connected to Pulsar")
    except Exception as e:
        logger.error(f"Failed to connect to Pulsar: {e}")
        
    # Initialize Ignite client
    try:
        ignite_client = await get_ignite_client()
        logger.info("Connected to Ignite")
    except Exception as e:
        logger.error(f"Failed to connect to Ignite: {e}")
        
    # Start event consumer
    asyncio.create_task(consume_events())
    
    logger.info("Neuromorphic Processing Service started")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    if pulsar_client:
        await pulsar_client.close()
    if ignite_client:
        ignite_client.close()
        
    # Close WebSocket connections
    for ws in active_connections.values():
        await ws.close()
        
    logger.info("Neuromorphic Processing Service stopped")


# --- Event Processing Endpoints ---
@app.post("/api/v1/events/process", response_model=AnomalyResponse)
async def process_event(event_data: EventData):
    """Process a single event for anomaly detection"""
    try:
        # Convert to Event object
        event = Event(
            event_id=event_data.event_id,
            event_type=event_data.event_type,
            timestamp=datetime.fromisoformat(event_data.timestamp),
            source=event_data.source,
            data=event_data.data,
            metadata=event_data.metadata
        )
        
        # Process with neuromorphic engine
        start_time = datetime.now()
        anomaly_result = await event_processor.process_event(event)
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Store in Ignite cache if anomaly detected
        if anomaly_result.is_anomaly and ignite_client:
            cache_key = f"anomaly_{event.event_id}"
            cache_value = {
                "event": event_data.dict(),
                "anomaly": {
                    "type": anomaly_result.anomaly_type,
                    "confidence": anomaly_result.confidence,
                    "factors": anomaly_result.contributing_factors,
                    "timestamp": datetime.now().isoformat()
                }
            }
            await ignite_client.put_async("anomalies", cache_key, cache_value)
            
        # Broadcast to WebSocket clients if anomaly
        if anomaly_result.is_anomaly:
            await broadcast_anomaly(event_data, anomaly_result)
            
        return AnomalyResponse(
            is_anomaly=anomaly_result.is_anomaly,
            confidence=anomaly_result.confidence,
            anomaly_type=anomaly_result.anomaly_type,
            contributing_factors=anomaly_result.contributing_factors,
            recommended_actions=anomaly_result.recommended_actions,
            processing_time_ms=processing_time
        )
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/events/batch")
async def process_batch_events(events: List[EventData]):
    """Process a batch of events"""
    try:
        # Convert to Event objects
        event_objects = []
        for event_data in events:
            event_objects.append(Event(
                event_id=event_data.event_id,
                event_type=event_data.event_type,
                timestamp=datetime.fromisoformat(event_data.timestamp),
                source=event_data.source,
                data=event_data.data,
                metadata=event_data.metadata
            ))
            
        # Analyze event stream
        analysis = await event_processor.analyze_event_stream(event_objects)
        
        return {
            "status": "success",
            "analysis": analysis
        }
        
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/events/predict")
async def predict_next_event():
    """Predict the next event based on recent history"""
    try:
        prediction = await event_processor.predict_next_event()
        return prediction
    except Exception as e:
        logger.error(f"Error predicting event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Optimization Endpoints ---
@app.post("/api/v1/optimize", response_model=OptimizationResponse)
async def optimize(request: OptimizationRequest):
    """Solve optimization problem using brain-inspired algorithms"""
    try:
        # Parse problem type
        if request.problem_type == "qubo":
            # Quadratic Unconstrained Binary Optimization
            Q = np.array(request.problem_data["matrix"])
            method = request.solver_params.get("method", "spiking_annealing")
            
            result = await brain_optimizer.solve_qubo(Q, method=method)
            
            return OptimizationResponse(
                status="success",
                solution={
                    "binary_vector": result["solution"].tolist(),
                    "iterations": result["iterations"],
                    "spike_activity": result["spike_activity"],
                    "energy_consumed": result["energy_consumed"]
                },
                objective_value=result["objective_value"]
            )
            
        elif request.problem_type == "continuous":
            # Continuous optimization
            # Define objective function from string or coefficients
            dimension = request.problem_data["dimension"]
            bounds = request.problem_data.get("bounds", [[-10, 10]] * dimension)
            
            # Create objective function
            if "function" in request.problem_data:
                # Simple polynomial for demo
                coeffs = request.problem_data["function"]["coefficients"]
                def objective(x):
                    return sum(c * x[i]**p for i, (c, p) in enumerate(coeffs))
            else:
                # Default: sphere function
                def objective(x):
                    return np.sum(x**2)
                    
            problem = OptimizationProblem(
                objective_function=objective,
                constraints=[],
                bounds=bounds,
                dimension=dimension,
                problem_type="minimize"
            )
            
            method = request.solver_params.get("method", "neural_swarm")
            max_iterations = request.solver_params.get("max_iterations", 1000)
            
            result = await brain_optimizer.optimize(
                problem,
                method=method,
                max_iterations=max_iterations
            )
            
            return OptimizationResponse(
                status="success",
                solution={
                    "vector": result.solution.tolist(),
                    "iterations": result.iterations,
                    "convergence_history": result.convergence_history[-100:],  # Last 100
                    "spike_activity": result.spike_activity,
                    "energy_consumed": result.energy_consumed
                },
                objective_value=result.objective_value
            )
            
        else:
            return OptimizationResponse(
                status="error",
                error=f"Unknown problem type: {request.problem_type}"
            )
            
    except Exception as e:
        logger.error(f"Optimization error: {e}")
        return OptimizationResponse(
            status="error",
            error=str(e)
        )


@app.post("/api/v1/optimize/benchmark")
async def benchmark_optimization(request: OptimizationRequest):
    """Benchmark neuromorphic vs classical optimization"""
    try:
        if request.problem_type != "continuous":
            raise ValueError("Benchmarking only supports continuous problems")
            
        # Create problem
        dimension = request.problem_data["dimension"]
        bounds = request.problem_data.get("bounds", [[-10, 10]] * dimension)
        
        # Rastrigin function (challenging optimization problem)
        def rastrigin(x):
            A = 10
            return A * len(x) + sum(xi**2 - A * np.cos(2 * np.pi * xi) for xi in x)
            
        problem = OptimizationProblem(
            objective_function=rastrigin,
            constraints=[],
            bounds=bounds,
            dimension=dimension,
            problem_type="minimize"
        )
        
        # Run benchmark
        results = brain_optimizer.benchmark_against_classical(problem)
        
        return results
        
    except Exception as e:
        logger.error(f"Benchmark error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Training Endpoints ---
@app.post("/api/v1/train")
async def train_model(request: TrainingRequest, background_tasks: BackgroundTasks):
    """Train neuromorphic models"""
    
    async def _train():
        try:
            if request.model_type == "anomaly":
                # Train anomaly detection model
                # Convert training data to events
                events = []
                for data in request.training_data:
                    events.append(Event(
                        event_id=data["event_id"],
                        event_type=data["event_type"],
                        timestamp=datetime.fromisoformat(data["timestamp"]),
                        source=data["source"],
                        data=data["data"],
                        metadata=data.get("metadata", {})
                    ))
                    
                # Process and learn
                for event in events:
                    anomaly = await event_processor.process_event(event)
                    
                    # Update accuracy metrics if labels provided
                    if "is_anomaly" in event.metadata:
                        event_processor.update_detection_accuracy(
                            predicted=anomaly.is_anomaly,
                            actual=event.metadata["is_anomaly"]
                        )
                        
                logger.info(f"Training completed for {len(events)} events")
                
        except Exception as e:
            logger.error(f"Training error: {e}")
            
    background_tasks.add_task(_train)
    
    return {
        "status": "training_started",
        "model_type": request.model_type,
        "num_samples": len(request.training_data)
    }


# --- Monitoring Endpoints ---
@app.get("/api/v1/metrics")
async def get_metrics():
    """Get performance metrics"""
    try:
        metrics = event_processor.get_performance_metrics()
        
        # Add neuromorphic-specific metrics
        metrics["neuromorphic_advantages"] = {
            "ultra_low_latency": metrics["processing_latency"]["mean_ms"] < 1.0,
            "energy_efficiency": metrics["neuromorphic_stats"]["energy_efficiency"],
            "spike_based_processing": True,
            "online_learning": event_processor.learning_enabled
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/metrics/reset")
async def reset_metrics():
    """Reset performance metrics"""
    event_processor.reset_metrics()
    return {"status": "metrics_reset"}


# --- WebSocket Endpoints ---
@app.websocket("/ws/anomalies/{client_id}")
async def anomaly_websocket(websocket: WebSocket, client_id: str):
    """WebSocket for real-time anomaly notifications"""
    await websocket.accept()
    active_connections[client_id] = websocket
    
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if client_id in active_connections:
            del active_connections[client_id]


async def broadcast_anomaly(event: EventData, anomaly: AnomalyDetection):
    """Broadcast anomaly to all connected WebSocket clients"""
    message = {
        "type": "anomaly_detected",
        "timestamp": datetime.now().isoformat(),
        "event": event.dict(),
        "anomaly": {
            "is_anomaly": anomaly.is_anomaly,
            "confidence": anomaly.confidence,
            "type": anomaly.anomaly_type,
            "factors": anomaly.contributing_factors,
            "actions": anomaly.recommended_actions
        }
    }
    
    disconnected = []
    for client_id, websocket in active_connections.items():
        try:
            await websocket.send_json(message)
        except:
            disconnected.append(client_id)
            
    # Remove disconnected clients
    for client_id in disconnected:
        del active_connections[client_id]


# --- Event Consumer ---
async def consume_events():
    """Consume events from Pulsar for real-time processing"""
    if not pulsar_client:
        logger.warning("Pulsar client not available")
        return
        
    try:
        consumer = await pulsar_client.subscribe(
            "persistent://public/default/platform-events",
            "neuromorphic-processor",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        while True:
            try:
                msg = await consumer.receive_async(timeout_millis=1000)
                
                # Parse event
                event_data = json.loads(msg.data())
                
                # Create Event object
                event = Event(
                    event_id=event_data.get("event_id", str(msg.message_id())),
                    event_type=event_data.get("event_type", "unknown"),
                    timestamp=datetime.fromisoformat(event_data.get("timestamp", datetime.now().isoformat())),
                    source=event_data.get("source", "pulsar"),
                    data=event_data.get("data", {}),
                    metadata=event_data.get("metadata", {})
                )
                
                # Process event
                anomaly = await event_processor.process_event(event)
                
                # Acknowledge message
                await consumer.acknowledge_async(msg)
                
                # Log if anomaly
                if anomaly.is_anomaly:
                    logger.warning(f"Anomaly detected: {anomaly.anomaly_type} - {event.event_id}")
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error consuming event: {e}")
                
    except Exception as e:
        logger.error(f"Consumer error: {e}")


# --- Utility Endpoints ---
@app.get("/api/v1/snn/visualize/{model_type}")
async def visualize_snn(model_type: str):
    """Get SNN visualization data"""
    try:
        if model_type == "anomaly":
            snn = event_processor.anomaly_detector
        elif model_type == "pattern":
            snn = event_processor.pattern_recognizer
        elif model_type == "sequence":
            snn = event_processor.sequence_predictor
        else:
            raise ValueError(f"Unknown model type: {model_type}")
            
        activity = snn.analyze_activity()
        
        # Add network structure
        activity["network_structure"] = {
            "input_size": snn.input_size,
            "hidden_layers": snn.hidden_sizes,
            "output_size": snn.output_size,
            "neuron_model": snn.neuron_model,
            "total_neurons": snn.input_size + sum(snn.hidden_sizes) + snn.output_size
        }
        
        return activity
        
    except Exception as e:
        logger.error(f"Visualization error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Service health check"""
    return {
        "status": "healthy",
        "service": "neuromorphic-processing-service",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "event_processor": event_processor is not None,
            "brain_optimizer": brain_optimizer is not None,
            "pulsar_connected": pulsar_client is not None,
            "ignite_connected": ignite_client is not None,
            "active_websockets": len(active_connections)
        },
        "neuromorphic_features": {
            "spiking_neural_networks": True,
            "event_based_processing": True,
            "brain_inspired_optimization": True,
            "ultra_low_latency": True,
            "energy_efficient": True
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 