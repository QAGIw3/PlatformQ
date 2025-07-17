"""
Flink Job: Simulation ML Trigger

Monitors simulation events and triggers federated learning sessions
when optimization opportunities are detected.
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import httpx
import asyncio
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationMetricsAnalyzer(KeyedProcessFunction):
    """Analyzes simulation metrics to detect ML training opportunities"""
    
    def __init__(self, federated_learning_url: str, mlops_url: str):
        self.fl_url = federated_learning_url
        self.mlops_url = mlops_url
        self.metrics_state = None
        self.training_history = None
        self.http_client = None
        
    def open(self, runtime_context):
        """Initialize state and connections"""
        # State for tracking simulation metrics
        self.metrics_state = runtime_context.get_state(
            ValueStateDescriptor(
                "simulation_metrics",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT())
            )
        )
        
        # State for tracking training history
        self.training_history = runtime_context.get_list_state(
            ListStateDescriptor(
                "training_history",
                type_info=DataTypes.ROW([
                    DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
                    DataTypes.FIELD("session_id", DataTypes.STRING()),
                    DataTypes.FIELD("model_type", DataTypes.STRING()),
                    DataTypes.FIELD("performance_gain", DataTypes.FLOAT())
                ])
            )
        )
        
        # HTTP client for service calls
        self.http_client = httpx.Client(timeout=30.0)
        
    def close(self):
        """Clean up resources"""
        if self.http_client:
            self.http_client.close()
            
    def process_element(self, value, ctx):
        """Process simulation state events"""
        try:
            simulation_id = ctx.get_current_key()
            event = value
            
            # Update metrics state
            current_metrics = self.metrics_state.value() or {}
            
            if event["event_type"] == "SIMULATION_STATE_UPDATE":
                metrics = event.get("metrics", {})
                
                # Track key performance indicators
                current_metrics["convergence_rate"] = metrics.get("convergence_rate", 0)
                current_metrics["total_steps"] = metrics.get("total_steps", 0)
                current_metrics["objective_value"] = metrics.get("objective_value", float('inf'))
                current_metrics["stability_score"] = metrics.get("stability_score", 0)
                current_metrics["last_update"] = ctx.timestamp()
                
                self.metrics_state.update(current_metrics)
                
                # Check if ML training should be triggered
                if self._should_trigger_ml_training(current_metrics, simulation_id):
                    training_request = self._create_training_request(
                        simulation_id,
                        event,
                        current_metrics
                    )
                    
                    # Output training request
                    yield training_request
                    
            elif event["event_type"] == "SIMULATION_COMPLETED":
                # Final opportunity to trigger training
                if current_metrics and current_metrics.get("total_steps", 0) > 1000:
                    training_request = self._create_training_request(
                        simulation_id,
                        event,
                        current_metrics,
                        is_final=True
                    )
                    yield training_request
                    
        except Exception as e:
            logger.error(f"Error processing simulation event: {e}")
            
    def _should_trigger_ml_training(self, 
                                  metrics: Dict[str, float],
                                  simulation_id: str) -> bool:
        """Determine if ML training should be triggered"""
        # Check minimum data requirements
        if metrics.get("total_steps", 0) < 500:
            return False
            
        # Check if recently trained
        training_history = list(self.training_history.get() or [])
        if training_history:
            latest_training = max(training_history, key=lambda x: x[0])
            time_since_training = datetime.now().timestamp() - latest_training[0] / 1000
            if time_since_training < 3600:  # Less than 1 hour
                return False
                
        # Trigger conditions
        convergence_rate = metrics.get("convergence_rate", 1.0)
        stability_score = metrics.get("stability_score", 1.0)
        
        # Poor convergence
        if convergence_rate < 0.3:
            return True
            
        # Unstable simulation
        if stability_score < 0.5:
            return True
            
        # Periodic optimization (every 10000 steps)
        if metrics.get("total_steps", 0) % 10000 == 0:
            return True
            
        return False
        
    def _create_training_request(self,
                               simulation_id: str,
                               event: Dict[str, Any],
                               metrics: Dict[str, float],
                               is_final: bool = False) -> Dict[str, Any]:
        """Create federated learning training request"""
        # Determine model type based on metrics
        if metrics.get("convergence_rate", 1.0) < 0.5:
            model_type = "parameter_tuning"
        elif metrics.get("stability_score", 1.0) < 0.7:
            model_type = "agent_behavior"
        else:
            model_type = "outcome_prediction"
            
        return {
            "event_type": "ML_TRAINING_REQUEST",
            "simulation_id": simulation_id,
            "tenant_id": event.get("tenant_id"),
            "timestamp": datetime.utcnow().isoformat(),
            "request_details": {
                "model_type": model_type,
                "training_mode": "federated",
                "urgency": "high" if not is_final else "normal",
                "current_metrics": metrics,
                "simulation_parameters": event.get("parameters", {}),
                "data_window_hours": 24,
                "min_participants": 1,
                "privacy_budget": 1.0
            }
        }


class MLSessionOrchestrator(ProcessFunction):
    """Orchestrates federated learning sessions"""
    
    def __init__(self, services_config: Dict[str, str]):
        self.fl_service_url = services_config["federated_learning_service"]
        self.mlops_url = services_config["mlops_service"]
        self.simulation_url = services_config["simulation_service"]
        self.http_client = None
        
    def open(self, runtime_context):
        """Initialize HTTP client"""
        self.http_client = httpx.Client(timeout=30.0)
        
    def close(self):
        """Clean up resources"""
        if self.http_client:
            self.http_client.close()
            
    def process_element(self, value, ctx):
        """Process ML training requests"""
        try:
            request = value
            simulation_id = request["simulation_id"]
            tenant_id = request["tenant_id"]
            details = request["request_details"]
            
            # Create federated learning session
            fl_session_request = {
                "session_name": f"Simulation {simulation_id} Optimization",
                "model_type": self._map_to_fl_model_type(details["model_type"]),
                "min_participants": details["min_participants"],
                "rounds": 10,
                "privacy_parameters": {
                    "mechanism": "GAUSSIAN",
                    "epsilon": details["privacy_budget"],
                    "delta": 1e-5
                },
                "aggregation_strategy": "WEIGHTED_AVERAGE",
                "data_requirements": {
                    "min_samples": 100,
                    "data_window_hours": details["data_window_hours"]
                }
            }
            
            # Call federated learning service
            response = self.http_client.post(
                f"{self.fl_service_url}/api/v1/sessions",
                json=fl_session_request,
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 201:
                fl_session = response.json()
                
                # Notify simulation service
                notification = {
                    "simulation_id": simulation_id,
                    "fl_session_id": fl_session["session_id"],
                    "model_type": details["model_type"],
                    "status": "initiated"
                }
                
                sim_response = self.http_client.post(
                    f"{self.simulation_url}/api/v1/simulations/{simulation_id}/ml-session",
                    json=notification,
                    headers={"X-Tenant-ID": tenant_id}
                )
                
                # Output success event
                yield {
                    "event_type": "ML_SESSION_INITIATED",
                    "simulation_id": simulation_id,
                    "fl_session_id": fl_session["session_id"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "details": fl_session
                }
            else:
                logger.error(f"Failed to create FL session: {response.text}")
                
                # Output failure event
                yield {
                    "event_type": "ML_SESSION_FAILED",
                    "simulation_id": simulation_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "error": response.text
                }
                
        except Exception as e:
            logger.error(f"Error orchestrating ML session: {e}")
            
            # Output error event
            yield {
                "event_type": "ML_SESSION_ERROR",
                "simulation_id": request.get("simulation_id", "unknown"),
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }
            
    def _map_to_fl_model_type(self, model_type: str) -> str:
        """Map simulation model type to FL service model type"""
        mapping = {
            "agent_behavior": "neural_network",
            "parameter_tuning": "gradient_boosting",
            "outcome_prediction": "lstm"
        }
        return mapping.get(model_type, "neural_network")


class ModelPerformanceTracker(KeyedProcessFunction):
    """Tracks ML model performance and triggers retraining"""
    
    def __init__(self, mlops_url: str):
        self.mlops_url = mlops_url
        self.performance_state = None
        self.http_client = None
        
    def open(self, runtime_context):
        """Initialize state"""
        self.performance_state = runtime_context.get_state(
            ValueStateDescriptor(
                "model_performance",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT())
            )
        )
        self.http_client = httpx.Client(timeout=30.0)
        
    def close(self):
        """Clean up resources"""
        if self.http_client:
            self.http_client.close()
            
    def process_element(self, value, ctx):
        """Track model performance metrics"""
        try:
            model_id = ctx.get_current_key()
            event = value
            
            if event["event_type"] == "MODEL_PREDICTION":
                # Update performance metrics
                current_perf = self.performance_state.value() or {}
                
                prediction_error = abs(event["prediction"] - event.get("actual", event["prediction"]))
                
                # Update rolling metrics
                current_perf["error_sum"] = current_perf.get("error_sum", 0) + prediction_error
                current_perf["prediction_count"] = current_perf.get("prediction_count", 0) + 1
                current_perf["avg_error"] = current_perf["error_sum"] / current_perf["prediction_count"]
                current_perf["last_update"] = ctx.timestamp()
                
                self.performance_state.update(current_perf)
                
                # Check if retraining needed
                if current_perf["prediction_count"] > 100 and current_perf["avg_error"] > 0.1:
                    # Trigger retraining
                    yield {
                        "event_type": "MODEL_RETRAINING_REQUEST",
                        "model_id": model_id,
                        "current_performance": current_perf,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    # Reset metrics after triggering retraining
                    self.performance_state.clear()
                    
        except Exception as e:
            logger.error(f"Error tracking model performance: {e}")


def create_simulation_ml_trigger_job():
    """Create and configure the Flink job"""
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Enable checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    
    # Service configuration
    services_config = {
        "federated_learning_service": "http://federated-learning-service:8000",
        "mlops_service": "http://mlops-service:8000",
        "simulation_service": "http://simulation-service:8000"
    }
    
    # Configure Pulsar source for simulation events
    simulation_source = FlinkKafkaConsumer(
        topics=[
            'simulation-state-events',
            'simulation-metrics-events',
            'simulation-completed-events'
        ],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'simulation-ml-trigger-group'
        }
    )
    
    # Create data stream with watermarks
    simulation_stream = env.add_source(simulation_source) \
        .map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(lambda x: int(datetime.fromisoformat(x['timestamp']).timestamp() * 1000))
        )
    
    # Key by simulation ID and analyze metrics
    ml_requests = simulation_stream \
        .key_by(lambda x: x['simulation_id']) \
        .process(SimulationMetricsAnalyzer(
            services_config["federated_learning_service"],
            services_config["mlops_service"]
        ))
    
    # Orchestrate ML sessions
    ml_sessions = ml_requests.process(MLSessionOrchestrator(services_config))
    
    # Configure model performance tracking
    model_source = FlinkKafkaConsumer(
        topics=['model-prediction-events'],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'model-performance-group'
        }
    )
    
    model_stream = env.add_source(model_source) \
        .map(lambda x: json.loads(x)) \
        .key_by(lambda x: x['model_id']) \
        .process(ModelPerformanceTracker(services_config["mlops_service"]))
    
    # Configure Pulsar sink for ML events
    ml_sink = FlinkKafkaProducer(
        topic='ml-training-events',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    # Write all ML-related events
    ml_sessions.map(lambda x: json.dumps(x)).add_sink(ml_sink)
    model_stream.map(lambda x: json.dumps(x)).add_sink(ml_sink)
    
    # Execute job
    env.execute("Simulation ML Trigger Job")


if __name__ == "__main__":
    create_simulation_ml_trigger_job() 