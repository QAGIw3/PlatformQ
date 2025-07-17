"""
Flink Job: Collaborative ML Data Processing

Processes project assets for collaborative ML training,
handles federated learning data distribution, and aggregates results.
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AssetFeatureExtractor(ProcessFunction):
    """Extracts ML features from project assets"""
    
    def __init__(self, connector_service_url: str):
        self.connector_url = connector_service_url
        self.http_client = None
        
    def open(self, runtime_context):
        """Initialize HTTP client"""
        import httpx
        self.http_client = httpx.Client(timeout=30.0)
        
    def close(self):
        """Clean up resources"""
        if self.http_client:
            self.http_client.close()
            
    def process_element(self, value, ctx):
        """Process asset and extract features"""
        try:
            asset_event = value
            
            if asset_event["event_type"] == "ASSET_ADDED_TO_PROJECT":
                asset = asset_event["asset"]
                project_id = asset_event["project_id"]
                
                # Extract features based on asset type
                features = self._extract_features(asset)
                
                if features:
                    # Output feature event
                    yield {
                        "event_type": "ASSET_FEATURES_EXTRACTED",
                        "project_id": project_id,
                        "asset_id": asset["cid"],
                        "asset_type": asset["asset_type"],
                        "features": features,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error extracting asset features: {e}")
            
    def _extract_features(self, asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract features from asset"""
        asset_type = asset.get("asset_type")
        
        try:
            if asset_type == "dataset":
                return self._extract_dataset_features(asset)
            elif asset_type == "3d_model":
                return self._extract_3d_features(asset)
            elif asset_type == "document":
                return self._extract_document_features(asset)
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error extracting features for {asset_type}: {e}")
            return None
            
    def _extract_dataset_features(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features from dataset"""
        metadata = asset.get("metadata", {})
        
        return {
            "feature_type": "tabular",
            "num_rows": metadata.get("row_count", 0),
            "num_columns": metadata.get("column_count", 0),
            "columns": metadata.get("columns", []),
            "data_types": metadata.get("data_types", {}),
            "statistics": metadata.get("statistics", {})
        }
        
    def _extract_3d_features(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features from 3D model"""
        metadata = asset.get("metadata", {})
        
        # Call connector service for advanced feature extraction
        if self.http_client:
            try:
                response = self.http_client.post(
                    f"{self.connector_url}/api/v1/extract-features",
                    json={
                        "asset_id": asset["cid"],
                        "extraction_type": "geometry_features"
                    }
                )
                
                if response.status_code == 200:
                    return response.json()["features"]
            except:
                pass
                
        # Fallback to basic metadata
        return {
            "feature_type": "geometry",
            "vertices": metadata.get("vertices", 0),
            "faces": metadata.get("faces", 0),
            "materials": metadata.get("materials", []),
            "bounding_box": metadata.get("bounding_box", {})
        }
        
    def _extract_document_features(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features from document"""
        metadata = asset.get("metadata", {})
        
        return {
            "feature_type": "text",
            "content_type": metadata.get("content_type", "unknown"),
            "word_count": metadata.get("word_count", 0),
            "language": metadata.get("language", "en"),
            "topics": metadata.get("topics", [])
        }


class FederatedDataDistributor(KeyedProcessFunction):
    """Distributes data to federated learning participants"""
    
    def __init__(self):
        self.participant_data = None
        self.training_config = None
        
    def open(self, runtime_context):
        """Initialize state"""
        self.participant_data = runtime_context.get_map_state(
            MapStateDescriptor(
                "participant_data",
                type_info=DataTypes.STRING(),
                value_type_info=DataTypes.PICKLED_BYTE_ARRAY()
            )
        )
        
        self.training_config = runtime_context.get_state(
            ValueStateDescriptor(
                "training_config",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
            )
        )
        
    def process_element(self, value, ctx):
        """Distribute data to participants"""
        try:
            training_id = ctx.get_current_key()
            event = value
            
            if event["event_type"] == "TRAINING_INITIATED":
                # Store training configuration
                config = event["config"]
                self.training_config.update(config)
                
            elif event["event_type"] == "ASSET_FEATURES_EXTRACTED":
                # Get training config
                config = self.training_config.value()
                if not config:
                    return
                    
                # Determine data distribution strategy
                participants = config.get("participants", [])
                privacy_mode = config.get("privacy_mode", "federated")
                
                # Distribute features to participants
                distributed_data = self._distribute_features(
                    event["features"],
                    participants,
                    privacy_mode
                )
                
                for participant_id, participant_data in distributed_data.items():
                    # Store participant data
                    current_data = self.participant_data.get(participant_id) or []
                    current_data.append(participant_data)
                    self.participant_data.put(participant_id, current_data)
                    
                    # Output participant data event
                    yield {
                        "event_type": "PARTICIPANT_DATA_READY",
                        "training_id": training_id,
                        "participant_id": participant_id,
                        "data_batch": participant_data,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error distributing federated data: {e}")
            
    def _distribute_features(self,
                           features: Dict[str, Any],
                           participants: List[str],
                           privacy_mode: str) -> Dict[str, Dict[str, Any]]:
        """Distribute features to participants based on privacy mode"""
        distributed = {}
        
        if privacy_mode == "federated":
            # Each participant gets full features
            for participant in participants:
                distributed[participant] = features
                
        elif privacy_mode == "differential_privacy":
            # Add noise to features
            noisy_features = self._add_differential_privacy(features)
            for participant in participants:
                distributed[participant] = noisy_features
                
        elif privacy_mode == "secure_multiparty":
            # Split features into shares
            shares = self._create_secret_shares(features, len(participants))
            for i, participant in enumerate(participants):
                distributed[participant] = shares[i]
                
        return distributed
        
    def _add_differential_privacy(self,
                                features: Dict[str, Any],
                                epsilon: float = 1.0) -> Dict[str, Any]:
        """Add differential privacy noise to features"""
        noisy_features = features.copy()
        
        # Add Laplacian noise to numeric features
        for key, value in features.items():
            if isinstance(value, (int, float)):
                sensitivity = 1.0  # Assume normalized
                scale = sensitivity / epsilon
                noise = np.random.laplace(0, scale)
                noisy_features[key] = value + noise
                
        return noisy_features
        
    def _create_secret_shares(self,
                            features: Dict[str, Any],
                            num_shares: int) -> List[Dict[str, Any]]:
        """Create secret shares for secure multiparty computation"""
        shares = [{} for _ in range(num_shares)]
        
        # Simple additive secret sharing
        for key, value in features.items():
            if isinstance(value, (int, float)):
                # Generate random shares that sum to value
                random_shares = np.random.random(num_shares - 1)
                random_shares = random_shares * value / np.sum(random_shares)
                last_share = value - np.sum(random_shares)
                
                all_shares = list(random_shares) + [last_share]
                
                for i in range(num_shares):
                    shares[i][key] = float(all_shares[i])
            else:
                # Non-numeric values go to first share only
                shares[0][key] = value
                
        return shares


class ModelAggregator(CoProcessFunction):
    """Aggregates model updates from federated participants"""
    
    def __init__(self):
        self.model_updates = None
        self.aggregation_config = None
        
    def open(self, runtime_context):
        """Initialize state"""
        self.model_updates = runtime_context.get_list_state(
            ListStateDescriptor(
                "model_updates",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT())
            )
        )
        
        self.aggregation_config = runtime_context.get_state(
            ValueStateDescriptor(
                "aggregation_config",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
            )
        )
        
    def process_element1(self, value, ctx):
        """Process model updates from participants"""
        try:
            update_event = value
            
            if update_event["event_type"] == "MODEL_UPDATE":
                # Store model update
                self.model_updates.add(update_event["model_weights"])
                
                # Check if we have enough updates
                updates = list(self.model_updates.get())
                config = self.aggregation_config.value() or {}
                min_participants = int(config.get("min_participants", 2))
                
                if len(updates) >= min_participants:
                    # Aggregate models
                    aggregated_model = self._aggregate_models(updates, config)
                    
                    # Clear updates
                    self.model_updates.clear()
                    
                    # Output aggregated model
                    yield {
                        "event_type": "MODEL_AGGREGATED",
                        "training_id": update_event["training_id"],
                        "round": update_event.get("round", 0),
                        "aggregated_weights": aggregated_model,
                        "num_participants": len(updates),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error processing model update: {e}")
            
    def process_element2(self, value, ctx):
        """Process configuration updates"""
        try:
            config_event = value
            
            if config_event["event_type"] == "AGGREGATION_CONFIG":
                self.aggregation_config.update(config_event["config"])
                
        except Exception as e:
            logger.error(f"Error processing config update: {e}")
            
    def _aggregate_models(self,
                        updates: List[Dict[str, float]],
                        config: Dict[str, Any]) -> Dict[str, float]:
        """Aggregate model updates using federated averaging"""
        aggregation_strategy = config.get("aggregation_strategy", "WEIGHTED_AVERAGE")
        
        if not updates:
            return {}
            
        if aggregation_strategy == "WEIGHTED_AVERAGE":
            # Simple averaging
            aggregated = {}
            
            # Get all parameter names
            all_params = set()
            for update in updates:
                all_params.update(update.keys())
                
            # Average each parameter
            for param in all_params:
                values = [update.get(param, 0.0) for update in updates]
                aggregated[param] = sum(values) / len(values)
                
            return aggregated
            
        elif aggregation_strategy == "MEDIAN":
            # Median aggregation (more robust to outliers)
            aggregated = {}
            
            all_params = set()
            for update in updates:
                all_params.update(update.keys())
                
            for param in all_params:
                values = [update.get(param, 0.0) for update in updates]
                aggregated[param] = float(np.median(values))
                
            return aggregated
            
        else:
            # Default to simple average
            return self._aggregate_models(updates, {"aggregation_strategy": "WEIGHTED_AVERAGE"})


def create_collaborative_ml_job():
    """Create and configure the collaborative ML Flink job"""
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Enable checkpointing
    env.enable_checkpointing(60000)  # 1 minute
    
    # Service URLs
    connector_service_url = "http://connector-service:8000"
    
    # Configure Pulsar sources
    asset_source = FlinkKafkaConsumer(
        topics=['project-asset-events'],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'collaborative-ml-asset-group'
        }
    )
    
    training_source = FlinkKafkaConsumer(
        topics=['ml-training-events'],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'collaborative-ml-training-group'
        }
    )
    
    model_source = FlinkKafkaConsumer(
        topics=['model-update-events'],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'collaborative-ml-model-group'
        }
    )
    
    # Create data streams
    asset_stream = env.add_source(asset_source) \
        .map(lambda x: json.loads(x))
        
    training_stream = env.add_source(training_source) \
        .map(lambda x: json.loads(x))
        
    model_stream = env.add_source(model_source) \
        .map(lambda x: json.loads(x))
    
    # Extract features from assets
    feature_stream = asset_stream.process(
        AssetFeatureExtractor(connector_service_url)
    )
    
    # Key streams by training ID
    keyed_training_stream = training_stream.key_by(lambda x: x.get('training_id', ''))
    keyed_feature_stream = feature_stream.key_by(lambda x: x.get('training_id', ''))
    
    # Distribute data to participants
    distributed_stream = keyed_feature_stream.union(keyed_training_stream) \
        .process(FederatedDataDistributor())
    
    # Aggregate model updates
    keyed_model_stream = model_stream.key_by(lambda x: x.get('training_id', ''))
    config_stream = training_stream.filter(lambda x: x['event_type'] == 'AGGREGATION_CONFIG') \
        .key_by(lambda x: x.get('training_id', ''))
        
    aggregated_stream = keyed_model_stream.connect(config_stream) \
        .process(ModelAggregator())
    
    # Configure Pulsar sinks
    ml_sink = FlinkKafkaProducer(
        topic='collaborative-ml-results',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    # Write results
    distributed_stream.map(lambda x: json.dumps(x)).add_sink(ml_sink)
    aggregated_stream.map(lambda x: json.dumps(x)).add_sink(ml_sink)
    
    # Execute job
    env.execute("Collaborative ML Data Processing Job")


if __name__ == "__main__":
    create_collaborative_ml_job() 