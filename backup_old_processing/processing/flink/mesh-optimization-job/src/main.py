"""
Real-time 3D Mesh Optimization Flink Job

This job processes 3D mesh data streams for:
- Real-time mesh decimation
- Level-of-Detail (LOD) generation  
- Mesh quality analysis
- Collaborative editing updates
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.window_assigners import ProcessingTimeSessionWindows
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import RuntimeContext
import json
import logging
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import trimesh
import hashlib
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MeshData:
    """Container for mesh data"""
    def __init__(self, vertices: np.ndarray, faces: np.ndarray, 
                 normals: Optional[np.ndarray] = None):
        self.vertices = vertices
        self.faces = faces
        self.normals = normals
        self.hash = self._compute_hash()
        
    def _compute_hash(self) -> str:
        """Compute hash of mesh data for deduplication"""
        data = np.concatenate([self.vertices.flatten(), self.faces.flatten()])
        return hashlib.md5(data.tobytes()).hexdigest()


class MeshOptimizationFunction(KeyedProcessFunction):
    """Process function for mesh optimization with stateful processing"""
    
    def __init__(self, target_reduction: float = 0.5, quality_threshold: float = 0.9):
        self.target_reduction = target_reduction
        self.quality_threshold = quality_threshold
        self.mesh_state = None
        self.quality_metrics_state = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state descriptors"""
        # Store current mesh state per key (asset_id)
        self.mesh_state = runtime_context.get_state(
            ValueStateDescriptor(
                "mesh_state",
                type_info=DataTypes.PICKLED_BYTE_ARRAY()  # Store serialized mesh
            )
        )
        
        # Store quality metrics history
        self.quality_metrics_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "quality_metrics",
                type_info=DataTypes.ROW([
                    DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP()),
                    DataTypes.FIELD("vertex_count", DataTypes.INT()),
                    DataTypes.FIELD("face_count", DataTypes.INT()),
                    DataTypes.FIELD("quality_score", DataTypes.FLOAT())
                ])
            )
        )
        
    def process_element(self, value: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Process mesh optimization request"""
        try:
            event_type = value.get('event_type')
            asset_id = value.get('asset_id')
            
            if event_type == 'MESH_UPDATE':
                self._process_mesh_update(value, ctx)
            elif event_type == 'OPTIMIZE_REQUEST':
                self._optimize_mesh(value, ctx)
            elif event_type == 'LOD_GENERATION_REQUEST':
                self._generate_lods(value, ctx)
            elif event_type == 'QUALITY_CHECK':
                self._check_mesh_quality(value, ctx)
                
        except Exception as e:
            logger.error(f"Error processing mesh: {e}")
            self._emit_error(asset_id, str(e), ctx)
    
    def _process_mesh_update(self, event: Dict[str, Any], ctx):
        """Process mesh update and store in state"""
        mesh_data = event.get('mesh_data', {})
        vertices = np.array(mesh_data.get('vertices', []))
        faces = np.array(mesh_data.get('faces', []))
        
        if len(vertices) == 0 or len(faces) == 0:
            return
            
        # Create mesh object
        mesh = MeshData(vertices, faces)
        
        # Store in state
        self.mesh_state.update(mesh)
        
        # Emit mesh statistics
        stats = {
            'asset_id': event['asset_id'],
            'event_type': 'MESH_STATS_UPDATE',
            'timestamp': datetime.utcnow().isoformat(),
            'stats': {
                'vertex_count': len(vertices),
                'face_count': len(faces),
                'bounding_box': self._calculate_bounding_box(vertices),
                'mesh_hash': mesh.hash
            }
        }
        ctx.output(stats)
        
    def _optimize_mesh(self, event: Dict[str, Any], ctx):
        """Optimize mesh using decimation algorithms"""
        mesh = self.mesh_state.value()
        if not mesh:
            return
            
        # Create trimesh object for optimization
        tri_mesh = trimesh.Trimesh(
            vertices=mesh.vertices,
            faces=mesh.faces
        )
        
        # Calculate target face count
        target_faces = int(len(mesh.faces) * (1 - self.target_reduction))
        
        # Perform decimation
        simplified = tri_mesh.simplify_quadric_decimation(target_faces)
        
        # Calculate quality metrics
        quality = self._calculate_mesh_quality(tri_mesh, simplified)
        
        # Only accept if quality threshold is met
        if quality >= self.quality_threshold:
            optimized_mesh = MeshData(
                vertices=simplified.vertices,
                faces=simplified.faces
            )
            
            # Update state
            self.mesh_state.update(optimized_mesh)
            
            # Emit optimized mesh
            result = {
                'asset_id': event['asset_id'],
                'event_type': 'MESH_OPTIMIZED',
                'timestamp': datetime.utcnow().isoformat(),
                'mesh_data': {
                    'vertices': simplified.vertices.tolist(),
                    'faces': simplified.faces.tolist()
                },
                'optimization_stats': {
                    'original_vertices': len(mesh.vertices),
                    'optimized_vertices': len(simplified.vertices),
                    'original_faces': len(mesh.faces),
                    'optimized_faces': len(simplified.faces),
                    'reduction_ratio': 1 - (len(simplified.faces) / len(mesh.faces)),
                    'quality_score': quality
                }
            }
            ctx.output(result)
            
    def _generate_lods(self, event: Dict[str, Any], ctx):
        """Generate multiple LOD levels"""
        mesh = self.mesh_state.value()
        if not mesh:
            return
            
        lod_levels = event.get('lod_levels', [0.1, 0.25, 0.5, 0.75])
        
        tri_mesh = trimesh.Trimesh(
            vertices=mesh.vertices,
            faces=mesh.faces
        )
        
        lods = []
        for level in lod_levels:
            target_faces = int(len(mesh.faces) * level)
            simplified = tri_mesh.simplify_quadric_decimation(target_faces)
            
            lods.append({
                'level': level,
                'vertex_count': len(simplified.vertices),
                'face_count': len(simplified.faces),
                'vertices': simplified.vertices.tolist(),
                'faces': simplified.faces.tolist()
            })
            
        # Emit LODs
        result = {
            'asset_id': event['asset_id'],
            'event_type': 'LODS_GENERATED',
            'timestamp': datetime.utcnow().isoformat(),
            'lods': lods
        }
        ctx.output(result)
        
    def _check_mesh_quality(self, event: Dict[str, Any], ctx):
        """Analyze mesh quality metrics"""
        mesh = self.mesh_state.value()
        if not mesh:
            return
            
        tri_mesh = trimesh.Trimesh(
            vertices=mesh.vertices,
            faces=mesh.faces
        )
        
        # Calculate various quality metrics
        metrics = {
            'is_watertight': tri_mesh.is_watertight,
            'is_winding_consistent': tri_mesh.is_winding_consistent,
            'is_valid': tri_mesh.is_valid,
            'euler_number': tri_mesh.euler_number,
            'body_count': tri_mesh.body_count,
            'face_angles': {
                'min': float(np.min(tri_mesh.face_angles)),
                'max': float(np.max(tri_mesh.face_angles)),
                'mean': float(np.mean(tri_mesh.face_angles))
            },
            'vertex_defects': len(tri_mesh.vertex_defects),
            'duplicate_faces': len(tri_mesh.facets_boundary)
        }
        
        # Store in metrics history
        self.quality_metrics_state.add({
            'timestamp': datetime.utcnow(),
            'vertex_count': len(mesh.vertices),
            'face_count': len(mesh.faces),
            'quality_score': self._calculate_overall_quality(metrics)
        })
        
        # Emit quality report
        result = {
            'asset_id': event['asset_id'],
            'event_type': 'MESH_QUALITY_REPORT',
            'timestamp': datetime.utcnow().isoformat(),
            'quality_metrics': metrics
        }
        ctx.output(result)
        
    def _calculate_mesh_quality(self, original: trimesh.Trimesh, 
                               simplified: trimesh.Trimesh) -> float:
        """Calculate quality score between original and simplified mesh"""
        # Hausdorff distance as quality metric
        distance = trimesh.comparison.hausdorff_distance(original, simplified)
        
        # Normalize to 0-1 range (inverse of distance)
        # Assuming max acceptable distance is 1% of bounding box diagonal
        bbox_diag = np.linalg.norm(original.bounding_box.extents)
        normalized_distance = distance / (0.01 * bbox_diag)
        
        quality = max(0, 1 - normalized_distance)
        return quality
        
    def _calculate_overall_quality(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall quality score from metrics"""
        score = 1.0
        
        # Penalize for not being watertight
        if not metrics['is_watertight']:
            score *= 0.8
            
        # Penalize for inconsistent winding
        if not metrics['is_winding_consistent']:
            score *= 0.9
            
        # Penalize for vertex defects
        if metrics['vertex_defects'] > 0:
            score *= (1 - min(0.5, metrics['vertex_defects'] / 1000))
            
        return score
        
    def _calculate_bounding_box(self, vertices: np.ndarray) -> Dict[str, List[float]]:
        """Calculate bounding box of vertices"""
        min_point = np.min(vertices, axis=0)
        max_point = np.max(vertices, axis=0)
        
        return {
            'min': min_point.tolist(),
            'max': max_point.tolist(),
            'center': ((min_point + max_point) / 2).tolist(),
            'size': (max_point - min_point).tolist()
        }
        
    def _emit_error(self, asset_id: str, error: str, ctx):
        """Emit error event"""
        error_event = {
            'asset_id': asset_id,
            'event_type': 'MESH_PROCESSING_ERROR',
            'timestamp': datetime.utcnow().isoformat(),
            'error': error
        }
        ctx.output(error_event)


class CollaborativeMeshProcessor(CoProcessFunction):
    """Process collaborative mesh editing operations"""
    
    def process_element1(self, value: Dict[str, Any], ctx: CoProcessFunction.Context):
        """Process mesh updates from stream 1"""
        # Handle mesh updates from CAD collaboration service
        pass
        
    def process_element2(self, value: Dict[str, Any], ctx: CoProcessFunction.Context):
        """Process collaborative operations from stream 2"""
        # Handle collaborative operations (transforms, selections, etc.)
        pass


class QuantumOptimizationTrigger(ProcessFunction):
    """Triggers quantum optimization for significant mesh changes"""
    
    def __init__(self, quantum_service_url: str, cad_service_url: str):
        self.quantum_url = quantum_service_url
        self.cad_url = cad_service_url
        self.http_client = None
        
    def open(self, runtime_context):
        """Initialize HTTP client"""
        self.http_client = httpx.Client(timeout=30.0)
        
    def close(self):
        """Clean up resources"""
        if self.http_client:
            self.http_client.close()
            
    def process_element(self, value, ctx):
        """Process mesh updates and trigger quantum optimization"""
        try:
            mesh_event = value
            
            # Check if quantum optimization is warranted
            if self._should_trigger_quantum(mesh_event):
                # Prepare quantum optimization request
                quantum_request = {
                    "mesh_id": mesh_event["mesh_id"],
                    "optimization_type": self._determine_optimization_type(mesh_event),
                    "quality_threshold": 0.95,
                    "quantum_backend": "simulator",
                    "constraints": mesh_event.get("constraints", {})
                }
                
                # Notify CAD service to queue quantum optimization
                response = self.http_client.post(
                    f"{self.cad_url}/api/v1/sessions/{mesh_event['session_id']}/quantum-optimize",
                    json=quantum_request
                )
                
                if response.status_code == 200:
                    # Output quantum optimization event
                    yield {
                        "event_type": "QUANTUM_OPTIMIZATION_TRIGGERED",
                        "session_id": mesh_event["session_id"],
                        "mesh_id": mesh_event["mesh_id"],
                        "optimization_type": quantum_request["optimization_type"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error triggering quantum optimization: {e}")
            
    def _should_trigger_quantum(self, event: Dict[str, Any]) -> bool:
        """Determine if quantum optimization should be triggered"""
        # Trigger for complex meshes
        if event.get("vertex_count", 0) > 10000:
            return True
            
        # Trigger for quality degradation
        if event.get("quality_score", 1.0) < 0.8:
            return True
            
        # Trigger for specific operations
        if event.get("operation_type") in ["TOPOLOGY_CHANGE", "MATERIAL_ASSIGNMENT"]:
            return True
            
        return False
        
    def _determine_optimization_type(self, event: Dict[str, Any]) -> str:
        """Determine optimization type based on event"""
        op_type = event.get("operation_type")
        
        if op_type == "DECIMATION":
            return "lod_generation"
        elif op_type == "TOPOLOGY_CHANGE":
            return "topology"
        elif op_type == "MATERIAL_ASSIGNMENT":
            return "material"
        else:
            # Default based on mesh characteristics
            if event.get("vertex_count", 0) > event.get("target_vertices", float('inf')):
                return "lod_generation"
            return "topology"


def create_mesh_optimization_job():
    """Create and configure the mesh optimization Flink job"""
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Configure checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    
    # Create table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Service URLs
    quantum_service_url = "http://quantum-optimization-service:8000"
    cad_service_url = "http://cad-collaboration-service:8000"
    
    # Configure Pulsar source
    pulsar_source = FlinkKafkaConsumer(
        topics=['mesh-update-events', 'mesh-optimization-requests'],
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'pulsar://pulsar:6650',
            'group.id': 'mesh-optimization-group'
        }
    )
    
    # Create data stream
    mesh_stream = env.add_source(pulsar_source) \
        .map(lambda x: json.loads(x)) \
        .key_by(lambda x: x['asset_id'])
    
    # Apply mesh optimization
    optimized_stream = mesh_stream.process(MeshOptimizationFunction())
    
    # Add quantum optimization trigger
    quantum_stream = optimized_stream.process(
        QuantumOptimizationTrigger(quantum_service_url, cad_service_url)
    )
    
    # Configure sink to Pulsar
    pulsar_sink = FlinkKafkaProducer(
        topic='mesh-optimization-results',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    # Write results
    optimized_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    quantum_stream.map(lambda x: json.dumps(x)).add_sink(pulsar_sink)
    
    # Execute job
    env.execute("Real-time Mesh Optimization Job with Quantum Integration")


if __name__ == "__main__":
    create_mesh_optimization_job() 