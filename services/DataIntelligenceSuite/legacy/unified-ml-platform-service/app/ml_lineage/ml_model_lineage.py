"""
ML Model Lineage Tracking

Implements comprehensive model lineage tracking using JanusGraph including:
- Model genealogy and version relationships
- Dataset dependencies and transformations
- Code version tracking
- Experiment relationships
- Feature engineering lineage
- Performance evolution tracking
"""

import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict
import numpy as np

from janusgraph_python import JanusGraphClient
from gremlin_python.process.traversal import T, P, Order
from gremlin_python.process.graph_traversal import __

logger = logging.getLogger(__name__)


class ModelRelationType(Enum):
    """Types of relationships between ML artifacts"""
    DERIVED_FROM = "derived_from"
    TRAINED_ON = "trained_on"
    USES_FEATURES = "uses_features"
    PART_OF_EXPERIMENT = "part_of_experiment"
    REPLACED_BY = "replaced_by"
    ENSEMBLE_MEMBER = "ensemble_member"
    FINE_TUNED_FROM = "fine_tuned_from"
    DISTILLED_FROM = "distilled_from"


class ArtifactType(Enum):
    """Types of ML artifacts in lineage"""
    MODEL = "model"
    DATASET = "dataset"
    FEATURE_SET = "feature_set"
    EXPERIMENT = "experiment"
    CODE_VERSION = "code_version"
    PIPELINE = "pipeline"
    DEPLOYMENT = "deployment"


@dataclass
class ModelNode:
    """Represents a model in the lineage graph"""
    model_id: str
    name: str
    version: str
    algorithm: str
    framework: str
    created_at: datetime
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    status: str = "active"  # active, deprecated, retired


@dataclass
class DatasetNode:
    """Represents a dataset in the lineage graph"""
    dataset_id: str
    name: str
    version: str
    size_bytes: int
    row_count: int
    feature_count: int
    created_at: datetime
    schema_hash: str
    source_type: str  # raw, processed, synthetic
    tags: List[str] = field(default_factory=list)


@dataclass
class LineageEdge:
    """Represents a relationship in the lineage graph"""
    from_id: str
    to_id: str
    relationship_type: ModelRelationType
    created_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageImpact:
    """Impact analysis result"""
    affected_models: List[str]
    affected_deployments: List[str]
    impact_score: float
    risk_level: str
    recommendations: List[str]


class MLModelLineageTracker:
    """Tracks ML model lineage and dependencies"""
    
    def __init__(self, janusgraph_client: JanusGraphClient):
        self.graph = janusgraph_client
        self.g = self.graph.traversal()
        self._initialize_schema()
        
    def _initialize_schema(self):
        """Initialize graph schema for ML lineage"""
        try:
            # Create vertex labels
            mgmt = self.graph.management()
            
            # Artifact vertex labels
            for artifact_type in ArtifactType:
                if not mgmt.vertex_label_exists(artifact_type.value):
                    mgmt.make_vertex_label(artifact_type.value)
                    
            # Relationship edge labels
            for rel_type in ModelRelationType:
                if not mgmt.edge_label_exists(rel_type.value):
                    mgmt.make_edge_label(rel_type.value)
                    
            # Property keys
            properties = [
                "artifact_id", "name", "version", "algorithm", "framework",
                "created_at", "updated_at", "status", "metrics", "parameters",
                "tags", "size_bytes", "row_count", "feature_count", "schema_hash",
                "source_type", "impact_score", "risk_score"
            ]
            
            for prop in properties:
                if not mgmt.property_key_exists(prop):
                    mgmt.make_property_key(prop).datatype(str).single()
                    
            # Indexes
            if not mgmt.index_exists("model_by_id"):
                mgmt.build_index("model_by_id", "vertex").add_key("artifact_id").build()
            if not mgmt.index_exists("model_by_status"):
                mgmt.build_index("model_by_status", "vertex").add_key("status").build()
            if not mgmt.index_exists("artifact_by_tag"):
                mgmt.build_index("artifact_by_tag", "vertex").add_key("tags").build()
                
            mgmt.commit()
            logger.info("ML lineage schema initialized")
            
        except Exception as e:
            logger.error(f"Error initializing schema: {e}")
            
    async def add_model(self, model: ModelNode) -> str:
        """Add a model to the lineage graph"""
        try:
            vertex = self.g.addV(ArtifactType.MODEL.value) \
                .property("artifact_id", model.model_id) \
                .property("name", model.name) \
                .property("version", model.version) \
                .property("algorithm", model.algorithm) \
                .property("framework", model.framework) \
                .property("created_at", model.created_at.isoformat()) \
                .property("metrics", json.dumps(model.metrics)) \
                .property("parameters", json.dumps(model.parameters)) \
                .property("tags", json.dumps(model.tags)) \
                .property("status", model.status) \
                .next()
                
            logger.info(f"Added model {model.model_id} to lineage graph")
            return model.model_id
            
        except Exception as e:
            logger.error(f"Error adding model: {e}")
            raise
            
    async def add_dataset(self, dataset: DatasetNode) -> str:
        """Add a dataset to the lineage graph"""
        try:
            vertex = self.g.addV(ArtifactType.DATASET.value) \
                .property("artifact_id", dataset.dataset_id) \
                .property("name", dataset.name) \
                .property("version", dataset.version) \
                .property("size_bytes", str(dataset.size_bytes)) \
                .property("row_count", str(dataset.row_count)) \
                .property("feature_count", str(dataset.feature_count)) \
                .property("created_at", dataset.created_at.isoformat()) \
                .property("schema_hash", dataset.schema_hash) \
                .property("source_type", dataset.source_type) \
                .property("tags", json.dumps(dataset.tags)) \
                .next()
                
            logger.info(f"Added dataset {dataset.dataset_id} to lineage graph")
            return dataset.dataset_id
            
        except Exception as e:
            logger.error(f"Error adding dataset: {e}")
            raise
            
    async def add_lineage_relationship(self, edge: LineageEdge) -> bool:
        """Add a lineage relationship between artifacts"""
        try:
            # Find source and target vertices
            source = self.g.V().has("artifact_id", edge.from_id).next()
            target = self.g.V().has("artifact_id", edge.to_id).next()
            
            # Create edge
            self.g.V(source).addE(edge.relationship_type.value) \
                .to(target) \
                .property("created_at", edge.created_at.isoformat()) \
                .property("metadata", json.dumps(edge.metadata)) \
                .iterate()
                
            logger.info(f"Added lineage edge: {edge.from_id} -> {edge.to_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding lineage relationship: {e}")
            return False
            
    async def get_model_lineage(self, model_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get complete lineage for a model"""
        try:
            # Get ancestors (what this model depends on)
            ancestors = self._trace_ancestors(model_id, depth)
            
            # Get descendants (what depends on this model)
            descendants = self._trace_descendants(model_id, depth)
            
            # Get related experiments
            experiments = self._get_related_experiments(model_id)
            
            # Get feature lineage
            features = self._get_feature_lineage(model_id)
            
            return {
                "model_id": model_id,
                "ancestors": ancestors,
                "descendants": descendants,
                "experiments": experiments,
                "features": features,
                "lineage_depth": depth,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting model lineage: {e}")
            return {}
            
    def _trace_ancestors(self, artifact_id: str, depth: int) -> Dict[str, Any]:
        """Trace ancestors of an artifact"""
        ancestors = {
            "models": [],
            "datasets": [],
            "features": [],
            "code_versions": []
        }
        
        try:
            # Traverse incoming edges up to specified depth
            paths = self.g.V().has("artifact_id", artifact_id) \
                .repeat(__.in_().simplePath()) \
                .times(depth) \
                .path() \
                .toList()
                
            for path in paths:
                for vertex in path:
                    if hasattr(vertex, "label"):
                        artifact_type = vertex.label
                        artifact_data = self._vertex_to_dict(vertex)
                        
                        if artifact_type == ArtifactType.MODEL.value:
                            ancestors["models"].append(artifact_data)
                        elif artifact_type == ArtifactType.DATASET.value:
                            ancestors["datasets"].append(artifact_data)
                        elif artifact_type == ArtifactType.FEATURE_SET.value:
                            ancestors["features"].append(artifact_data)
                        elif artifact_type == ArtifactType.CODE_VERSION.value:
                            ancestors["code_versions"].append(artifact_data)
                            
        except Exception as e:
            logger.error(f"Error tracing ancestors: {e}")
            
        return ancestors
        
    def _trace_descendants(self, artifact_id: str, depth: int) -> Dict[str, Any]:
        """Trace descendants of an artifact"""
        descendants = {
            "models": [],
            "deployments": [],
            "affected_experiments": []
        }
        
        try:
            # Traverse outgoing edges up to specified depth
            paths = self.g.V().has("artifact_id", artifact_id) \
                .repeat(__.out().simplePath()) \
                .times(depth) \
                .path() \
                .toList()
                
            for path in paths:
                for vertex in path:
                    if hasattr(vertex, "label"):
                        artifact_type = vertex.label
                        artifact_data = self._vertex_to_dict(vertex)
                        
                        if artifact_type == ArtifactType.MODEL.value:
                            descendants["models"].append(artifact_data)
                        elif artifact_type == ArtifactType.DEPLOYMENT.value:
                            descendants["deployments"].append(artifact_data)
                        elif artifact_type == ArtifactType.EXPERIMENT.value:
                            descendants["affected_experiments"].append(artifact_data)
                            
        except Exception as e:
            logger.error(f"Error tracing descendants: {e}")
            
        return descendants
        
    def _get_related_experiments(self, model_id: str) -> List[Dict[str, Any]]:
        """Get experiments related to a model"""
        experiments = []
        
        try:
            # Find experiments through PART_OF_EXPERIMENT relationship
            exp_vertices = self.g.V().has("artifact_id", model_id) \
                .both(ModelRelationType.PART_OF_EXPERIMENT.value) \
                .has("label", ArtifactType.EXPERIMENT.value) \
                .toList()
                
            for vertex in exp_vertices:
                experiments.append(self._vertex_to_dict(vertex))
                
        except Exception as e:
            logger.error(f"Error getting related experiments: {e}")
            
        return experiments
        
    def _get_feature_lineage(self, model_id: str) -> Dict[str, Any]:
        """Get feature lineage for a model"""
        feature_lineage = {
            "direct_features": [],
            "derived_features": [],
            "feature_transformations": []
        }
        
        try:
            # Get direct features
            direct = self.g.V().has("artifact_id", model_id) \
                .out(ModelRelationType.USES_FEATURES.value) \
                .has("label", ArtifactType.FEATURE_SET.value) \
                .toList()
                
            for vertex in direct:
                feature_data = self._vertex_to_dict(vertex)
                feature_lineage["direct_features"].append(feature_data)
                
                # Get derived features
                derived = self.g.V(vertex) \
                    .out(ModelRelationType.DERIVED_FROM.value) \
                    .has("label", ArtifactType.FEATURE_SET.value) \
                    .toList()
                    
                for derived_vertex in derived:
                    feature_lineage["derived_features"].append(
                        self._vertex_to_dict(derived_vertex)
                    )
                    
        except Exception as e:
            logger.error(f"Error getting feature lineage: {e}")
            
        return feature_lineage
        
    async def analyze_change_impact(self, artifact_id: str, 
                                   change_type: str = "update") -> LineageImpact:
        """Analyze impact of changes to an artifact"""
        try:
            # Get all downstream dependencies
            affected_models = set()
            affected_deployments = set()
            
            # Find affected models
            model_paths = self.g.V().has("artifact_id", artifact_id) \
                .repeat(__.out().simplePath()) \
                .emit() \
                .has("label", ArtifactType.MODEL.value) \
                .values("artifact_id") \
                .toList()
                
            affected_models.update(model_paths)
            
            # Find affected deployments
            deployment_paths = self.g.V().has("artifact_id", artifact_id) \
                .repeat(__.out().simplePath()) \
                .emit() \
                .has("label", ArtifactType.DEPLOYMENT.value) \
                .values("artifact_id") \
                .toList()
                
            affected_deployments.update(deployment_paths)
            
            # Calculate impact score
            impact_score = self._calculate_impact_score(
                len(affected_models),
                len(affected_deployments),
                change_type
            )
            
            # Determine risk level
            risk_level = self._determine_risk_level(impact_score)
            
            # Generate recommendations
            recommendations = self._generate_impact_recommendations(
                affected_models,
                affected_deployments,
                change_type,
                risk_level
            )
            
            return LineageImpact(
                affected_models=list(affected_models),
                affected_deployments=list(affected_deployments),
                impact_score=impact_score,
                risk_level=risk_level,
                recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Error analyzing change impact: {e}")
            return LineageImpact([], [], 0.0, "unknown", [])
            
    def _calculate_impact_score(self, model_count: int, deployment_count: int,
                               change_type: str) -> float:
        """Calculate impact score based on affected artifacts"""
        base_score = 0.0
        
        # Weight by number of affected models
        model_weight = min(model_count * 0.1, 0.5)
        
        # Weight by number of affected deployments
        deployment_weight = min(deployment_count * 0.2, 0.4)
        
        # Weight by change type
        change_weights = {
            "delete": 1.0,
            "major_update": 0.8,
            "update": 0.5,
            "minor_update": 0.3
        }
        change_weight = change_weights.get(change_type, 0.5)
        
        impact_score = (base_score + model_weight + deployment_weight) * change_weight
        return min(impact_score, 1.0)
        
    def _determine_risk_level(self, impact_score: float) -> str:
        """Determine risk level from impact score"""
        if impact_score >= 0.8:
            return "critical"
        elif impact_score >= 0.6:
            return "high"
        elif impact_score >= 0.4:
            return "medium"
        elif impact_score >= 0.2:
            return "low"
        return "minimal"
        
    def _generate_impact_recommendations(self, affected_models: Set[str],
                                       affected_deployments: Set[str],
                                       change_type: str,
                                       risk_level: str) -> List[str]:
        """Generate recommendations based on impact analysis"""
        recommendations = []
        
        if risk_level in ["critical", "high"]:
            recommendations.append("Schedule change during maintenance window")
            recommendations.append("Prepare rollback plan")
            recommendations.append("Notify all stakeholders")
            
        if len(affected_deployments) > 0:
            recommendations.append(f"Test changes on {len(affected_deployments)} deployments")
            recommendations.append("Consider phased rollout")
            
        if len(affected_models) > 5:
            recommendations.append("Run comprehensive regression tests")
            recommendations.append("Update downstream model documentation")
            
        if change_type == "delete":
            recommendations.append("Archive artifact before deletion")
            recommendations.append("Update all dependent systems")
            
        return recommendations
        
    async def find_similar_models(self, model_id: str, similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Find models similar to a given model based on lineage patterns"""
        similar_models = []
        
        try:
            # Get the reference model's characteristics
            ref_model = self.g.V().has("artifact_id", model_id).next()
            ref_props = self._vertex_to_dict(ref_model)
            
            # Get datasets used by reference model
            ref_datasets = set(self.g.V(ref_model)
                             .out(ModelRelationType.TRAINED_ON.value)
                             .values("artifact_id")
                             .toList())
            
            # Get features used by reference model
            ref_features = set(self.g.V(ref_model)
                             .out(ModelRelationType.USES_FEATURES.value)
                             .values("artifact_id")
                             .toList())
            
            # Find other models
            all_models = self.g.V().has("label", ArtifactType.MODEL.value) \
                .has("artifact_id", P.neq(model_id)) \
                .toList()
            
            for model in all_models:
                model_props = self._vertex_to_dict(model)
                
                # Get datasets and features for comparison
                model_datasets = set(self.g.V(model)
                                   .out(ModelRelationType.TRAINED_ON.value)
                                   .values("artifact_id")
                                   .toList())
                
                model_features = set(self.g.V(model)
                                   .out(ModelRelationType.USES_FEATURES.value)
                                   .values("artifact_id")
                                   .toList())
                
                # Calculate similarity
                dataset_similarity = len(ref_datasets & model_datasets) / max(len(ref_datasets), 1)
                feature_similarity = len(ref_features & model_features) / max(len(ref_features), 1)
                
                # Check algorithm similarity
                algo_similarity = 1.0 if model_props.get("algorithm") == ref_props.get("algorithm") else 0.0
                
                # Combined similarity score
                similarity_score = (dataset_similarity + feature_similarity + algo_similarity) / 3
                
                if similarity_score >= similarity_threshold:
                    similar_models.append({
                        "model": model_props,
                        "similarity_score": similarity_score,
                        "shared_datasets": list(ref_datasets & model_datasets),
                        "shared_features": list(ref_features & model_features)
                    })
            
            # Sort by similarity score
            similar_models.sort(key=lambda x: x["similarity_score"], reverse=True)
            
        except Exception as e:
            logger.error(f"Error finding similar models: {e}")
            
        return similar_models
        
    async def get_model_evolution(self, model_name: str) -> Dict[str, Any]:
        """Track evolution of a model across versions"""
        evolution = {
            "model_name": model_name,
            "versions": [],
            "performance_trend": {},
            "major_changes": []
        }
        
        try:
            # Get all versions of the model
            versions = self.g.V().has("label", ArtifactType.MODEL.value) \
                .has("name", model_name) \
                .order().by("created_at", Order.asc) \
                .toList()
            
            previous_metrics = None
            
            for i, version in enumerate(versions):
                version_data = self._vertex_to_dict(version)
                evolution["versions"].append(version_data)
                
                # Track performance metrics
                metrics = json.loads(version_data.get("metrics", "{}"))
                for metric_name, value in metrics.items():
                    if metric_name not in evolution["performance_trend"]:
                        evolution["performance_trend"][metric_name] = []
                    evolution["performance_trend"][metric_name].append({
                        "version": version_data["version"],
                        "value": value,
                        "timestamp": version_data["created_at"]
                    })
                
                # Detect major changes
                if previous_metrics:
                    changes = self._detect_major_changes(previous_metrics, metrics)
                    if changes:
                        evolution["major_changes"].extend(changes)
                
                previous_metrics = metrics
                
        except Exception as e:
            logger.error(f"Error getting model evolution: {e}")
            
        return evolution
        
    def _detect_major_changes(self, prev_metrics: Dict, curr_metrics: Dict) -> List[Dict]:
        """Detect major changes between model versions"""
        changes = []
        threshold = 0.1  # 10% change threshold
        
        for metric, curr_value in curr_metrics.items():
            if metric in prev_metrics:
                prev_value = prev_metrics[metric]
                if prev_value > 0:
                    change_pct = abs(curr_value - prev_value) / prev_value
                    if change_pct > threshold:
                        changes.append({
                            "metric": metric,
                            "previous": prev_value,
                            "current": curr_value,
                            "change_percent": round(change_pct * 100, 2),
                            "direction": "improved" if curr_value > prev_value else "degraded"
                        })
                        
        return changes
        
    def _vertex_to_dict(self, vertex) -> Dict[str, Any]:
        """Convert vertex to dictionary"""
        result = {
            "id": str(vertex.id),
            "label": vertex.label
        }
        
        # Get all properties
        properties = self.g.V(vertex).properties().toList()
        for prop in properties:
            key = prop.key
            value = prop.value
            
            # Try to parse JSON properties
            if key in ["metrics", "parameters", "tags"]:
                try:
                    value = json.loads(value)
                except:
                    pass
                    
            result[key] = value
            
        return result
        
    async def visualize_lineage(self, artifact_id: str, format: str = "cytoscape") -> Dict[str, Any]:
        """Generate visualization data for lineage graph"""
        try:
            # Get nodes and edges for visualization
            nodes = []
            edges = []
            visited = set()
            
            # BFS to collect nodes and edges
            queue = [(artifact_id, 0)]
            
            while queue:
                current_id, depth = queue.pop(0)
                
                if current_id in visited or depth > 3:
                    continue
                    
                visited.add(current_id)
                
                # Get node
                vertex = self.g.V().has("artifact_id", current_id).next()
                node_data = self._vertex_to_dict(vertex)
                
                nodes.append({
                    "data": {
                        "id": current_id,
                        "label": node_data.get("name", current_id),
                        "type": node_data["label"],
                        "metrics": node_data.get("metrics", {}),
                        "depth": depth
                    }
                })
                
                # Get edges
                out_edges = self.g.V(vertex).outE().toList()
                for edge in out_edges:
                    target_id = self.g.E(edge).inV().values("artifact_id").next()
                    edges.append({
                        "data": {
                            "id": f"{current_id}-{target_id}",
                            "source": current_id,
                            "target": target_id,
                            "label": edge.label
                        }
                    })
                    
                    if target_id not in visited:
                        queue.append((target_id, depth + 1))
                        
                # Get incoming edges
                in_edges = self.g.V(vertex).inE().toList()
                for edge in in_edges:
                    source_id = self.g.E(edge).outV().values("artifact_id").next()
                    edges.append({
                        "data": {
                            "id": f"{source_id}-{current_id}",
                            "source": source_id,
                            "target": current_id,
                            "label": edge.label
                        }
                    })
                    
                    if source_id not in visited:
                        queue.append((source_id, depth + 1))
                        
            if format == "cytoscape":
                return {
                    "elements": {
                        "nodes": nodes,
                        "edges": edges
                    },
                    "style": self._get_cytoscape_style()
                }
            else:
                return {
                    "nodes": nodes,
                    "edges": edges
                }
                
        except Exception as e:
            logger.error(f"Error visualizing lineage: {e}")
            return {"elements": {"nodes": [], "edges": []}}
            
    def _get_cytoscape_style(self) -> List[Dict]:
        """Get Cytoscape visualization style"""
        return [
            {
                "selector": "node",
                "style": {
                    "label": "data(label)",
                    "background-color": "#666",
                    "text-valign": "center",
                    "text-halign": "center"
                }
            },
            {
                "selector": "node[type='model']",
                "style": {
                    "background-color": "#4CAF50",
                    "shape": "round-rectangle"
                }
            },
            {
                "selector": "node[type='dataset']",
                "style": {
                    "background-color": "#2196F3",
                    "shape": "diamond"
                }
            },
            {
                "selector": "node[type='feature_set']",
                "style": {
                    "background-color": "#FF9800",
                    "shape": "hexagon"
                }
            },
            {
                "selector": "edge",
                "style": {
                    "label": "data(label)",
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "font-size": "10px"
                }
            }
        ] 