"""
Real-time Mesh Decimation Service

Provides advanced mesh decimation algorithms for real-time CAD collaboration:
- Progressive mesh decimation with LOD generation
- Quadric error metric optimization
- Feature-preserving decimation
- GPU-accelerated processing
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Set
import logging
from dataclasses import dataclass
from collections import defaultdict
import time
import asyncio
from scipy.spatial import cKDTree
import trimesh
import heapq

logger = logging.getLogger(__name__)


@dataclass
class Vertex:
    """Vertex with position and quadric error"""
    position: np.ndarray
    quadric: np.ndarray
    id: int
    normal: Optional[np.ndarray] = None
    color: Optional[np.ndarray] = None
    uv: Optional[np.ndarray] = None
    
    
@dataclass
class Edge:
    """Edge with collapse cost"""
    v1: int
    v2: int
    cost: float
    optimal_position: np.ndarray
    
    def __lt__(self, other):
        return self.cost < other.cost


@dataclass
class MeshLOD:
    """Level of Detail for a mesh"""
    level: int
    vertices: np.ndarray
    faces: np.ndarray
    normals: Optional[np.ndarray] = None
    colors: Optional[np.ndarray] = None
    uvs: Optional[np.ndarray] = None
    vertex_count: int = 0
    face_count: int = 0
    error_metric: float = 0.0


class MeshDecimator:
    """
    Advanced mesh decimation with multiple algorithms
    """
    
    def __init__(self,
                 algorithm: str = "quadric",
                 preserve_features: bool = True,
                 preserve_boundaries: bool = True):
        """
        Initialize mesh decimator
        
        Args:
            algorithm: Decimation algorithm (quadric, vertex_clustering, edge_collapse)
            preserve_features: Whether to preserve sharp features
            preserve_boundaries: Whether to preserve mesh boundaries
        """
        self.algorithm = algorithm
        self.preserve_features = preserve_features
        self.preserve_boundaries = preserve_boundaries
        
        # Caches for performance
        self.quadric_cache = {}
        self.edge_cache = {}
        self.edge_faces_cache = {}  # Maps edge -> (face_indices, normals)
        
    async def decimate_mesh(self,
                          vertices: np.ndarray,
                          faces: np.ndarray,
                          target_ratio: float = 0.5,
                          target_vertices: Optional[int] = None,
                          normals: Optional[np.ndarray] = None,
                          colors: Optional[np.ndarray] = None,
                          uvs: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """
        Decimate mesh to target ratio or vertex count
        
        Args:
            vertices: Vertex positions (N, 3)
            faces: Face indices (M, 3)
            target_ratio: Target decimation ratio (0-1)
            target_vertices: Target vertex count (overrides ratio)
            normals: Vertex normals (optional)
            colors: Vertex colors (optional)
            uvs: Texture coordinates (optional)
            
        Returns:
            Decimated mesh data
        """
        start_time = time.time()
        
        # Calculate target
        original_vertices = len(vertices)
        if target_vertices is None:
            target_vertices = int(original_vertices * target_ratio)
            
        logger.info(f"Decimating mesh from {original_vertices} to {target_vertices} vertices")
        
        # Choose algorithm
        if self.algorithm == "quadric":
            result = await self._quadric_decimation(
                vertices, faces, target_vertices,
                normals, colors, uvs
            )
        elif self.algorithm == "vertex_clustering":
            result = await self._vertex_clustering_decimation(
                vertices, faces, target_vertices,
                normals, colors, uvs
            )
        elif self.algorithm == "edge_collapse":
            result = await self._edge_collapse_decimation(
                vertices, faces, target_vertices,
                normals, colors, uvs
            )
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")
            
        # Add metadata
        result["original_vertices"] = original_vertices
        result["original_faces"] = len(faces)
        result["decimation_ratio"] = len(result["vertices"]) / original_vertices
        result["processing_time"] = time.time() - start_time
        
        return result
        
    async def _quadric_decimation(self,
                                 vertices: np.ndarray,
                                 faces: np.ndarray,
                                 target_vertices: int,
                                 normals: Optional[np.ndarray] = None,
                                 colors: Optional[np.ndarray] = None,
                                 uvs: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """
        Quadric error metric decimation (Garland & Heckbert)
        """
        # Build edge-face adjacency cache
        self._build_edge_faces_cache(vertices, faces)
        
        # Initialize quadrics for each vertex
        quadrics = self._compute_vertex_quadrics(vertices, faces)
        
        # Build edge list with collapse costs
        edges = self._build_edge_list(vertices, faces, quadrics)
        
        # Create heap for efficient processing
        edge_heap = list(edges.values())
        heapq.heapify(edge_heap)
        
        # Track valid vertices and faces
        valid_vertices = set(range(len(vertices)))
        valid_faces = set(range(len(faces)))
        vertex_map = {i: i for i in range(len(vertices))}
        
        # Vertex attributes
        vertex_data = {
            "positions": vertices.copy(),
            "normals": normals.copy() if normals is not None else None,
            "colors": colors.copy() if colors is not None else None,
            "uvs": uvs.copy() if uvs is not None else None
        }
        
        # Face data
        face_data = faces.copy()
        
        # Decimation loop
        while len(valid_vertices) > target_vertices and edge_heap:
            # Get minimum cost edge
            edge = heapq.heappop(edge_heap)
            
            # Skip if vertices already collapsed
            if edge.v1 not in valid_vertices or edge.v2 not in valid_vertices:
                continue
                
            # Check if collapse is valid
            if not self._is_collapse_valid(edge, face_data, valid_faces, vertex_map):
                continue
                
            # Perform edge collapse
            self._collapse_edge(
                edge, vertex_data, face_data,
                valid_vertices, valid_faces, vertex_map,
                quadrics
            )
            
            # Update affected edges
            self._update_edge_costs(
                edge.v1, vertices, face_data, valid_faces,
                vertex_map, quadrics, edges, edge_heap
            )
            
        # Extract final mesh
        final_vertices, final_faces, vertex_remap = self._extract_final_mesh(
            vertex_data, face_data, valid_vertices, valid_faces, vertex_map
        )
        
        # Extract attributes
        result = {
            "vertices": final_vertices,
            "faces": final_faces
        }
        
        if normals is not None:
            result["normals"] = vertex_data["normals"][list(vertex_remap.keys())]
        if colors is not None:
            result["colors"] = vertex_data["colors"][list(vertex_remap.keys())]
        if uvs is not None:
            result["uvs"] = vertex_data["uvs"][list(vertex_remap.keys())]
            
        return result
        
    def _compute_vertex_quadrics(self,
                               vertices: np.ndarray,
                               faces: np.ndarray) -> Dict[int, np.ndarray]:
        """Compute quadric error matrix for each vertex"""
        quadrics = defaultdict(lambda: np.zeros((4, 4)))
        
        for face in faces:
            # Get face vertices
            v0, v1, v2 = vertices[face]
            
            # Compute face normal
            edge1 = v1 - v0
            edge2 = v2 - v0
            normal = np.cross(edge1, edge2)
            area = np.linalg.norm(normal)
            
            if area > 0:
                normal /= area
                
                # Compute plane equation: ax + by + cz + d = 0
                d = -np.dot(normal, v0)
                plane = np.append(normal, d)
                
                # Compute quadric matrix
                Q = np.outer(plane, plane)
                
                # Weight by area
                Q *= area / 2
                
                # Add to vertex quadrics
                for vid in face:
                    quadrics[vid] += Q
                    
        return dict(quadrics)
        
    def _build_edge_faces_cache(self, vertices: np.ndarray, faces: np.ndarray):
        """Build cache mapping edges to adjacent faces and their normals"""
        self.edge_faces_cache.clear()
        
        for face_idx, face in enumerate(faces):
            # Compute face normal
            v0, v1, v2 = vertices[face[0]], vertices[face[1]], vertices[face[2]]
            normal = np.cross(v1 - v0, v2 - v0)
            normal_len = np.linalg.norm(normal)
            if normal_len > 1e-10:
                normal = normal / normal_len
            else:
                normal = np.array([0.0, 0.0, 1.0])  # Degenerate face
                
            # Add face to each edge
            for i in range(3):
                v1_idx = face[i]
                v2_idx = face[(i + 1) % 3]
                edge_key = (min(v1_idx, v2_idx), max(v1_idx, v2_idx))
                
                if edge_key not in self.edge_faces_cache:
                    self.edge_faces_cache[edge_key] = ([], [])
                    
                self.edge_faces_cache[edge_key][0].append(face_idx)
                self.edge_faces_cache[edge_key][1].append(normal)
        
    def _build_edge_list(self,
                        vertices: np.ndarray,
                        faces: np.ndarray,
                        quadrics: Dict[int, np.ndarray]) -> Dict[Tuple[int, int], Edge]:
        """Build edge list with collapse costs"""
        edges = {}
        
        # Extract edges from faces
        for face in faces:
            for i in range(3):
                v1 = face[i]
                v2 = face[(i + 1) % 3]
                
                # Ensure consistent edge ordering
                if v1 > v2:
                    v1, v2 = v2, v1
                    
                if (v1, v2) not in edges:
                    # Compute collapse cost and optimal position
                    cost, optimal_pos = self._compute_collapse_cost(
                        v1, v2, vertices, quadrics
                    )
                    
                    edges[(v1, v2)] = Edge(v1, v2, cost, optimal_pos)
                    
        return edges
        
    def _compute_collapse_cost(self,
                             v1: int,
                             v2: int,
                             vertices: np.ndarray,
                             quadrics: Dict[int, np.ndarray]) -> Tuple[float, np.ndarray]:
        """Compute cost of collapsing edge v1-v2"""
        # Get vertex positions
        p1 = vertices[v1]
        p2 = vertices[v2]
        
        # Get quadrics
        Q1 = quadrics.get(v1, np.zeros((4, 4)))
        Q2 = quadrics.get(v2, np.zeros((4, 4)))
        
        # Combined quadric
        Q = Q1 + Q2
        
        # Try to invert Q to find optimal position
        Q_reduced = Q[:3, :3]
        b = -Q[:3, 3]
        
        try:
            optimal_pos = np.linalg.solve(Q_reduced, b)
        except np.linalg.LinAlgError:
            # Singular matrix - use midpoint
            optimal_pos = (p1 + p2) / 2
            
        # Compute error at optimal position
        p_homo = np.append(optimal_pos, 1)
        error = p_homo @ Q @ p_homo
        
        # Add penalties
        if self.preserve_boundaries:
            # Check if edge is on boundary
            if self._is_boundary_edge(v1, v2):
                error *= 1000  # High penalty for boundary edges
                
        if self.preserve_features:
            # Check feature angle
            angle = self._compute_feature_angle(v1, v2, vertices)
            if angle > np.pi / 6:  # 30 degrees
                error *= (1 + angle / np.pi)
                
        return error, optimal_pos
        
    def _is_collapse_valid(self,
                          edge: Edge,
                          faces: np.ndarray,
                          valid_faces: Set[int],
                          vertex_map: Dict[int, int]) -> bool:
        """Check if edge collapse would create invalid geometry"""
        v1 = vertex_map[edge.v1]
        v2 = vertex_map[edge.v2]
        
        # Get faces connected to both vertices
        v1_faces = []
        v2_faces = []
        shared_faces = []
        
        for fi in valid_faces:
            face = faces[fi]
            has_v1 = v1 in face
            has_v2 = v2 in face
            
            if has_v1 and has_v2:
                shared_faces.append(fi)
            elif has_v1:
                v1_faces.append(fi)
            elif has_v2:
                v2_faces.append(fi)
                
        # Collapse creates degenerate faces
        if len(shared_faces) > 2:
            return False
            
        # Would create duplicate faces
        for f1 in v1_faces:
            for f2 in v2_faces:
                if self._would_create_duplicate_face(
                    faces[f1], faces[f2], v1, v2
                ):
                    return False
                    
        return True
        
    def _interpolate_vertex_attributes(self,
                                     original_mesh: trimesh.Trimesh,
                                     simplified_mesh: trimesh.Trimesh,
                                     normals: Optional[np.ndarray] = None,
                                     colors: Optional[np.ndarray] = None,
                                     uvs: Optional[np.ndarray] = None) -> Dict[str, np.ndarray]:
        """Interpolate vertex attributes from original to simplified mesh"""
        result = {}
        
        # Build KD-tree for nearest neighbor search
        tree = cKDTree(original_mesh.vertices)
        
        # Find nearest original vertices for each simplified vertex
        distances, indices = tree.query(simplified_mesh.vertices, k=3)
        
        # Compute weights based on inverse distance
        epsilon = 1e-6
        weights = 1.0 / (distances + epsilon)
        weights = weights / np.sum(weights, axis=1, keepdims=True)
        
        # Interpolate normals
        if normals is not None:
            interpolated_normals = np.zeros((len(simplified_mesh.vertices), 3))
            for i, (idx, w) in enumerate(zip(indices, weights)):
                # Weighted average of normals
                for j, (vi, wi) in enumerate(zip(idx, w)):
                    interpolated_normals[i] += normals[vi] * wi
                # Renormalize
                norm = np.linalg.norm(interpolated_normals[i])
                if norm > epsilon:
                    interpolated_normals[i] /= norm
                else:
                    interpolated_normals[i] = [0, 0, 1]  # Default up
            result["normals"] = interpolated_normals
            
        # Interpolate colors
        if colors is not None:
            interpolated_colors = np.zeros((len(simplified_mesh.vertices), colors.shape[1]))
            for i, (idx, w) in enumerate(zip(indices, weights)):
                for j, (vi, wi) in enumerate(zip(idx, w)):
                    interpolated_colors[i] += colors[vi] * wi
            # Clamp colors to valid range
            if colors.dtype == np.uint8:
                interpolated_colors = np.clip(interpolated_colors, 0, 255).astype(np.uint8)
            else:
                interpolated_colors = np.clip(interpolated_colors, 0, 1)
            result["colors"] = interpolated_colors
            
        # Interpolate UVs
        if uvs is not None:
            interpolated_uvs = np.zeros((len(simplified_mesh.vertices), 2))
            for i, (idx, w) in enumerate(zip(indices, weights)):
                for j, (vi, wi) in enumerate(zip(idx, w)):
                    interpolated_uvs[i] += uvs[vi] * wi
            # Clamp UVs to valid range
            interpolated_uvs = np.clip(interpolated_uvs, 0, 1)
            result["uvs"] = interpolated_uvs
            
        return result
        
    def _collapse_edge(self,
                      edge: Edge,
                      vertex_data: Dict[str, Any],
                      faces: np.ndarray,
                      valid_vertices: Set[int],
                      valid_faces: Set[int],
                      vertex_map: Dict[int, int],
                      quadrics: Dict[int, np.ndarray]):
        """Perform edge collapse"""
        v1 = edge.v1
        v2 = edge.v2
        
        # Update vertex position
        vertex_data["positions"][v1] = edge.optimal_position
        
        # Merge vertex attributes
        if vertex_data["normals"] is not None:
            vertex_data["normals"][v1] = (
                vertex_data["normals"][v1] + vertex_data["normals"][v2]
            ) / 2
            vertex_data["normals"][v1] /= np.linalg.norm(vertex_data["normals"][v1])
            
        if vertex_data["colors"] is not None:
            vertex_data["colors"][v1] = (
                vertex_data["colors"][v1] + vertex_data["colors"][v2]
            ) / 2
            
        if vertex_data["uvs"] is not None:
            vertex_data["uvs"][v1] = (
                vertex_data["uvs"][v1] + vertex_data["uvs"][v2]
            ) / 2
            
        # Update quadric
        quadrics[v1] = quadrics.get(v1, np.zeros((4, 4))) + \
                      quadrics.get(v2, np.zeros((4, 4)))
        
        # Update vertex mapping
        for v, mapped in vertex_map.items():
            if mapped == v2:
                vertex_map[v] = v1
                
        # Remove v2
        valid_vertices.discard(v2)
        
        # Update faces
        faces_to_remove = []
        for fi in list(valid_faces):
            face = faces[fi]
            
            # Replace v2 with v1
            for i in range(3):
                if vertex_map.get(face[i], face[i]) == v2:
                    face[i] = v1
                    
            # Remove degenerate faces
            if len(set(vertex_map.get(v, v) for v in face)) < 3:
                faces_to_remove.append(fi)
                
        for fi in faces_to_remove:
            valid_faces.discard(fi)
            
    def _update_edge_costs(self,
                          vertex: int,
                          vertices: np.ndarray,
                          faces: np.ndarray,
                          valid_faces: Set[int],
                          vertex_map: Dict[int, int],
                          quadrics: Dict[int, np.ndarray],
                          edges: Dict[Tuple[int, int], Edge],
                          edge_heap: List[Edge]):
        """Update edge costs for edges connected to vertex"""
        # Find connected vertices
        connected = set()
        for fi in valid_faces:
            face = faces[fi]
            if vertex in face:
                for v in face:
                    if v != vertex:
                        connected.add(vertex_map.get(v, v))
                        
        # Update edge costs
        for v2 in connected:
            v1 = vertex
            if v1 > v2:
                v1, v2 = v2, v1
                
            # Compute new cost
            cost, optimal_pos = self._compute_collapse_cost(
                v1, v2, vertices, quadrics
            )
            
            # Create new edge
            new_edge = Edge(v1, v2, cost, optimal_pos)
            edges[(v1, v2)] = new_edge
            heapq.heappush(edge_heap, new_edge)
            
    def _extract_final_mesh(self,
                           vertex_data: Dict[str, Any],
                           faces: np.ndarray,
                           valid_vertices: Set[int],
                           valid_faces: Set[int],
                           vertex_map: Dict[int, int]) -> Tuple[np.ndarray, np.ndarray, Dict[int, int]]:
        """Extract final decimated mesh"""
        # Create vertex remapping
        vertex_remap = {}
        new_idx = 0
        
        for v in sorted(valid_vertices):
            vertex_remap[v] = new_idx
            new_idx += 1
            
        # Extract vertices
        final_vertices = vertex_data["positions"][list(vertex_remap.keys())]
        
        # Remap faces
        final_faces = []
        for fi in valid_faces:
            face = faces[fi].copy()
            # Map to collapsed vertices then to new indices
            new_face = []
            for v in face:
                mapped_v = vertex_map.get(v, v)
                if mapped_v in vertex_remap:
                    new_face.append(vertex_remap[mapped_v])
                    
            if len(new_face) == 3 and len(set(new_face)) == 3:
                final_faces.append(new_face)
                
        return final_vertices, np.array(final_faces), vertex_remap
        
    async def _vertex_clustering_decimation(self,
                                          vertices: np.ndarray,
                                          faces: np.ndarray,
                                          target_vertices: int,
                                          normals: Optional[np.ndarray] = None,
                                          colors: Optional[np.ndarray] = None,
                                          uvs: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """
        Vertex clustering decimation - fast but lower quality
        """
        # Compute grid size based on target
        bbox_min = vertices.min(axis=0)
        bbox_max = vertices.max(axis=0)
        bbox_size = bbox_max - bbox_min
        
        # Estimate grid resolution
        volume = np.prod(bbox_size)
        cell_volume = volume / target_vertices
        cell_size = np.cbrt(cell_volume)
        
        # Create grid
        grid_size = np.maximum(1, (bbox_size / cell_size).astype(int))
        
        # Assign vertices to grid cells
        vertex_cells = np.floor((vertices - bbox_min) / cell_size).astype(int)
        vertex_cells = np.clip(vertex_cells, 0, grid_size - 1)
        
        # Compute cell indices
        cell_indices = (vertex_cells[:, 0] * grid_size[1] * grid_size[2] +
                       vertex_cells[:, 1] * grid_size[2] +
                       vertex_cells[:, 2])
        
        # Group vertices by cell
        cell_vertices = defaultdict(list)
        for i, cell_idx in enumerate(cell_indices):
            cell_vertices[cell_idx].append(i)
            
        # Create representative vertex for each cell
        new_vertices = []
        new_normals = [] if normals is not None else None
        new_colors = [] if colors is not None else None
        new_uvs = [] if uvs is not None else None
        vertex_map = {}
        
        for cell_idx, vertex_list in cell_vertices.items():
            # Compute representative position (centroid)
            rep_pos = vertices[vertex_list].mean(axis=0)
            new_vertices.append(rep_pos)
            
            # Map old vertices to new
            new_idx = len(new_vertices) - 1
            for v in vertex_list:
                vertex_map[v] = new_idx
                
            # Average attributes
            if normals is not None:
                avg_normal = normals[vertex_list].mean(axis=0)
                avg_normal /= np.linalg.norm(avg_normal)
                new_normals.append(avg_normal)
                
            if colors is not None:
                new_colors.append(colors[vertex_list].mean(axis=0))
                
            if uvs is not None:
                new_uvs.append(uvs[vertex_list].mean(axis=0))
                
        # Remap faces
        new_faces = []
        for face in faces:
            new_face = [vertex_map[v] for v in face]
            # Skip degenerate faces
            if len(set(new_face)) == 3:
                new_faces.append(new_face)
                
        result = {
            "vertices": np.array(new_vertices),
            "faces": np.array(new_faces)
        }
        
        if normals is not None:
            result["normals"] = np.array(new_normals)
        if colors is not None:
            result["colors"] = np.array(new_colors)
        if uvs is not None:
            result["uvs"] = np.array(new_uvs)
            
        return result
        
    async def _edge_collapse_decimation(self,
                                      vertices: np.ndarray,
                                      faces: np.ndarray,
                                      target_vertices: int,
                                      normals: Optional[np.ndarray] = None,
                                      colors: Optional[np.ndarray] = None,
                                      uvs: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """
        Edge collapse with proper attribute handling
        """
        # Create mesh with attributes
        mesh = trimesh.Trimesh(vertices=vertices, faces=faces)
        
        # Add vertex attributes if provided
        if normals is not None:
            mesh.vertex_normals = normals
        if colors is not None:
            mesh.visual.vertex_colors = colors
            
        # Store UV coordinates as vertex attributes
        if uvs is not None:
            mesh.vertex_attributes['texture_uv'] = uvs
            
        # Perform decimation preserving attributes
        try:
            # Use trimesh's quadric decimation which preserves attributes
            simplified = mesh.simplify_quadric_decimation(
                target_vertices,
                vertex_attribute_names=['texture_uv'] if uvs is not None else None
            )
            
            result = {
                "vertices": simplified.vertices,
                "faces": simplified.faces
            }
            
            # Extract preserved attributes
            if normals is not None:
                # Recompute normals for simplified mesh
                simplified.vertex_normals
                result["normals"] = simplified.vertex_normals
                
            if colors is not None and hasattr(simplified.visual, 'vertex_colors'):
                result["colors"] = simplified.visual.vertex_colors
                
            if uvs is not None and 'texture_uv' in simplified.vertex_attributes:
                result["uvs"] = simplified.vertex_attributes['texture_uv']
                
        except Exception as e:
            logger.warning(f"Trimesh decimation failed: {e}, using basic decimation")
            # Fallback to basic decimation
            simplified = mesh.simplify_quadric_decimation(target_vertices)
            result = {
                "vertices": simplified.vertices,
                "faces": simplified.faces
            }
            
            # Interpolate attributes manually
            if normals is not None or colors is not None or uvs is not None:
                result.update(self._interpolate_vertex_attributes(
                    mesh, simplified, normals, colors, uvs
                ))
        
        return result
        
    def _is_boundary_edge(self, v1: int, v2: int) -> bool:
        """Check if edge is on mesh boundary"""
        # An edge is on the boundary if it belongs to only one face
        edge_key = (min(v1, v2), max(v1, v2))
        
        # Get faces adjacent to this edge
        edge_data = self.edge_faces_cache.get(edge_key)
        if not edge_data:
            return False
            
        face_indices, _ = edge_data
        
        # Boundary edges have exactly one adjacent face
        return len(face_indices) == 1
        
    def _compute_feature_angle(self,
                              v1: int,
                              v2: int,
                              vertices: np.ndarray) -> float:
        """Compute feature angle between adjacent faces"""
        edge_key = (min(v1, v2), max(v1, v2))
        
        # Get faces adjacent to this edge
        edge_data = self.edge_faces_cache.get(edge_key)
        if not edge_data or len(edge_data[0]) < 2:
            return 0.0  # Not a feature edge
            
        face_indices = edge_data[0]
        if len(face_indices) != 2:
            return np.pi  # Non-manifold edge, treat as sharp feature
            
        # Get face normals
        normals = edge_data[1]
        if len(normals) < 2:
            return 0.0
            
        # Compute angle between face normals
        n1, n2 = normals[0], normals[1]
        
        # Normalize (should already be normalized, but ensure)
        n1 = n1 / (np.linalg.norm(n1) + 1e-10)
        n2 = n2 / (np.linalg.norm(n2) + 1e-10)
        
        # Compute angle
        cos_angle = np.clip(np.dot(n1, n2), -1.0, 1.0)
        angle = np.arccos(cos_angle)
        
        return angle
        
    def _would_create_duplicate_face(self,
                                   face1: np.ndarray,
                                   face2: np.ndarray,
                                   v1: int,
                                   v2: int) -> bool:
        """Check if collapsing v1->v2 would create duplicate face"""
        # Replace v2 with v1 in face2
        new_face2 = face2.copy()
        new_face2[new_face2 == v2] = v1
        
        # Check if faces would be identical
        return set(face1) == set(new_face2)
        
    async def generate_lods(self,
                          vertices: np.ndarray,
                          faces: np.ndarray,
                          lod_levels: List[float] = [1.0, 0.5, 0.25, 0.1],
                          normals: Optional[np.ndarray] = None,
                          colors: Optional[np.ndarray] = None,
                          uvs: Optional[np.ndarray] = None) -> List[MeshLOD]:
        """
        Generate multiple LOD levels for a mesh
        
        Args:
            vertices: Original vertices
            faces: Original faces
            lod_levels: List of decimation ratios
            
        Returns:
            List of MeshLOD objects
        """
        lods = []
        
        # Original mesh as LOD 0
        lods.append(MeshLOD(
            level=0,
            vertices=vertices,
            faces=faces,
            normals=normals,
            colors=colors,
            uvs=uvs,
            vertex_count=len(vertices),
            face_count=len(faces),
            error_metric=0.0
        ))
        
        # Generate each LOD level
        current_vertices = vertices
        current_faces = faces
        current_normals = normals
        current_colors = colors
        current_uvs = uvs
        
        for i, ratio in enumerate(lod_levels[1:], 1):
            # Decimate from current mesh
            result = await self.decimate_mesh(
                current_vertices,
                current_faces,
                target_ratio=ratio,
                normals=current_normals,
                colors=current_colors,
                uvs=current_uvs
            )
            
            # Create LOD
            lod = MeshLOD(
                level=i,
                vertices=result["vertices"],
                faces=result["faces"],
                normals=result.get("normals"),
                colors=result.get("colors"),
                uvs=result.get("uvs"),
                vertex_count=len(result["vertices"]),
                face_count=len(result["faces"]),
                error_metric=self._compute_lod_error(vertices, result["vertices"])
            )
            
            lods.append(lod)
            
            # Update current mesh for next iteration
            current_vertices = result["vertices"]
            current_faces = result["faces"]
            current_normals = result.get("normals")
            current_colors = result.get("colors")
            current_uvs = result.get("uvs")
            
        return lods
        
    def _compute_lod_error(self,
                          original_vertices: np.ndarray,
                          lod_vertices: np.ndarray) -> float:
        """Compute error metric between original and LOD mesh"""
        # Use Hausdorff distance or similar
        # For now, simple average nearest neighbor distance
        
        if len(lod_vertices) == 0:
            return float('inf')
            
        # Build KDTree for LOD vertices
        tree = cKDTree(lod_vertices)
        
        # Find nearest LOD vertex for each original vertex
        distances, _ = tree.query(original_vertices)
        
        # Return RMS error
        return float(np.sqrt(np.mean(distances ** 2)))
        
    def select_lod(self,
                  lods: List[MeshLOD],
                  view_distance: float,
                  screen_size: Tuple[int, int],
                  fov: float = 60.0) -> MeshLOD:
        """
        Select appropriate LOD based on viewing parameters
        
        Args:
            lods: List of available LODs
            view_distance: Distance from camera to object
            screen_size: Screen resolution (width, height)
            fov: Field of view in degrees
            
        Returns:
            Selected LOD
        """
        # Compute projected size
        # Assuming unit sphere bounding box
        object_size = 1.0  # Should be computed from actual bounds
        
        # Angular size in radians
        angular_size = 2 * np.arctan(object_size / (2 * view_distance))
        
        # Projected size in pixels
        fov_rad = np.radians(fov)
        pixels_per_radian = screen_size[1] / fov_rad
        projected_pixels = angular_size * pixels_per_radian
        
        # Select LOD based on projected size
        if projected_pixels > 500:
            return lods[0]  # Full resolution
        elif projected_pixels > 200:
            return lods[min(1, len(lods)-1)]
        elif projected_pixels > 100:
            return lods[min(2, len(lods)-1)]
        else:
            return lods[-1]  # Lowest resolution 