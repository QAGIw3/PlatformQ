use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use nalgebra::{Vector3, Matrix4};

#[derive(Debug, Serialize, Deserialize)]
struct MeshData {
    vertices: Vec<[f32; 3]>,
    indices: Vec<u32>,
    normals: Option<Vec<[f32; 3]>>,
    uvs: Option<Vec<[f32; 2]>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DecimationResult {
    mesh: MeshData,
    metrics: DecimationMetrics,
}

#[derive(Debug, Serialize, Deserialize)]
struct DecimationMetrics {
    original_vertex_count: usize,
    original_face_count: usize,
    decimated_vertex_count: usize,
    decimated_face_count: usize,
    reduction_percentage: f32,
    max_error: f32,
    mean_error: f32,
}

// Quadric error metric for edge collapse
#[derive(Clone, Copy)]
struct Quadric {
    matrix: [[f64; 4]; 4],
}

impl Quadric {
    fn new() -> Self {
        Self {
            matrix: [[0.0; 4]; 4],
        }
    }

    fn from_plane(a: f64, b: f64, c: f64, d: f64) -> Self {
        let mut q = Self::new();
        let plane = [a, b, c, d];
        
        for i in 0..4 {
            for j in 0..4 {
                q.matrix[i][j] = plane[i] * plane[j];
            }
        }
        
        q
    }

    fn add(&mut self, other: &Quadric) {
        for i in 0..4 {
            for j in 0..4 {
                self.matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    fn evaluate(&self, v: &[f64; 3]) -> f64 {
        let v4 = [v[0], v[1], v[2], 1.0];
        let mut result = 0.0;
        
        for i in 0..4 {
            let mut sum = 0.0;
            for j in 0..4 {
                sum += self.matrix[i][j] * v4[j];
            }
            result += v4[i] * sum;
        }
        
        result
    }
}

#[derive(Clone)]
struct EdgeCollapse {
    v1: usize,
    v2: usize,
    target_position: [f64; 3],
    cost: f64,
}

impl PartialEq for EdgeCollapse {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost
    }
}

impl Eq for EdgeCollapse {}

impl PartialOrd for EdgeCollapse {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse order for min-heap behavior
        other.cost.partial_cmp(&self.cost)
    }
}

impl Ord for EdgeCollapse {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[wasm_bindgen]
pub fn decimate_mesh(
    mesh_json: &str,
    target_ratio: f32
) -> String {
    let mesh: MeshData = match serde_json::from_str(mesh_json) {
        Ok(m) => m,
        Err(e) => return format!(r#"{{"error": "Failed to parse mesh: {}"}}"#, e),
    };

    let result = perform_decimation(mesh, target_ratio);
    
    match serde_json::to_string(&result) {
        Ok(json) => json,
        Err(e) => format!(r#"{{"error": "Failed to serialize result: {}"}}"#, e),
    }
}

fn perform_decimation(mut mesh: MeshData, target_ratio: f32) -> DecimationResult {
    let original_vertex_count = mesh.vertices.len();
    let original_face_count = mesh.indices.len() / 3;
    let target_vertex_count = (original_vertex_count as f32 * target_ratio) as usize;
    
    // Build vertex quadrics
    let mut vertex_quadrics = vec![Quadric::new(); mesh.vertices.len()];
    
    // Calculate quadrics for each face
    for i in (0..mesh.indices.len()).step_by(3) {
        let v0 = mesh.indices[i] as usize;
        let v1 = mesh.indices[i + 1] as usize;
        let v2 = mesh.indices[i + 2] as usize;
        
        let p0 = Vector3::new(
            mesh.vertices[v0][0] as f64,
            mesh.vertices[v0][1] as f64,
            mesh.vertices[v0][2] as f64,
        );
        let p1 = Vector3::new(
            mesh.vertices[v1][0] as f64,
            mesh.vertices[v1][1] as f64,
            mesh.vertices[v1][2] as f64,
        );
        let p2 = Vector3::new(
            mesh.vertices[v2][0] as f64,
            mesh.vertices[v2][1] as f64,
            mesh.vertices[v2][2] as f64,
        );
        
        // Calculate face normal
        let normal = (p1 - p0).cross(&(p2 - p0)).normalize();
        let d = -normal.dot(&p0);
        
        let face_quadric = Quadric::from_plane(normal.x, normal.y, normal.z, d);
        
        vertex_quadrics[v0].add(&face_quadric);
        vertex_quadrics[v1].add(&face_quadric);
        vertex_quadrics[v2].add(&face_quadric);
    }
    
    // Build edge list
    let mut edges = HashMap::new();
    for i in (0..mesh.indices.len()).step_by(3) {
        let v0 = mesh.indices[i] as usize;
        let v1 = mesh.indices[i + 1] as usize;
        let v2 = mesh.indices[i + 2] as usize;
        
        add_edge(&mut edges, v0, v1);
        add_edge(&mut edges, v1, v2);
        add_edge(&mut edges, v2, v0);
    }
    
    // Initialize collapse candidates
    let mut collapse_heap = BinaryHeap::new();
    let mut vertex_map: Vec<usize> = (0..mesh.vertices.len()).collect();
    let mut removed_vertices = vec![false; mesh.vertices.len()];
    
    for (&(v1, v2), _) in &edges {
        if let Some(collapse) = compute_collapse_cost(
            v1, v2, &mesh.vertices, &vertex_quadrics
        ) {
            collapse_heap.push(collapse);
        }
    }
    
    // Perform edge collapses
    let mut current_vertex_count = original_vertex_count;
    let mut max_error = 0.0f32;
    let mut total_error = 0.0f32;
    let mut collapse_count = 0;
    
    while current_vertex_count > target_vertex_count && !collapse_heap.is_empty() {
        let collapse = collapse_heap.pop().unwrap();
        
        // Skip if vertices have been removed
        if removed_vertices[collapse.v1] || removed_vertices[collapse.v2] {
            continue;
        }
        
        // Update vertex position
        mesh.vertices[collapse.v1] = [
            collapse.target_position[0] as f32,
            collapse.target_position[1] as f32,
            collapse.target_position[2] as f32,
        ];
        
        // Mark v2 as removed and map it to v1
        removed_vertices[collapse.v2] = true;
        vertex_map[collapse.v2] = collapse.v1;
        
        // Update quadric
        vertex_quadrics[collapse.v1].add(&vertex_quadrics[collapse.v2]);
        
        // Update error metrics
        let error = collapse.cost as f32;
        max_error = max_error.max(error);
        total_error += error;
        collapse_count += 1;
        
        current_vertex_count -= 1;
        
        // Update edges and recompute costs for neighbors
        // (simplified for brevity)
    }
    
    // Build decimated mesh
    let mut new_vertices = Vec::new();
    let mut vertex_remap = HashMap::new();
    
    for (i, &removed) in removed_vertices.iter().enumerate() {
        if !removed {
            let new_index = new_vertices.len() as u32;
            vertex_remap.insert(i, new_index);
            new_vertices.push(mesh.vertices[i]);
        }
    }
    
    // Remap indices
    let mut new_indices = Vec::new();
    for i in (0..mesh.indices.len()).step_by(3) {
        let v0 = resolve_vertex_mapping(mesh.indices[i] as usize, &vertex_map);
        let v1 = resolve_vertex_mapping(mesh.indices[i + 1] as usize, &vertex_map);
        let v2 = resolve_vertex_mapping(mesh.indices[i + 2] as usize, &vertex_map);
        
        // Skip degenerate triangles
        if v0 != v1 && v1 != v2 && v2 != v0 {
            if let (Some(&nv0), Some(&nv1), Some(&nv2)) = 
                (vertex_remap.get(&v0), vertex_remap.get(&v1), vertex_remap.get(&v2)) {
                new_indices.push(nv0);
                new_indices.push(nv1);
                new_indices.push(nv2);
            }
        }
    }
    
    let decimated_mesh = MeshData {
        vertices: new_vertices,
        indices: new_indices,
        normals: None, // Recalculate if needed
        uvs: None,     // Preserve if needed
    };
    
    let metrics = DecimationMetrics {
        original_vertex_count,
        original_face_count,
        decimated_vertex_count: decimated_mesh.vertices.len(),
        decimated_face_count: decimated_mesh.indices.len() / 3,
        reduction_percentage: 1.0 - (decimated_mesh.vertices.len() as f32 / original_vertex_count as f32),
        max_error,
        mean_error: if collapse_count > 0 { total_error / collapse_count as f32 } else { 0.0 },
    };
    
    DecimationResult {
        mesh: decimated_mesh,
        metrics,
    }
}

fn add_edge(edges: &mut HashMap<(usize, usize), ()>, v1: usize, v2: usize) {
    let key = if v1 < v2 { (v1, v2) } else { (v2, v1) };
    edges.insert(key, ());
}

fn compute_collapse_cost(
    v1: usize,
    v2: usize,
    vertices: &[[f32; 3]],
    quadrics: &[Quadric],
) -> Option<EdgeCollapse> {
    // Compute optimal collapse position
    let p1 = &vertices[v1];
    let p2 = &vertices[v2];
    
    // For simplicity, use midpoint (could solve for optimal position)
    let target = [
        (p1[0] + p2[0]) as f64 / 2.0,
        (p1[1] + p2[1]) as f64 / 2.0,
        (p1[2] + p2[2]) as f64 / 2.0,
    ];
    
    // Compute error
    let mut combined_quadric = quadrics[v1].clone();
    combined_quadric.add(&quadrics[v2]);
    let cost = combined_quadric.evaluate(&target);
    
    Some(EdgeCollapse {
        v1,
        v2,
        target_position: target,
        cost,
    })
}

fn resolve_vertex_mapping(v: usize, vertex_map: &[usize]) -> usize {
    let mut current = v;
    while vertex_map[current] != current {
        current = vertex_map[current];
    }
    current
}

#[wasm_bindgen]
pub fn calculate_mesh_quality(mesh_json: &str) -> String {
    let mesh: MeshData = match serde_json::from_str(mesh_json) {
        Ok(m) => m,
        Err(e) => return format!(r#"{{"error": "Failed to parse mesh: {}"}}"#, e),
    };
    
    let mut quality_score = 1.0f32;
    let mut issues = Vec::new();
    
    // Check for degenerate triangles
    let mut degenerate_count = 0;
    for i in (0..mesh.indices.len()).step_by(3) {
        let v0 = mesh.vertices[mesh.indices[i] as usize];
        let v1 = mesh.vertices[mesh.indices[i + 1] as usize];
        let v2 = mesh.vertices[mesh.indices[i + 2] as usize];
        
        let area = triangle_area(&v0, &v1, &v2);
        if area < 1e-6 {
            degenerate_count += 1;
        }
    }
    
    if degenerate_count > 0 {
        quality_score *= 0.9;
        issues.push(format!("{} degenerate triangles", degenerate_count));
    }
    
    // Check aspect ratios
    let mut bad_aspect_count = 0;
    for i in (0..mesh.indices.len()).step_by(3) {
        let v0 = mesh.vertices[mesh.indices[i] as usize];
        let v1 = mesh.vertices[mesh.indices[i + 1] as usize];
        let v2 = mesh.vertices[mesh.indices[i + 2] as usize];
        
        let aspect = triangle_aspect_ratio(&v0, &v1, &v2);
        if aspect > 10.0 {
            bad_aspect_count += 1;
        }
    }
    
    if bad_aspect_count > 0 {
        quality_score *= 0.95;
        issues.push(format!("{} triangles with poor aspect ratio", bad_aspect_count));
    }
    
    let result = serde_json::json!({
        "quality_score": quality_score,
        "vertex_count": mesh.vertices.len(),
        "face_count": mesh.indices.len() / 3,
        "issues": issues,
    });
    
    result.to_string()
}

fn triangle_area(v0: &[f32; 3], v1: &[f32; 3], v2: &[f32; 3]) -> f32 {
    let a = Vector3::new(v1[0] - v0[0], v1[1] - v0[1], v1[2] - v0[2]);
    let b = Vector3::new(v2[0] - v0[0], v2[1] - v0[1], v2[2] - v0[2]);
    a.cross(&b).magnitude() * 0.5
}

fn triangle_aspect_ratio(v0: &[f32; 3], v1: &[f32; 3], v2: &[f32; 3]) -> f32 {
    let a = distance(v0, v1);
    let b = distance(v1, v2);
    let c = distance(v2, v0);
    
    let max_edge = a.max(b).max(c);
    let area = triangle_area(v0, v1, v2);
    
    if area < 1e-6 {
        f32::INFINITY
    } else {
        max_edge * max_edge / area
    }
}

fn distance(v0: &[f32; 3], v1: &[f32; 3]) -> f32 {
    let dx = v1[0] - v0[0];
    let dy = v1[1] - v0[1];
    let dz = v1[2] - v0[2];
    (dx * dx + dy * dy + dz * dz).sqrt()
} 