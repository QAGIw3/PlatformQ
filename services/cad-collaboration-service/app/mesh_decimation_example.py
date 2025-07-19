"""
Mesh Decimation Example

Demonstrates the enhanced mesh decimation features including:
- Boundary edge preservation
- Feature angle detection
- Proper attribute handling
- Multi-level LOD generation
"""

import numpy as np
import trimesh
import asyncio
import logging
from typing import Dict, List, Tuple, Optional
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

from mesh_decimator import MeshDecimator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_test_mesh_with_features() -> Tuple[np.ndarray, np.ndarray, Dict]:
    """Create a test mesh with sharp features and attributes"""
    # Create a cube with a notch (to test feature preservation)
    vertices = np.array([
        # Bottom face
        [0, 0, 0], [2, 0, 0], [2, 2, 0], [0, 2, 0],
        # Top face
        [0, 0, 2], [2, 0, 2], [2, 2, 2], [0, 2, 2],
        # Notch vertices
        [0.5, 0, 1], [1.5, 0, 1], [0.5, 0.5, 1], [1.5, 0.5, 1]
    ], dtype=np.float32)
    
    faces = np.array([
        # Bottom face
        [0, 1, 2], [0, 2, 3],
        # Top face
        [4, 6, 5], [4, 7, 6],
        # Front face (with notch)
        [0, 8, 4], [8, 10, 4], [10, 8, 9], [10, 9, 11],
        [9, 1, 5], [9, 5, 11], [11, 5, 6], [11, 6, 10],
        [10, 6, 7], [10, 7, 4],
        # Back face
        [2, 6, 3], [3, 6, 7],
        # Left face
        [0, 4, 3], [3, 4, 7],
        # Right face
        [1, 2, 5], [2, 6, 5],
        # Notch faces
        [8, 9, 10], [9, 11, 10]
    ], dtype=np.int32)
    
    # Generate vertex colors (gradient based on height)
    colors = np.zeros((len(vertices), 4), dtype=np.uint8)
    for i, v in enumerate(vertices):
        colors[i] = [
            int(255 * (v[2] / 2.0)),  # Red channel based on height
            int(255 * (1 - v[2] / 2.0)),  # Green inverse of height
            128,  # Blue constant
            255  # Alpha
        ]
    
    # Generate UV coordinates (simple planar projection)
    uvs = np.zeros((len(vertices), 2), dtype=np.float32)
    for i, v in enumerate(vertices):
        uvs[i] = [v[0] / 2.0, v[1] / 2.0]
    
    # Calculate vertex normals
    mesh = trimesh.Trimesh(vertices=vertices, faces=faces)
    normals = mesh.vertex_normals.astype(np.float32)
    
    attributes = {
        "normals": normals,
        "colors": colors,
        "uvs": uvs
    }
    
    return vertices, faces, attributes


async def test_decimation_algorithms():
    """Test different decimation algorithms"""
    # Create test mesh
    vertices, faces, attributes = await create_test_mesh_with_features()
    
    logger.info(f"Original mesh: {len(vertices)} vertices, {len(faces)} faces")
    
    # Test different algorithms
    algorithms = ["quadric", "edge_collapse", "vertex_clustering"]
    results = {}
    
    for algo in algorithms:
        logger.info(f"\nTesting {algo} algorithm...")
        
        decimator = MeshDecimator(
            algorithm=algo,
            preserve_features=True,
            preserve_boundaries=True
        )
        
        # Decimate to 50% of original vertices
        result = await decimator.decimate_mesh(
            vertices=vertices,
            faces=faces,
            target_ratio=0.5,
            normals=attributes["normals"],
            colors=attributes["colors"],
            uvs=attributes["uvs"]
        )
        
        results[algo] = result
        
        logger.info(f"{algo} result: {result['decimated_vertices']} vertices, "
                   f"{result['decimated_faces']} faces, "
                   f"processing time: {result['processing_time']:.3f}s")
        
        # Check attribute preservation
        has_normals = "normals" in result
        has_colors = "colors" in result
        has_uvs = "uvs" in result
        
        logger.info(f"Attributes preserved - Normals: {has_normals}, "
                   f"Colors: {has_colors}, UVs: {has_uvs}")
    
    return results


async def test_feature_preservation():
    """Test feature angle detection and preservation"""
    # Create a mesh with sharp edges
    vertices = np.array([
        # Base square
        [0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0],
        # Peak (sharp feature)
        [0.5, 0.5, 1]
    ], dtype=np.float32)
    
    faces = np.array([
        # Base
        [0, 1, 2], [0, 2, 3],
        # Pyramid sides (sharp edges)
        [0, 4, 1], [1, 4, 2], [2, 4, 3], [3, 4, 0]
    ], dtype=np.int32)
    
    # Test with and without feature preservation
    for preserve_features in [False, True]:
        logger.info(f"\nTesting with preserve_features={preserve_features}")
        
        decimator = MeshDecimator(
            algorithm="quadric",
            preserve_features=preserve_features,
            preserve_boundaries=True
        )
        
        # Build edge-face cache for feature detection
        decimator._build_edge_faces_cache(vertices, faces)
        
        # Check feature angles
        sharp_edges = []
        for i in range(len(faces)):
            face = faces[i]
            for j in range(3):
                v1 = face[j]
                v2 = face[(j + 1) % 3]
                
                angle = decimator._compute_feature_angle(v1, v2, vertices)
                if angle > np.pi / 6:  # 30 degrees
                    sharp_edges.append((v1, v2, np.degrees(angle)))
        
        logger.info(f"Found {len(sharp_edges)} sharp edges:")
        for v1, v2, angle in sharp_edges:
            logger.info(f"  Edge ({v1}, {v2}): {angle:.1f} degrees")
        
        # Check boundary edges
        boundary_edges = []
        for i in range(len(faces)):
            face = faces[i]
            for j in range(3):
                v1 = face[j]
                v2 = face[(j + 1) % 3]
                
                if decimator._is_boundary_edge(v1, v2):
                    boundary_edges.append((v1, v2))
        
        logger.info(f"Found {len(boundary_edges)} boundary edges")


async def test_lod_generation():
    """Test multi-level LOD generation"""
    # Load or create a more complex mesh
    try:
        # Try to load a standard mesh
        mesh = trimesh.load_mesh('bunny.ply')
        vertices = mesh.vertices.astype(np.float32)
        faces = mesh.faces.astype(np.int32)
    except:
        # Create a subdivided sphere as test mesh
        mesh = trimesh.creation.icosphere(subdivisions=3)
        vertices = mesh.vertices.astype(np.float32)
        faces = mesh.faces.astype(np.int32)
    
    logger.info(f"\nGenerating LODs for mesh with {len(vertices)} vertices")
    
    decimator = MeshDecimator(
        algorithm="quadric",
        preserve_features=True,
        preserve_boundaries=True
    )
    
    # Generate multiple LOD levels
    lod_ratios = [1.0, 0.5, 0.25, 0.1, 0.05]
    lods = []
    
    for ratio in lod_ratios:
        if ratio == 1.0:
            # Original mesh
            lods.append({
                "vertices": vertices,
                "faces": faces,
                "ratio": ratio,
                "vertex_count": len(vertices),
                "face_count": len(faces)
            })
        else:
            # Decimate
            result = await decimator.decimate_mesh(
                vertices=vertices,
                faces=faces,
                target_ratio=ratio
            )
            
            lods.append({
                "vertices": result["vertices"],
                "faces": result["faces"],
                "ratio": ratio,
                "vertex_count": len(result["vertices"]),
                "face_count": len(result["faces"]),
                "processing_time": result["processing_time"]
            })
            
    # Print LOD statistics
    logger.info("\nLOD Statistics:")
    logger.info("Level | Ratio | Vertices | Faces | Time (s)")
    logger.info("------|-------|----------|-------|----------")
    
    for i, lod in enumerate(lods):
        time_str = f"{lod.get('processing_time', 0):.3f}" if 'processing_time' in lod else "N/A"
        logger.info(f"LOD{i}  | {lod['ratio']:5.2f} | {lod['vertex_count']:8d} | "
                   f"{lod['face_count']:5d} | {time_str:>8s}")
    
    return lods


async def visualize_decimation_results(results: Dict):
    """Visualize decimation results"""
    fig = plt.figure(figsize=(15, 5))
    
    # Original mesh
    vertices, faces, attributes = await create_test_mesh_with_features()
    
    ax1 = fig.add_subplot(141, projection='3d')
    ax1.set_title("Original Mesh")
    mesh = trimesh.Trimesh(vertices=vertices, faces=faces)
    mesh.plot(ax=ax1)
    
    # Decimated meshes
    for i, (algo, result) in enumerate(results.items()):
        ax = fig.add_subplot(142 + i, projection='3d')
        ax.set_title(f"{algo.capitalize()} Decimation")
        
        if "vertices" in result and "faces" in result:
            mesh = trimesh.Trimesh(
                vertices=result["vertices"],
                faces=result["faces"]
            )
            mesh.plot(ax=ax)
    
    plt.tight_layout()
    plt.savefig("mesh_decimation_comparison.png", dpi=150)
    logger.info("Saved visualization to mesh_decimation_comparison.png")


async def benchmark_performance():
    """Benchmark decimation performance on meshes of different sizes"""
    logger.info("\nBenchmarking decimation performance...")
    
    # Create meshes of different sizes
    sizes = [10, 20, 40, 80]  # Subdivision levels
    results = []
    
    for size in sizes:
        # Create icosphere with increasing complexity
        mesh = trimesh.creation.icosphere(subdivisions=int(np.log2(size)))
        vertices = mesh.vertices.astype(np.float32)
        faces = mesh.faces.astype(np.int32)
        
        logger.info(f"\nMesh size: {len(vertices)} vertices, {len(faces)} faces")
        
        # Benchmark each algorithm
        for algo in ["quadric", "edge_collapse"]:
            decimator = MeshDecimator(algorithm=algo)
            
            # Time the decimation
            import time
            start = time.time()
            
            result = await decimator.decimate_mesh(
                vertices=vertices,
                faces=faces,
                target_ratio=0.1  # Aggressive decimation
            )
            
            elapsed = time.time() - start
            
            results.append({
                "original_vertices": len(vertices),
                "original_faces": len(faces),
                "algorithm": algo,
                "time": elapsed,
                "final_vertices": result["decimated_vertices"],
                "final_faces": result["decimated_faces"]
            })
            
            logger.info(f"{algo}: {elapsed:.3f}s, "
                       f"reduced to {result['decimated_vertices']} vertices")
    
    return results


async def main():
    """Run all tests"""
    logger.info("Starting mesh decimation tests...")
    
    # Test 1: Compare algorithms
    logger.info("\n=== Test 1: Algorithm Comparison ===")
    results = await test_decimation_algorithms()
    
    # Test 2: Feature preservation
    logger.info("\n=== Test 2: Feature Preservation ===")
    await test_feature_preservation()
    
    # Test 3: LOD generation
    logger.info("\n=== Test 3: LOD Generation ===")
    lods = await test_lod_generation()
    
    # Test 4: Performance benchmark
    logger.info("\n=== Test 4: Performance Benchmark ===")
    perf_results = await benchmark_performance()
    
    # Visualize results (if matplotlib available)
    try:
        await visualize_decimation_results(results)
    except Exception as e:
        logger.warning(f"Could not create visualization: {e}")
    
    logger.info("\nAll tests completed!")


if __name__ == "__main__":
    asyncio.run(main()) 