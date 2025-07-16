"""
Distributed Blender Processor using Apache Spark

This processor enables parallel rendering of Blender scenes by splitting
the work across multiple Spark executors.
"""

import os
import json
import subprocess
import tempfile
import shutil
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

from processor_base import ProcessorBase


class BlenderDistributedProcessor(ProcessorBase):
    """Distributed processor for Blender scene rendering"""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("BlenderDistributed", tenant_id, config)
        
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate Blender processing configuration"""
        required_fields = ["output_format", "resolution"]
        
        for field in required_fields:
            if field not in processing_config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate output format
        valid_formats = ["PNG", "JPEG", "EXR", "TIFF"]
        if processing_config["output_format"].upper() not in valid_formats:
            raise ValueError(f"Invalid output format: {processing_config['output_format']}")
        
        # Validate resolution
        if not isinstance(processing_config["resolution"], dict):
            raise ValueError("Resolution must be a dict with 'x' and 'y' keys")
        
        if "x" not in processing_config["resolution"] or "y" not in processing_config["resolution"]:
            raise ValueError("Resolution must have 'x' and 'y' values")
        
        return True
    
    def split_work(self, asset_uri: str, processing_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Split rendering work into frame chunks"""
        # Get frame range
        start_frame = processing_config.get("start_frame", 1)
        end_frame = processing_config.get("end_frame", 1)
        chunk_size = processing_config.get("chunk_size", 10)
        
        chunks = []
        current_frame = start_frame
        
        while current_frame <= end_frame:
            chunk_end = min(current_frame + chunk_size - 1, end_frame)
            
            chunk = {
                "asset_uri": asset_uri,
                "chunk_id": len(chunks),
                "start_frame": current_frame,
                "end_frame": chunk_end,
                "config": processing_config
            }
            chunks.append(chunk)
            
            current_frame = chunk_end + 1
        
        return chunks
    
    def process_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """Process a chunk of frames"""
        start_time = datetime.utcnow()
        chunk_id = chunk["chunk_id"]
        start_frame = chunk["start_frame"]
        end_frame = chunk["end_frame"]
        config = chunk["config"]
        
        # Create temporary directory for this chunk
        temp_dir = tempfile.mkdtemp(prefix=f"blender_chunk_{chunk_id}_")
        
        try:
            # Download blend file
            local_blend = os.path.join(temp_dir, "scene.blend")
            self.download_from_s3(chunk["asset_uri"], local_blend)
            
            # Prepare output directory
            output_dir = os.path.join(temp_dir, "output")
            os.makedirs(output_dir)
            
            # Build Blender command
            blender_cmd = [
                "blender",
                "-b", local_blend,  # Background mode
                "-o", os.path.join(output_dir, "frame_####"),
                "-F", config["output_format"].upper(),
                "-x", "1",  # Add frame numbers to filenames
                "-s", str(start_frame),
                "-e", str(end_frame),
                "-a"  # Render animation
            ]
            
            # Set resolution if specified
            if "resolution" in config:
                blender_cmd.extend([
                    "-x", str(config["resolution"]["x"]),
                    "-y", str(config["resolution"]["y"])
                ])
            
            # Set render engine if specified
            if "render_engine" in config:
                engine_map = {
                    "CYCLES": "CYCLES",
                    "EEVEE": "BLENDER_EEVEE",
                    "WORKBENCH": "BLENDER_WORKBENCH"
                }
                if config["render_engine"].upper() in engine_map:
                    blender_cmd.extend(["-E", engine_map[config["render_engine"].upper()]])
            
            # Execute Blender
            result = subprocess.run(blender_cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Blender rendering failed: {result.stderr}")
            
            # Upload rendered frames
            rendered_frames = []
            output_base_uri = config.get("output_uri", f"s3://renders/{self.tenant_id}/")
            
            for filename in os.listdir(output_dir):
                if filename.startswith("frame_"):
                    local_path = os.path.join(output_dir, filename)
                    s3_path = f"{output_base_uri}/{filename}"
                    self.upload_to_s3(local_path, s3_path)
                    rendered_frames.append(s3_path)
            
            # Extract metadata from Blender output
            metadata = self._parse_blender_output(result.stdout)
            metadata["frames_rendered"] = len(rendered_frames)
            metadata["frame_range"] = f"{start_frame}-{end_frame}"
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                "chunk_id": chunk_id,
                "status": "success",
                "frames_rendered": len(rendered_frames),
                "frame_uris": rendered_frames,
                "processing_time": processing_time,
                "metadata": metadata
            }
            
        except Exception as e:
            return {
                "chunk_id": chunk_id,
                "status": "error",
                "error": str(e),
                "processing_time": (datetime.utcnow() - start_time).total_seconds()
            }
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def merge_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge results from all chunks"""
        merged = super().merge_results(results)
        
        # Collect all rendered frames
        all_frames = []
        total_frames = 0
        errors = []
        
        for result in results:
            if result["status"] == "success":
                all_frames.extend(result.get("frame_uris", []))
                total_frames += result.get("frames_rendered", 0)
            else:
                errors.append({
                    "chunk_id": result["chunk_id"],
                    "error": result.get("error", "Unknown error")
                })
        
        merged["total_frames_rendered"] = total_frames
        merged["frame_uris"] = sorted(all_frames)  # Sort by frame number
        merged["errors"] = errors
        merged["success_rate"] = (len(results) - len(errors)) / len(results) if results else 0
        
        return merged
    
    def _parse_blender_output(self, output: str) -> Dict[str, Any]:
        """Parse Blender console output for metadata"""
        metadata = {}
        
        # Extract render time
        if "Time:" in output:
            for line in output.split('\n'):
                if "Time:" in line and "Render" in line:
                    # Parse render time
                    parts = line.split("Time:")
                    if len(parts) > 1:
                        metadata["render_time"] = parts[1].strip()
        
        # Extract memory usage
        if "Peak:" in output:
            for line in output.split('\n'):
                if "Peak:" in line:
                    parts = line.split("Peak:")
                    if len(parts) > 1:
                        metadata["peak_memory"] = parts[1].strip()
        
        # Extract scene statistics
        scene_stats = {}
        stats_keywords = ["Verts:", "Faces:", "Tris:", "Objects:", "Lamps:", "Memory:"]
        
        for line in output.split('\n'):
            for keyword in stats_keywords:
                if keyword in line:
                    parts = line.split(keyword)
                    if len(parts) > 1:
                        value = parts[1].split()[0] if parts[1].split() else ""
                        scene_stats[keyword.rstrip(':')] = value
        
        if scene_stats:
            metadata["scene_statistics"] = scene_stats
        
        return metadata
    
    def process(self, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Main processing method - delegates to run()"""
        asset_id = processing_config.get("asset_id", "unknown")
        return self.run(asset_id, asset_uri, processing_config)


def main():
    """Main entry point for Spark job submission"""
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: spark-submit blender_distributed.py <tenant_id> <asset_uri> <config_json>")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    asset_uri = sys.argv[2]
    config = json.loads(sys.argv[3])
    
    # Create processor
    processor = BlenderDistributedProcessor(tenant_id)
    
    # Run processing
    result = processor.process(asset_uri, config)
    
    # Output result as JSON
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main() 