"""
Multimedia processor for image, audio, and video processing
"""

import logging
import json
from typing import Dict, Any, List
import os

from data_intelligence_common.base_service import BaseFileProcessor

logger = logging.getLogger(__name__)


class MultimediaProcessor(BaseFileProcessor):
    """
    Unified processor for multimedia files (images, audio, video)
    """
    
    # File type categories
    IMAGE_FORMATS = ["jpg", "jpeg", "png", "gif", "bmp", "tiff", "svg", "webp", "xcf"]
    AUDIO_FORMATS = ["mp3", "wav", "flac", "ogg", "m4a", "aac", "wma", "aup", "aup3"]
    VIDEO_FORMATS = ["mp4", "avi", "mov", "mkv", "webm", "flv", "wmv", "mpg", "mpeg", "osp"]
    
    @property
    def processor_type(self) -> str:
        return "multimedia"
    
    @property
    def supported_formats(self) -> List[str]:
        return self.IMAGE_FORMATS + self.AUDIO_FORMATS + self.VIDEO_FORMATS
    
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Enhanced Spark config for multimedia processing"""
        config = super().spark_config
        config.update({
            "spark.executor.memory": "6g",
            "spark.executor.cores": "4",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        })
        return config
    
    def get_media_type(self, file_path: str) -> str:
        """Determine the media type of a file"""
        ext = os.path.splitext(file_path)[1].lower().lstrip('.')
        if ext in self.IMAGE_FORMATS:
            return "image"
        elif ext in self.AUDIO_FORMATS:
            return "audio"
        elif ext in self.VIDEO_FORMATS:
            return "video"
        else:
            return "unknown"
    
    async def validate_input(self, file_path: str) -> bool:
        """Validate multimedia file"""
        try:
            if not self.supports_file(file_path):
                return False
            
            if not os.path.exists(file_path):
                return False
            
            # Check minimum file size based on type
            file_size = os.path.getsize(file_path)
            media_type = self.get_media_type(file_path)
            
            min_sizes = {
                "image": 100,      # 100 bytes
                "audio": 1000,     # 1 KB
                "video": 10000,    # 10 KB
                "unknown": 10
            }
            
            if file_size < min_sizes.get(media_type, 10):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating multimedia file: {e}")
            return False
    
    async def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from multimedia file"""
        media_type = self.get_media_type(file_path)
        
        metadata = {
            "file_path": file_path,
            "file_size": os.path.getsize(file_path),
            "file_format": os.path.splitext(file_path)[1].lower().lstrip('.'),
            "media_type": media_type,
            "processor": self.processor_type
        }
        
        # Type-specific metadata would be extracted during processing
        return metadata
    
    def get_spark_job_script(self) -> str:
        """Get the Spark job script based on media type"""
        # In practice, this would return different scripts based on the file type
        return "/opt/spark-jobs/multimedia_distributed_processing.py"


# Spark job script content (would be deployed separately)
MULTIMEDIA_SPARK_JOB = '''
"""
Distributed multimedia processing using Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import subprocess
import os
import tempfile
from PIL import Image
import numpy as np

def process_image_batch(image_files):
    """Process a batch of images using GIMP operations"""
    results = []
    
    for file_data in image_files:
        input_file = file_data["input_file"]
        operations = file_data.get("operations", ["thumbnail"])
        output_path = file_data["output_path"]
        
        try:
            # Use PIL for basic operations, GIMP for advanced
            if "thumbnail" in operations:
                img = Image.open(input_file)
                img.thumbnail((256, 256), Image.Resampling.LANCZOS)
                thumb_path = os.path.join(output_path, "thumb_" + os.path.basename(input_file))
                img.save(thumb_path)
                
            if "resize" in operations:
                target_size = file_data.get("target_size", (1920, 1080))
                img = Image.open(input_file)
                img = img.resize(target_size, Image.Resampling.LANCZOS)
                resized_path = os.path.join(output_path, "resized_" + os.path.basename(input_file))
                img.save(resized_path)
                
            if "filter" in operations:
                # Use GIMP for advanced filters
                gimp_script = create_gimp_script(input_file, output_path, file_data.get("filters", []))
                subprocess.run(["gimp", "-i", "-b", gimp_script, "-b", "(gimp-quit 0)"])
            
            results.append({
                "status": "success",
                "file": input_file,
                "operations": operations
            })
            
        except Exception as e:
            results.append({
                "status": "failed",
                "file": input_file,
                "error": str(e)
            })
    
    return results

def process_audio_batch(audio_files):
    """Process a batch of audio files using sox/ffmpeg"""
    results = []
    
    for file_data in audio_files:
        input_file = file_data["input_file"]
        operations = file_data.get("operations", ["normalize"])
        output_path = file_data["output_path"]
        
        try:
            output_file = os.path.join(output_path, "processed_" + os.path.basename(input_file))
            
            # Build ffmpeg command based on operations
            cmd = ["ffmpeg", "-i", input_file]
            
            if "normalize" in operations:
                cmd.extend(["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"])
                
            if "compress" in operations:
                cmd.extend(["-codec:a", "libmp3lame", "-b:a", "192k"])
                
            if "trim" in operations:
                start = file_data.get("trim_start", 0)
                duration = file_data.get("trim_duration", 30)
                cmd.extend(["-ss", str(start), "-t", str(duration)])
            
            cmd.extend(["-y", output_file])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            results.append({
                "status": "success" if result.returncode == 0 else "failed",
                "file": input_file,
                "operations": operations,
                "output": output_file
            })
            
        except Exception as e:
            results.append({
                "status": "failed",
                "file": input_file,
                "error": str(e)
            })
    
    return results

def process_video_batch(video_files):
    """Process a batch of video files using ffmpeg"""
    results = []
    
    for file_data in video_files:
        input_file = file_data["input_file"]
        operations = file_data.get("operations", ["transcode"])
        output_path = file_data["output_path"]
        
        try:
            output_file = os.path.join(output_path, "processed_" + os.path.basename(input_file))
            
            # Build ffmpeg command
            cmd = ["ffmpeg", "-i", input_file]
            
            if "transcode" in operations:
                cmd.extend([
                    "-c:v", "libx264",
                    "-preset", "fast",
                    "-crf", "22",
                    "-c:a", "aac",
                    "-b:a", "192k"
                ])
                
            if "scale" in operations:
                target_res = file_data.get("target_resolution", "1920:1080")
                cmd.extend(["-vf", f"scale={target_res}"])
                
            if "watermark" in operations:
                watermark = file_data.get("watermark_path")
                if watermark:
                    cmd.extend(["-i", watermark, "-filter_complex", "overlay=10:10"])
            
            cmd.extend(["-y", output_file])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            results.append({
                "status": "success" if result.returncode == 0 else "failed",
                "file": input_file,
                "operations": operations,
                "output": output_file
            })
            
        except Exception as e:
            results.append({
                "status": "failed",
                "file": input_file,
                "error": str(e)
            })
    
    return results

def create_gimp_script(input_file, output_path, filters):
    """Create GIMP script for advanced image processing"""
    script = f"""
    (define (process-image filename)
      (let* ((image (car (gimp-file-load RUN-NONINTERACTIVE filename filename)))
             (drawable (car (gimp-image-get-active-layer image))))
        
        ; Apply filters
        {"; ".join([f"(plug-in-{f} RUN-NONINTERACTIVE image drawable)" for f in filters])}
        
        ; Save
        (gimp-file-save RUN-NONINTERACTIVE image drawable 
                        "{output_path}/gimp_{os.path.basename(input_file)}"
                        "{output_path}/gimp_{os.path.basename(input_file)}")
        (gimp-image-delete image)))
    
    (process-image "{input_file}")
    """
    return script

def main():
    spark = SparkSession.builder \\
        .appName("MultimediaDistributedProcessing") \\
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Load job configuration
    config = json.loads(sc.getConf().get("spark.job.config"))
    
    input_files = config.get("input_files", [])
    output_path = config["output_path"]
    
    # Group files by type
    image_files = []
    audio_files = []
    video_files = []
    
    for file_path in input_files:
        ext = os.path.splitext(file_path)[1].lower().lstrip('.')
        file_data = {
            "input_file": file_path,
            "output_path": output_path,
            "operations": config.get("operations", []),
            **config.get("options", {})
        }
        
        if ext in {config.get("image_formats", [])}:
            image_files.append(file_data)
        elif ext in {config.get("audio_formats", [])}:
            audio_files.append(file_data)
        elif ext in {config.get("video_formats", [])}:
            video_files.append(file_data)
    
    # Process in parallel by type
    results = []
    
    if image_files:
        image_rdd = sc.parallelize(image_files, len(image_files) // 10 + 1)
        image_results = image_rdd.mapPartitions(process_image_batch).collect()
        results.extend(image_results)
    
    if audio_files:
        audio_rdd = sc.parallelize(audio_files, len(audio_files) // 10 + 1)
        audio_results = audio_rdd.mapPartitions(process_audio_batch).collect()
        results.extend(audio_results)
    
    if video_files:
        video_rdd = sc.parallelize(video_files, len(video_files) // 5 + 1)
        video_results = video_rdd.mapPartitions(process_video_batch).collect()
        results.extend(video_results)
    
    # Save results
    results_path = os.path.join(output_path, "multimedia_processing_results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    
    spark.stop()

if __name__ == "__main__":
    main()
''' 