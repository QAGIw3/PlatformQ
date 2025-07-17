"""Media processing utility functions."""
import os
import subprocess
import logging
import json
import mimetypes
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
from PIL import Image
import cv2
import moviepy.editor as mp
from pydub import AudioSegment
import librosa
import soundfile as sf

logger = logging.getLogger(__name__)


def detect_media_type(file_path: Path) -> str:
    """Detect media type from file."""
    # Try mimetype detection
    mime_type, _ = mimetypes.guess_type(str(file_path))
    
    if mime_type:
        if mime_type.startswith('image/'):
            return 'image'
        elif mime_type.startswith('video/'):
            return 'video'
        elif mime_type.startswith('audio/'):
            return 'audio'
    
    # Fallback to file extension
    ext = file_path.suffix.lower()
    
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.svg'}
    video_exts = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.mpg', '.mpeg'}
    audio_exts = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus'}
    
    if ext in image_exts:
        return 'image'
    elif ext in video_exts:
        return 'video'
    elif ext in audio_exts:
        return 'audio'
    
    # Use file command as last resort
    try:
        result = subprocess.run(
            ['file', '--mime-type', '-b', str(file_path)],
            capture_output=True,
            text=True
        )
        mime_type = result.stdout.strip()
        
        if 'image' in mime_type:
            return 'image'
        elif 'video' in mime_type:
            return 'video'
        elif 'audio' in mime_type:
            return 'audio'
    except:
        pass
    
    return 'unknown'


def split_video_segments(video_path: Path, output_dir: Path, 
                        segment_duration: int = 60) -> List[Path]:
    """Split video into segments for distributed processing."""
    output_dir.mkdir(parents=True, exist_ok=True)
    segments = []
    
    # Get video duration
    cmd = [
        'ffprobe', '-v', 'error', '-show_entries',
        'format=duration', '-of', 'json', str(video_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    info = json.loads(result.stdout)
    duration = float(info['format']['duration'])
    
    # Calculate number of segments
    num_segments = int(np.ceil(duration / segment_duration))
    
    # Split video
    for i in range(num_segments):
        start_time = i * segment_duration
        output_file = output_dir / f"segment_{i:04d}.mp4"
        
        cmd = [
            'ffmpeg', '-i', str(video_path),
            '-ss', str(start_time),
            '-t', str(segment_duration),
            '-c', 'copy',
            '-y', str(output_file)
        ]
        
        subprocess.run(cmd, check=True)
        segments.append(output_file)
    
    return segments


def split_audio_segments(audio_path: Path, output_dir: Path,
                        segment_duration: int = 60) -> List[Path]:
    """Split audio into segments for distributed processing."""
    output_dir.mkdir(parents=True, exist_ok=True)
    segments = []
    
    # Load audio
    audio = AudioSegment.from_file(str(audio_path))
    duration_ms = len(audio)
    segment_duration_ms = segment_duration * 1000
    
    # Split audio
    num_segments = int(np.ceil(duration_ms / segment_duration_ms))
    
    for i in range(num_segments):
        start_ms = i * segment_duration_ms
        end_ms = min((i + 1) * segment_duration_ms, duration_ms)
        
        segment = audio[start_ms:end_ms]
        output_file = output_dir / f"segment_{i:04d}.wav"
        
        segment.export(str(output_file), format="wav")
        segments.append(output_file)
    
    return segments


def split_image_regions(image_path: Path, output_dir: Path,
                       grid_size: Tuple[int, int] = (2, 2)) -> List[Dict[str, Any]]:
    """Split image into regions for distributed processing."""
    output_dir.mkdir(parents=True, exist_ok=True)
    regions = []
    
    # Load image
    img = Image.open(image_path)
    width, height = img.size
    
    # Calculate region dimensions
    region_width = width // grid_size[0]
    region_height = height // grid_size[1]
    
    # Split image
    for row in range(grid_size[1]):
        for col in range(grid_size[0]):
            x = col * region_width
            y = row * region_height
            
            # Handle edge cases for last row/column
            x2 = width if col == grid_size[0] - 1 else (col + 1) * region_width
            y2 = height if row == grid_size[1] - 1 else (row + 1) * region_height
            
            # Extract region
            region = img.crop((x, y, x2, y2))
            
            # Save region
            output_file = output_dir / f"region_{row}_{col}.png"
            region.save(output_file)
            
            regions.append({
                "file": output_file,
                "position": (x, y),
                "size": (x2 - x, y2 - y),
                "row": row,
                "col": col
            })
    
    return regions


def process_image_batch(image_files: List[Path], output_dir: Path,
                       parameters: Dict[str, Any]) -> List[Path]:
    """Process a batch of images."""
    processed = []
    
    for img_file in image_files:
        # Apply processing based on parameters
        img = Image.open(img_file)
        
        # Example: resize
        if "resize" in parameters:
            size = parameters["resize"]
            img = img.resize(size, Image.Resampling.LANCZOS)
        
        # Example: convert format
        if "format" in parameters:
            output_format = parameters["format"]
            output_file = output_dir / f"{img_file.stem}.{output_format}"
        else:
            output_file = output_dir / img_file.name
        
        img.save(output_file)
        processed.append(output_file)
    
    return processed


def process_video_segment(segment_path: Path, output_dir: Path,
                         parameters: Dict[str, Any]) -> Path:
    """Process a single video segment."""
    output_file = output_dir / f"processed_{segment_path.name}"
    
    # Build ffmpeg command based on parameters
    cmd = ['ffmpeg', '-i', str(segment_path)]
    
    # Video filters
    filters = []
    
    if "scale" in parameters:
        filters.append(f"scale={parameters['scale']}")
    
    if "fps" in parameters:
        filters.append(f"fps={parameters['fps']}")
    
    if filters:
        cmd.extend(['-vf', ','.join(filters)])
    
    # Codec settings
    if "video_codec" in parameters:
        cmd.extend(['-c:v', parameters['video_codec']])
    
    if "audio_codec" in parameters:
        cmd.extend(['-c:a', parameters['audio_codec']])
    
    cmd.extend(['-y', str(output_file)])
    
    subprocess.run(cmd, check=True)
    
    return output_file


def process_audio_segment(segment_path: Path, output_dir: Path,
                         parameters: Dict[str, Any]) -> Path:
    """Process a single audio segment."""
    output_file = output_dir / f"processed_{segment_path.name}"
    
    # Load audio
    y, sr = librosa.load(str(segment_path), sr=None)
    
    # Apply processing
    if "pitch_shift" in parameters:
        n_steps = parameters["pitch_shift"]
        y = librosa.effects.pitch_shift(y, sr=sr, n_steps=n_steps)
    
    if "time_stretch" in parameters:
        rate = parameters["time_stretch"]
        y = librosa.effects.time_stretch(y, rate=rate)
    
    # Save processed audio
    sf.write(str(output_file), y, sr)
    
    return output_file


def merge_video_segments(segments: List[Path], output_file: Path) -> Path:
    """Merge video segments back together."""
    # Create concat file
    concat_file = output_file.parent / "concat.txt"
    
    with open(concat_file, 'w') as f:
        for segment in sorted(segments):
            f.write(f"file '{segment.absolute()}'\n")
    
    # Merge using ffmpeg
    cmd = [
        'ffmpeg', '-f', 'concat', '-safe', '0',
        '-i', str(concat_file),
        '-c', 'copy',
        '-y', str(output_file)
    ]
    
    subprocess.run(cmd, check=True)
    
    # Clean up
    concat_file.unlink()
    
    return output_file


def merge_audio_segments(segments: List[Path], output_file: Path) -> Path:
    """Merge audio segments back together."""
    # Load and concatenate all segments
    combined = AudioSegment.empty()
    
    for segment in sorted(segments):
        audio = AudioSegment.from_file(str(segment))
        combined += audio
    
    # Export combined audio
    combined.export(str(output_file), format=output_file.suffix[1:])
    
    return output_file


def merge_image_tiles(tiles: List[Dict[str, Any]], output_file: Path,
                     original_size: Tuple[int, int]) -> Path:
    """Merge image tiles back into single image."""
    # Create blank canvas
    result = Image.new('RGBA', original_size)
    
    # Paste each tile
    for tile_info in tiles:
        tile_img = Image.open(tile_info["file"])
        position = tile_info["position"]
        result.paste(tile_img, position)
    
    # Save merged image
    result.save(output_file)
    
    return output_file


def apply_video_effects(video_path: Path, output_path: Path,
                       effects: List[Dict[str, Any]]) -> Path:
    """Apply video effects using moviepy."""
    # Load video
    video = mp.VideoFileClip(str(video_path))
    
    # Apply effects
    for effect in effects:
        effect_type = effect["type"]
        
        if effect_type == "fadeIn":
            duration = effect.get("duration", 1.0)
            video = video.fx(mp.vfx.fadein, duration)
            
        elif effect_type == "fadeOut":
            duration = effect.get("duration", 1.0)
            video = video.fx(mp.vfx.fadeout, duration)
            
        elif effect_type == "speedx":
            factor = effect.get("factor", 2.0)
            video = video.fx(mp.vfx.speedx, factor)
            
        elif effect_type == "colorx":
            factor = effect.get("factor", 1.5)
            video = video.fx(mp.vfx.colorx, factor)
    
    # Write output
    video.write_videofile(str(output_path))
    video.close()
    
    return output_path


def analyze_media_properties(file_path: Path) -> Dict[str, Any]:
    """Analyze media file properties."""
    media_type = detect_media_type(file_path)
    properties = {"type": media_type, "file": str(file_path)}
    
    if media_type == "image":
        img = Image.open(file_path)
        properties.update({
            "width": img.width,
            "height": img.height,
            "format": img.format,
            "mode": img.mode,
            "has_transparency": img.mode in ('RGBA', 'LA')
        })
        
    elif media_type == "video":
        # Use ffprobe to get video properties
        cmd = [
            'ffprobe', '-v', 'error', '-show_entries',
            'format=duration,bit_rate:stream=width,height,r_frame_rate,codec_name',
            '-of', 'json', str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        info = json.loads(result.stdout)
        
        properties.update({
            "duration": float(info['format'].get('duration', 0)),
            "bit_rate": int(info['format'].get('bit_rate', 0)),
            "streams": info.get('streams', [])
        })
        
    elif media_type == "audio":
        # Get audio properties
        y, sr = librosa.load(str(file_path), sr=None)
        duration = len(y) / sr
        
        properties.update({
            "duration": duration,
            "sample_rate": sr,
            "channels": 1 if len(y.shape) == 1 else y.shape[0],
            "samples": len(y)
        })
    
    return properties 