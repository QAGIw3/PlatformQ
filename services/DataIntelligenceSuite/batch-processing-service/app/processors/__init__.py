"""
File processors for batch processing
"""

from .base import BaseFileProcessor
from .cad.blender import BlenderProcessor
from .cad.freecad import FreeCADProcessor
from .media.multimedia import MultimediaProcessor
from .simulation.openfoam import OpenFOAMProcessor
from .simulation.flightgear import FlightGearProcessor

# Registry of available processors
PROCESSOR_REGISTRY = {
    "blender": BlenderProcessor,
    "freecad": FreeCADProcessor,
    "multimedia": MultimediaProcessor,
    "openfoam": OpenFOAMProcessor,
    "flightgear": FlightGearProcessor
}

# Map file extensions to processors
FILE_PROCESSOR_MAP = {
    # Blender
    "blend": BlenderProcessor,
    "blend1": BlenderProcessor,
    "blend2": BlenderProcessor,
    
    # FreeCAD and CAD formats
    "fcstd": FreeCADProcessor,
    "fcstd1": FreeCADProcessor,
    "step": FreeCADProcessor,
    "stp": FreeCADProcessor,
    "iges": FreeCADProcessor,
    "igs": FreeCADProcessor,
    "stl": FreeCADProcessor,
    "obj": FreeCADProcessor,
    "dxf": FreeCADProcessor,
    "dwg": FreeCADProcessor,
    
    # Multimedia - Images
    "jpg": MultimediaProcessor,
    "jpeg": MultimediaProcessor,
    "png": MultimediaProcessor,
    "gif": MultimediaProcessor,
    "bmp": MultimediaProcessor,
    "tiff": MultimediaProcessor,
    "svg": MultimediaProcessor,
    "webp": MultimediaProcessor,
    "xcf": MultimediaProcessor,
    
    # Multimedia - Audio
    "mp3": MultimediaProcessor,
    "wav": MultimediaProcessor,
    "flac": MultimediaProcessor,
    "ogg": MultimediaProcessor,
    "m4a": MultimediaProcessor,
    "aac": MultimediaProcessor,
    "wma": MultimediaProcessor,
    "aup": MultimediaProcessor,
    "aup3": MultimediaProcessor,
    
    # Multimedia - Video
    "mp4": MultimediaProcessor,
    "avi": MultimediaProcessor,
    "mov": MultimediaProcessor,
    "mkv": MultimediaProcessor,
    "webm": MultimediaProcessor,
    "flv": MultimediaProcessor,
    "wmv": MultimediaProcessor,
    "mpg": MultimediaProcessor,
    "mpeg": MultimediaProcessor,
    "osp": MultimediaProcessor,
    
    # OpenFOAM
    "foam": OpenFOAMProcessor,
    "openfoam": OpenFOAMProcessor,
    "msh": OpenFOAMProcessor,
    "cas": OpenFOAMProcessor,
    "dat": OpenFOAMProcessor,
    
    # FlightGear
    "fgfs": FlightGearProcessor,
    "fgt": FlightGearProcessor,
    "nas": FlightGearProcessor,
    "ac": FlightGearProcessor,
    "stg": FlightGearProcessor,
    "btg": FlightGearProcessor
}


def get_processor_for_file(file_path: str) -> BaseFileProcessor:
    """Get the appropriate processor for a file based on its extension"""
    import os
    
    ext = os.path.splitext(file_path)[1].lower().lstrip('.')
    processor_class = FILE_PROCESSOR_MAP.get(ext)
    
    if processor_class:
        return processor_class
    
    return None


__all__ = [
    "BaseFileProcessor",
    "BlenderProcessor",
    "FreeCADProcessor",
    "MultimediaProcessor",
    "OpenFOAMProcessor",
    "FlightGearProcessor",
    "PROCESSOR_REGISTRY",
    "FILE_PROCESSOR_MAP",
    "get_processor_for_file"
] 