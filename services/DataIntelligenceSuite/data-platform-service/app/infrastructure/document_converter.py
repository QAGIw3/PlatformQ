"""
Document Converter Infrastructure

Handles document format conversions using various backends
"""

import asyncio
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List
import shutil

from data_intelligence_common import StructuredLogger

from ..domain.models.storage import ConversionFormat

logger = StructuredLogger.get_logger(__name__)


class DocumentConverter:
    """Document converter using LibreOffice and other tools"""
    
    def __init__(self):
        self.libreoffice_path = self._find_libreoffice()
        self.pandoc_path = shutil.which("pandoc")
        self.imagemagick_path = shutil.which("convert")
        
    def _find_libreoffice(self) -> Optional[str]:
        """Find LibreOffice installation"""
        possible_paths = [
            "/usr/bin/libreoffice",
            "/usr/bin/soffice",
            "/opt/libreoffice/program/soffice",
            "/Applications/LibreOffice.app/Contents/MacOS/soffice",
            "C:\\Program Files\\LibreOffice\\program\\soffice.exe"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
                
        # Try to find using which
        return shutil.which("libreoffice") or shutil.which("soffice")
        
    async def convert(
        self,
        source_path: str,
        target_format: ConversionFormat,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Convert document to target format"""
        
        source_path = Path(source_path)
        source_format = source_path.suffix.lower().lstrip('.')
        
        # Check if conversion is needed
        if source_format == target_format.value:
            return str(source_path)
            
        # Determine conversion method
        if self._can_use_libreoffice(source_format, target_format.value):
            return await self._convert_with_libreoffice(
                source_path,
                target_format.value,
                options
            )
        elif self._can_use_pandoc(source_format, target_format.value):
            return await self._convert_with_pandoc(
                source_path,
                target_format.value,
                options
            )
        elif self._can_use_imagemagick(source_format, target_format.value):
            return await self._convert_with_imagemagick(
                source_path,
                target_format.value,
                options
            )
        else:
            raise ValueError(
                f"Conversion from {source_format} to {target_format.value} not supported"
            )
            
    def _can_use_libreoffice(self, source: str, target: str) -> bool:
        """Check if LibreOffice can handle conversion"""
        if not self.libreoffice_path:
            return False
            
        libreoffice_formats = {
            # Documents
            "doc", "docx", "odt", "rtf", "txt", "html", "pdf",
            # Spreadsheets
            "xls", "xlsx", "ods", "csv",
            # Presentations
            "ppt", "pptx", "odp"
        }
        
        return source in libreoffice_formats and target in libreoffice_formats
        
    def _can_use_pandoc(self, source: str, target: str) -> bool:
        """Check if Pandoc can handle conversion"""
        if not self.pandoc_path:
            return False
            
        pandoc_formats = {
            "md", "markdown", "html", "pdf", "docx", "odt",
            "rtf", "txt", "latex", "tex", "epub", "json"
        }
        
        return source in pandoc_formats and target in pandoc_formats
        
    def _can_use_imagemagick(self, source: str, target: str) -> bool:
        """Check if ImageMagick can handle conversion"""
        if not self.imagemagick_path:
            return False
            
        image_formats = {
            "jpg", "jpeg", "png", "gif", "webp", "svg",
            "tiff", "bmp", "ico", "pdf"
        }
        
        return source in image_formats and target in image_formats
        
    async def _convert_with_libreoffice(
        self,
        source_path: Path,
        target_format: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Convert using LibreOffice"""
        
        # Create temporary directory for output
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Build command
            cmd = [
                self.libreoffice_path,
                "--headless",
                "--convert-to", target_format,
                "--outdir", temp_dir,
                str(source_path)
            ]
            
            # Add options if provided
            if options:
                if options.get("pdf_export_options"):
                    cmd.extend(["--export-options", options["pdf_export_options"]])
                    
            # Run conversion
            logger.info("converting_with_libreoffice",
                       source=str(source_path),
                       target=target_format)
                       
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise RuntimeError(
                    f"LibreOffice conversion failed: {stderr.decode()}"
                )
                
            # Find output file
            output_files = list(Path(temp_dir).glob(f"*.{target_format}"))
            if not output_files:
                raise RuntimeError("No output file generated")
                
            # Move to final location
            output_path = source_path.with_suffix(f".{target_format}")
            shutil.move(str(output_files[0]), str(output_path))
            
            return str(output_path)
            
        finally:
            # Clean up temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            
    async def _convert_with_pandoc(
        self,
        source_path: Path,
        target_format: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Convert using Pandoc"""
        
        output_path = source_path.with_suffix(f".{target_format}")
        
        # Build command
        cmd = [
            self.pandoc_path,
            "-f", self._get_pandoc_format(source_path.suffix.lstrip('.')),
            "-t", self._get_pandoc_format(target_format),
            "-o", str(output_path),
            str(source_path)
        ]
        
        # Add options
        if options:
            if options.get("standalone"):
                cmd.append("--standalone")
            if options.get("toc"):
                cmd.append("--toc")
                
        # Run conversion
        logger.info("converting_with_pandoc",
                   source=str(source_path),
                   target=target_format)
                   
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise RuntimeError(f"Pandoc conversion failed: {stderr.decode()}")
            
        return str(output_path)
        
    async def _convert_with_imagemagick(
        self,
        source_path: Path,
        target_format: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Convert using ImageMagick"""
        
        output_path = source_path.with_suffix(f".{target_format}")
        
        # Build command
        cmd = [
            self.imagemagick_path,
            str(source_path)
        ]
        
        # Add options
        if options:
            if options.get("resize"):
                cmd.extend(["-resize", options["resize"]])
            if options.get("quality"):
                cmd.extend(["-quality", str(options["quality"])])
            if options.get("density"):
                cmd.extend(["-density", str(options["density"])])
                
        cmd.append(str(output_path))
        
        # Run conversion
        logger.info("converting_with_imagemagick",
                   source=str(source_path),
                   target=target_format)
                   
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise RuntimeError(f"ImageMagick conversion failed: {stderr.decode()}")
            
        return str(output_path)
        
    def _get_pandoc_format(self, format: str) -> str:
        """Map file extension to Pandoc format"""
        format_map = {
            "md": "markdown",
            "tex": "latex",
            "htm": "html",
            "yml": "yaml"
        }
        return format_map.get(format, format)
        
    async def generate_preview(
        self,
        source_path: str,
        preview_type: str = "thumbnail",
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate preview for document"""
        
        source_path = Path(source_path)
        
        if preview_type == "thumbnail":
            return await self._generate_thumbnail(source_path, options)
        elif preview_type == "text_extract":
            return await self._extract_text(source_path, options)
        elif preview_type == "first_page":
            return await self._extract_first_page(source_path, options)
        else:
            raise ValueError(f"Unknown preview type: {preview_type}")
            
    async def _generate_thumbnail(
        self,
        source_path: Path,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate thumbnail image"""
        
        # Default options
        width = options.get("width", 200) if options else 200
        height = options.get("height", 200) if options else 200
        quality = options.get("quality", 85) if options else 85
        
        output_path = source_path.with_suffix(".thumbnail.jpg")
        
        if source_path.suffix.lower() in ['.pdf', '.doc', '.docx', '.ppt', '.pptx']:
            # Convert to image first
            temp_image = await self._convert_with_imagemagick(
                source_path,
                "jpg",
                {"density": 150}
            )
            source_path = Path(temp_image)
            
        # Generate thumbnail
        cmd = [
            self.imagemagick_path,
            str(source_path) + "[0]",  # First page only
            "-thumbnail", f"{width}x{height}>",
            "-quality", str(quality),
            str(output_path)
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        await process.communicate()
        
        return str(output_path)
        
    async def _extract_text(
        self,
        source_path: Path,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Extract text from document"""
        
        # Convert to text format
        text_path = await self.convert(str(source_path), ConversionFormat.TXT)
        
        # Read and optionally truncate
        max_length = options.get("max_length", 5000) if options else 5000
        
        with open(text_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read(max_length)
            
        # Clean up if different from source
        if text_path != str(source_path):
            os.unlink(text_path)
            
        # Save preview
        preview_path = source_path.with_suffix(".preview.txt")
        with open(preview_path, 'w', encoding='utf-8') as f:
            f.write(text)
            
        return str(preview_path)
        
    async def _extract_first_page(
        self,
        source_path: Path,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Extract first page as image"""
        
        if source_path.suffix.lower() not in ['.pdf']:
            # Convert to PDF first
            pdf_path = await self.convert(str(source_path), ConversionFormat.PDF)
            source_path = Path(pdf_path)
            
        output_path = source_path.with_suffix(".page1.jpg")
        
        # Extract first page
        cmd = [
            self.imagemagick_path,
            "-density", "150",
            str(source_path) + "[0]",
            "-quality", "90",
            str(output_path)
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        await process.communicate()
        
        return str(output_path) 