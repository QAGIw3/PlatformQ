"""
Preview Generator for documents and media files.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import os
import tempfile
import uuid
from pathlib import Path
from PIL import Image
import PyPDF2
import fitz  # PyMuPDF

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class PreviewType(str, Enum):
    """Types of previews."""
    THUMBNAIL = "thumbnail"
    FIRST_PAGE = "first_page"
    TEXT_EXTRACT = "text_extract"
    METADATA = "metadata"
    FULL_TEXT = "full_text"
    PAGE_IMAGES = "page_images"


@dataclass
class PreviewOptions:
    """Options for preview generation."""
    width: int = 300
    height: int = 400
    quality: int = 85
    format: str = "jpg"
    page_number: int = 1
    max_text_length: int = 1000
    extract_images: bool = False
    extract_metadata: bool = True


@dataclass
class PreviewResult:
    """Result of preview generation."""
    preview_id: str
    preview_type: PreviewType
    source_identifier: str
    preview_path: Optional[str] = None
    preview_data: Optional[bytes] = None
    text_content: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    size: Optional[int] = None
    format: Optional[str] = None


class PreviewGenerator:
    """
    Generates previews for various file types.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        storage_manager: Any = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.storage_manager = storage_manager
        
        # Temporary directory for previews
        self.temp_dir = Path(tempfile.gettempdir()) / "platformq-previews"
        self.temp_dir.mkdir(exist_ok=True)
        
        # Supported file types
        self.supported_types = {
            # Documents
            ".pdf": ["thumbnail", "first_page", "text_extract", "metadata", "full_text"],
            ".docx": ["thumbnail", "first_page", "text_extract", "metadata"],
            ".doc": ["thumbnail", "first_page", "text_extract"],
            ".txt": ["text_extract", "metadata", "full_text"],
            ".md": ["text_extract", "metadata", "full_text"],
            
            # Images
            ".jpg": ["thumbnail", "metadata"],
            ".jpeg": ["thumbnail", "metadata"],
            ".png": ["thumbnail", "metadata"],
            ".gif": ["thumbnail", "metadata"],
            ".webp": ["thumbnail", "metadata"],
            ".svg": ["thumbnail", "metadata"],
            
            # Spreadsheets
            ".xlsx": ["thumbnail", "first_page", "text_extract", "metadata"],
            ".xls": ["thumbnail", "first_page", "text_extract"],
            ".csv": ["text_extract", "metadata"],
            
            # Presentations
            ".pptx": ["thumbnail", "first_page", "metadata"],
            ".ppt": ["thumbnail", "first_page"]
        }
        
        logger.info("Preview Generator initialized")
        
    async def initialize(self):
        """Initialize preview generator."""
        # Subscribe to events
        await self.event_bus.subscribe("preview.requested", self._handle_preview_request)
        
        logger.info("Preview Generator ready")
        
    async def cleanup(self):
        """Cleanup generator resources."""
        # Clean up temp directory
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        logger.info("Preview Generator cleaned up")
        
    async def generate_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: Optional[PreviewOptions] = None
    ) -> PreviewResult:
        """Generate a preview for a file."""
        options = options or PreviewOptions()
        
        # Check if file type is supported
        file_ext = Path(source_path).suffix.lower()
        if file_ext not in self.supported_types:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        if preview_type.value not in self.supported_types[file_ext]:
            raise ValueError(f"Preview type {preview_type} not supported for {file_ext}")
        
        # Generate preview ID
        preview_id = str(uuid.uuid4())
        
        # Route to appropriate generator
        if file_ext == ".pdf":
            result = await self._generate_pdf_preview(
                source_path, preview_type, options, preview_id
            )
        elif file_ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]:
            result = await self._generate_image_preview(
                source_path, preview_type, options, preview_id
            )
        elif file_ext in [".docx", ".doc"]:
            result = await self._generate_document_preview(
                source_path, preview_type, options, preview_id
            )
        elif file_ext in [".txt", ".md", ".csv"]:
            result = await self._generate_text_preview(
                source_path, preview_type, options, preview_id
            )
        else:
            # Use generic preview
            result = await self._generate_generic_preview(
                source_path, preview_type, options, preview_id
            )
        
        # Cache result
        await self.cache_manager.set(
            f"preview:{preview_id}",
            result.__dict__,
            ttl=3600  # 1 hour
        )
        
        # Publish event
        await self.event_bus.publish("preview.generated", {
            "preview_id": preview_id,
            "preview_type": preview_type.value,
            "source": source_path
        })
        
        return result
        
    async def generate_preview_async(
        self,
        source_identifier: str,
        preview_type: PreviewType,
        tenant_id: str,
        options: Optional[PreviewOptions] = None
    ) -> str:
        """Generate a preview asynchronously."""
        options = options or PreviewOptions()
        
        # Create task
        preview_id = str(uuid.uuid4())
        
        # Run in background
        asyncio.create_task(
            self._generate_preview_background(
                source_identifier,
                preview_type,
                tenant_id,
                options,
                preview_id
            )
        )
        
        return preview_id
        
    async def get_preview(self, preview_id: str) -> Optional[PreviewResult]:
        """Get a generated preview."""
        cached = await self.cache_manager.get(f"preview:{preview_id}")
        if cached:
            return PreviewResult(**cached)
        return None
        
    def can_generate_preview(
        self,
        file_path: str,
        preview_type: Optional[PreviewType] = None
    ) -> bool:
        """Check if preview can be generated for file."""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext not in self.supported_types:
            return False
        
        if preview_type:
            return preview_type.value in self.supported_types[file_ext]
        
        return True
        
    async def _generate_preview_background(
        self,
        source_identifier: str,
        preview_type: PreviewType,
        tenant_id: str,
        options: PreviewOptions,
        preview_id: str
    ):
        """Generate preview in background."""
        try:
            # Download source file if needed
            if self.storage_manager:
                source_obj = await self.storage_manager.download(
                    identifier=source_identifier,
                    tenant_id=tenant_id
                )
                
                # Save to temp file
                source_path = self.temp_dir / f"{preview_id}_source{Path(source_identifier).suffix}"
                with open(source_path, 'wb') as f:
                    f.write(source_obj.data)
            else:
                source_path = source_identifier
            
            # Generate preview
            result = await self.generate_preview(
                str(source_path),
                preview_type,
                options
            )
            
            # Update result
            result.preview_id = preview_id
            result.source_identifier = source_identifier
            
            # Upload preview if storage manager available
            if self.storage_manager and result.preview_path:
                with open(result.preview_path, 'rb') as f:
                    preview_identifier = await self.storage_manager.upload(
                        data=f.read(),
                        filename=Path(result.preview_path).name,
                        tenant_id=tenant_id
                    )
                
                result.metadata["preview_identifier"] = preview_identifier
            
            # Update cache
            await self.cache_manager.set(
                f"preview:{preview_id}",
                result.__dict__,
                ttl=3600
            )
            
            # Clean up temp file
            if isinstance(source_path, Path) and source_path.parent == self.temp_dir:
                source_path.unlink(missing_ok=True)
            
        except Exception as e:
            logger.error(f"Error generating preview: {e}")
            
    async def _generate_pdf_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: PreviewOptions,
        preview_id: str
    ) -> PreviewResult:
        """Generate preview for PDF files."""
        result = PreviewResult(
            preview_id=preview_id,
            preview_type=preview_type,
            source_identifier=source_path
        )
        
        # Open PDF
        pdf_document = fitz.open(source_path)
        
        try:
            if preview_type == PreviewType.THUMBNAIL:
                # Generate thumbnail from first page
                page = pdf_document[0]
                mat = fitz.Matrix(options.width / page.rect.width, options.height / page.rect.height)
                pix = page.get_pixmap(matrix=mat)
                
                # Save as image
                preview_path = self.temp_dir / f"{preview_id}_thumb.{options.format}"
                pix.save(str(preview_path))
                
                result.preview_path = str(preview_path)
                result.size = preview_path.stat().st_size
                result.format = options.format
                
            elif preview_type == PreviewType.FIRST_PAGE:
                # Render first page at full resolution
                page = pdf_document[options.page_number - 1]
                pix = page.get_pixmap(dpi=150)
                
                preview_path = self.temp_dir / f"{preview_id}_page.png"
                pix.save(str(preview_path))
                
                result.preview_path = str(preview_path)
                result.size = preview_path.stat().st_size
                result.format = "png"
                
            elif preview_type == PreviewType.TEXT_EXTRACT:
                # Extract text from first page
                page = pdf_document[0]
                text = page.get_text()
                
                if len(text) > options.max_text_length:
                    text = text[:options.max_text_length] + "..."
                
                result.text_content = text
                
            elif preview_type == PreviewType.METADATA:
                # Extract metadata
                metadata = pdf_document.metadata
                result.metadata = {
                    "title": metadata.get("title", ""),
                    "author": metadata.get("author", ""),
                    "subject": metadata.get("subject", ""),
                    "keywords": metadata.get("keywords", ""),
                    "creator": metadata.get("creator", ""),
                    "producer": metadata.get("producer", ""),
                    "created": metadata.get("creationDate", ""),
                    "modified": metadata.get("modDate", ""),
                    "pages": pdf_document.page_count,
                    "encrypted": pdf_document.is_encrypted
                }
                
            elif preview_type == PreviewType.FULL_TEXT:
                # Extract all text
                text_parts = []
                for page_num in range(pdf_document.page_count):
                    page = pdf_document[page_num]
                    text_parts.append(page.get_text())
                
                result.text_content = "\n\n".join(text_parts)
                
        finally:
            pdf_document.close()
        
        return result
        
    async def _generate_image_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: PreviewOptions,
        preview_id: str
    ) -> PreviewResult:
        """Generate preview for image files."""
        result = PreviewResult(
            preview_id=preview_id,
            preview_type=preview_type,
            source_identifier=source_path
        )
        
        if preview_type == PreviewType.THUMBNAIL:
            # Open image
            with Image.open(source_path) as img:
                # Convert to RGB if necessary
                if img.mode in ('RGBA', 'LA', 'P'):
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    rgb_img.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = rgb_img
                
                # Generate thumbnail
                img.thumbnail((options.width, options.height), Image.Resampling.LANCZOS)
                
                # Save thumbnail
                preview_path = self.temp_dir / f"{preview_id}_thumb.{options.format}"
                img.save(str(preview_path), quality=options.quality)
                
                result.preview_path = str(preview_path)
                result.size = preview_path.stat().st_size
                result.format = options.format
                
        elif preview_type == PreviewType.METADATA:
            # Extract image metadata
            with Image.open(source_path) as img:
                result.metadata = {
                    "format": img.format,
                    "mode": img.mode,
                    "size": img.size,
                    "width": img.width,
                    "height": img.height,
                    "info": dict(img.info)
                }
                
                # EXIF data for JPEG
                if hasattr(img, '_getexif') and img._getexif():
                    from PIL.ExifTags import TAGS
                    exif = {}
                    for tag, value in img._getexif().items():
                        decoded = TAGS.get(tag, tag)
                        exif[decoded] = value
                    result.metadata["exif"] = exif
        
        return result
        
    async def _generate_document_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: PreviewOptions,
        preview_id: str
    ) -> PreviewResult:
        """Generate preview for document files."""
        result = PreviewResult(
            preview_id=preview_id,
            preview_type=preview_type,
            source_identifier=source_path
        )
        
        if preview_type in [PreviewType.THUMBNAIL, PreviewType.FIRST_PAGE]:
            # Convert to PDF first, then generate image
            from ..document_converter import DocumentConverter, ConversionFormat
            
            converter = DocumentConverter(self.event_bus, self.cache_manager)
            pdf_path = await converter.convert(
                source_path,
                ConversionFormat.PDF
            )
            
            # Generate preview from PDF
            pdf_result = await self._generate_pdf_preview(
                pdf_path,
                preview_type,
                options,
                preview_id
            )
            
            # Clean up temp PDF
            Path(pdf_path).unlink(missing_ok=True)
            
            return pdf_result
            
        elif preview_type == PreviewType.TEXT_EXTRACT:
            # Extract text from document
            import docx2txt
            
            if source_path.endswith('.docx'):
                text = docx2txt.process(source_path)
                
                if len(text) > options.max_text_length:
                    text = text[:options.max_text_length] + "..."
                
                result.text_content = text
            else:
                # Use generic text extraction
                result = await self._generate_text_preview(
                    source_path, preview_type, options, preview_id
                )
                
        elif preview_type == PreviewType.METADATA:
            # Extract document metadata
            if source_path.endswith('.docx'):
                import zipfile
                import xml.etree.ElementTree as ET
                
                metadata = {}
                
                with zipfile.ZipFile(source_path, 'r') as docx:
                    # Read core properties
                    if 'docProps/core.xml' in docx.namelist():
                        core_xml = docx.read('docProps/core.xml')
                        root = ET.fromstring(core_xml)
                        
                        ns = {
                            'cp': 'http://schemas.openxmlformats.org/package/2006/metadata/core-properties',
                            'dc': 'http://purl.org/dc/elements/1.1/',
                            'dcterms': 'http://purl.org/dc/terms/'
                        }
                        
                        metadata['title'] = root.findtext('.//dc:title', '', ns)
                        metadata['creator'] = root.findtext('.//dc:creator', '', ns)
                        metadata['subject'] = root.findtext('.//dc:subject', '', ns)
                        metadata['description'] = root.findtext('.//dc:description', '', ns)
                        metadata['created'] = root.findtext('.//dcterms:created', '', ns)
                        metadata['modified'] = root.findtext('.//dcterms:modified', '', ns)
                
                result.metadata = metadata
        
        return result
        
    async def _generate_text_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: PreviewOptions,
        preview_id: str
    ) -> PreviewResult:
        """Generate preview for text files."""
        result = PreviewResult(
            preview_id=preview_id,
            preview_type=preview_type,
            source_identifier=source_path
        )
        
        if preview_type in [PreviewType.TEXT_EXTRACT, PreviewType.FULL_TEXT]:
            # Read text content
            with open(source_path, 'r', encoding='utf-8', errors='ignore') as f:
                if preview_type == PreviewType.TEXT_EXTRACT:
                    text = f.read(options.max_text_length)
                    if len(text) == options.max_text_length:
                        text += "..."
                else:
                    text = f.read()
                
                result.text_content = text
                
        elif preview_type == PreviewType.METADATA:
            # Get file metadata
            stat = os.stat(source_path)
            result.metadata = {
                "size": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "encoding": self._detect_encoding(source_path)
            }
            
            # Line count for text files
            with open(source_path, 'r', encoding='utf-8', errors='ignore') as f:
                line_count = sum(1 for _ in f)
                result.metadata["lines"] = line_count
        
        return result
        
    async def _generate_generic_preview(
        self,
        source_path: str,
        preview_type: PreviewType,
        options: PreviewOptions,
        preview_id: str
    ) -> PreviewResult:
        """Generate generic preview using system tools."""
        result = PreviewResult(
            preview_id=preview_id,
            preview_type=preview_type,
            source_identifier=source_path
        )
        
        # For now, just extract basic metadata
        if preview_type == PreviewType.METADATA:
            stat = os.stat(source_path)
            result.metadata = {
                "size": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "extension": Path(source_path).suffix
            }
        
        return result
        
    def _detect_encoding(self, file_path: str) -> str:
        """Detect text file encoding."""
        import chardet
        
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
            
    async def _handle_preview_request(self, event_data: Dict[str, Any]):
        """Handle preview request event."""
        try:
            preview_id = await self.generate_preview_async(
                source_identifier=event_data.get("source_identifier"),
                preview_type=PreviewType(event_data.get("preview_type")),
                tenant_id=event_data.get("tenant_id"),
                options=PreviewOptions(**event_data.get("options", {}))
            )
            
            # Publish response
            await self.event_bus.publish("preview.request.accepted", {
                "request_id": event_data.get("request_id"),
                "preview_id": preview_id
            })
            
        except Exception as e:
            logger.error(f"Error handling preview request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get generator statistics."""
        return {
            "supported_file_types": len(self.supported_types),
            "preview_types": len(PreviewType),
            "temp_dir": str(self.temp_dir)
        } 