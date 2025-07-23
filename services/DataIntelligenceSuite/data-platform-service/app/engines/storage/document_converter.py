"""
Document Converter for various file formats.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import os
import tempfile
import uuid
from pathlib import Path
import subprocess

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ConversionFormat(str, Enum):
    """Supported conversion formats."""
    # Documents
    PDF = "pdf"
    DOCX = "docx"
    DOC = "doc"
    ODT = "odt"
    RTF = "rtf"
    TXT = "txt"
    HTML = "html"
    EPUB = "epub"
    MARKDOWN = "md"
    
    # Spreadsheets
    XLSX = "xlsx"
    XLS = "xls"
    ODS = "ods"
    CSV = "csv"
    TSV = "tsv"
    
    # Presentations
    PPTX = "pptx"
    PPT = "ppt"
    ODP = "odp"
    
    # Images
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    WEBP = "webp"
    SVG = "svg"
    TIFF = "tiff"
    BMP = "bmp"
    
    # Data formats
    JSON = "json"
    XML = "xml"
    YAML = "yaml"


class ConversionStatus(str, Enum):
    """Conversion job status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ConversionOptions:
    """Options for document conversion."""
    quality: str = "high"  # low, medium, high
    dpi: int = 300
    page_range: Optional[str] = None  # e.g., "1-5,8,10-"
    grayscale: bool = False
    compress: bool = True
    password: Optional[str] = None  # For encrypted documents
    metadata: Dict[str, Any] = field(default_factory=dict)
    custom_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConversionJob:
    """Represents a conversion job."""
    job_id: str
    source_path: str
    source_format: ConversionFormat
    target_format: ConversionFormat
    target_path: Optional[str] = None
    status: ConversionStatus = ConversionStatus.PENDING
    options: ConversionOptions = field(default_factory=ConversionOptions)
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    progress: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DocumentConverter:
    """
    Converts documents between various formats.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        storage_manager: Any = None  # Avoid circular import
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.storage_manager = storage_manager
        
        # Conversion mappings
        self.conversion_map = self._build_conversion_map()
        
        # Job queue and workers
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.active_jobs: Dict[str, ConversionJob] = {}
        
        # Worker configuration
        self.num_workers = 4
        self.workers: List[asyncio.Task] = []
        
        # Temporary directory for conversions
        self.temp_dir = Path(tempfile.gettempdir()) / "platformq-conversions"
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info("Document Converter initialized")
        
    async def initialize(self):
        """Initialize document converter."""
        # Subscribe to events
        await self.event_bus.subscribe("conversion.requested", self._handle_conversion_request)
        
        # Start workers
        for i in range(self.num_workers):
            worker = asyncio.create_task(self._conversion_worker(i))
            self.workers.append(worker)
        
        logger.info(f"Started {self.num_workers} conversion workers")
        
    async def cleanup(self):
        """Cleanup converter resources."""
        # Stop workers
        for worker in self.workers:
            worker.cancel()
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        # Clean up temp directory
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        logger.info("Document Converter cleaned up")
        
    async def convert(
        self,
        source_path: str,
        target_format: ConversionFormat,
        options: Optional[ConversionOptions] = None
    ) -> str:
        """Convert a document synchronously."""
        options = options or ConversionOptions()
        
        # Detect source format
        source_format = self._detect_format(source_path)
        
        # Create job
        job = ConversionJob(
            job_id=str(uuid.uuid4()),
            source_path=source_path,
            source_format=source_format,
            target_format=target_format,
            options=options
        )
        
        # Perform conversion
        try:
            job.status = ConversionStatus.PROCESSING
            job.started_at = datetime.utcnow()
            
            target_path = await self._perform_conversion(job)
            
            job.target_path = target_path
            job.status = ConversionStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.progress = 100.0
            
            return target_path
            
        except Exception as e:
            job.status = ConversionStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            logger.error(f"Conversion failed: {e}")
            raise
            
    async def convert_async(
        self,
        source_identifier: str,
        target_format: ConversionFormat,
        tenant_id: str,
        options: Optional[ConversionOptions] = None
    ) -> str:
        """Convert a document asynchronously."""
        options = options or ConversionOptions()
        
        # Create job
        job_id = str(uuid.uuid4())
        
        # Download source file
        if self.storage_manager:
            source_obj = await self.storage_manager.download(
                identifier=source_identifier,
                tenant_id=tenant_id
            )
            
            # Save to temp file
            source_path = self.temp_dir / f"{job_id}_source{Path(source_identifier).suffix}"
            with open(source_path, 'wb') as f:
                f.write(source_obj.data)
        else:
            source_path = source_identifier
        
        # Detect source format
        source_format = self._detect_format(str(source_path))
        
        # Create job
        job = ConversionJob(
            job_id=job_id,
            source_path=str(source_path),
            source_format=source_format,
            target_format=target_format,
            options=options,
            metadata={
                "source_identifier": source_identifier,
                "tenant_id": tenant_id
            }
        )
        
        # Add to queue
        await self.job_queue.put(job)
        self.active_jobs[job_id] = job
        
        # Cache job status
        await self.cache_manager.set(
            f"conversion:job:{job_id}",
            job.__dict__,
            ttl=86400  # 24 hours
        )
        
        # Publish event
        await self.event_bus.publish("conversion.queued", {
            "job_id": job_id,
            "source_format": source_format.value,
            "target_format": target_format.value
        })
        
        logger.info(f"Queued conversion job: {job_id}")
        
        return job_id
        
    async def get_job_status(self, job_id: str) -> Optional[ConversionJob]:
        """Get status of a conversion job."""
        # Check active jobs
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        
        # Check cache
        cached = await self.cache_manager.get(f"conversion:job:{job_id}")
        if cached:
            return ConversionJob(**cached)
        
        return None
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a conversion job."""
        job = self.active_jobs.get(job_id)
        
        if job and job.status in [ConversionStatus.PENDING, ConversionStatus.PROCESSING]:
            job.status = ConversionStatus.CANCELLED
            job.completed_at = datetime.utcnow()
            
            # Update cache
            await self.cache_manager.set(
                f"conversion:job:{job_id}",
                job.__dict__,
                ttl=86400
            )
            
            # Publish event
            await self.event_bus.publish("conversion.cancelled", {
                "job_id": job_id
            })
            
            logger.info(f"Cancelled conversion job: {job_id}")
            return True
        
        return False
        
    def get_supported_conversions(
        self,
        source_format: ConversionFormat
    ) -> List[ConversionFormat]:
        """Get supported target formats for a source format."""
        return self.conversion_map.get(source_format, [])
        
    def is_conversion_supported(
        self,
        source_format: ConversionFormat,
        target_format: ConversionFormat
    ) -> bool:
        """Check if a conversion is supported."""
        return target_format in self.get_supported_conversions(source_format)
        
    async def _conversion_worker(self, worker_id: int):
        """Worker to process conversion jobs."""
        logger.info(f"Conversion worker {worker_id} started")
        
        while True:
            try:
                # Get job from queue
                job = await self.job_queue.get()
                
                if job.status == ConversionStatus.CANCELLED:
                    continue
                
                logger.info(f"Worker {worker_id} processing job {job.job_id}")
                
                # Update job status
                job.status = ConversionStatus.PROCESSING
                job.started_at = datetime.utcnow()
                await self._update_job_status(job)
                
                try:
                    # Perform conversion
                    target_path = await self._perform_conversion(job)
                    
                    # Upload result if storage manager available
                    if self.storage_manager and "tenant_id" in job.metadata:
                        with open(target_path, 'rb') as f:
                            target_identifier = await self.storage_manager.upload(
                                data=f.read(),
                                filename=Path(target_path).name,
                                tenant_id=job.metadata["tenant_id"]
                            )
                        
                        job.metadata["target_identifier"] = target_identifier
                    
                    # Update job status
                    job.target_path = target_path
                    job.status = ConversionStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                    job.progress = 100.0
                    
                    # Publish success event
                    await self.event_bus.publish("conversion.completed", {
                        "job_id": job.job_id,
                        "target_path": target_path,
                        "target_identifier": job.metadata.get("target_identifier")
                    })
                    
                except Exception as e:
                    logger.error(f"Conversion failed for job {job.job_id}: {e}")
                    
                    # Update job status
                    job.status = ConversionStatus.FAILED
                    job.error_message = str(e)
                    job.completed_at = datetime.utcnow()
                    
                    # Publish failure event
                    await self.event_bus.publish("conversion.failed", {
                        "job_id": job.job_id,
                        "error": str(e)
                    })
                
                # Update final status
                await self._update_job_status(job)
                
                # Clean up active jobs
                if job.job_id in self.active_jobs:
                    del self.active_jobs[job.job_id]
                
                # Clean up temp files
                await self._cleanup_job_files(job)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"Conversion worker {worker_id} stopped")
        
    async def _perform_conversion(self, job: ConversionJob) -> str:
        """Perform the actual conversion."""
        source_path = Path(job.source_path)
        
        # Generate target path
        target_filename = f"{source_path.stem}.{job.target_format.value}"
        target_path = self.temp_dir / f"{job.job_id}_{target_filename}"
        
        # Check if conversion is supported
        if not self.is_conversion_supported(job.source_format, job.target_format):
            raise ValueError(
                f"Conversion from {job.source_format} to {job.target_format} not supported"
            )
        
        # Route to appropriate converter
        if job.source_format in [ConversionFormat.PDF, ConversionFormat.DOCX, ConversionFormat.DOC] and \
           job.target_format in [ConversionFormat.PDF, ConversionFormat.DOCX, ConversionFormat.HTML]:
            await self._convert_document(job, source_path, target_path)
            
        elif job.source_format in [ConversionFormat.XLSX, ConversionFormat.XLS, ConversionFormat.CSV] and \
             job.target_format in [ConversionFormat.XLSX, ConversionFormat.CSV, ConversionFormat.JSON]:
            await self._convert_spreadsheet(job, source_path, target_path)
            
        elif job.source_format in [ConversionFormat.PNG, ConversionFormat.JPG, ConversionFormat.JPEG] and \
             job.target_format in [ConversionFormat.PNG, ConversionFormat.JPG, ConversionFormat.WEBP]:
            await self._convert_image(job, source_path, target_path)
            
        else:
            # Use generic converter
            await self._convert_generic(job, source_path, target_path)
        
        if not target_path.exists():
            raise RuntimeError(f"Conversion failed - output file not created")
        
        return str(target_path)
        
    async def _convert_document(
        self,
        job: ConversionJob,
        source_path: Path,
        target_path: Path
    ):
        """Convert document formats using LibreOffice."""
        # Build command
        cmd = [
            "libreoffice",
            "--headless",
            "--convert-to", job.target_format.value,
            "--outdir", str(target_path.parent),
            str(source_path)
        ]
        
        # Add quality options
        if job.options.quality == "high":
            cmd.extend(["--infilter", "writer_pdf_import:writer_pdf_import"])
        
        # Execute conversion
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Document conversion failed: {error_msg}")
        
        # Rename output file to match expected name
        output_files = list(target_path.parent.glob(f"{source_path.stem}.*"))
        if output_files:
            output_files[0].rename(target_path)
            
    async def _convert_spreadsheet(
        self,
        job: ConversionJob,
        source_path: Path,
        target_path: Path
    ):
        """Convert spreadsheet formats."""
        if job.target_format == ConversionFormat.CSV:
            # Use pandas for CSV conversion
            import pandas as pd
            
            if job.source_format == ConversionFormat.XLSX:
                df = pd.read_excel(source_path)
            elif job.source_format == ConversionFormat.CSV:
                df = pd.read_csv(source_path)
            else:
                # Use LibreOffice
                await self._convert_document(job, source_path, target_path)
                return
            
            df.to_csv(target_path, index=False)
            
        elif job.target_format == ConversionFormat.JSON:
            # Convert to JSON
            import pandas as pd
            
            if job.source_format == ConversionFormat.XLSX:
                df = pd.read_excel(source_path)
            elif job.source_format == ConversionFormat.CSV:
                df = pd.read_csv(source_path)
            else:
                raise ValueError(f"Cannot convert {job.source_format} to JSON")
            
            df.to_json(target_path, orient='records', indent=2)
            
        else:
            # Use LibreOffice for other conversions
            await self._convert_document(job, source_path, target_path)
            
    async def _convert_image(
        self,
        job: ConversionJob,
        source_path: Path,
        target_path: Path
    ):
        """Convert image formats using ImageMagick."""
        # Build command
        cmd = [
            "convert",
            str(source_path)
        ]
        
        # Add options
        if job.options.quality:
            quality_map = {"low": 60, "medium": 80, "high": 95}
            cmd.extend(["-quality", str(quality_map.get(job.options.quality, 80))])
        
        if job.options.dpi:
            cmd.extend(["-density", str(job.options.dpi)])
        
        if job.options.grayscale:
            cmd.append("-grayscale")
        
        if job.options.compress:
            cmd.extend(["-compress", "JPEG"])
        
        # Add target
        cmd.append(str(target_path))
        
        # Execute conversion
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Image conversion failed: {error_msg}")
            
    async def _convert_generic(
        self,
        job: ConversionJob,
        source_path: Path,
        target_path: Path
    ):
        """Generic conversion using pandoc."""
        # Build command
        cmd = [
            "pandoc",
            "-f", job.source_format.value,
            "-t", job.target_format.value,
            "-o", str(target_path),
            str(source_path)
        ]
        
        # Execute conversion
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Generic conversion failed: {error_msg}")
            
    async def _update_job_status(self, job: ConversionJob):
        """Update job status in cache."""
        await self.cache_manager.set(
            f"conversion:job:{job.job_id}",
            job.__dict__,
            ttl=86400
        )
        
        # Publish status update
        await self.event_bus.publish("conversion.status.updated", {
            "job_id": job.job_id,
            "status": job.status.value,
            "progress": job.progress
        })
        
    async def _cleanup_job_files(self, job: ConversionJob):
        """Clean up temporary files for a job."""
        try:
            # Remove source file if it's in temp dir
            source_path = Path(job.source_path)
            if source_path.parent == self.temp_dir:
                source_path.unlink(missing_ok=True)
            
            # Remove target file after some time
            if job.target_path and job.status == ConversionStatus.COMPLETED:
                # Schedule cleanup after 1 hour
                asyncio.create_task(self._delayed_cleanup(job.target_path, 3600))
                
        except Exception as e:
            logger.error(f"Error cleaning up job files: {e}")
            
    async def _delayed_cleanup(self, file_path: str, delay: int):
        """Clean up a file after a delay."""
        await asyncio.sleep(delay)
        try:
            Path(file_path).unlink(missing_ok=True)
        except Exception:
            pass
            
    def _detect_format(self, file_path: str) -> ConversionFormat:
        """Detect file format from extension."""
        extension = Path(file_path).suffix.lower().lstrip('.')
        
        # Map extensions to formats
        for format_enum in ConversionFormat:
            if format_enum.value == extension:
                return format_enum
        
        # Special cases
        if extension == "jpeg":
            return ConversionFormat.JPG
        
        raise ValueError(f"Unknown file format: {extension}")
        
    def _build_conversion_map(self) -> Dict[ConversionFormat, List[ConversionFormat]]:
        """Build mapping of supported conversions."""
        return {
            # Document conversions
            ConversionFormat.PDF: [
                ConversionFormat.DOCX, ConversionFormat.HTML, ConversionFormat.TXT,
                ConversionFormat.PNG, ConversionFormat.JPG
            ],
            ConversionFormat.DOCX: [
                ConversionFormat.PDF, ConversionFormat.HTML, ConversionFormat.TXT,
                ConversionFormat.ODT, ConversionFormat.RTF, ConversionFormat.EPUB
            ],
            ConversionFormat.DOC: [
                ConversionFormat.PDF, ConversionFormat.DOCX, ConversionFormat.HTML,
                ConversionFormat.TXT
            ],
            ConversionFormat.HTML: [
                ConversionFormat.PDF, ConversionFormat.DOCX, ConversionFormat.TXT,
                ConversionFormat.MARKDOWN
            ],
            ConversionFormat.MARKDOWN: [
                ConversionFormat.HTML, ConversionFormat.PDF, ConversionFormat.DOCX
            ],
            
            # Spreadsheet conversions
            ConversionFormat.XLSX: [
                ConversionFormat.CSV, ConversionFormat.PDF, ConversionFormat.HTML,
                ConversionFormat.JSON, ConversionFormat.ODS
            ],
            ConversionFormat.XLS: [
                ConversionFormat.XLSX, ConversionFormat.CSV, ConversionFormat.PDF
            ],
            ConversionFormat.CSV: [
                ConversionFormat.XLSX, ConversionFormat.JSON, ConversionFormat.HTML
            ],
            
            # Presentation conversions
            ConversionFormat.PPTX: [
                ConversionFormat.PDF, ConversionFormat.ODP, ConversionFormat.HTML
            ],
            ConversionFormat.PPT: [
                ConversionFormat.PPTX, ConversionFormat.PDF
            ],
            
            # Image conversions
            ConversionFormat.PNG: [
                ConversionFormat.JPG, ConversionFormat.WEBP, ConversionFormat.PDF,
                ConversionFormat.SVG, ConversionFormat.TIFF
            ],
            ConversionFormat.JPG: [
                ConversionFormat.PNG, ConversionFormat.WEBP, ConversionFormat.PDF,
                ConversionFormat.TIFF
            ],
            ConversionFormat.WEBP: [
                ConversionFormat.PNG, ConversionFormat.JPG
            ],
            ConversionFormat.SVG: [
                ConversionFormat.PNG, ConversionFormat.PDF
            ]
        }
        
    async def _handle_conversion_request(self, event_data: Dict[str, Any]):
        """Handle conversion request event."""
        try:
            job_id = await self.convert_async(
                source_identifier=event_data.get("source_identifier"),
                target_format=ConversionFormat(event_data.get("target_format")),
                tenant_id=event_data.get("tenant_id"),
                options=ConversionOptions(**event_data.get("options", {}))
            )
            
            # Publish response
            await self.event_bus.publish("conversion.request.accepted", {
                "request_id": event_data.get("request_id"),
                "job_id": job_id
            })
            
        except Exception as e:
            logger.error(f"Error handling conversion request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get converter statistics."""
        return {
            "active_jobs": len(self.active_jobs),
            "queue_size": self.job_queue.qsize(),
            "num_workers": self.num_workers,
            "supported_formats": len(ConversionFormat),
            "conversion_mappings": len(self.conversion_map)
        } 