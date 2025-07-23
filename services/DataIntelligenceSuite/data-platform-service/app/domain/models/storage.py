"""
Storage Domain Models

Domain entities for storage functionality
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class StorageTier(str, Enum):
    """Storage tier levels"""
    HOT = "hot"      # Frequently accessed (SSD/NVMe)
    WARM = "warm"    # Occasionally accessed (HDD)
    COLD = "cold"    # Rarely accessed (Archive)


class ConversionFormat(str, Enum):
    """Supported conversion formats"""
    # Documents
    PDF = "pdf"
    DOCX = "docx"
    DOC = "doc"
    TXT = "txt"
    RTF = "rtf"
    ODT = "odt"
    HTML = "html"
    MARKDOWN = "md"
    
    # Spreadsheets
    XLSX = "xlsx"
    XLS = "xls"
    CSV = "csv"
    ODS = "ods"
    
    # Presentations
    PPTX = "pptx"
    PPT = "ppt"
    ODP = "odp"
    
    # Images
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    GIF = "gif"
    WEBP = "webp"
    SVG = "svg"
    TIFF = "tiff"
    
    # Other
    JSON = "json"
    XML = "xml"
    YAML = "yaml"


class ConversionStatus(str, Enum):
    """Conversion job status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StorageMetadata(BaseModel):
    """Storage object metadata"""
    original_filename: Optional[str] = None
    upload_id: Optional[str] = None
    source_object_id: Optional[str] = None
    conversion_format: Optional[str] = None
    converted_at: Optional[datetime] = None
    tags: List[str] = Field(default_factory=list)
    custom: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        extra = "allow"  # Allow additional fields


class StorageObject(BaseModel):
    """Storage object entity"""
    id: str
    filename: str
    path: str
    bucket: str
    size: int
    content_type: str
    hash: Optional[str] = None
    tenant_id: str
    storage_tier: StorageTier = StorageTier.HOT
    created_at: datetime
    updated_at: Optional[datetime] = None
    accessed_at: Optional[datetime] = None
    metadata: StorageMetadata = Field(default_factory=StorageMetadata)
    encryption: Optional[str] = None
    compression: Optional[str] = None
    
    class Config:
        use_enum_values = True


class StorageQuota(BaseModel):
    """Storage quota for a tenant"""
    tenant_id: str
    quota_bytes: int
    used_bytes: int = 0
    file_count: int = 0
    max_file_size: Optional[int] = None
    allowed_formats: Optional[List[str]] = None
    
    @property
    def available_bytes(self) -> int:
        return max(0, self.quota_bytes - self.used_bytes)
    
    @property
    def usage_percent(self) -> float:
        if self.quota_bytes == 0:
            return 0.0
        return (self.used_bytes / self.quota_bytes) * 100


class ConversionJob(BaseModel):
    """Document conversion job"""
    id: str
    source_object_id: str
    target_format: ConversionFormat
    status: ConversionStatus = ConversionStatus.PENDING
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result_object_id: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class PreviewMetadata(BaseModel):
    """Preview/thumbnail metadata"""
    object_id: str
    preview_type: str  # thumbnail, text_extract, first_page
    preview_object_id: str
    width: Optional[int] = None
    height: Optional[int] = None
    page_count: Optional[int] = None
    text_length: Optional[int] = None
    created_at: datetime


class StorageStats(BaseModel):
    """Storage statistics"""
    tenant_id: str
    total_size: int
    file_count: int
    file_types: Dict[str, int] = Field(default_factory=dict)
    storage_by_tier: Dict[StorageTier, int] = Field(default_factory=dict)
    largest_file_size: Optional[int] = None
    average_file_size: Optional[float] = None
    last_upload_at: Optional[datetime] = None
    
    
class UploadSession(BaseModel):
    """Multipart upload session"""
    session_id: str
    object_id: str
    filename: str
    tenant_id: str
    total_parts: int
    uploaded_parts: List[int] = Field(default_factory=list)
    part_size: int
    created_at: datetime
    expires_at: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_complete(self) -> bool:
        return len(self.uploaded_parts) == self.total_parts
        
    @property
    def progress_percent(self) -> float:
        if self.total_parts == 0:
            return 0.0
        return (len(self.uploaded_parts) / self.total_parts) * 100 