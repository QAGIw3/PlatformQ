# Storage Proxy Service

Unified storage interface with document conversion capabilities for the PlatformQ ecosystem.

## Overview

The Storage Proxy Service provides:
- **Object Storage**: MinIO-based file storage with multi-tenancy
- **Document Conversion**: Format conversion using LibreOffice/Apache Tika
- **Preview Generation**: Automatic thumbnails and text extraction
- **License Management**: Integration with verifiable credentials for access control
- **Async Processing**: Background conversion jobs with status tracking

## Features

### Storage Management
- Multi-tenant file isolation
- S3-compatible object storage via MinIO
- Quota management per tenant
- File listing with pagination
- Metadata extraction and indexing

### Document Conversion
- **Supported Formats**:
  - Documents: PDF, DOCX, HTML, TXT, RTF, ODT, EPUB
  - Spreadsheets: XLSX, CSV, ODS
  - Presentations: PPTX, ODP
  - Images: PNG, JPG, WEBP, SVG

- **Conversion Modes**:
  - Synchronous: On-demand conversion during download
  - Asynchronous: Background jobs with progress tracking
  - Batch: Convert multiple documents at once

### Preview Generation
- Thumbnail generation for documents and images
- First page extraction for PDFs
- Text extraction for searchability
- Configurable preview quality and size

## API Endpoints

### Storage Operations

#### Upload File
```http
POST /api/v1/upload
Content-Type: multipart/form-data

Parameters:
- file: File to upload
- auto_convert: boolean (generate common formats)
- preview: boolean (generate preview)
```

Response:
```json
{
  "identifier": "abc123-def456",
  "filename": "document.pdf",
  "size": 1024000,
  "content_type": "application/pdf",
  "conversions": [
    {
      "job_id": "job-123",
      "target_format": "docx"
    }
  ],
  "preview_id": "preview-789"
}
```

#### Download File
```http
GET /api/v1/download/{identifier}?format=pdf

Parameters:
- identifier: File identifier
- format: Optional target format for conversion
- asset_id: Optional for license verification
- require_license: boolean
```

#### List Files
```http
GET /api/v1/storage/list?prefix=documents/&limit=50

Response:
{
  "files": [
    {
      "identifier": "abc123",
      "filename": "report.pdf",
      "size": 2048000,
      "created_at": "2024-01-15T10:00:00Z",
      "available_formats": ["pdf", "docx", "html"]
    }
  ],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

### Conversion Operations

#### Convert Document
```http
POST /api/v1/convert
Content-Type: application/json

{
  "source_identifier": "abc123",
  "target_format": "docx",
  "options": {
    "quality": "high",
    "preserve_formatting": true
  },
  "callback_url": "https://example.com/webhook"
}
```

Response:
```json
{
  "job_id": "job-456",
  "status": "processing",
  "source_identifier": "abc123",
  "target_format": "docx",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z",
  "progress": 0
}
```

#### Check Conversion Status
```http
GET /api/v1/convert/status/{job_id}

Response:
{
  "job_id": "job-456",
  "status": "completed",
  "source_identifier": "abc123",
  "target_format": "docx",
  "output_identifier": "xyz789",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:05:00Z",
  "progress": 100
}
```

#### Batch Conversion
```http
POST /api/v1/convert/batch

{
  "conversions": [
    {
      "source_identifier": "doc1",
      "target_format": "pdf"
    },
    {
      "source_identifier": "doc2",
      "target_format": "html"
    }
  ],
  "notify_on_completion": true
}
```

### Preview Operations

#### Generate Preview
```http
POST /api/v1/preview

{
  "identifier": "abc123",
  "preview_type": "thumbnail",
  "options": {
    "width": 200,
    "height": 200,
    "quality": 85
  }
}
```

### Utility Operations

#### Get Storage Quota
```http
GET /api/v1/storage/quota

Response:
{
  "used_bytes": 1073741824,
  "quota_bytes": 10737418240,
  "used_percentage": 10.0,
  "remaining_bytes": 9663676416
}
```

#### Get Supported Formats
```http
GET /api/v1/convert/formats
```

## Configuration

### Environment Variables

```bash
# Storage
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_PREFIX=platformq

# Conversion
LIBREOFFICE_PATH=/usr/bin/libreoffice
CONVERSION_TIMEOUT=300
MAX_CONVERSION_WORKERS=4
TEMP_DIR=/tmp/conversions

# Preview
PREVIEW_MAX_WIDTH=1920
PREVIEW_MAX_HEIGHT=1080
THUMBNAIL_SIZE=256

# Pulsar
PULSAR_URL=pulsar://pulsar:6650
```

## Event Integration

### Published Events

- `document-conversion-requested`: When a conversion job is created
- `document-conversion-completed`: When conversion finishes
- `document-conversion-failed`: When conversion fails
- `storage-quota-exceeded`: When tenant exceeds quota

### Consumed Events

- `document-conversion-requests`: Process conversion requests from other services

## Storage Backends

The service supports multiple storage backends through a plugin architecture:

1. **MinIO** (default): S3-compatible object storage
2. **Local**: Filesystem storage for development
3. **S3**: AWS S3 or compatible services

## Security

### Multi-tenancy
- Complete file isolation between tenants
- Tenant ID required for all operations
- Automatic path prefixing in storage

### License Verification
- Integration with verifiable-credential-service
- Blockchain-based license validation
- Usage tracking for metered licenses

### Access Control
- JWT-based authentication
- Per-file access permissions
- Temporary signed URLs for direct access

## Performance Optimization

### Caching
- Converted file caching
- Preview caching with TTL
- Metadata caching in Ignite

### Async Processing
- Background conversion workers
- Queue-based job distribution
- Configurable worker pool size

### Resource Limits
- Conversion timeout protection
- Memory limits for large files
- Concurrent job throttling

## Monitoring

### Metrics
- Storage usage per tenant
- Conversion success/failure rates
- Average conversion times
- Queue depths

### Health Checks
```http
GET /health

Response:
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:00:00Z",
  "services": {
    "storage": "healthy",
    "conversion": "healthy",
    "async_worker": "healthy"
  }
}
```

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Install LibreOffice (for conversions)
apt-get install libreoffice

# Start MinIO
docker run -p 9000:9000 minio/minio server /data

# Run service
uvicorn app.main:app --reload --port 8080
```

### Testing

```bash
# Unit tests
pytest tests/unit

# Integration tests (requires MinIO)
pytest tests/integration

# Test conversion
curl -X POST http://localhost:8080/api/v1/convert \
  -H "Content-Type: application/json" \
  -d '{"source_identifier": "test.docx", "target_format": "pdf"}'
```

## Troubleshooting

### Common Issues

1. **Conversion Timeout**
   - Check file size and complexity
   - Increase CONVERSION_TIMEOUT
   - Monitor LibreOffice memory usage

2. **Storage Errors**
   - Verify MinIO connectivity
   - Check bucket permissions
   - Monitor disk space

3. **Preview Generation Fails**
   - Ensure ImageMagick is installed
   - Check supported formats
   - Verify memory limits

### Debug Mode

Enable debug logging:
```python
import logging
logging.getLogger("storage-proxy-service").setLevel(logging.DEBUG)
```

## Future Enhancements

- OCR support for scanned documents
- Video transcoding capabilities
- AI-powered document analysis
- Distributed conversion workers
- Edge caching integration 