# Digital Asset Service

The Digital Asset Service is the core engine for managing all digital assets within PlatformQ. It provides comprehensive APIs for asset creation, management, peer review, and marketplace operations.

## 🎯 Overview

This service manages the lifecycle of digital assets including:
- 3D models, simulations, datasets, documents, code, and images
- Peer review and quality assurance workflows
- Decentralized marketplace with blockchain integration
- Royalty tracking and distribution
- License management for time-based access

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Messaging**: Apache Pulsar
- **Storage**: MinIO (S3-compatible)
- **Cache**: Apache Ignite
- **Blockchain**: Ethereum/Polygon for marketplace transactions

### Key Components

1. **Asset Management**
   - CRUD operations for digital assets
   - Metadata extraction and indexing
   - Version control and lineage tracking
   - Multi-format support

2. **Marketplace Integration**
   - List assets for sale or license
   - Smart contract integration for ownership
   - Automatic royalty distribution
   - Time-based usage licenses

3. **Peer Review System**
   - Quality assurance workflows
   - Reputation-based reviewing
   - Consensus mechanisms
   - Review history tracking

4. **Event Processing**
   - Real-time asset creation events
   - Marketplace transaction events
   - Review submission notifications
   - Lineage update propagation

## 📡 API Endpoints

### Asset Management
- `GET /api/v1/digital-assets` - List all assets with filtering
- `POST /api/v1/digital-assets` - Create new asset
- `GET /api/v1/digital-assets/{cid}` - Get asset details
- `PUT /api/v1/digital-assets/{cid}` - Update asset
- `DELETE /api/v1/digital-assets/{cid}` - Delete asset

### Marketplace Operations
- `GET /api/v1/marketplace/assets` - Browse marketplace listings
- `POST /api/v1/digital-assets/{cid}/list-for-sale` - List asset for sale
- `POST /api/v1/digital-assets/{cid}/create-license-offer` - Create license terms
- `DELETE /api/v1/digital-assets/{cid}/unlist` - Remove from marketplace
- `GET /api/v1/digital-assets/{cid}/marketplace-info` - Get marketplace details

### Peer Reviews
- `GET /api/v1/digital-assets/{cid}/reviews` - Get asset reviews
- `POST /api/v1/digital-assets/{cid}/reviews` - Submit review
- `PUT /api/v1/reviews/{review_id}` - Update review
- `DELETE /api/v1/reviews/{review_id}` - Delete review

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 13+
- Apache Pulsar
- MinIO

### Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost/digitalassets"
   export PULSAR_URL="pulsar://localhost:6650"
   export MINIO_ENDPOINT="localhost:9000"
   export MINIO_ACCESS_KEY="minioadmin"
   export MINIO_SECRET_KEY="minioadmin"
   ```

3. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

4. **Start the service**
   ```bash
   uvicorn app.main:app --reload --port 8001
   ```

## 📊 Database Schema

### Assets Table
```sql
CREATE TABLE assets (
    asset_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    asset_name VARCHAR(255) NOT NULL,
    asset_type VARCHAR(50) NOT NULL,
    description TEXT,
    s3_url VARCHAR(500),
    metadata JSONB,
    -- Marketplace fields
    is_for_sale BOOLEAN DEFAULT FALSE,
    sale_price NUMERIC(18, 8),
    sale_currency VARCHAR(10),
    is_licensable BOOLEAN DEFAULT FALSE,
    license_terms JSONB,
    royalty_percentage INTEGER DEFAULT 250, -- basis points
    blockchain_address VARCHAR(42),
    lineage_parent_id UUID,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Reviews Table
```sql
CREATE TABLE peer_reviews (
    review_id UUID PRIMARY KEY,
    asset_id UUID REFERENCES assets(asset_id),
    reviewer_id UUID NOT NULL,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_content TEXT,
    verified_credential_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 🔐 Security

- **Authentication**: JWT-based with RBAC
- **Data Encryption**: AES-256 for sensitive metadata
- **Blockchain Integration**: Smart contract verification
- **Input Validation**: Pydantic models with strict validation
- **Rate Limiting**: Configurable per endpoint

## 🧪 Testing

Run the test suite:
```bash
pytest tests/ -v --cov=app
```

Integration tests:
```bash
pytest tests/integration/ -v
```

## 📈 Monitoring

The service exposes metrics at `/metrics` in Prometheus format:
- Request latency histograms
- Asset creation counters
- Marketplace transaction rates
- Error rates by endpoint

## 🤝 Event Schema

### AssetCreated Event
```json
{
  "asset_id": "uuid",
  "tenant_id": "uuid",
  "asset_name": "string",
  "asset_type": "enum",
  "creator_id": "uuid",
  "timestamp": "datetime"
}
```

### AssetListed Event
```json
{
  "asset_id": "uuid",
  "sale_price": "decimal",
  "currency": "string",
  "royalty_percentage": "integer",
  "timestamp": "datetime"
}
```

## 🔧 Configuration

See `app/core/config.py` for all configuration options. Key settings:
- `MAX_UPLOAD_SIZE`: Maximum asset file size (default: 5GB)
- `ROYALTY_CAP`: Maximum royalty percentage (default: 50%)
- `LICENSE_DURATION_MAX`: Maximum license duration (default: 365 days)
- `PEER_REVIEW_THRESHOLD`: Minimum reviews for verification

## 📝 License

This service is part of the PlatformQ project and is licensed under the MIT License. 