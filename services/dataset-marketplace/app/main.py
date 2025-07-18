"""
Dataset Marketplace Service

Specialized marketplace for training data sales with trust-weighted access control
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import hashlib
import json
import uuid
import asyncio

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
import httpx

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db

from .models import (
    DatasetListing,
    DatasetPurchase,
    DatasetType,
    LicenseType,
    DatasetStatus,
    PurchaseStatus,
    DatasetReview,
    DatasetAnalytics,
    DatasetQualityCheck
)
from .repository import (
    DatasetListingRepository,
    DatasetPurchaseRepository,
    DatasetReviewRepository,
    DatasetAnalyticsRepository,
    DatasetQualityCheckRepository
)
from .event_processors import DatasetMarketplaceEventProcessor

# Import trust-weighted components
from .engines.trust_weighted_data_engine import TrustWeightedDataEngine
from .api import trust_weighted_data
from .integrations import (
    GraphIntelligenceClient,
    IgniteCache,
    PulsarEventPublisher,
    SeaTunnelClient,
    SparkClient,
    FlinkClient,
    ElasticsearchClient,
    MinIOClient,
    CassandraClient,
    JanusGraphClient
)
from .integrations.seatunnel_quality_integration import SeaTunnelQualityIntegration
from .integrations.graph_intelligence_integration import GraphIntelligenceIntegration
from .monitoring import PrometheusMetrics

# Setup logging
logger = logging.getLogger(__name__)

# Global instances
event_processor = None
trust_engine: Optional[TrustWeightedDataEngine] = None
graph_intelligence: Optional[GraphIntelligenceIntegration] = None
seatunnel_integration: Optional[SeaTunnelQualityIntegration] = None
metrics: Optional[PrometheusMetrics] = None
ignite_cache: Optional[IgniteCache] = None
pulsar_publisher: Optional[PulsarEventPublisher] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor, trust_engine, graph_intelligence, seatunnel_integration
    global metrics, ignite_cache, pulsar_publisher
    
    # Startup
    logger.info("Initializing Dataset Marketplace Service with Trust-Weighted Data System...")
    
    # Initialize Ignite cache
    ignite_cache = IgniteCache()
    await ignite_cache.connect()
    
    # Initialize Pulsar publisher
    pulsar_client = get_pulsar_client()
    pulsar_publisher = PulsarEventPublisher(pulsar_client)
    await pulsar_publisher.initialize()
    
    # Initialize integrations
    graph_client = GraphIntelligenceClient()
    seatunnel_client = SeaTunnelClient()
    spark_client = SparkClient()
    flink_client = FlinkClient()
    es_client = ElasticsearchClient()
    minio_client = MinIOClient()
    cassandra_client = CassandraClient()
    janusgraph_client = JanusGraphClient()
    
    # Initialize Graph Intelligence integration
    graph_intelligence = GraphIntelligenceIntegration(
        graph_service_url="http://graph-intelligence-service:8000",
        ignite_cache=ignite_cache,
        pulsar_publisher=pulsar_publisher
    )
    
    # Initialize Trust-Weighted Data Engine
    trust_engine = TrustWeightedDataEngine(
        graph_client=graph_client,
        ignite=ignite_cache,
        pulsar=pulsar_publisher,
        seatunnel=seatunnel_client,
        spark=spark_client,
        flink=flink_client,
        elasticsearch=es_client
    )
    
    # Initialize SeaTunnel Quality Integration
    seatunnel_integration = SeaTunnelQualityIntegration(
        seatunnel_client=seatunnel_client,
        ignite_cache=ignite_cache,
        pulsar_publisher=pulsar_publisher,
        elasticsearch=es_client,
        cassandra=cassandra_client,
        janusgraph=janusgraph_client
    )
    
    # Initialize metrics
    metrics = PrometheusMetrics()
    
    # Initialize event processor
    event_processor = DatasetMarketplaceEventProcessor(
        pulsar_client=pulsar_client,
        service_name="dataset-marketplace-service",
        trust_engine=trust_engine
    )
    
    # Start background tasks
    await event_processor.start()
    asyncio.create_task(trust_engine.start_monitoring())
    asyncio.create_task(seatunnel_integration.start_quality_pipelines())
    
    # Setup data quality pipelines
    await setup_quality_monitoring_pipelines(flink_client, seatunnel_client)
    
    # Setup data synchronization pipelines
    await setup_data_sync_pipelines(seatunnel_integration)
    
    logger.info("Dataset Marketplace Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Dataset Marketplace Service...")
    
    if event_processor:
        await event_processor.stop()
    
    if trust_engine:
        await trust_engine.stop()
        
    if seatunnel_integration:
        await seatunnel_integration.stop()
    
    # Close connections
    await ignite_cache.close()
    await pulsar_publisher.close()
    
    logger.info("Dataset Marketplace Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="Dataset Marketplace Service",
    description="Marketplace for buying and selling training datasets with trust-weighted access control",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(trust_weighted_data.router)


class DatasetType(Enum):
    IMAGE = "image"
    TEXT = "text"
    AUDIO = "audio"
    VIDEO = "video"
    TABULAR = "tabular"
    MULTIMODAL = "multimodal"
    SYNTHETIC = "synthetic"


class LicenseType(Enum):
    ACADEMIC = "academic"
    COMMERCIAL = "commercial"
    NON_COMMERCIAL = "non_commercial"
    CUSTOM = "custom"


class DatasetListingRequest(BaseModel):
    name: str
    description: str
    dataset_type: DatasetType
    size_mb: float
    num_samples: int
    features: Dict[str, Any]
    sample_data_url: Optional[str]
    license_type: LicenseType
    price: float
    tags: List[str] = []
    metadata: Dict[str, Any] = {}


class DatasetPurchaseRequest(BaseModel):
    dataset_id: str
    buyer_id: str
    intended_use: str
    license_type: LicenseType
    duration_days: Optional[int] = None  # For time-limited licenses


class DataQualityMetrics(BaseModel):
    completeness: float  # 0-1
    accuracy: float  # 0-1
    consistency: float  # 0-1
    uniqueness: float  # 0-1
    timeliness: float  # 0-1
    validity: float  # 0-1


class DatasetManager:
    """Manages dataset listings and transactions"""
    
    def __init__(self):
        self.datasets: Dict[str, Dict[str, Any]] = {}
        self.purchases: Dict[str, Dict[str, Any]] = {}
        self.quality_scores: Dict[str, DataQualityMetrics] = {}
        
    async def create_dataset_listing(self, seller_id: str, listing: DatasetListingRequest) -> str:
        """Create a new dataset listing"""
        dataset_id = f"ds_{hashlib.sha256(f'{seller_id}{listing.name}{datetime.utcnow()}'.encode()).hexdigest()[:12]}"
        
        self.datasets[dataset_id] = {
            "dataset_id": dataset_id,
            "seller_id": seller_id,
            "name": listing.name,
            "description": listing.description,
            "dataset_type": listing.dataset_type.value,
            "size_mb": listing.size_mb,
            "num_samples": listing.num_samples,
            "features": listing.features,
            "sample_data_url": listing.sample_data_url,
            "license_type": listing.license_type.value,
            "price": listing.price,
            "tags": listing.tags,
            "metadata": listing.metadata,
            "created_at": datetime.utcnow(),
            "total_sales": 0,
            "rating": 0.0,
            "reviews": []
        }
        
        return dataset_id
        
    async def validate_dataset_quality(self, dataset_id: str, sample_data: bytes) -> DataQualityMetrics:
        """Validate dataset quality metrics"""
        # Simplified quality validation
        # In production, implement comprehensive data quality checks
        
        quality = DataQualityMetrics(
            completeness=0.95,
            accuracy=0.92,
            consistency=0.88,
            uniqueness=0.99,
            timeliness=0.90,
            validity=0.94
        )
        
        self.quality_scores[dataset_id] = quality
        return quality
        
    async def create_purchase(self, purchase_request: DatasetPurchaseRequest) -> str:
        """Create a dataset purchase"""
        dataset = self.datasets.get(purchase_request.dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
            
        purchase_id = f"dp_{hashlib.sha256(f'{purchase_request.buyer_id}{purchase_request.dataset_id}{datetime.utcnow()}'.encode()).hexdigest()[:12]}"
        
        self.purchases[purchase_id] = {
            "purchase_id": purchase_id,
            "dataset_id": purchase_request.dataset_id,
            "buyer_id": purchase_request.buyer_id,
            "seller_id": dataset["seller_id"],
            "intended_use": purchase_request.intended_use,
            "license_type": purchase_request.license_type.value,
            "price_paid": dataset["price"],
            "purchase_date": datetime.utcnow(),
            "expiry_date": datetime.utcnow() + timedelta(days=purchase_request.duration_days) if purchase_request.duration_days else None,
            "download_count": 0,
            "max_downloads": 5
        }
        
        # Update dataset stats
        dataset["total_sales"] += 1
        
        return purchase_id
        
    async def search_datasets(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search datasets with filters"""
        results = []
        
        for dataset in self.datasets.values():
            # Apply filters
            if filters.get("dataset_type") and dataset["dataset_type"] != filters["dataset_type"]:
                continue
            if filters.get("max_price") and dataset["price"] > filters["max_price"]:
                continue
            if filters.get("min_samples") and dataset["num_samples"] < filters["min_samples"]:
                continue
            if filters.get("license_type") and dataset["license_type"] != filters["license_type"]:
                continue
                
            # Add quality score if available
            if dataset["dataset_id"] in self.quality_scores:
                dataset["quality_metrics"] = self.quality_scores[dataset["dataset_id"]]
                
            results.append(dataset)
            
        return results


# Initialize dataset manager
dataset_manager = DatasetManager()


@app.get("/")
async def root():
    return {"service": "Dataset Marketplace", "status": "operational"}


@app.post("/api/v1/datasets", response_model=Dict[str, Any])
async def create_dataset_listing(
    listing: DatasetListingRequest,
    seller_id: str,
    background_tasks: BackgroundTasks
):
    """Create a new dataset listing"""
    try:
        # Create listing
        dataset_id = await dataset_manager.create_dataset_listing(seller_id, listing)
        
        # Schedule quality validation if sample provided
        if listing.sample_data_url:
            background_tasks.add_task(
                validate_dataset_quality_async,
                dataset_id,
                listing.sample_data_url
            )
        
        # Create blockchain record
        background_tasks.add_task(
            create_blockchain_dataset_record,
            dataset_id,
            seller_id
        )
        
        return {
            "dataset_id": dataset_id,
            "status": "created",
            "listing_url": f"/api/v1/datasets/{dataset_id}"
        }
        
    except Exception as e:
        logger.error(f"Error creating dataset listing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/datasets/{dataset_id}/validate", response_model=DataQualityMetrics)
async def validate_dataset(
    dataset_id: str,
    sample_file: UploadFile = File(...)
):
    """Validate dataset quality with sample data"""
    try:
        # Read sample data
        sample_data = await sample_file.read()
        
        # Validate quality
        quality_metrics = await dataset_manager.validate_dataset_quality(dataset_id, sample_data)
        
        return quality_metrics
        
    except Exception as e:
        logger.error(f"Error validating dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/purchases", response_model=Dict[str, Any])
async def purchase_dataset(
    purchase_request: DatasetPurchaseRequest,
    background_tasks: BackgroundTasks
):
    """Purchase a dataset"""
    try:
        # Create purchase
        purchase_id = await dataset_manager.create_purchase(purchase_request)
        
        # Process payment (simplified)
        background_tasks.add_task(
            process_dataset_payment,
            purchase_id,
            purchase_request.buyer_id
        )
        
        # Generate access token
        access_token = generate_dataset_access_token(purchase_id)
        
        return {
            "purchase_id": purchase_id,
            "dataset_id": purchase_request.dataset_id,
            "access_token": access_token,
            "download_url": f"/api/v1/datasets/{purchase_request.dataset_id}/download",
            "expiry_date": dataset_manager.purchases[purchase_id].get("expiry_date")
        }
        
    except Exception as e:
        logger.error(f"Error purchasing dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/datasets/search", response_model=List[Dict[str, Any]])
async def search_datasets(
    dataset_type: Optional[DatasetType] = None,
    max_price: Optional[float] = None,
    min_samples: Optional[int] = None,
    license_type: Optional[LicenseType] = None,
    tags: Optional[List[str]] = None,
    min_quality_score: Optional[float] = None
):
    """Search for datasets"""
    try:
        filters = {
            "dataset_type": dataset_type.value if dataset_type else None,
            "max_price": max_price,
            "min_samples": min_samples,
            "license_type": license_type.value if license_type else None
        }
        
        results = await dataset_manager.search_datasets(filters)
        
        # Filter by quality score if specified
        if min_quality_score:
            results = [
                r for r in results
                if r.get("quality_metrics") and 
                   calculate_overall_quality(r["quality_metrics"]) >= min_quality_score
            ]
        
        # Filter by tags if specified
        if tags:
            results = [
                r for r in results
                if any(tag in r.get("tags", []) for tag in tags)
            ]
        
        return results
        
    except Exception as e:
        logger.error(f"Error searching datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/datasets/{dataset_id}", response_model=Dict[str, Any])
async def get_dataset_details(dataset_id: str):
    """Get detailed information about a dataset"""
    try:
        dataset = dataset_manager.datasets.get(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Add quality metrics if available
        if dataset_id in dataset_manager.quality_scores:
            dataset["quality_metrics"] = dataset_manager.quality_scores[dataset_id]
            dataset["overall_quality_score"] = calculate_overall_quality(
                dataset_manager.quality_scores[dataset_id]
            )
        
        # Add schema information
        dataset["schema"] = await get_dataset_schema(dataset_id)
        
        # Add statistics
        dataset["statistics"] = await get_dataset_statistics(dataset_id)
        
        return dataset
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dataset details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/datasets/{dataset_id}/preview", response_model=Dict[str, Any])
async def get_dataset_preview(
    dataset_id: str,
    num_samples: int = 10
):
    """Get a preview of the dataset"""
    try:
        dataset = dataset_manager.datasets.get(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Generate preview based on dataset type
        preview_data = await generate_dataset_preview(
            dataset_id,
            dataset["dataset_type"],
            num_samples
        )
        
        return {
            "dataset_id": dataset_id,
            "preview_samples": preview_data,
            "total_samples": dataset["num_samples"],
            "features": dataset["features"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating dataset preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/datasets/{dataset_id}/augment", response_model=Dict[str, Any])
async def create_augmented_dataset(
    dataset_id: str,
    augmentation_config: Dict[str, Any],
    buyer_id: str
):
    """Create an augmented version of a dataset"""
    try:
        # Verify purchase
        has_access = await verify_dataset_access(dataset_id, buyer_id)
        if not has_access:
            raise HTTPException(status_code=403, detail="No access to dataset")
        
        # Create augmentation job
        job_id = await create_augmentation_job(
            dataset_id,
            augmentation_config,
            buyer_id
        )
        
        return {
            "job_id": job_id,
            "status": "processing",
            "estimated_completion": (datetime.utcnow() + timedelta(hours=2)).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating augmented dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/datasets/{dataset_id}/lineage", response_model=Dict[str, Any])
async def get_dataset_lineage(dataset_id: str):
    """Get dataset lineage and provenance information"""
    try:
        lineage = await fetch_dataset_lineage(dataset_id)
        
        return {
            "dataset_id": dataset_id,
            "sources": lineage.get("sources", []),
            "transformations": lineage.get("transformations", []),
            "version_history": lineage.get("versions", []),
            "citations": lineage.get("citations", [])
        }
        
    except Exception as e:
        logger.error(f"Error getting dataset lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions
async def validate_dataset_quality_async(dataset_id: str, sample_url: str):
    """Async task to validate dataset quality"""
    try:
        # Download sample data
        async with httpx.AsyncClient() as client:
            response = await client.get(sample_url)
            sample_data = response.content
        
        # Validate quality
        await dataset_manager.validate_dataset_quality(dataset_id, sample_data)
        
    except Exception as e:
        logger.error(f"Error in async quality validation: {e}")


async def create_blockchain_dataset_record(dataset_id: str, seller_id: str):
    """Create blockchain record for dataset"""
    # Implementation would interact with blockchain service
    pass


async def process_dataset_payment(purchase_id: str, buyer_id: str):
    """Process payment for dataset purchase"""
    # Implementation would handle payment processing
    pass


def generate_dataset_access_token(purchase_id: str) -> str:
    """Generate access token for dataset download"""
    # Simplified token generation
    return hashlib.sha256(f"{purchase_id}{datetime.utcnow()}".encode()).hexdigest()


def calculate_overall_quality(metrics: DataQualityMetrics) -> float:
    """Calculate overall quality score from individual metrics"""
    weights = {
        "completeness": 0.2,
        "accuracy": 0.3,
        "consistency": 0.15,
        "uniqueness": 0.15,
        "timeliness": 0.1,
        "validity": 0.1
    }
    
    score = (
        metrics.completeness * weights["completeness"] +
        metrics.accuracy * weights["accuracy"] +
        metrics.consistency * weights["consistency"] +
        metrics.uniqueness * weights["uniqueness"] +
        metrics.timeliness * weights["timeliness"] +
        metrics.validity * weights["validity"]
    )
    
    return round(score, 2)


async def get_dataset_schema(dataset_id: str) -> Dict[str, Any]:
    """Get dataset schema information"""
    # Simplified schema generation
    return {
        "fields": [],
        "data_types": {},
        "constraints": []
    }


async def get_dataset_statistics(dataset_id: str) -> Dict[str, Any]:
    """Get dataset statistics"""
    # Simplified statistics
    return {
        "mean": {},
        "std": {},
        "min": {},
        "max": {},
        "missing_values": {}
    }


async def generate_dataset_preview(dataset_id: str, dataset_type: str, num_samples: int) -> List[Any]:
    """Generate preview samples"""
    # Simplified preview generation
    return []


async def verify_dataset_access(dataset_id: str, buyer_id: str) -> bool:
    """Verify if buyer has access to dataset"""
    for purchase in dataset_manager.purchases.values():
        if purchase["dataset_id"] == dataset_id and purchase["buyer_id"] == buyer_id:
            if purchase.get("expiry_date"):
                return datetime.utcnow() < purchase["expiry_date"]
            return True
    return False


async def create_augmentation_job(dataset_id: str, config: Dict[str, Any], buyer_id: str) -> str:
    """Create dataset augmentation job"""
    # Simplified job creation
    return f"aug_{hashlib.sha256(f'{dataset_id}{buyer_id}{datetime.utcnow()}'.encode()).hexdigest()[:12]}"


async def fetch_dataset_lineage(dataset_id: str) -> Dict[str, Any]:
    """Fetch dataset lineage information"""
    # Simplified lineage
    return {
        "sources": [],
        "transformations": [],
        "versions": [],
        "citations": []
    }


async def setup_quality_monitoring_pipelines(flink_client: FlinkClient, seatunnel_client: SeaTunnelClient):
    """Setup Flink jobs for real-time data quality monitoring"""
    
    # Data quality monitoring job configuration
    quality_job_config = {
        "job_name": "data-quality-monitoring",
        "parallelism": 4,
        "checkpointing_interval": 60000,  # 1 minute
        "source": {
            "type": "pulsar",
            "topic": "dataset-uploads",
            "subscription": "quality-monitor"
        },
        "pipeline": [
            {
                "operator": "quality_assessor",
                "class": "com.platformq.flink.quality.QualityAssessor",
                "config": {
                    "dimensions": [
                        "completeness",
                        "accuracy",
                        "consistency",
                        "timeliness",
                        "validity",
                        "uniqueness"
                    ],
                    "sampling_rate": 0.1  # Sample 10% for large datasets
                }
            },
            {
                "operator": "anomaly_detector",
                "class": "com.platformq.flink.quality.AnomalyDetector",
                "config": {
                    "algorithms": ["isolation_forest", "autoencoder"],
                    "threshold": 0.95
                }
            },
            {
                "operator": "drift_detector",
                "class": "com.platformq.flink.quality.DriftDetector",
                "config": {
                    "window_size": "1h",
                    "reference_window": "7d",
                    "metrics": ["kl_divergence", "js_divergence"]
                }
            }
        ],
        "sinks": [
            {
                "type": "pulsar",
                "topic": "quality-assessments"
            },
            {
                "type": "ignite",
                "cache": "quality_scores"
            },
            {
                "type": "elasticsearch",
                "index": "data-quality-metrics"
            }
        ]
    }
    
    await flink_client.submit_job(quality_job_config)
    logger.info("Data quality monitoring pipeline started")


async def setup_data_sync_pipelines(seatunnel_integration: SeaTunnelQualityIntegration):
    """Setup SeaTunnel pipelines for data synchronization"""
    
    # Quality assessment sync pipeline
    quality_sync_pipeline = {
        "name": "quality_assessment_sync",
        "source": {
            "type": "pulsar",
            "topic": "quality-assessments",
            "subscription": "seatunnel-quality-sync"
        },
        "transform": [
            {
                "type": "sql",
                "query": """
                    SELECT 
                        assessment_id,
                        dataset_id,
                        asset_id,
                        overall_quality_score,
                        trust_adjusted_score,
                        quality_dimensions,
                        assessor_trust_score,
                        timestamp
                    FROM source
                """
            }
        ],
        "sink": [
            {
                "type": "cassandra",
                "keyspace": "platformq",
                "table": "quality_assessments",
                "consistency": "QUORUM"
            },
            {
                "type": "elasticsearch",
                "index": "quality-assessments",
                "id_field": "assessment_id"
            }
        ]
    }
    
    # Trust-weighted access log pipeline
    access_log_pipeline = {
        "name": "trust_access_log",
        "source": {
            "type": "pulsar",
            "topic": "data-access-requests",
            "subscription": "seatunnel-access-sync"
        },
        "transform": [
            {
                "type": "enrichment",
                "enrichments": [
                    {
                        "field": "requester_id",
                        "lookup": "graph_intelligence",
                        "output": "trust_scores"
                    }
                ]
            }
        ],
        "sink": [
            {
                "type": "janusgraph",
                "graph": "platformq-knowledge",
                "vertex_label": "DataAccess"
            },
            {
                "type": "influxdb",
                "bucket": "access_metrics",
                "measurement": "trust_weighted_access"
            }
        ]
    }
    
    # Data lineage tracking pipeline
    lineage_pipeline = {
        "name": "data_lineage_tracking",
        "source": {
            "type": "pulsar",
            "topic": "data-lineage-events",
            "subscription": "seatunnel-lineage-sync"
        },
        "transform": [
            {
                "type": "graph_builder",
                "vertices": {
                    "dataset": ["dataset_id", "name", "type"],
                    "user": ["user_id", "trust_score"],
                    "quality_check": ["check_id", "score"]
                },
                "edges": {
                    "created_by": ["dataset", "user"],
                    "assessed_by": ["dataset", "quality_check"]
                }
            }
        ],
        "sink": [
            {
                "type": "janusgraph",
                "graph": "platformq-knowledge"
            }
        ]
    }
    
    await seatunnel_integration.create_pipeline(quality_sync_pipeline)
    await seatunnel_integration.create_pipeline(access_log_pipeline)
    await seatunnel_integration.create_pipeline(lineage_pipeline)
    logger.info("Data synchronization pipelines created")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 