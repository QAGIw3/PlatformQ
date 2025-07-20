"""
Digital Asset Service with Vault & Consul Integration
"""

from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
import os
import hashlib
import mimetypes
from pathlib import Path

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.middleware.security_middleware import SecurityMiddleware

from .vault_consul_integration import (
    DigitalAssetVaultIntegration,
    DigitalAssetConsulIntegration,
    StorageConfig,
    ProcessingConfig,
    AssetStorageProvider,
    AssetType
)
from .models import Asset, AssetMetadata, AssetUploadResponse, AssetSearchQuery
from .storage import StorageManager
from .processing import AssetProcessor
from .integrations.event_driven_assets import EventDrivenAssetIntegration, AssetEventType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DigitalAssetService:
    """Digital Asset Service with Vault & Consul Integration"""
    
    def __init__(self):
        self.app = FastAPI(title="Digital Asset Service", version="2.0.0")
        self.vault_integration: Optional[DigitalAssetVaultIntegration] = None
        self.consul_integration: Optional[DigitalAssetConsulIntegration] = None
        self.storage_manager: Optional[StorageManager] = None
        self.asset_processor: Optional[AssetProcessor] = None
        self.event_integration: Optional[EventDrivenAssetIntegration] = None
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan management"""
        # Startup
        await self.startup()
        yield
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Service startup procedure"""
        logger.info("Starting Digital Asset Service with Vault & Consul integration")
        
        try:
            # Initialize Vault client
            vault_client = VaultClient(
                vault_addr=os.getenv("VAULT_ADDR", "http://vault:8200"),
                role_id=os.getenv("VAULT_ROLE_ID"),
                secret_id=os.getenv("VAULT_SECRET_ID")
            )
            await vault_client.initialize()
            
            # Initialize Consul client
            consul_client = ConsulClient(
                host=os.getenv("CONSUL_HOST", "consul"),
                port=int(os.getenv("CONSUL_PORT", "8500"))
            )
            
            # Initialize integrations
            self.vault_integration = DigitalAssetVaultIntegration(vault_client)
            await self.vault_integration.initialize()
            
            self.consul_integration = DigitalAssetConsulIntegration(consul_client)
            await self.consul_integration.initialize()
            
            # Initialize storage manager
            storage_config = await self.consul_integration.get_storage_config()
            self.storage_manager = StorageManager(
                vault_integration=self.vault_integration,
                storage_config=storage_config
            )
            await self.storage_manager.initialize()
            
            # Initialize asset processor
            processing_config = await self.consul_integration.get_processing_config()
            self.asset_processor = AssetProcessor(
                vault_integration=self.vault_integration,
                processing_config=processing_config
            )
            await self.asset_processor.initialize()

            # Initialize event-driven integration
            self.event_integration = EventDrivenAssetIntegration(
                vault_consul_integration=self.vault_integration
            )
            await self.event_integration.initialize()
            
            # Set up routes
            self._setup_routes()
            
            # Add security middleware
            security_middleware = SecurityMiddleware(
                vault_client=vault_client,
                consul_client=consul_client,
                service_name="digital-asset-service"
            )
            self.app.add_middleware(security_middleware)
            
            # Start background tasks
            asyncio.create_task(self._health_check_loop())
            asyncio.create_task(self._process_pending_assets())
            asyncio.create_task(self._cleanup_expired_assets())
            
            logger.info("Digital Asset Service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Digital Asset Service: {e}")
            raise
            
    async def shutdown(self):
        """Service shutdown procedure"""
        logger.info("Shutting down Digital Asset Service")
        
        # Cancel background tasks
        for task in asyncio.all_tasks():
            if task.get_name() in ["health_check", "asset_processor", "cleanup"]:
                task.cancel()
                
        # Close storage connections
        if self.storage_manager:
            await self.storage_manager.close()
            
        # Deregister from Consul
        if self.consul_integration:
            await self.consul_integration.consul.deregister_service()
            
        logger.info("Digital Asset Service shutdown complete")
        
    def _setup_routes(self):
        """Set up API routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check Vault connectivity
                vault_healthy = await self._check_vault_health()
                
                # Check Consul connectivity
                consul_healthy = await self._check_consul_health()
                
                # Check storage health
                storage_health = await self._check_storage_health()
                
                overall_status = "healthy"
                if not all([vault_healthy, consul_healthy]):
                    overall_status = "unhealthy"
                elif not all(storage_health.values()):
                    overall_status = "degraded"
                    
                health_data = {
                    "status": overall_status,
                    "service": "digital-asset-service",
                    "checks": {
                        "vault": "healthy" if vault_healthy else "unhealthy",
                        "consul": "healthy" if consul_healthy else "unhealthy",
                        "storage": storage_health
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if overall_status == "unhealthy":
                    raise HTTPException(status_code=503, detail=health_data)
                    
                return health_data
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=503, detail="Service unhealthy")
                
        @self.app.post("/api/v1/assets/upload", response_model=AssetUploadResponse)
        async def upload_asset(
            file: UploadFile = File(...),
            metadata: str = Form(...),
            parent_asset_id: Optional[str] = Form(None)
        ):
            """Upload a new digital asset"""
            try:
                # Parse metadata
                asset_metadata = AssetMetadata.parse_raw(metadata)
                
                # Process and store asset
                asset_id = await self.storage_manager.store_asset(
                    file=file,
                    metadata=asset_metadata
                )
                
                # Publish asset created event
                await self.event_integration.publish_asset_event(
                    AssetEventType.ASSET_CREATED,
                    {
                        "asset_metadata": {
                            "asset_id": asset_id,
                            "cid": asset_metadata.cid,
                            "name": asset_metadata.name,
                            "type": asset_metadata.asset_type,
                            "owner_id": asset_metadata.owner_id,
                            "size_bytes": asset_metadata.size_bytes,
                            "format": asset_metadata.format,
                            "version": asset_metadata.version,
                            "tags": asset_metadata.tags,
                            "license_type": asset_metadata.license_type,
                            "price": asset_metadata.price
                        },
                        "source_service": "digital-asset-service",
                        "parent_asset_id": parent_asset_id,
                        "creation_metadata": {
                            "description": asset_metadata.description,
                            "derivation_type": "derived" if parent_asset_id else None
                        }
                    }
                )
                
                return AssetUploadResponse(
                    asset_id=asset_id,
                    cid=asset_metadata.cid,
                    status="uploaded",
                    message="Asset uploaded successfully"
                )
                
            except Exception as e:
                logger.error(f"Error uploading asset: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/v1/assets/{asset_id}/reviews")
        async def submit_review(
            asset_id: str,
            review_data: Dict[str, Any]
        ):
            """Submit a review for an asset"""
            try:
                # Process review
                review_id = await self.process_review(asset_id, review_data)
                
                # Publish review completed event
                await self.event_integration.publish_asset_event(
                    AssetEventType.REVIEW_COMPLETED,
                    {
                        "event_type": AssetEventType.REVIEW_COMPLETED.value,
                        "asset_id": asset_id,
                        "review_id": review_id,
                        "reviewer_id": review_data.get("reviewer_id"),
                        "rating": review_data.get("rating"),
                        "review_type": review_data.get("review_type", "quality"),
                        "comments": review_data.get("comments"),
                        "metadata": {
                            "verified": review_data.get("verified", False)
                        }
                    }
                )
                
                return {"review_id": review_id, "status": "submitted"}
                
            except Exception as e:
                logger.error(f"Error submitting review: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/v1/marketplace/purchase")
        async def purchase_asset(
            purchase_data: Dict[str, Any]
        ):
            """Purchase an asset from the marketplace"""
            try:
                # Process purchase
                transaction_id = await self.process_purchase(purchase_data)
                
                # Publish purchase event
                await self.event_integration.publish_asset_event(
                    AssetEventType.ASSET_PURCHASED,
                    {
                        "event_type": AssetEventType.ASSET_PURCHASED.value,
                        "asset_id": purchase_data.get("asset_id"),
                        "transaction_id": transaction_id,
                        "buyer_id": purchase_data.get("buyer_id"),
                        "seller_id": purchase_data.get("seller_id"),
                        "price": purchase_data.get("price"),
                        "currency": purchase_data.get("currency", "USD"),
                        "transaction_type": "purchase",
                        "blockchain_tx_hash": purchase_data.get("blockchain_tx_hash"),
                        "royalty_distributions": purchase_data.get("royalty_distributions", [])
                    }
                )
                
                return {"transaction_id": transaction_id, "status": "completed"}
                
            except Exception as e:
                logger.error(f"Error processing purchase: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/v1/assets/{asset_id}/lineage")
        async def get_asset_lineage(asset_id: str, depth: int = 3):
            """Get asset lineage information"""
            try:
                lineage = await self.event_integration.get_asset_lineage(asset_id, depth)
                return lineage
            except Exception as e:
                logger.error(f"Error getting asset lineage: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/v1/users/{user_id}/reputation")
        async def get_user_reputation(user_id: str):
            """Get user reputation score"""
            try:
                reputation = await self.event_integration.get_user_reputation(user_id)
                return reputation
            except Exception as e:
                logger.error(f"Error getting user reputation: {e}")
                raise HTTPException(status_code=500, detail=str(e))
            
        @self.app.get("/api/v1/assets/{asset_id}")
        async def get_asset_metadata(asset_id: str):
            """Get asset metadata"""
            metadata = await self.consul_integration.get_asset_metadata(asset_id)
            
            if not metadata:
                raise HTTPException(404, f"Asset {asset_id} not found")
                
            # Verify metadata signature
            signature = metadata.get("metadata_signature")
            if signature:
                metadata_copy = metadata.copy()
                del metadata_copy["metadata_signature"]
                
                valid = await self.vault_integration.verify_asset_metadata(
                    metadata_copy, signature, asset_id
                )
                
                if not valid:
                    logger.error(f"Invalid metadata signature for asset {asset_id}")
                    raise HTTPException(500, "Asset metadata integrity check failed")
                    
            return metadata
            
        @self.app.get("/api/v1/assets/{asset_id}/download")
        async def download_asset(asset_id: str):
            """Download asset content"""
            # Get metadata
            metadata = await self.consul_integration.get_asset_metadata(asset_id)
            if not metadata:
                raise HTTPException(404, f"Asset {asset_id} not found")
                
            # Get content from storage
            content = await self.storage_manager.download_asset(
                asset_id=asset_id,
                storage_path=metadata["storage_path"],
                provider=AssetStorageProvider(metadata["storage_provider"])
            )
            
            # Decrypt if encrypted
            if metadata.get("encryption_enabled"):
                content = await self.vault_integration.decrypt_asset_data(content, asset_id)
                
            # Return as streaming response
            return StreamingResponse(
                io.BytesIO(content),
                media_type=metadata["mime_type"],
                headers={
                    "Content-Disposition": f'attachment; filename="{metadata["name"]}"',
                    "Content-Length": str(len(content))
                }
            )
            
        @self.app.get("/api/v1/assets/{asset_id}/thumbnail/{size}")
        async def get_asset_thumbnail(asset_id: str, size: str = "medium"):
            """Get asset thumbnail"""
            # Get metadata
            metadata = await self.consul_integration.get_asset_metadata(asset_id)
            if not metadata:
                raise HTTPException(404, f"Asset {asset_id} not found")
                
            # Check if thumbnails exist
            thumbnails = metadata.get("thumbnails", {})
            if size not in thumbnails:
                raise HTTPException(404, f"Thumbnail size {size} not available")
                
            # Get thumbnail from storage
            thumbnail_path = thumbnails[size]["path"]
            content = await self.storage_manager.download_asset(
                asset_id=f"{asset_id}-thumb-{size}",
                storage_path=thumbnail_path,
                provider=AssetStorageProvider(metadata["storage_provider"])
            )
            
            return StreamingResponse(
                io.BytesIO(content),
                media_type="image/jpeg",
                headers={
                    "Content-Length": str(len(content)),
                    "Cache-Control": "public, max-age=86400"  # 24 hour cache
                }
            )
            
        @self.app.post("/api/v1/assets/search")
        async def search_assets(query: AssetSearchQuery):
            """Search for assets"""
            results = []
            
            # Simple search implementation
            for asset_id, metadata in self.consul_integration._asset_registry.items():
                # Filter by tags
                if query.tags:
                    if not any(tag in metadata.get("tags", []) for tag in query.tags):
                        continue
                        
                # Filter by mime type
                if query.mime_types:
                    if metadata.get("mime_type") not in query.mime_types:
                        continue
                        
                # Filter by date range
                if query.created_after:
                    created_at = datetime.fromisoformat(metadata.get("created_at", ""))
                    if created_at < query.created_after:
                        continue
                        
                if query.created_before:
                    created_at = datetime.fromisoformat(metadata.get("created_at", ""))
                    if created_at > query.created_before:
                        continue
                        
                # Text search in name
                if query.text:
                    if query.text.lower() not in metadata.get("name", "").lower():
                        continue
                        
                results.append(Asset(
                    id=asset_id,
                    name=metadata["name"],
                    mime_type=metadata["mime_type"],
                    size_bytes=metadata["size_bytes"],
                    created_at=datetime.fromisoformat(metadata["created_at"]),
                    tags=metadata.get("tags", []),
                    processing_status=metadata.get("processing_status", "unknown")
                ))
                
                # Limit results
                if len(results) >= query.limit:
                    break
                    
            return {"assets": results, "total": len(results)}
            
        @self.app.delete("/api/v1/assets/{asset_id}")
        async def delete_asset(asset_id: str):
            """Delete an asset"""
            # Get metadata
            metadata = await self.consul_integration.get_asset_metadata(asset_id)
            if not metadata:
                raise HTTPException(404, f"Asset {asset_id} not found")
                
            # Delete from storage
            await self.storage_manager.delete_asset(
                asset_id=asset_id,
                storage_path=metadata["storage_path"],
                provider=AssetStorageProvider(metadata["storage_provider"])
            )
            
            # Update metrics
            asset_type = self._determine_asset_type(metadata["mime_type"])
            await self.consul_integration.update_storage_metrics(
                asset_type.value,
                metadata["size_bytes"],
                metadata["storage_provider"],
                "remove"
            )
            
            # Remove from registry
            await self.consul_integration.consul.kv_delete(
                f"services/digital-asset-service/assets/{asset_id}"
            )
            
            return {"status": "success", "message": f"Asset {asset_id} deleted"}
            
        @self.app.get("/api/v1/storage/metrics")
        async def get_storage_metrics():
            """Get storage usage metrics"""
            return await self.consul_integration.get_storage_metrics()
            
        @self.app.post("/api/v1/storage/rotate-credentials/{provider}")
        async def rotate_storage_credentials(provider: str):
            """Rotate storage provider credentials"""
            try:
                provider_enum = AssetStorageProvider(provider)
                await self.vault_integration.rotate_storage_credentials(provider_enum)
                return {"status": "success", "message": f"Credentials rotated for {provider}"}
            except ValueError:
                raise HTTPException(400, f"Invalid provider: {provider}")
            except Exception as e:
                logger.error(f"Credential rotation failed: {e}")
                raise HTTPException(500, f"Rotation failed: {str(e)}")
                
    def _determine_asset_type(self, mime_type: str) -> AssetType:
        """Determine asset type from mime type"""
        if mime_type.startswith("image/"):
            return AssetType.IMAGE
        elif mime_type.startswith("video/"):
            return AssetType.VIDEO
        elif mime_type.startswith("audio/"):
            return AssetType.AUDIO
        elif mime_type.startswith("application/pdf") or mime_type.startswith("text/"):
            return AssetType.DOCUMENT
        elif mime_type in ["application/x-blender", "model/gltf+json", "model/gltf-binary"]:
            return AssetType.MODEL_3D
        elif mime_type.startswith("application/") and any(x in mime_type for x in ["json", "xml", "yaml"]):
            return AssetType.CODE
        else:
            return AssetType.OTHER
            
    async def _check_duplicate_asset(self, file_hash: str) -> Optional[Dict]:
        """Check if asset with same hash already exists"""
        for asset_id, metadata in self.consul_integration._asset_registry.items():
            if metadata.get("hash") == file_hash:
                return metadata
        return None
        
    async def _process_asset(self, asset_id: str):
        """Process uploaded asset"""
        try:
            # Update status
            await self.consul_integration.update_asset_status(asset_id, "processing")
            
            # Get metadata
            metadata = await self.consul_integration.get_asset_metadata(asset_id)
            
            # Run processing based on type
            asset_type = self._determine_asset_type(metadata["mime_type"])
            processing_config = await self.consul_integration.get_processing_config()
            
            updates = {}
            
            # Generate thumbnails for images/videos
            if asset_type in [AssetType.IMAGE, AssetType.VIDEO] and processing_config.auto_thumbnail:
                thumbnails = await self.asset_processor.generate_thumbnails(
                    asset_id, metadata, asset_type
                )
                updates["thumbnails"] = thumbnails
                
            # Extract metadata
            if processing_config.auto_metadata_extraction:
                extracted_metadata = await self.asset_processor.extract_metadata(
                    asset_id, metadata, asset_type
                )
                updates["extracted_metadata"] = extracted_metadata
                
            # Virus scanning
            if processing_config.virus_scanning:
                scan_result = await self.asset_processor.scan_for_viruses(
                    asset_id, metadata
                )
                updates["virus_scan"] = scan_result
                
            # Content moderation
            if processing_config.content_moderation and asset_type == AssetType.IMAGE:
                moderation_result = await self.asset_processor.moderate_content(
                    asset_id, metadata
                )
                updates["content_moderation"] = moderation_result
                
            # Update metadata
            if updates:
                await self.consul_integration.consul.kv_merge(
                    f"services/digital-asset-service/assets/{asset_id}/metadata",
                    updates
                )
                
            # Update status
            await self.consul_integration.update_asset_status(asset_id, "completed")
            
        except Exception as e:
            logger.error(f"Asset processing failed for {asset_id}: {e}")
            await self.consul_integration.update_asset_status(
                asset_id, 
                "failed",
                {"error": str(e)}
            )
        finally:
            await self.consul_integration.release_processing_slot(asset_id)
            
    async def _check_vault_health(self) -> bool:
        """Check Vault connectivity"""
        try:
            await self.vault_integration.vault.get_secret("digital-asset-service/health-check")
            return True
        except:
            return False
            
    async def _check_consul_health(self) -> bool:
        """Check Consul connectivity"""
        try:
            await self.consul_integration.consul.kv_get("services/digital-asset-service/health/status")
            return True
        except:
            return False
            
    async def _check_storage_health(self) -> Dict[str, bool]:
        """Check storage provider health"""
        storage_health = {}
        
        for provider in AssetStorageProvider:
            healthy = await self.consul_integration.check_storage_health(provider)
            storage_health[provider.value] = healthy
            
        return storage_health
        
    async def _health_check_loop(self):
        """Periodic health check"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                
                # Check storage health
                for provider in AssetStorageProvider:
                    try:
                        # Test storage connectivity
                        healthy = await self.storage_manager.test_connection(provider)
                        await self.consul_integration.update_storage_health(provider, healthy)
                    except:
                        await self.consul_integration.update_storage_health(provider, False)
                        
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                
    async def _process_pending_assets(self):
        """Process assets that are pending"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Find pending assets
                for asset_id, metadata in self.consul_integration._asset_registry.items():
                    if metadata.get("processing_status") == "pending":
                        if await self.consul_integration.acquire_processing_slot(asset_id):
                            asyncio.create_task(self._process_asset(asset_id))
                            
            except Exception as e:
                logger.error(f"Pending asset processor error: {e}")
                
    async def _cleanup_expired_assets(self):
        """Clean up expired or orphaned assets"""
        while True:
            try:
                await asyncio.sleep(3600)  # Every hour
                
                # This would implement cleanup logic
                logger.info("Running asset cleanup")
                
            except Exception as e:
                logger.error(f"Asset cleanup error: {e}")


# Create app instance
asset_service = DigitalAssetService()
app = asset_service.app

# Set up lifespan
app.router.lifespan_context = asset_service.lifespan

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
