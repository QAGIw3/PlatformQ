"""
Quota Manager for storage limits and usage tracking.
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class QuotaPolicy(str, Enum):
    """Quota enforcement policies."""
    SOFT = "soft"  # Warn but allow
    HARD = "hard"  # Block when exceeded
    METERED = "metered"  # Allow but charge extra


@dataclass
class TenantQuota:
    """Storage quota for a tenant."""
    tenant_id: str
    storage_bytes: int  # Total storage quota in bytes
    bandwidth_bytes: int  # Monthly bandwidth quota
    file_count: int  # Maximum number of files
    max_file_size: int  # Maximum size per file
    policy: QuotaPolicy = QuotaPolicy.HARD
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "tenant_id": self.tenant_id,
            "storage_bytes": self.storage_bytes,
            "bandwidth_bytes": self.bandwidth_bytes,
            "file_count": self.file_count,
            "max_file_size": self.max_file_size,
            "policy": self.policy.value,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class UsageStats:
    """Storage usage statistics."""
    tenant_id: str
    used_storage_bytes: int = 0
    used_bandwidth_bytes: int = 0
    file_count: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    # Detailed stats
    storage_by_type: Dict[str, int] = field(default_factory=dict)
    largest_files: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "tenant_id": self.tenant_id,
            "used_storage_bytes": self.used_storage_bytes,
            "used_bandwidth_bytes": self.used_bandwidth_bytes,
            "file_count": self.file_count,
            "last_updated": self.last_updated.isoformat(),
            "storage_by_type": self.storage_by_type,
            "largest_files": self.largest_files
        }


class QuotaManager:
    """
    Manages storage quotas and usage tracking.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        
        # In-memory quota cache
        self.quotas: Dict[str, TenantQuota] = {}
        self.usage_stats: Dict[str, UsageStats] = {}
        
        # Default quotas
        self.default_quota = TenantQuota(
            tenant_id="default",
            storage_bytes=10 * 1024 * 1024 * 1024,  # 10GB
            bandwidth_bytes=100 * 1024 * 1024 * 1024,  # 100GB/month
            file_count=10000,
            max_file_size=100 * 1024 * 1024  # 100MB
        )
        
        # Background tasks
        self._monitor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info("Quota Manager initialized")
        
    async def initialize(self):
        """Initialize quota manager."""
        # Subscribe to events
        await self.event_bus.subscribe("storage.uploaded", self._handle_storage_uploaded)
        await self.event_bus.subscribe("storage.downloaded", self._handle_storage_downloaded)
        await self.event_bus.subscribe("storage.deleted", self._handle_storage_deleted)
        
        # Load quotas from cache
        await self._load_quotas()
        
        # Start background tasks
        self._monitor_task = asyncio.create_task(self._monitor_usage())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_stats())
        
        logger.info("Quota Manager ready")
        
    async def cleanup(self):
        """Cleanup quota manager resources."""
        # Cancel background tasks
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Save quotas to cache
        await self._save_quotas()
        
        logger.info("Quota Manager cleaned up")
        
    async def set_quota(self, quota: TenantQuota) -> bool:
        """Set quota for a tenant."""
        try:
            # Store in memory
            self.quotas[quota.tenant_id] = quota
            
            # Cache quota
            await self.cache_manager.set(
                f"quota:tenant:{quota.tenant_id}",
                quota.to_dict(),
                ttl=None  # Permanent
            )
            
            # Publish event
            await self.event_bus.publish("quota.updated", {
                "tenant_id": quota.tenant_id,
                "storage_bytes": quota.storage_bytes,
                "policy": quota.policy.value
            })
            
            logger.info(f"Set quota for tenant {quota.tenant_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting quota: {e}")
            return False
            
    async def get_quota(self, tenant_id: str) -> TenantQuota:
        """Get quota for a tenant."""
        # Check memory cache
        if tenant_id in self.quotas:
            return self.quotas[tenant_id]
        
        # Check persistent cache
        cached = await self.cache_manager.get(f"quota:tenant:{tenant_id}")
        if cached:
            quota = TenantQuota(**cached)
            self.quotas[tenant_id] = quota
            return quota
        
        # Return default quota
        default = self.default_quota
        default.tenant_id = tenant_id
        return default
        
    async def get_usage(self, tenant_id: str) -> UsageStats:
        """Get usage statistics for a tenant."""
        # Check memory cache
        if tenant_id in self.usage_stats:
            return self.usage_stats[tenant_id]
        
        # Check persistent cache
        cached = await self.cache_manager.get(f"usage:tenant:{tenant_id}")
        if cached:
            usage = UsageStats(**cached)
            self.usage_stats[tenant_id] = usage
            return usage
        
        # Return empty usage
        return UsageStats(tenant_id=tenant_id)
        
    async def check_quota(
        self,
        tenant_id: str,
        file_size: int,
        operation: str = "upload"
    ) -> Dict[str, Any]:
        """Check if operation would exceed quota."""
        quota = await self.get_quota(tenant_id)
        usage = await self.get_usage(tenant_id)
        
        result = {
            "allowed": True,
            "reason": None,
            "quota": quota.to_dict(),
            "usage": usage.to_dict()
        }
        
        # Check file size limit
        if file_size > quota.max_file_size:
            result["allowed"] = False
            result["reason"] = f"File size {file_size} exceeds maximum {quota.max_file_size}"
            
        # Check storage quota
        elif usage.used_storage_bytes + file_size > quota.storage_bytes:
            if quota.policy == QuotaPolicy.HARD:
                result["allowed"] = False
                result["reason"] = f"Storage quota exceeded: {usage.used_storage_bytes + file_size} > {quota.storage_bytes}"
            elif quota.policy == QuotaPolicy.SOFT:
                result["warning"] = "Storage quota would be exceeded"
            elif quota.policy == QuotaPolicy.METERED:
                result["overage"] = usage.used_storage_bytes + file_size - quota.storage_bytes
                
        # Check file count quota
        elif operation == "upload" and usage.file_count >= quota.file_count:
            if quota.policy == QuotaPolicy.HARD:
                result["allowed"] = False
                result["reason"] = f"File count quota exceeded: {usage.file_count} >= {quota.file_count}"
            else:
                result["warning"] = "File count quota exceeded"
        
        # Log quota check
        if not result["allowed"]:
            logger.warning(f"Quota exceeded for tenant {tenant_id}: {result['reason']}")
            
            # Publish event
            await self.event_bus.publish("quota.exceeded", {
                "tenant_id": tenant_id,
                "operation": operation,
                "reason": result["reason"]
            })
        
        return result
        
    async def update_usage(
        self,
        tenant_id: str,
        storage_delta: int = 0,
        bandwidth_delta: int = 0,
        file_count_delta: int = 0,
        file_info: Optional[Dict[str, Any]] = None
    ):
        """Update usage statistics."""
        # Get current usage
        usage = await self.get_usage(tenant_id)
        
        # Update values
        usage.used_storage_bytes = max(0, usage.used_storage_bytes + storage_delta)
        usage.used_bandwidth_bytes = max(0, usage.used_bandwidth_bytes + bandwidth_delta)
        usage.file_count = max(0, usage.file_count + file_count_delta)
        usage.last_updated = datetime.utcnow()
        
        # Update storage by type if file info provided
        if file_info and storage_delta > 0:
            content_type = file_info.get("content_type", "unknown")
            usage.storage_by_type[content_type] = \
                usage.storage_by_type.get(content_type, 0) + storage_delta
            
            # Track largest files
            if storage_delta > 0:
                file_entry = {
                    "identifier": file_info.get("identifier"),
                    "size": storage_delta,
                    "filename": file_info.get("filename"),
                    "uploaded_at": datetime.utcnow().isoformat()
                }
                
                usage.largest_files.append(file_entry)
                usage.largest_files.sort(key=lambda x: x["size"], reverse=True)
                usage.largest_files = usage.largest_files[:10]  # Keep top 10
        
        # Store in memory
        self.usage_stats[tenant_id] = usage
        
        # Cache usage
        await self.cache_manager.set(
            f"usage:tenant:{tenant_id}",
            usage.to_dict(),
            ttl=3600  # 1 hour
        )
        
        # Check if approaching limits
        quota = await self.get_quota(tenant_id)
        storage_percent = (usage.used_storage_bytes / quota.storage_bytes * 100) if quota.storage_bytes > 0 else 0
        
        if storage_percent > 90:
            await self.event_bus.publish("quota.warning", {
                "tenant_id": tenant_id,
                "type": "storage",
                "percent_used": storage_percent
            })
            
        logger.debug(f"Updated usage for tenant {tenant_id}: storage={usage.used_storage_bytes}")
        
    async def reset_bandwidth_usage(self, tenant_id: str):
        """Reset monthly bandwidth usage."""
        usage = await self.get_usage(tenant_id)
        usage.used_bandwidth_bytes = 0
        
        # Update cache
        await self.cache_manager.set(
            f"usage:tenant:{tenant_id}",
            usage.to_dict(),
            ttl=3600
        )
        
        logger.info(f"Reset bandwidth usage for tenant {tenant_id}")
        
    async def get_all_tenant_usage(self) -> List[Dict[str, Any]]:
        """Get usage for all tenants."""
        all_usage = []
        
        # Get from cache
        tenant_keys = await self.cache_manager.keys("usage:tenant:*")
        
        for key in tenant_keys:
            usage_data = await self.cache_manager.get(key)
            if usage_data:
                usage = UsageStats(**usage_data)
                quota = await self.get_quota(usage.tenant_id)
                
                all_usage.append({
                    "tenant_id": usage.tenant_id,
                    "usage": usage.to_dict(),
                    "quota": quota.to_dict(),
                    "storage_percent": (usage.used_storage_bytes / quota.storage_bytes * 100) if quota.storage_bytes > 0 else 0
                })
        
        # Sort by storage usage
        all_usage.sort(key=lambda x: x["usage"]["used_storage_bytes"], reverse=True)
        
        return all_usage
        
    async def _load_quotas(self):
        """Load quotas from cache."""
        try:
            # Get all quota keys
            quota_keys = await self.cache_manager.keys("quota:tenant:*")
            
            for key in quota_keys:
                quota_data = await self.cache_manager.get(key)
                if quota_data:
                    quota = TenantQuota(**quota_data)
                    self.quotas[quota.tenant_id] = quota
            
            logger.info(f"Loaded {len(self.quotas)} tenant quotas")
            
        except Exception as e:
            logger.error(f"Error loading quotas: {e}")
            
    async def _save_quotas(self):
        """Save quotas to cache."""
        try:
            for tenant_id, quota in self.quotas.items():
                await self.cache_manager.set(
                    f"quota:tenant:{tenant_id}",
                    quota.to_dict(),
                    ttl=None
                )
            
            logger.info(f"Saved {len(self.quotas)} tenant quotas")
            
        except Exception as e:
            logger.error(f"Error saving quotas: {e}")
            
    async def _monitor_usage(self):
        """Monitor usage and enforce quotas."""
        while True:
            try:
                # Check all tenants
                for tenant_id in list(self.usage_stats.keys()):
                    usage = await self.get_usage(tenant_id)
                    quota = await self.get_quota(tenant_id)
                    
                    # Check storage usage
                    if usage.used_storage_bytes > quota.storage_bytes:
                        if quota.policy == QuotaPolicy.HARD:
                            # This would trigger cleanup or blocking
                            await self.event_bus.publish("quota.enforcement", {
                                "tenant_id": tenant_id,
                                "type": "storage",
                                "action": "block"
                            })
                        elif quota.policy == QuotaPolicy.METERED:
                            # Calculate overage
                            overage = usage.used_storage_bytes - quota.storage_bytes
                            await self.event_bus.publish("quota.overage", {
                                "tenant_id": tenant_id,
                                "type": "storage",
                                "overage_bytes": overage
                            })
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in usage monitoring: {e}")
                await asyncio.sleep(300)
                
    async def _cleanup_old_stats(self):
        """Clean up old statistics."""
        while True:
            try:
                # Reset monthly bandwidth on first day of month
                now = datetime.utcnow()
                if now.day == 1 and now.hour == 0:
                    for tenant_id in list(self.usage_stats.keys()):
                        await self.reset_bandwidth_usage(tenant_id)
                
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(3600)
                
    async def _handle_storage_uploaded(self, event_data: Dict[str, Any]):
        """Handle storage upload event."""
        try:
            await self.update_usage(
                tenant_id=event_data.get("tenant_id"),
                storage_delta=event_data.get("size", 0),
                file_count_delta=1,
                file_info=event_data
            )
            
        except Exception as e:
            logger.error(f"Error handling storage upload: {e}")
            
    async def _handle_storage_downloaded(self, event_data: Dict[str, Any]):
        """Handle storage download event."""
        try:
            await self.update_usage(
                tenant_id=event_data.get("tenant_id"),
                bandwidth_delta=event_data.get("size", 0)
            )
            
        except Exception as e:
            logger.error(f"Error handling storage download: {e}")
            
    async def _handle_storage_deleted(self, event_data: Dict[str, Any]):
        """Handle storage delete event."""
        try:
            # Need to get file size first
            # This would be retrieved from storage metadata
            file_size = event_data.get("size", 0)
            
            await self.update_usage(
                tenant_id=event_data.get("tenant_id"),
                storage_delta=-file_size,
                file_count_delta=-1
            )
            
        except Exception as e:
            logger.error(f"Error handling storage delete: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get quota manager statistics."""
        total_storage = sum(u.used_storage_bytes for u in self.usage_stats.values())
        total_bandwidth = sum(u.used_bandwidth_bytes for u in self.usage_stats.values())
        
        return {
            "total_tenants": len(self.quotas),
            "total_storage_used": total_storage,
            "total_bandwidth_used": total_bandwidth,
            "default_quota": self.default_quota.to_dict()
        } 