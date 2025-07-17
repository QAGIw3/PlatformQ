"""
Model Versioning Marketplace

Sell access to specific model versions with different pricing tiers
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
import httpx
from enum import Enum

logger = logging.getLogger(__name__)


class VersionAccessType(Enum):
    LATEST_ONLY = "latest_only"
    SPECIFIC_VERSION = "specific_version"
    VERSION_RANGE = "version_range"
    ALL_VERSIONS = "all_versions"
    BETA_ACCESS = "beta_access"


@dataclass
class VersionLicense:
    license_id: str
    model_name: str
    tenant_id: str
    buyer_id: str
    access_type: VersionAccessType
    allowed_versions: List[str]
    start_date: datetime
    end_date: Optional[datetime]
    usage_limit: Optional[int]
    usage_count: int
    price_paid: float
    is_active: bool


@dataclass
class VersionPricingTier:
    tier_id: str
    name: str
    description: str
    access_type: VersionAccessType
    price_multiplier: float
    features: List[str]
    limitations: Dict[str, Any]


class ModelVersioningMarketplace:
    """Manages version-specific access and pricing for models"""
    
    def __init__(self,
                 mlflow_url: str = "http://mlflow:5000",
                 digital_asset_service_url: str = "http://digital-asset-service:8000",
                 blockchain_service_url: str = "http://blockchain-event-bridge:8000"):
        self.mlflow_url = mlflow_url
        self.digital_asset_service_url = digital_asset_service_url
        self.blockchain_service_url = blockchain_service_url
        
        # Version licenses
        self.version_licenses: Dict[str, VersionLicense] = {}
        
        # Pricing tiers
        self.pricing_tiers = self._initialize_pricing_tiers()
        
        # Model version metadata
        self.version_metadata: Dict[str, Dict[str, Any]] = {}
        
    def _initialize_pricing_tiers(self) -> Dict[str, VersionPricingTier]:
        """Initialize default pricing tiers"""
        return {
            "basic": VersionPricingTier(
                tier_id="basic",
                name="Basic",
                description="Access to latest stable version only",
                access_type=VersionAccessType.LATEST_ONLY,
                price_multiplier=1.0,
                features=[
                    "Latest stable version",
                    "Basic support",
                    "Usage analytics"
                ],
                limitations={
                    "max_requests_per_day": 1000,
                    "support_response_time": "48h",
                    "beta_access": False
                }
            ),
            "professional": VersionPricingTier(
                tier_id="professional",
                name="Professional",
                description="Access to specific versions with version pinning",
                access_type=VersionAccessType.SPECIFIC_VERSION,
                price_multiplier=2.0,
                features=[
                    "Version pinning",
                    "Priority support",
                    "Advanced analytics",
                    "Version rollback"
                ],
                limitations={
                    "max_requests_per_day": 10000,
                    "support_response_time": "24h",
                    "max_pinned_versions": 3,
                    "beta_access": False
                }
            ),
            "enterprise": VersionPricingTier(
                tier_id="enterprise",
                name="Enterprise",
                description="Full version access with beta features",
                access_type=VersionAccessType.ALL_VERSIONS,
                price_multiplier=5.0,
                features=[
                    "All versions access",
                    "Beta version access",
                    "24/7 priority support",
                    "Custom SLA",
                    "Version migration assistance"
                ],
                limitations={
                    "max_requests_per_day": -1,  # Unlimited
                    "support_response_time": "1h",
                    "beta_access": True,
                    "custom_deployment": True
                }
            )
        }
        
    async def create_version_listing(self,
                                   model_name: str,
                                   tenant_id: str,
                                   version: str,
                                   version_metadata: Dict[str, Any]) -> str:
        """Create a listing for a specific model version"""
        listing_id = f"{model_name}_v{version}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        # Store version metadata
        version_key = f"{tenant_id}:{model_name}:{version}"
        self.version_metadata[version_key] = {
            "listing_id": listing_id,
            "model_name": model_name,
            "version": version,
            "tenant_id": tenant_id,
            "created_at": datetime.utcnow(),
            "performance_metrics": version_metadata.get("metrics", {}),
            "changelog": version_metadata.get("changelog", ""),
            "breaking_changes": version_metadata.get("breaking_changes", False),
            "deprecated": version_metadata.get("deprecated", False),
            "end_of_life": version_metadata.get("end_of_life"),
            "dependencies": version_metadata.get("dependencies", []),
            "hardware_requirements": version_metadata.get("hardware_requirements", {}),
            "api_changes": version_metadata.get("api_changes", [])
        }
        
        # Create blockchain listing
        await self._create_blockchain_listing(listing_id, model_name, version, tenant_id)
        
        logger.info(f"Created version listing {listing_id} for {model_name} v{version}")
        return listing_id
        
    async def purchase_version_access(self,
                                    model_name: str,
                                    seller_tenant_id: str,
                                    buyer_id: str,
                                    buyer_tenant_id: str,
                                    tier_id: str,
                                    duration_days: Optional[int] = None,
                                    specific_versions: Optional[List[str]] = None) -> str:
        """Purchase access to model versions based on tier"""
        tier = self.pricing_tiers.get(tier_id)
        if not tier:
            raise ValueError(f"Invalid pricing tier: {tier_id}")
            
        # Calculate price
        base_price = await self._get_model_base_price(model_name, seller_tenant_id)
        final_price = base_price * tier.price_multiplier
        
        # Determine allowed versions
        if tier.access_type == VersionAccessType.LATEST_ONLY:
            allowed_versions = ["latest"]
        elif tier.access_type == VersionAccessType.SPECIFIC_VERSION:
            allowed_versions = specific_versions or ["latest"]
        elif tier.access_type == VersionAccessType.ALL_VERSIONS:
            allowed_versions = await self._get_all_model_versions(model_name, seller_tenant_id)
        else:
            allowed_versions = specific_versions or []
            
        # Create license
        license_id = f"vl_{model_name}_{buyer_tenant_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        license = VersionLicense(
            license_id=license_id,
            model_name=model_name,
            tenant_id=seller_tenant_id,
            buyer_id=buyer_id,
            access_type=tier.access_type,
            allowed_versions=allowed_versions,
            start_date=datetime.utcnow(),
            end_date=datetime.utcnow() + timedelta(days=duration_days) if duration_days else None,
            usage_limit=tier.limitations.get("max_requests_per_day", -1) * (duration_days or 365),
            usage_count=0,
            price_paid=final_price,
            is_active=True
        )
        
        self.version_licenses[license_id] = license
        
        # Process blockchain transaction
        await self._process_version_purchase(license, final_price)
        
        logger.info(f"Created version license {license_id} for {buyer_id}")
        return license_id
        
    async def check_version_access(self,
                                 model_name: str,
                                 version: str,
                                 tenant_id: str,
                                 user_id: str) -> bool:
        """Check if user has access to specific model version"""
        # Find active licenses
        for license in self.version_licenses.values():
            if (license.model_name == model_name and
                license.buyer_id == user_id and
                license.tenant_id == tenant_id and
                license.is_active):
                
                # Check expiration
                if license.end_date and datetime.utcnow() > license.end_date:
                    license.is_active = False
                    continue
                    
                # Check usage limit
                if license.usage_limit > 0 and license.usage_count >= license.usage_limit:
                    license.is_active = False
                    continue
                    
                # Check version access
                if license.access_type == VersionAccessType.ALL_VERSIONS:
                    return True
                elif license.access_type == VersionAccessType.LATEST_ONLY:
                    latest_version = await self._get_latest_version(model_name, tenant_id)
                    return version == latest_version
                elif version in license.allowed_versions:
                    return True
                    
        return False
        
    async def record_version_usage(self,
                                 model_name: str,
                                 version: str,
                                 tenant_id: str,
                                 user_id: str):
        """Record usage of a model version"""
        for license in self.version_licenses.values():
            if (license.model_name == model_name and
                license.buyer_id == user_id and
                license.tenant_id == tenant_id and
                license.is_active):
                
                license.usage_count += 1
                
                # Check if license should be deactivated
                if license.usage_limit > 0 and license.usage_count >= license.usage_limit:
                    license.is_active = False
                    await self._notify_license_expiration(license)
                    
                break
                
    async def get_version_changelog(self,
                                  model_name: str,
                                  from_version: str,
                                  to_version: str,
                                  tenant_id: str) -> Dict[str, Any]:
        """Get changelog between model versions"""
        changelog = {
            "from_version": from_version,
            "to_version": to_version,
            "changes": [],
            "breaking_changes": [],
            "performance_improvements": [],
            "bug_fixes": [],
            "new_features": []
        }
        
        # Get all versions between from and to
        all_versions = await self._get_all_model_versions(model_name, tenant_id)
        
        from_idx = all_versions.index(from_version) if from_version in all_versions else -1
        to_idx = all_versions.index(to_version) if to_version in all_versions else -1
        
        if from_idx >= 0 and to_idx >= 0 and from_idx < to_idx:
            for i in range(from_idx + 1, to_idx + 1):
                version_key = f"{tenant_id}:{model_name}:{all_versions[i]}"
                if version_key in self.version_metadata:
                    metadata = self.version_metadata[version_key]
                    
                    if metadata.get("breaking_changes"):
                        changelog["breaking_changes"].append({
                            "version": all_versions[i],
                            "changes": metadata.get("api_changes", [])
                        })
                        
                    # Add other changes
                    changelog["changes"].append({
                        "version": all_versions[i],
                        "description": metadata.get("changelog", ""),
                        "date": metadata.get("created_at", datetime.utcnow()).isoformat()
                    })
                    
        return changelog
        
    async def get_version_migration_guide(self,
                                        model_name: str,
                                        from_version: str,
                                        to_version: str,
                                        tenant_id: str) -> Dict[str, Any]:
        """Generate migration guide between versions"""
        changelog = await self.get_version_changelog(model_name, from_version, to_version, tenant_id)
        
        migration_guide = {
            "from_version": from_version,
            "to_version": to_version,
            "estimated_effort": "low",  # Calculate based on changes
            "steps": [],
            "code_changes_required": [],
            "testing_recommendations": []
        }
        
        # Analyze breaking changes
        if changelog["breaking_changes"]:
            migration_guide["estimated_effort"] = "high"
            
            for change in changelog["breaking_changes"]:
                migration_guide["steps"].append({
                    "step": f"Update API calls for version {change['version']}",
                    "description": f"Breaking changes in {change['version']}",
                    "required": True
                })
                
                migration_guide["code_changes_required"].extend(change["changes"])
                
        # Add testing recommendations
        migration_guide["testing_recommendations"] = [
            "Run regression tests on all endpoints",
            "Validate model outputs match expected format",
            "Performance test to ensure no degradation",
            "Test error handling for new API changes"
        ]
        
        return migration_guide
        
    async def _get_model_base_price(self, model_name: str, tenant_id: str) -> float:
        """Get base price for model"""
        # Query digital asset service
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.digital_asset_service_url}/api/v1/models/{model_name}/pricing",
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json().get("base_price", 0.1)
                
        return 0.1  # Default price
        
    async def _get_all_model_versions(self, model_name: str, tenant_id: str) -> List[str]:
        """Get all versions of a model"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.mlflow_url}/api/2.0/mlflow/registered-models/get",
                params={"name": f"{tenant_id}_{model_name}"}
            )
            
            if response.status_code == 200:
                model_info = response.json()
                versions = []
                
                for version in model_info.get("registered_model", {}).get("latest_versions", []):
                    versions.append(version["version"])
                    
                return sorted(versions)
                
        return []
        
    async def _get_latest_version(self, model_name: str, tenant_id: str) -> Optional[str]:
        """Get latest version of a model"""
        versions = await self._get_all_model_versions(model_name, tenant_id)
        return versions[-1] if versions else None
        
    async def _create_blockchain_listing(self, listing_id: str, model_name: str, version: str, tenant_id: str):
        """Create blockchain listing for version"""
        # Implementation would interact with blockchain service
        pass
        
    async def _process_version_purchase(self, license: VersionLicense, price: float):
        """Process version purchase on blockchain"""
        # Implementation would interact with blockchain service
        pass
        
    async def _notify_license_expiration(self, license: VersionLicense):
        """Notify user of license expiration"""
        logger.info(f"License {license.license_id} expired for user {license.buyer_id}")
        # Implementation would send notification 