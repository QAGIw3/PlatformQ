"""
Digital Asset Lineage API

Provides RESTful endpoints for digital asset lineage tracking and analysis.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
from decimal import Decimal

from .asset_lineage import (
    DigitalAssetLineageTracker, AssetNode, ReviewNode, TransactionNode,
    AssetRelationType, AssetNodeType
)
from platformq_shared.api.deps import get_current_tenant_and_user

router = APIRouter(prefix="/api/v1/asset-lineage", tags=["Asset Lineage"])


# Request/Response Models
class AssetNodeRequest(BaseModel):
    """Request model for adding an asset node"""
    asset_id: str
    cid: str
    name: str
    asset_type: str
    owner_id: str
    size_bytes: int
    format: str
    version: str = "1.0"
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AssetDerivationRequest(BaseModel):
    """Request model for asset derivation"""
    child_id: str
    parent_id: str
    derivation_type: str = "derived"  # derived, fork, version


class ReviewNodeRequest(BaseModel):
    """Request model for adding a review"""
    review_id: str
    asset_id: str
    reviewer_id: str
    rating: int = Field(ge=1, le=5)
    review_type: str
    comments: Optional[str] = None
    verified: bool = False


class TransactionNodeRequest(BaseModel):
    """Request model for adding a transaction"""
    transaction_id: str
    asset_id: str
    buyer_id: str
    seller_id: str
    price: float
    currency: str
    transaction_type: str  # purchase, license
    blockchain_tx_hash: Optional[str] = None


class ImpactAnalysisRequest(BaseModel):
    """Request model for impact analysis"""
    asset_id: str
    change_type: str = "update"  # update, delete


# Dependency to get lineage tracker
from fastapi import Request

async def get_lineage_tracker(request: Request) -> DigitalAssetLineageTracker:
    """Get asset lineage tracker instance"""
    tracker = request.app.state.asset_lineage_tracker
    if tracker is None:
        raise HTTPException(
            status_code=503,
            detail="Asset lineage tracking is not available. JanusGraph may not be configured."
        )
    return tracker


@router.post("/assets")
async def add_asset(
    asset: AssetNodeRequest,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a new asset to the lineage graph"""
    try:
        asset_node = AssetNode(
            asset_id=asset.asset_id,
            cid=asset.cid,
            name=asset.name,
            asset_type=asset.asset_type,
            owner_id=asset.owner_id,
            created_at=datetime.utcnow(),
            size_bytes=asset.size_bytes,
            format=asset.format,
            version=asset.version,
            tags=asset.tags,
            metadata=asset.metadata
        )
        
        asset_id = await tracker.add_asset(asset_node)
        
        return {
            "status": "success",
            "asset_id": asset_id,
            "message": "Asset added to lineage graph"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/derivations")
async def add_derivation(
    derivation: AssetDerivationRequest,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add derivation relationship between assets"""
    try:
        success = await tracker.add_asset_derivation(
            derivation.child_id,
            derivation.parent_id,
            derivation.derivation_type
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Derivation added: {derivation.child_id} <- {derivation.parent_id}"
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to add derivation"
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reviews")
async def add_review(
    review: ReviewNodeRequest,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a review for an asset"""
    try:
        review_node = ReviewNode(
            review_id=review.review_id,
            asset_id=review.asset_id,
            reviewer_id=review.reviewer_id,
            rating=review.rating,
            review_type=review.review_type,
            created_at=datetime.utcnow(),
            comments=review.comments,
            verified=review.verified
        )
        
        review_id = await tracker.add_review(review_node)
        
        return {
            "status": "success",
            "review_id": review_id,
            "message": "Review added successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transactions")
async def add_transaction(
    transaction: TransactionNodeRequest,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a marketplace transaction"""
    try:
        tx_node = TransactionNode(
            transaction_id=transaction.transaction_id,
            asset_id=transaction.asset_id,
            buyer_id=transaction.buyer_id,
            seller_id=transaction.seller_id,
            price=transaction.price,
            currency=transaction.currency,
            transaction_type=transaction.transaction_type,
            timestamp=datetime.utcnow(),
            blockchain_tx_hash=transaction.blockchain_tx_hash
        )
        
        tx_id = await tracker.add_transaction(tx_node)
        
        return {
            "status": "success",
            "transaction_id": tx_id,
            "message": "Transaction recorded successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/{asset_id}/lineage")
async def get_asset_lineage(
    asset_id: str,
    depth: int = Query(default=3, ge=1, le=10),
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Get complete lineage for an asset"""
    try:
        lineage = await tracker.get_asset_lineage(asset_id, depth)
        
        if not lineage:
            raise HTTPException(
                status_code=404,
                detail=f"Asset {asset_id} not found"
            )
            
        return lineage
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/impact-analysis")
async def analyze_impact(
    request: ImpactAnalysisRequest,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Analyze impact of changes to an asset"""
    try:
        impact = await tracker.analyze_asset_impact(
            request.asset_id,
            request.change_type
        )
        
        return {
            "asset_id": request.asset_id,
            "change_type": request.change_type,
            "impact": {
                "affected_assets": impact.affected_assets,
                "affected_users": impact.affected_users,
                "downstream_count": impact.downstream_count,
                "impact_score": impact.impact_score,
                "recommendations": impact.recommendations
            },
            "analyzed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/duplicates/{cid}")
async def find_duplicates(
    cid: str,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Find potential duplicate assets by content ID"""
    try:
        duplicates = await tracker.find_duplicate_assets(cid)
        
        return {
            "cid": cid,
            "duplicate_count": len(duplicates),
            "duplicates": duplicates
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/users/{user_id}/reputation")
async def get_user_reputation(
    user_id: str,
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Get user reputation based on assets and activity"""
    try:
        reputation = await tracker.get_user_reputation(user_id)
        return reputation
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch/assets")
async def add_assets_batch(
    assets: List[AssetNodeRequest],
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add multiple assets in batch"""
    results = {
        "successful": [],
        "failed": []
    }
    
    for asset in assets:
        try:
            asset_node = AssetNode(
                asset_id=asset.asset_id,
                cid=asset.cid,
                name=asset.name,
                asset_type=asset.asset_type,
                owner_id=asset.owner_id,
                created_at=datetime.utcnow(),
                size_bytes=asset.size_bytes,
                format=asset.format,
                version=asset.version,
                tags=asset.tags,
                metadata=asset.metadata
            )
            
            await tracker.add_asset(asset_node)
            results["successful"].append(asset.asset_id)
            
        except Exception as e:
            results["failed"].append({
                "asset_id": asset.asset_id,
                "error": str(e)
            })
            
    return {
        "total": len(assets),
        "successful": len(results["successful"]),
        "failed": len(results["failed"]),
        "results": results
    }


@router.post("/batch/derivations")
async def add_derivations_batch(
    derivations: List[AssetDerivationRequest],
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add multiple derivations in batch"""
    results = {
        "successful": [],
        "failed": []
    }
    
    for derivation in derivations:
        try:
            success = await tracker.add_asset_derivation(
                derivation.child_id,
                derivation.parent_id,
                derivation.derivation_type
            )
            
            if success:
                results["successful"].append(f"{derivation.child_id}<-{derivation.parent_id}")
            else:
                results["failed"].append({
                    "derivation": f"{derivation.child_id}<-{derivation.parent_id}",
                    "error": "Failed to create derivation"
                })
                
        except Exception as e:
            results["failed"].append({
                "derivation": f"{derivation.child_id}<-{derivation.parent_id}",
                "error": str(e)
            })
            
    return {
        "total": len(derivations),
        "successful": len(results["successful"]),
        "failed": len(results["failed"]),
        "results": results
    }


@router.get("/stats")
async def get_lineage_stats(
    tracker: DigitalAssetLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Get statistics about the asset lineage graph"""
    try:
        # Count different node types
        asset_count = tracker.g.V().has("label", AssetNodeType.ASSET.value).count().next()
        user_count = tracker.g.V().has("label", AssetNodeType.USER.value).count().next()
        review_count = tracker.g.V().has("label", AssetNodeType.REVIEW.value).count().next()
        transaction_count = tracker.g.V().has("label", AssetNodeType.TRANSACTION.value).count().next()
        
        # Count relationships
        total_edges = tracker.g.E().count().next()
        
        # Get asset type distribution
        asset_types = {}
        type_counts = tracker.g.V().has("label", AssetNodeType.ASSET.value) \
            .groupCount().by("asset_type").next()
        
        return {
            "nodes": {
                "assets": asset_count,
                "users": user_count,
                "reviews": review_count,
                "transactions": transaction_count
            },
            "relationships": total_edges,
            "asset_types": type_counts,
            "graph_density": total_edges / max(asset_count + user_count, 1),
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 