"""
Digital Asset Lineage Tracking

Implements comprehensive asset lineage tracking using JanusGraph including:
- Asset provenance and derivation chains
- Review and reputation tracking
- License and usage tracking
- Marketplace transaction history
- Asset relationship discovery
"""

import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict

from gremlin_python.process.traversal import T, P, Order
from gremlin_python.process.graph_traversal import __

logger = logging.getLogger(__name__)


class AssetRelationType(Enum):
    """Types of relationships between assets"""
    DERIVED_FROM = "derived_from"
    FORK_OF = "fork_of"
    VERSION_OF = "version_of"
    COMPONENT_OF = "component_of"
    REFERENCES = "references"
    REVIEWED_BY = "reviewed_by"
    PURCHASED_BY = "purchased_by"
    LICENSED_TO = "licensed_to"
    CREATED_BY = "created_by"


class AssetNodeType(Enum):
    """Types of nodes in asset lineage"""
    ASSET = "asset"
    USER = "user"
    REVIEW = "review"
    TRANSACTION = "transaction"
    LICENSE = "license"
    COLLECTION = "collection"


@dataclass
class AssetNode:
    """Represents a digital asset in the lineage graph"""
    asset_id: str
    cid: str  # Content ID
    name: str
    asset_type: str
    owner_id: str
    created_at: datetime
    size_bytes: int
    format: str
    version: str = "1.0"
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    quality_score: float = 0.0
    trust_score: float = 0.0


@dataclass
class ReviewNode:
    """Represents a review in the lineage graph"""
    review_id: str
    asset_id: str
    reviewer_id: str
    rating: int
    review_type: str
    created_at: datetime
    comments: Optional[str] = None
    verified: bool = False


@dataclass
class TransactionNode:
    """Represents a marketplace transaction"""
    transaction_id: str
    asset_id: str
    buyer_id: str
    seller_id: str
    price: float
    currency: str
    transaction_type: str
    timestamp: datetime
    blockchain_tx_hash: Optional[str] = None


@dataclass
class AssetLineageImpact:
    """Impact analysis for asset changes"""
    affected_assets: List[str]
    affected_users: List[str]
    downstream_count: int
    impact_score: float
    recommendations: List[str]


class DigitalAssetLineageTracker:
    """Tracks digital asset lineage and relationships"""
    
    def __init__(self, janusgraph_client):
        self.graph = janusgraph_client
        # Support both JanusGraphClient and raw Gremlin traversal objects
        if hasattr(janusgraph_client, 'traversal'):
            self.g = janusgraph_client.traversal()
        elif hasattr(janusgraph_client, 'g'):
            self.g = janusgraph_client.g
        else:
            self.g = janusgraph_client
        
    async def _initialize_schema(self):
        """Initialize graph schema for asset lineage"""
        try:
            # Only initialize schema if the graph client supports it
            if hasattr(self.graph, 'management'):
                mgmt = self.graph.management()
                
                # Create vertex labels
                for node_type in AssetNodeType:
                    if not mgmt.vertex_label_exists(node_type.value):
                        mgmt.make_vertex_label(node_type.value)
                        
                # Create edge labels
                for rel_type in AssetRelationType:
                    if not mgmt.edge_label_exists(rel_type.value):
                        mgmt.make_edge_label(rel_type.value)
                        
                # Property keys
                properties = [
                    "asset_id", "cid", "name", "asset_type", "owner_id",
                    "created_at", "updated_at", "size_bytes", "format", "version",
                    "tags", "metadata", "quality_score", "trust_score",
                    "review_id", "reviewer_id", "rating", "review_type",
                    "transaction_id", "buyer_id", "seller_id", "price",
                    "currency", "transaction_type", "blockchain_tx_hash"
                ]
                
                for prop in properties:
                    if not mgmt.property_key_exists(prop):
                        mgmt.make_property_key(prop).datatype(str).single()
                        
                # Indexes
                if not mgmt.index_exists("asset_by_id"):
                    mgmt.build_index("asset_by_id", "vertex").add_key("asset_id").unique().build()
                if not mgmt.index_exists("asset_by_cid"):
                    mgmt.build_index("asset_by_cid", "vertex").add_key("cid").build()
                if not mgmt.index_exists("asset_by_owner"):
                    mgmt.build_index("asset_by_owner", "vertex").add_key("owner_id").build()
                if not mgmt.index_exists("asset_by_type"):
                    mgmt.build_index("asset_by_type", "vertex").add_key("asset_type").build()
                    
                mgmt.commit()
            else:
                logger.warning("Graph client does not support schema management, skipping schema initialization")
            logger.info("Asset lineage schema initialized")
            
        except Exception as e:
            logger.error(f"Error initializing schema: {e}")
            
    async def add_asset(self, asset: AssetNode) -> str:
        """Add an asset to the lineage graph"""
        try:
            vertex = self.g.addV(AssetNodeType.ASSET.value) \
                .property("asset_id", asset.asset_id) \
                .property("cid", asset.cid) \
                .property("name", asset.name) \
                .property("asset_type", asset.asset_type) \
                .property("owner_id", asset.owner_id) \
                .property("created_at", asset.created_at.isoformat()) \
                .property("size_bytes", str(asset.size_bytes)) \
                .property("format", asset.format) \
                .property("version", asset.version) \
                .property("tags", json.dumps(asset.tags)) \
                .property("metadata", json.dumps(asset.metadata)) \
                .property("quality_score", str(asset.quality_score)) \
                .property("trust_score", str(asset.trust_score)) \
                .next()
                
            # Create creator relationship
            self._create_creator_relationship(asset.asset_id, asset.owner_id)
            
            logger.info(f"Added asset {asset.asset_id} to lineage graph")
            return asset.asset_id
            
        except Exception as e:
            logger.error(f"Error adding asset: {e}")
            raise
            
    def _create_creator_relationship(self, asset_id: str, user_id: str):
        """Create relationship between asset and creator"""
        try:
            # Ensure user node exists
            user = self.g.V().has("user_id", user_id).fold().coalesce(
                __.unfold(),
                __.addV(AssetNodeType.USER.value).property("user_id", user_id)
            ).next()
            
            # Create relationship
            asset = self.g.V().has("asset_id", asset_id).next()
            self.g.V(asset).addE(AssetRelationType.CREATED_BY.value).to(user).iterate()
            
        except Exception as e:
            logger.error(f"Error creating creator relationship: {e}")
            
    async def add_asset_derivation(self, child_id: str, parent_id: str,
                                 derivation_type: str = "derived") -> bool:
        """Add derivation relationship between assets"""
        try:
            parent = self.g.V().has("asset_id", parent_id).next()
            child = self.g.V().has("asset_id", child_id).next()
            
            rel_type = AssetRelationType.DERIVED_FROM
            if derivation_type == "fork":
                rel_type = AssetRelationType.FORK_OF
            elif derivation_type == "version":
                rel_type = AssetRelationType.VERSION_OF
                
            self.g.V(child).addE(rel_type.value) \
                .to(parent) \
                .property("created_at", datetime.utcnow().isoformat()) \
                .property("derivation_type", derivation_type) \
                .iterate()
                
            logger.info(f"Added derivation: {child_id} {rel_type.value} {parent_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding derivation: {e}")
            return False
            
    async def add_review(self, review: ReviewNode) -> str:
        """Add a review to the lineage graph"""
        try:
            # Create review node
            review_vertex = self.g.addV(AssetNodeType.REVIEW.value) \
                .property("review_id", review.review_id) \
                .property("asset_id", review.asset_id) \
                .property("reviewer_id", review.reviewer_id) \
                .property("rating", str(review.rating)) \
                .property("review_type", review.review_type) \
                .property("created_at", review.created_at.isoformat()) \
                .property("comments", review.comments or "") \
                .property("verified", str(review.verified)) \
                .next()
                
            # Create relationships
            asset = self.g.V().has("asset_id", review.asset_id).next()
            reviewer = self.g.V().has("user_id", review.reviewer_id).fold().coalesce(
                __.unfold(),
                __.addV(AssetNodeType.USER.value).property("user_id", review.reviewer_id)
            ).next()
            
            self.g.V(asset).addE(AssetRelationType.REVIEWED_BY.value) \
                .to(reviewer) \
                .property("review_id", review.review_id) \
                .property("rating", str(review.rating)) \
                .iterate()
                
            # Update asset quality score
            await self._update_asset_quality_score(review.asset_id)
            
            logger.info(f"Added review {review.review_id} for asset {review.asset_id}")
            return review.review_id
            
        except Exception as e:
            logger.error(f"Error adding review: {e}")
            raise
            
    async def _update_asset_quality_score(self, asset_id: str):
        """Update asset quality score based on reviews"""
        try:
            # Get all reviews for the asset
            reviews = self.g.V().has("asset_id", asset_id) \
                .in_(AssetRelationType.REVIEWED_BY.value) \
                .values("rating") \
                .toList()
                
            if reviews:
                avg_rating = sum(float(r) for r in reviews) / len(reviews)
                quality_score = avg_rating / 5.0  # Normalize to 0-1
                
                self.g.V().has("asset_id", asset_id) \
                    .property("quality_score", str(quality_score)) \
                    .iterate()
                    
        except Exception as e:
            logger.error(f"Error updating quality score: {e}")
            
    async def add_transaction(self, transaction: TransactionNode) -> str:
        """Add a marketplace transaction"""
        try:
            # Create transaction node
            tx_vertex = self.g.addV(AssetNodeType.TRANSACTION.value) \
                .property("transaction_id", transaction.transaction_id) \
                .property("asset_id", transaction.asset_id) \
                .property("buyer_id", transaction.buyer_id) \
                .property("seller_id", transaction.seller_id) \
                .property("price", str(transaction.price)) \
                .property("currency", transaction.currency) \
                .property("transaction_type", transaction.transaction_type) \
                .property("timestamp", transaction.timestamp.isoformat()) \
                .property("blockchain_tx_hash", transaction.blockchain_tx_hash or "") \
                .next()
                
            # Create relationships
            asset = self.g.V().has("asset_id", transaction.asset_id).next()
            buyer = self.g.V().has("user_id", transaction.buyer_id).fold().coalesce(
                __.unfold(),
                __.addV(AssetNodeType.USER.value).property("user_id", transaction.buyer_id)
            ).next()
            
            if transaction.transaction_type == "purchase":
                rel_type = AssetRelationType.PURCHASED_BY
            else:
                rel_type = AssetRelationType.LICENSED_TO
                
            self.g.V(asset).addE(rel_type.value) \
                .to(buyer) \
                .property("transaction_id", transaction.transaction_id) \
                .property("timestamp", transaction.timestamp.isoformat()) \
                .iterate()
                
            # Update asset trust score
            await self._update_asset_trust_score(transaction.asset_id)
            
            logger.info(f"Added transaction {transaction.transaction_id}")
            return transaction.transaction_id
            
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            raise
            
    async def _update_asset_trust_score(self, asset_id: str):
        """Update asset trust score based on transactions"""
        try:
            # Count transactions
            tx_count = self.g.V().has("asset_id", asset_id) \
                .out(AssetRelationType.PURCHASED_BY.value, AssetRelationType.LICENSED_TO.value) \
                .count().next()
                
            # Simple trust score calculation
            trust_score = min(1.0, tx_count / 10.0)  # Normalize to 0-1
            
            self.g.V().has("asset_id", asset_id) \
                .property("trust_score", str(trust_score)) \
                .iterate()
                
        except Exception as e:
            logger.error(f"Error updating trust score: {e}")
            
    async def get_asset_lineage(self, asset_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get complete lineage for an asset"""
        try:
            # Get asset details
            asset = self.g.V().has("asset_id", asset_id).next()
            asset_data = self._vertex_to_dict(asset)
            
            # Get provenance (ancestors)
            provenance = self._trace_provenance(asset_id, depth)
            
            # Get derivatives (descendants)
            derivatives = self._trace_derivatives(asset_id, depth)
            
            # Get reviews
            reviews = self._get_asset_reviews(asset_id)
            
            # Get transactions
            transactions = self._get_asset_transactions(asset_id)
            
            # Get related assets
            related = self._find_related_assets(asset_id)
            
            return {
                "asset": asset_data,
                "provenance": provenance,
                "derivatives": derivatives,
                "reviews": reviews,
                "transactions": transactions,
                "related_assets": related,
                "lineage_depth": depth,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting asset lineage: {e}")
            return {}
            
    def _trace_provenance(self, asset_id: str, depth: int) -> List[Dict[str, Any]]:
        """Trace asset provenance (ancestors)"""
        provenance = []
        
        try:
            paths = self.g.V().has("asset_id", asset_id) \
                .repeat(__.in_(
                    AssetRelationType.DERIVED_FROM.value,
                    AssetRelationType.FORK_OF.value,
                    AssetRelationType.VERSION_OF.value
                ).simplePath()) \
                .times(depth) \
                .path() \
                .toList()
                
            for path in paths:
                path_data = []
                for vertex in path:
                    if hasattr(vertex, "label") and vertex.label == AssetNodeType.ASSET.value:
                        path_data.append(self._vertex_to_dict(vertex))
                if path_data:
                    provenance.append(path_data)
                    
        except Exception as e:
            logger.error(f"Error tracing provenance: {e}")
            
        return provenance
        
    def _trace_derivatives(self, asset_id: str, depth: int) -> List[Dict[str, Any]]:
        """Trace asset derivatives (descendants)"""
        derivatives = []
        
        try:
            # Find all derivatives
            derivative_vertices = self.g.V().has("asset_id", asset_id) \
                .repeat(__.out_(
                    AssetRelationType.DERIVED_FROM.value,
                    AssetRelationType.FORK_OF.value,
                    AssetRelationType.VERSION_OF.value
                ).simplePath()) \
                .times(depth) \
                .toList()
                
            for vertex in derivative_vertices:
                derivatives.append(self._vertex_to_dict(vertex))
                
        except Exception as e:
            logger.error(f"Error tracing derivatives: {e}")
            
        return derivatives
        
    def _get_asset_reviews(self, asset_id: str) -> List[Dict[str, Any]]:
        """Get all reviews for an asset"""
        reviews = []
        
        try:
            review_edges = self.g.V().has("asset_id", asset_id) \
                .inE(AssetRelationType.REVIEWED_BY.value) \
                .toList()
                
            for edge in review_edges:
                review_data = {
                    "review_id": self.g.E(edge).values("review_id").next(),
                    "rating": float(self.g.E(edge).values("rating").next()),
                    "reviewer_id": self.g.E(edge).outV().values("user_id").next(),
                    "timestamp": self.g.E(edge).values("created_at").next()
                }
                reviews.append(review_data)
                
        except Exception as e:
            logger.error(f"Error getting reviews: {e}")
            
        return reviews
        
    def _get_asset_transactions(self, asset_id: str) -> List[Dict[str, Any]]:
        """Get all transactions for an asset"""
        transactions = []
        
        try:
            tx_edges = self.g.V().has("asset_id", asset_id) \
                .outE(AssetRelationType.PURCHASED_BY.value, AssetRelationType.LICENSED_TO.value) \
                .toList()
                
            for edge in tx_edges:
                tx_data = {
                    "transaction_id": self.g.E(edge).values("transaction_id").next(),
                    "type": edge.label,
                    "buyer_id": self.g.E(edge).inV().values("user_id").next(),
                    "timestamp": self.g.E(edge).values("timestamp").next()
                }
                transactions.append(tx_data)
                
        except Exception as e:
            logger.error(f"Error getting transactions: {e}")
            
        return transactions
        
    def _find_related_assets(self, asset_id: str) -> List[Dict[str, Any]]:
        """Find assets related by tags or type"""
        related = []
        
        try:
            # Get asset properties
            asset = self.g.V().has("asset_id", asset_id).next()
            asset_type = self.g.V(asset).values("asset_type").next()
            tags = json.loads(self.g.V(asset).values("tags").next())
            
            # Find assets with similar tags
            if tags:
                similar = self.g.V().has("label", AssetNodeType.ASSET.value) \
                    .has("asset_id", P.neq(asset_id)) \
                    .filter(__.values("tags").filter(
                        lambda t: any(tag in json.loads(t) for tag in tags)
                    )) \
                    .limit(10) \
                    .toList()
                    
                for vertex in similar:
                    related.append(self._vertex_to_dict(vertex))
                    
        except Exception as e:
            logger.error(f"Error finding related assets: {e}")
            
        return related
        
    async def analyze_asset_impact(self, asset_id: str, change_type: str = "update") -> AssetLineageImpact:
        """Analyze impact of changes to an asset"""
        try:
            # Find all affected downstream assets
            affected_assets = set()
            derivatives = self.g.V().has("asset_id", asset_id) \
                .repeat(__.out_(
                    AssetRelationType.DERIVED_FROM.value,
                    AssetRelationType.FORK_OF.value
                ).simplePath()) \
                .emit() \
                .values("asset_id") \
                .toList()
                
            affected_assets.update(derivatives)
            
            # Find affected users
            affected_users = set()
            for derivative_id in affected_assets:
                users = self.g.V().has("asset_id", derivative_id) \
                    .out(AssetRelationType.PURCHASED_BY.value, AssetRelationType.LICENSED_TO.value) \
                    .values("user_id") \
                    .toList()
                affected_users.update(users)
                
            # Calculate impact score
            impact_score = len(affected_assets) * 0.1 + len(affected_users) * 0.05
            impact_score = min(1.0, impact_score)
            
            # Generate recommendations
            recommendations = []
            if impact_score > 0.7:
                recommendations.append("Notify all affected users before making changes")
                recommendations.append("Consider versioning instead of updating")
            elif impact_score > 0.3:
                recommendations.append("Review derivative assets for compatibility")
                
            if change_type == "delete":
                recommendations.append("Archive asset before deletion")
                recommendations.append("Provide migration path for dependents")
                
            return AssetLineageImpact(
                affected_assets=list(affected_assets),
                affected_users=list(affected_users),
                downstream_count=len(affected_assets),
                impact_score=impact_score,
                recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Error analyzing asset impact: {e}")
            return AssetLineageImpact([], [], 0, 0.0, [])
            
    async def find_duplicate_assets(self, cid: str) -> List[Dict[str, Any]]:
        """Find potential duplicate assets by content ID"""
        duplicates = []
        
        try:
            # Find all assets with same CID
            assets = self.g.V().has("label", AssetNodeType.ASSET.value) \
                .has("cid", cid) \
                .toList()
                
            for asset in assets:
                asset_data = self._vertex_to_dict(asset)
                # Get owner info
                owner = self.g.V(asset).out(AssetRelationType.CREATED_BY.value) \
                    .values("user_id").next()
                asset_data["owner_id"] = owner
                duplicates.append(asset_data)
                
        except Exception as e:
            logger.error(f"Error finding duplicate assets: {e}")
            
        return duplicates
        
    async def get_user_reputation(self, user_id: str) -> Dict[str, Any]:
        """Calculate user reputation based on assets and reviews"""
        try:
            # Count created assets
            assets_created = self.g.V().has("user_id", user_id) \
                .in_(AssetRelationType.CREATED_BY.value) \
                .count().next()
                
            # Get average quality score of assets
            quality_scores = self.g.V().has("user_id", user_id) \
                .in_(AssetRelationType.CREATED_BY.value) \
                .values("quality_score") \
                .toList()
                
            avg_quality = sum(float(s) for s in quality_scores) / len(quality_scores) if quality_scores else 0
            
            # Count reviews given
            reviews_given = self.g.V().has("user_id", user_id) \
                .out(AssetRelationType.REVIEWED_BY.value) \
                .count().next()
                
            # Count purchases
            purchases = self.g.V().has("user_id", user_id) \
                .in_(AssetRelationType.PURCHASED_BY.value, AssetRelationType.LICENSED_TO.value) \
                .count().next()
                
            # Calculate reputation score
            reputation_score = (
                assets_created * 0.3 +
                avg_quality * 10 +
                reviews_given * 0.1 +
                purchases * 0.2
            ) / 10.0
            
            reputation_score = min(1.0, reputation_score)
            
            return {
                "user_id": user_id,
                "reputation_score": reputation_score,
                "assets_created": assets_created,
                "average_quality": avg_quality,
                "reviews_given": reviews_given,
                "purchases_made": purchases,
                "reputation_level": self._get_reputation_level(reputation_score)
            }
            
        except Exception as e:
            logger.error(f"Error getting user reputation: {e}")
            return {"user_id": user_id, "reputation_score": 0.0}
            
    def _get_reputation_level(self, score: float) -> str:
        """Convert reputation score to level"""
        if score >= 0.9:
            return "expert"
        elif score >= 0.7:
            return "trusted"
        elif score >= 0.5:
            return "established"
        elif score >= 0.3:
            return "contributor"
        return "newcomer"
        
    def _vertex_to_dict(self, vertex) -> Dict[str, Any]:
        """Convert vertex to dictionary"""
        result = {
            "id": str(vertex.id),
            "label": vertex.label
        }
        
        # Get all properties
        properties = self.g.V(vertex).properties().toList()
        for prop in properties:
            key = prop.key
            value = prop.value
            
            # Parse JSON properties
            if key in ["tags", "metadata"]:
                try:
                    value = json.loads(value)
                except:
                    pass
                    
            # Parse numeric properties
            if key in ["size_bytes", "rating", "price", "quality_score", "trust_score"]:
                try:
                    value = float(value)
                except:
                    pass
                    
            result[key] = value
            
        return result 