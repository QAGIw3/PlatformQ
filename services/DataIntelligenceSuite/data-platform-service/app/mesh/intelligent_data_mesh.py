"""
Intelligent Data Mesh Federation

Self-organizing data mesh that:
- Auto-discovers data products across services
- Negotiates optimal data placement
- Predicts access patterns
- Pre-fetches and pre-computes based on ML predictions
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import DBSCAN
import networkx as nx
import structlog

from ..dih.digital_integration_hub import DigitalIntegrationHub, CacheRegion, CacheStrategy
from ..lake.medallion_architecture import MedallionLakeManager
from ..federation.federated_query_engine import FederatedQueryEngine
from ..lineage.lineage_tracker import DataLineageTracker
from ..core.cache_manager import DataCacheManager

logger = structlog.get_logger()


class DataProductType(Enum):
    """Types of data products in the mesh"""
    STREAM = "stream"
    BATCH = "batch"
    ANALYTICAL = "analytical"
    OPERATIONAL = "operational"
    REFERENCE = "reference"


@dataclass
class DataProduct:
    """Data product in the mesh"""
    product_id: str
    name: str
    type: DataProductType
    owner: str
    location: str
    schema: Dict[str, Any]
    quality_score: float
    access_patterns: List[Dict[str, Any]]
    dependencies: List[str]
    metadata: Dict[str, Any]


@dataclass
class AccessPattern:
    """Data access pattern"""
    pattern_id: str
    data_products: List[str]
    frequency: float  # Accesses per hour
    latency_requirement: float  # ms
    user_segments: List[str]
    time_patterns: Dict[str, float]  # Hour of day -> probability
    predicted_growth: float


class IntelligentDataMesh:
    """
    Self-organizing data mesh with ML-driven optimization
    """
    
    def __init__(self,
                 dih: DigitalIntegrationHub,
                 lake_manager: MedallionLakeManager,
                 federated_engine: FederatedQueryEngine,
                 lineage_tracker: DataLineageTracker,
                 cache_manager: DataCacheManager):
        self.dih = dih
        self.lake_manager = lake_manager
        self.federated_engine = federated_engine
        self.lineage_tracker = lineage_tracker
        self.cache_manager = cache_manager
        
        # Data product registry
        self.data_products: Dict[str, DataProduct] = {}
        self.product_graph = nx.DiGraph()
        
        # Access pattern learning
        self.access_patterns: Dict[str, AccessPattern] = {}
        self.access_history: List[Dict[str, Any]] = []
        self.pattern_model = None
        
        # Placement optimization
        self.placement_optimizer = None
        self.current_placements: Dict[str, List[str]] = {}  # product_id -> locations
        
        # Background tasks
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
    async def start(self):
        """Start the intelligent data mesh"""
        self._running = True
        
        # Initialize ML models
        await self._initialize_models()
        
        # Start background tasks
        self._tasks.append(
            asyncio.create_task(self._discovery_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._optimization_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._prefetch_loop())
        )
        
        logger.info("Intelligent Data Mesh started")
        
    async def stop(self):
        """Stop the data mesh"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
            
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Intelligent Data Mesh stopped")
        
    async def register_data_product(self, product: DataProduct) -> str:
        """Register a new data product in the mesh"""
        try:
            # Store product
            self.data_products[product.product_id] = product
            
            # Add to graph
            self.product_graph.add_node(
                product.product_id,
                **product.__dict__
            )
            
            # Add dependencies
            for dep in product.dependencies:
                if dep in self.data_products:
                    self.product_graph.add_edge(dep, product.product_id)
                    
            # Analyze optimal placement
            placement = await self._analyze_optimal_placement(product)
            await self._apply_placement(product.product_id, placement)
            
            logger.info(f"Registered data product: {product.name}")
            return product.product_id
            
        except Exception as e:
            logger.error(f"Failed to register data product: {e}")
            raise
            
    async def autonomous_data_organization(self):
        """Main autonomous organization logic"""
        try:
            # Learn access patterns
            access_patterns = await self.learn_access_patterns()
            
            # Predict future data needs
            predictions = await self.predict_data_needs(
                time_horizon="24h",
                confidence_threshold=0.8
            )
            
            # Optimize data placement
            await self.optimize_data_placement(predictions)
            
            # Create smart materializations
            await self.create_smart_materializations(access_patterns)
            
            logger.info("Completed autonomous data organization cycle")
            
        except Exception as e:
            logger.error(f"Autonomous organization failed: {e}")
            
    async def learn_access_patterns(self) -> Dict[str, AccessPattern]:
        """Continuously learn data access patterns"""
        try:
            # Analyze recent access history
            recent_accesses = self.access_history[-10000:]  # Last 10k accesses
            
            if len(recent_accesses) < 100:
                return self.access_patterns
                
            # Convert to DataFrame for analysis
            df = pd.DataFrame(recent_accesses)
            
            # Cluster similar access patterns
            features = self._extract_access_features(df)
            clustering = DBSCAN(eps=0.3, min_samples=5).fit(features)
            
            # Analyze each cluster
            new_patterns = {}
            for cluster_id in set(clustering.labels_):
                if cluster_id == -1:  # Noise
                    continue
                    
                cluster_data = df[clustering.labels_ == cluster_id]
                pattern = self._analyze_cluster_pattern(cluster_data)
                new_patterns[pattern.pattern_id] = pattern
                
            # Update patterns
            self.access_patterns.update(new_patterns)
            
            return self.access_patterns
            
        except Exception as e:
            logger.error(f"Failed to learn access patterns: {e}")
            return self.access_patterns
            
    async def predict_data_needs(self,
                               time_horizon: str = "24h",
                               confidence_threshold: float = 0.8) -> List[Dict[str, Any]]:
        """Predict future data access needs"""
        try:
            # Parse time horizon
            hours = int(time_horizon.rstrip('h'))
            future_time = datetime.utcnow() + timedelta(hours=hours)
            
            predictions = []
            
            # Use pattern model to predict
            for product_id, product in self.data_products.items():
                # Extract features for prediction
                features = self._extract_product_features(product, future_time)
                
                if self.pattern_model:
                    # Predict access probability and volume
                    prediction = self.pattern_model.predict([features])[0]
                    confidence = self._calculate_prediction_confidence(features)
                    
                    if confidence >= confidence_threshold:
                        predictions.append({
                            "product_id": product_id,
                            "predicted_accesses": prediction[0],
                            "predicted_latency_requirement": prediction[1],
                            "confidence": confidence,
                            "time_window": time_horizon,
                            "recommended_action": self._recommend_action(
                                product_id, prediction
                            )
                        })
                        
            # Sort by predicted impact
            predictions.sort(
                key=lambda x: x["predicted_accesses"] * x["confidence"],
                reverse=True
            )
            
            return predictions
            
        except Exception as e:
            logger.error(f"Failed to predict data needs: {e}")
            return []
            
    async def optimize_data_placement(self, predictions: List[Dict[str, Any]]):
        """Optimize data placement based on predictions"""
        try:
            for prediction in predictions:
                product_id = prediction["product_id"]
                action = prediction["recommended_action"]
                
                if action == "promote_to_cache":
                    await self._promote_to_cache(product_id)
                elif action == "replicate":
                    await self._replicate_data_product(product_id)
                elif action == "move_to_edge":
                    await self._move_to_edge(product_id)
                elif action == "archive":
                    await self._archive_data_product(product_id)
                    
            logger.info(f"Optimized placement for {len(predictions)} data products")
            
        except Exception as e:
            logger.error(f"Placement optimization failed: {e}")
            
    async def create_smart_materializations(self, 
                                          access_patterns: Dict[str, AccessPattern]):
        """Create materialized views for common access patterns"""
        try:
            # Analyze patterns for materialization opportunities
            materialization_candidates = []
            
            for pattern_id, pattern in access_patterns.items():
                # Check if pattern is frequent enough
                if pattern.frequency > 10:  # More than 10 accesses per hour
                    # Check if involves multiple products (join)
                    if len(pattern.data_products) > 1:
                        score = self._calculate_materialization_score(pattern)
                        if score > 0.7:
                            materialization_candidates.append((pattern, score))
                            
            # Sort by score
            materialization_candidates.sort(key=lambda x: x[1], reverse=True)
            
            # Create top materializations
            for pattern, score in materialization_candidates[:5]:  # Top 5
                await self._create_materialized_view(pattern)
                
        except Exception as e:
            logger.error(f"Failed to create materializations: {e}")
            
    async def query_with_optimization(self,
                                    query: str,
                                    user_context: Dict[str, Any]) -> Any:
        """Execute query with intelligent optimization"""
        try:
            # Record access for learning
            self._record_access(query, user_context)
            
            # Check if materialized view exists
            materialized = await self._check_materialization(query)
            if materialized:
                logger.info("Using materialized view for query")
                return materialized
                
            # Predict if this query will be repeated
            repeat_probability = await self._predict_query_repeat(query, user_context)
            
            if repeat_probability > 0.7:
                # Execute and cache
                result = await self.federated_engine.execute_query(query)
                await self._cache_for_future(query, result)
            else:
                # Just execute
                result = await self.federated_engine.execute_query(query)
                
            return result
            
        except Exception as e:
            logger.error(f"Optimized query execution failed: {e}")
            raise
            
    # Private helper methods
    
    async def _initialize_models(self):
        """Initialize ML models for pattern learning"""
        # Initialize access pattern model
        self.pattern_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        # Initialize placement optimizer
        self.placement_optimizer = PlacementOptimizer()
        
    async def _discovery_loop(self):
        """Background loop for discovering new data products"""
        while self._running:
            try:
                # Discover from various sources
                await self._discover_from_catalog()
                await self._discover_from_lineage()
                await self._discover_from_usage()
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Discovery loop error: {e}")
                await asyncio.sleep(60)
                
    async def _optimization_loop(self):
        """Background loop for continuous optimization"""
        while self._running:
            try:
                # Run autonomous organization
                await self.autonomous_data_organization()
                
                # Clean up stale data
                await self._cleanup_stale_data()
                
                await asyncio.sleep(3600)  # Every hour
                
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")
                await asyncio.sleep(300)
                
    async def _prefetch_loop(self):
        """Background loop for predictive prefetching"""
        while self._running:
            try:
                # Get short-term predictions
                predictions = await self.predict_data_needs(
                    time_horizon="1h",
                    confidence_threshold=0.9
                )
                
                # Prefetch high-confidence predictions
                for prediction in predictions[:10]:  # Top 10
                    if prediction["recommended_action"] == "prefetch":
                        await self._prefetch_data(prediction["product_id"])
                        
                await asyncio.sleep(600)  # Every 10 minutes
                
            except Exception as e:
                logger.error(f"Prefetch loop error: {e}")
                await asyncio.sleep(120)
                
    async def _analyze_optimal_placement(self, 
                                       product: DataProduct) -> List[str]:
        """Analyze optimal placement for a data product"""
        placements = []
        
        # Always keep in lake
        placements.append("lake")
        
        # Analyze access patterns
        if product.access_patterns:
            avg_frequency = np.mean([p.get("frequency", 0) for p in product.access_patterns])
            
            # High frequency -> cache
            if avg_frequency > 100:  # More than 100 accesses per hour
                placements.append("cache")
                
            # Low latency requirement -> edge
            avg_latency = np.mean([p.get("latency_ms", 1000) for p in product.access_patterns])
            if avg_latency < 100:  # Less than 100ms requirement
                placements.append("edge")
                
        # Reference data -> replicate
        if product.type == DataProductType.REFERENCE:
            placements.append("replicated")
            
        return placements
        
    async def _apply_placement(self, product_id: str, placements: List[str]):
        """Apply placement strategy for a data product"""
        self.current_placements[product_id] = placements
        
        product = self.data_products[product_id]
        
        for placement in placements:
            if placement == "cache":
                # Create cache region
                await self.dih.create_cache_region(CacheRegion(
                    name=f"product_{product_id}",
                    cache_mode="PARTITIONED",
                    cache_strategy=CacheStrategy.READ_THROUGH,
                    eviction_max_size=1000000
                ))
                
            elif placement == "edge":
                # Would deploy to edge locations
                logger.info(f"Would deploy {product_id} to edge")
                
            elif placement == "replicated":
                # Create replicated cache
                await self.dih.create_cache_region(CacheRegion(
                    name=f"product_{product_id}_replicated",
                    cache_mode="REPLICATED",
                    cache_strategy=CacheStrategy.REFRESH_AHEAD
                ))
                
    def _extract_access_features(self, df: pd.DataFrame) -> np.ndarray:
        """Extract features from access data"""
        features = []
        
        for _, group in df.groupby(['product_id', 'user_segment']):
            feature_vector = [
                len(group),  # Access count
                group['timestamp'].diff().mean().total_seconds() if len(group) > 1 else 0,  # Avg time between accesses
                group['latency_ms'].mean() if 'latency_ms' in group else 0,
                group['data_size'].mean() if 'data_size' in group else 0,
                len(group['user_id'].unique()) if 'user_id' in group else 0
            ]
            features.append(feature_vector)
            
        return np.array(features)
        
    def _analyze_cluster_pattern(self, cluster_data: pd.DataFrame) -> AccessPattern:
        """Analyze a cluster to extract access pattern"""
        # Extract pattern characteristics
        products = cluster_data['product_id'].unique().tolist()
        
        # Time analysis
        cluster_data['hour'] = pd.to_datetime(cluster_data['timestamp']).dt.hour
        time_patterns = cluster_data['hour'].value_counts(normalize=True).to_dict()
        
        # Calculate metrics
        frequency = len(cluster_data) / (
            (cluster_data['timestamp'].max() - cluster_data['timestamp'].min()).total_seconds() / 3600
        )
        
        return AccessPattern(
            pattern_id=f"pattern_{datetime.utcnow().timestamp()}",
            data_products=products,
            frequency=frequency,
            latency_requirement=cluster_data['latency_ms'].mean() if 'latency_ms' in cluster_data else 1000,
            user_segments=cluster_data['user_segment'].unique().tolist() if 'user_segment' in cluster_data else [],
            time_patterns=time_patterns,
            predicted_growth=0.1  # Default 10% growth
        )
        
    def _extract_product_features(self, 
                                product: DataProduct,
                                future_time: datetime) -> List[float]:
        """Extract features for prediction"""
        features = []
        
        # Product characteristics
        features.append(1 if product.type == DataProductType.OPERATIONAL else 0)
        features.append(1 if product.type == DataProductType.ANALYTICAL else 0)
        features.append(product.quality_score)
        features.append(len(product.dependencies))
        
        # Time features
        features.append(future_time.hour)
        features.append(future_time.weekday())
        
        # Historical access features
        if product.access_patterns:
            features.append(len(product.access_patterns))
            features.append(np.mean([p.get("frequency", 0) for p in product.access_patterns]))
        else:
            features.extend([0, 0])
            
        return features
        
    def _recommend_action(self, product_id: str, prediction: np.ndarray) -> str:
        """Recommend action based on prediction"""
        predicted_accesses = prediction[0]
        predicted_latency = prediction[1]
        
        current_placement = self.current_placements.get(product_id, ["lake"])
        
        # High access, not in cache -> promote
        if predicted_accesses > 100 and "cache" not in current_placement:
            return "promote_to_cache"
            
        # Low latency requirement, not at edge -> move
        if predicted_latency < 50 and "edge" not in current_placement:
            return "move_to_edge"
            
        # Very high access -> replicate
        if predicted_accesses > 1000 and "replicated" not in current_placement:
            return "replicate"
            
        # Low access, in cache -> archive
        if predicted_accesses < 1 and "cache" in current_placement:
            return "archive"
            
        # Moderate access -> prefetch
        if 10 < predicted_accesses < 100:
            return "prefetch"
            
        return "no_action"
        
    async def _promote_to_cache(self, product_id: str):
        """Promote data product to cache"""
        product = self.data_products.get(product_id)
        if not product:
            return
            
        # Load data into cache
        data = await self.lake_manager.read_dataset(product.location)
        
        # Store in DIH cache
        cache = self.dih.caches.get(f"product_{product_id}")
        if cache:
            # Batch load
            await self.dih.bulk_load(
                region_name=f"product_{product_id}",
                data=[(row['id'], row) for _, row in data.iterrows()]
            )
            
        logger.info(f"Promoted {product_id} to cache")
        
    async def _create_materialized_view(self, pattern: AccessPattern):
        """Create materialized view for access pattern"""
        try:
            # Generate view name
            view_name = f"mv_{'_'.join(pattern.data_products[:3])}_{pattern.pattern_id[:8]}"
            
            # Generate optimal query for pattern
            query = self._generate_pattern_query(pattern)
            
            # Create in data platform
            await self.federated_engine.execute_query(f"""
                CREATE MATERIALIZED VIEW {view_name} AS
                {query}
            """)
            
            # Register as data product
            mv_product = DataProduct(
                product_id=f"mv_{pattern.pattern_id}",
                name=view_name,
                type=DataProductType.ANALYTICAL,
                owner="data_mesh",
                location=f"catalog.{view_name}",
                schema={},  # Would extract from query
                quality_score=0.9,
                access_patterns=[],
                dependencies=pattern.data_products,
                metadata={"pattern_id": pattern.pattern_id}
            )
            
            await self.register_data_product(mv_product)
            
            logger.info(f"Created materialized view: {view_name}")
            
        except Exception as e:
            logger.error(f"Failed to create materialized view: {e}")
            
    def _calculate_materialization_score(self, pattern: AccessPattern) -> float:
        """Calculate score for materialization benefit"""
        # Factors:
        # - Frequency (higher is better)
        # - Number of products joined (more is better) 
        # - Latency requirement (lower requirement = higher score)
        # - User segments (more segments = higher score)
        
        frequency_score = min(1.0, pattern.frequency / 100)
        join_score = min(1.0, len(pattern.data_products) / 5)
        latency_score = max(0, 1 - pattern.latency_requirement / 1000)
        segment_score = min(1.0, len(pattern.user_segments) / 10)
        
        # Weighted average
        score = (
            frequency_score * 0.4 +
            join_score * 0.3 +
            latency_score * 0.2 +
            segment_score * 0.1
        )
        
        return score
        
    def _record_access(self, query: str, user_context: Dict[str, Any]):
        """Record data access for pattern learning"""
        access_record = {
            "timestamp": datetime.utcnow(),
            "query": query,
            "user_id": user_context.get("user_id"),
            "user_segment": user_context.get("segment", "unknown"),
            "product_ids": self._extract_products_from_query(query),
            "latency_ms": user_context.get("latency_requirement", 1000),
            "data_size": 0  # Would be set after execution
        }
        
        self.access_history.append(access_record)
        
        # Keep only recent history
        if len(self.access_history) > 100000:
            self.access_history = self.access_history[-50000:]


class PlacementOptimizer:
    """Optimizes data placement across the mesh"""
    
    def __init__(self):
        self.placement_model = None
        
    async def optimize(self,
                      products: List[DataProduct],
                      constraints: Dict[str, Any]) -> Dict[str, List[str]]:
        """Optimize placement for multiple products"""
        # This would implement sophisticated optimization
        # For now, returning simple heuristic-based placement
        placements = {}
        
        for product in products:
            if product.access_patterns:
                avg_freq = np.mean([p.get("frequency", 0) for p in product.access_patterns])
                if avg_freq > 50:
                    placements[product.product_id] = ["lake", "cache"]
                else:
                    placements[product.product_id] = ["lake"]
            else:
                placements[product.product_id] = ["lake"]
                
        return placements 