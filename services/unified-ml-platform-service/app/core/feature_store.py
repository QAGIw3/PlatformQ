"""
Feature Store for managing ML features
"""
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError, NotFoundError

logger = get_logger(__name__)


class FeatureStore:
    """Feature store for managing ML features"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.feature_sets: Dict[str, Dict] = {}
        self.feature_views: Dict[str, Dict] = {}
        self.materialization_jobs: Dict[str, Dict] = {}
        
    async def create_feature_set(self, feature_set_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new feature set"""
        try:
            # Validate feature set
            required_fields = ["name", "features", "entity", "source"]
            for field in required_fields:
                if field not in feature_set_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate feature set ID
            fs_id = f"fs_{feature_set_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create feature set
            feature_set = {
                "feature_set_id": fs_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "status": "active",
                "version": 1,
                **feature_set_data
            }
            
            # Validate feature definitions
            for feature in feature_set["features"]:
                self._validate_feature_definition(feature)
            
            # Store feature set
            self.feature_sets[fs_id] = feature_set
            await self.cache.put(f"feature_store:set:{fs_id}", feature_set)
            
            # Register with data catalog
            await self._register_in_catalog(feature_set)
            
            # Publish event
            await self.event_publisher.publish("feature_store.set.created", {
                "feature_set_id": fs_id,
                "name": feature_set_data["name"],
                "feature_count": len(feature_set_data["features"])
            })
            
            logger.info(f"Created feature set: {fs_id}")
            return feature_set
            
        except Exception as e:
            logger.error(f"Failed to create feature set: {str(e)}")
            raise
    
    async def create_feature_view(self, view_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a feature view combining multiple feature sets"""
        try:
            # Validate view
            required_fields = ["name", "feature_sets", "entities", "ttl"]
            for field in required_fields:
                if field not in view_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate view ID
            view_id = f"fv_{view_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create feature view
            feature_view = {
                "view_id": view_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "materialized": False,
                **view_data
            }
            
            # Validate feature sets exist
            for fs_name in feature_view["feature_sets"]:
                if not await self._get_feature_set_by_name(fs_name):
                    raise ValidationError(f"Feature set {fs_name} not found")
            
            # Store view
            self.feature_views[view_id] = feature_view
            await self.cache.put(f"feature_store:view:{view_id}", feature_view)
            
            # Publish event
            await self.event_publisher.publish("feature_store.view.created", {
                "view_id": view_id,
                "name": view_data["name"]
            })
            
            return feature_view
            
        except Exception as e:
            logger.error(f"Failed to create feature view: {str(e)}")
            raise
    
    async def get_online_features(self,
                                entity_ids: List[str],
                                feature_names: List[str]) -> pd.DataFrame:
        """Get features for online serving"""
        try:
            # Find feature sets containing requested features
            feature_sets = await self._find_feature_sets(feature_names)
            
            # Fetch features from online store (Ignite)
            features_data = []
            
            for entity_id in entity_ids:
                entity_features = {"entity_id": entity_id}
                
                for fs in feature_sets:
                    # Get features from cache
                    cache_key = f"features:online:{fs['name']}:{entity_id}"
                    cached_features = await self.cache.get(cache_key)
                    
                    if cached_features:
                        # Filter requested features
                        for feature_name in feature_names:
                            if feature_name in cached_features:
                                entity_features[feature_name] = cached_features[feature_name]
                    else:
                        # Features not in cache, compute on-demand
                        computed = await self._compute_features_online(fs, entity_id, feature_names)
                        entity_features.update(computed)
                
                features_data.append(entity_features)
            
            # Convert to DataFrame
            df = pd.DataFrame(features_data)
            
            # Log feature serving
            await self._log_feature_serving("online", entity_ids, feature_names)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get online features: {str(e)}")
            raise
    
    async def get_historical_features(self,
                                    entity_df: pd.DataFrame,
                                    feature_names: List[str]) -> pd.DataFrame:
        """Get historical features for training"""
        try:
            # Validate entity DataFrame
            if "entity_id" not in entity_df.columns:
                raise ValidationError("entity_df must contain 'entity_id' column")
            
            if "event_timestamp" not in entity_df.columns:
                raise ValidationError("entity_df must contain 'event_timestamp' column")
            
            # Find feature sets
            feature_sets = await self._find_feature_sets(feature_names)
            
            # Fetch historical features
            result_df = entity_df.copy()
            
            for fs in feature_sets:
                # Get features from offline store
                fs_features = await self._fetch_historical_features(
                    fs, entity_df, feature_names
                )
                
                # Join features
                result_df = result_df.merge(
                    fs_features,
                    on=["entity_id", "event_timestamp"],
                    how="left"
                )
            
            # Fill missing values based on feature definitions
            result_df = await self._handle_missing_values(result_df, feature_sets)
            
            # Log feature serving
            await self._log_feature_serving(
                "historical", 
                entity_df["entity_id"].unique().tolist(),
                feature_names
            )
            
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to get historical features: {str(e)}")
            raise
    
    async def materialize_features(self,
                                 view_id: str,
                                 start_date: datetime,
                                 end_date: datetime) -> Dict[str, Any]:
        """Materialize features for a date range"""
        try:
            # Get feature view
            view = await self._get_feature_view(view_id)
            
            if not view:
                raise NotFoundError(f"Feature view {view_id} not found")
            
            # Create materialization job
            job_id = f"mat_{view_id}_{datetime.utcnow().timestamp()}"
            
            job = {
                "job_id": job_id,
                "view_id": view_id,
                "started_at": datetime.utcnow().isoformat(),
                "status": "running",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "progress": 0
            }
            
            self.materialization_jobs[job_id] = job
            await self.cache.put(f"feature_store:mat_job:{job_id}", job)
            
            # Start materialization asynchronously
            import asyncio
            asyncio.create_task(self._run_materialization(job_id))
            
            # Publish event
            await self.event_publisher.publish("feature_store.materialization.started", {
                "job_id": job_id,
                "view_id": view_id
            })
            
            return job
            
        except Exception as e:
            logger.error(f"Failed to start materialization: {str(e)}")
            raise
    
    async def validate_features(self,
                              feature_set_id: str,
                              validation_rules: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Validate feature quality"""
        try:
            feature_set = await self._get_feature_set(feature_set_id)
            
            if not feature_set:
                raise NotFoundError(f"Feature set {feature_set_id} not found")
            
            # Default validation rules
            if not validation_rules:
                validation_rules = [
                    {"type": "null_check", "threshold": 0.1},
                    {"type": "range_check", "enabled": True},
                    {"type": "uniqueness_check", "enabled": True}
                ]
            
            # Sample data for validation
            sample_data = await self._get_feature_sample(feature_set)
            
            # Run validation
            validation_results = {
                "feature_set_id": feature_set_id,
                "validated_at": datetime.utcnow().isoformat(),
                "results": {}
            }
            
            for feature in feature_set["features"]:
                feature_name = feature["name"]
                feature_results = []
                
                for rule in validation_rules:
                    result = await self._validate_feature_rule(
                        sample_data.get(feature_name, []),
                        feature,
                        rule
                    )
                    feature_results.append(result)
                
                validation_results["results"][feature_name] = feature_results
            
            # Calculate overall status
            all_passed = all(
                all(r["passed"] for r in results)
                for results in validation_results["results"].values()
            )
            validation_results["status"] = "passed" if all_passed else "failed"
            
            # Store results
            await self.cache.put(
                f"feature_store:validation:{feature_set_id}:{datetime.utcnow().timestamp()}",
                validation_results,
                ttl=86400
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate features: {str(e)}")
            raise
    
    async def get_feature_statistics(self, 
                                   feature_set_id: str,
                                   feature_name: str) -> Dict[str, Any]:
        """Get statistics for a feature"""
        try:
            feature_set = await self._get_feature_set(feature_set_id)
            
            # Get feature data sample
            sample_data = await self._get_feature_sample(feature_set)
            feature_values = sample_data.get(feature_name, [])
            
            if not feature_values:
                return {"error": "No data available"}
            
            # Calculate statistics
            stats = {
                "feature_name": feature_name,
                "count": len(feature_values),
                "null_count": sum(1 for v in feature_values if v is None),
                "null_ratio": sum(1 for v in feature_values if v is None) / len(feature_values)
            }
            
            # Numeric features
            numeric_values = [v for v in feature_values if isinstance(v, (int, float)) and v is not None]
            if numeric_values:
                stats.update({
                    "min": float(np.min(numeric_values)),
                    "max": float(np.max(numeric_values)),
                    "mean": float(np.mean(numeric_values)),
                    "median": float(np.median(numeric_values)),
                    "std": float(np.std(numeric_values)),
                    "percentiles": {
                        "25": float(np.percentile(numeric_values, 25)),
                        "50": float(np.percentile(numeric_values, 50)),
                        "75": float(np.percentile(numeric_values, 75)),
                        "95": float(np.percentile(numeric_values, 95))
                    }
                })
            
            # Categorical features
            unique_values = list(set(v for v in feature_values if v is not None))
            if len(unique_values) < 50:  # Only for low cardinality
                value_counts = {}
                for v in feature_values:
                    if v is not None:
                        value_counts[str(v)] = value_counts.get(str(v), 0) + 1
                stats["value_distribution"] = value_counts
                stats["unique_count"] = len(unique_values)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get feature statistics: {str(e)}")
            raise
    
    def _validate_feature_definition(self, feature: Dict):
        """Validate feature definition"""
        required_fields = ["name", "dtype", "description"]
        for field in required_fields:
            if field not in feature:
                raise ValidationError(f"Feature missing required field: {field}")
        
        # Validate data type
        valid_dtypes = ["int", "float", "string", "bool", "timestamp", "list", "dict"]
        if feature["dtype"] not in valid_dtypes:
            raise ValidationError(f"Invalid dtype: {feature['dtype']}")
    
    async def _find_feature_sets(self, feature_names: List[str]) -> List[Dict]:
        """Find feature sets containing requested features"""
        found_sets = []
        
        for fs in self.feature_sets.values():
            fs_feature_names = [f["name"] for f in fs["features"]]
            if any(fname in fs_feature_names for fname in feature_names):
                found_sets.append(fs)
        
        return found_sets
    
    async def _compute_features_online(self, 
                                     feature_set: Dict,
                                     entity_id: str,
                                     feature_names: List[str]) -> Dict[str, Any]:
        """Compute features on-demand for online serving"""
        # In production, compute from source data
        # For now, return mock features
        computed = {}
        for fname in feature_names:
            for feature in feature_set["features"]:
                if feature["name"] == fname:
                    if feature["dtype"] == "float":
                        computed[fname] = np.random.randn()
                    elif feature["dtype"] == "int":
                        computed[fname] = np.random.randint(0, 100)
                    elif feature["dtype"] == "string":
                        computed[fname] = f"value_{entity_id}"
                    elif feature["dtype"] == "bool":
                        computed[fname] = np.random.choice([True, False])
        
        # Cache computed features
        cache_key = f"features:online:{feature_set['name']}:{entity_id}"
        await self.cache.put(cache_key, computed, ttl=feature_set.get("ttl", 3600))
        
        return computed
    
    async def _fetch_historical_features(self,
                                       feature_set: Dict,
                                       entity_df: pd.DataFrame,
                                       feature_names: List[str]) -> pd.DataFrame:
        """Fetch historical features from offline store"""
        # In production, query from data warehouse
        # For now, generate mock historical data
        result_data = []
        
        for _, row in entity_df.iterrows():
            features = {
                "entity_id": row["entity_id"],
                "event_timestamp": row["event_timestamp"]
            }
            
            for fname in feature_names:
                for feature in feature_set["features"]:
                    if feature["name"] == fname:
                        # Generate time-aware mock data
                        base_value = hash(f"{row['entity_id']}_{fname}") % 100
                        time_offset = (datetime.now() - row["event_timestamp"]).days * 0.1
                        
                        if feature["dtype"] == "float":
                            features[fname] = base_value + time_offset + np.random.randn()
                        elif feature["dtype"] == "int":
                            features[fname] = int(base_value + time_offset)
                        else:
                            features[fname] = f"hist_{base_value}"
            
            result_data.append(features)
        
        return pd.DataFrame(result_data)
    
    async def _handle_missing_values(self, 
                                   df: pd.DataFrame,
                                   feature_sets: List[Dict]) -> pd.DataFrame:
        """Handle missing values based on feature definitions"""
        for fs in feature_sets:
            for feature in fs["features"]:
                fname = feature["name"]
                if fname in df.columns:
                    # Apply default value or imputation strategy
                    if "default_value" in feature:
                        df[fname].fillna(feature["default_value"], inplace=True)
                    elif feature["dtype"] in ["int", "float"]:
                        df[fname].fillna(0, inplace=True)
                    elif feature["dtype"] == "string":
                        df[fname].fillna("", inplace=True)
        
        return df
    
    async def _run_materialization(self, job_id: str):
        """Run feature materialization job"""
        try:
            job = self.materialization_jobs[job_id]
            view = await self._get_feature_view(job["view_id"])
            
            # Simulate materialization progress
            for progress in [25, 50, 75, 100]:
                await asyncio.sleep(2)  # Simulate work
                
                job["progress"] = progress
                await self.cache.put(f"feature_store:mat_job:{job_id}", job)
                
                # Publish progress
                await self.event_publisher.publish("feature_store.materialization.progress", {
                    "job_id": job_id,
                    "progress": progress
                })
            
            # Complete job
            job["status"] = "completed"
            job["completed_at"] = datetime.utcnow().isoformat()
            await self.cache.put(f"feature_store:mat_job:{job_id}", job)
            
            # Update view
            view["materialized"] = True
            view["last_materialized"] = datetime.utcnow().isoformat()
            await self._update_feature_view(view["view_id"], view)
            
            # Publish completion
            await self.event_publisher.publish("feature_store.materialization.completed", {
                "job_id": job_id,
                "view_id": job["view_id"]
            })
            
        except Exception as e:
            logger.error(f"Materialization job {job_id} failed: {str(e)}")
            job["status"] = "failed"
            job["error"] = str(e)
            await self.cache.put(f"feature_store:mat_job:{job_id}", job)
    
    async def _validate_feature_rule(self, 
                                   values: List[Any],
                                   feature: Dict,
                                   rule: Dict) -> Dict[str, Any]:
        """Validate a feature against a rule"""
        result = {
            "rule_type": rule["type"],
            "passed": True,
            "details": {}
        }
        
        if rule["type"] == "null_check":
            null_ratio = sum(1 for v in values if v is None) / len(values) if values else 0
            result["passed"] = null_ratio <= rule["threshold"]
            result["details"]["null_ratio"] = null_ratio
            result["details"]["threshold"] = rule["threshold"]
        
        elif rule["type"] == "range_check" and feature["dtype"] in ["int", "float"]:
            numeric_values = [v for v in values if isinstance(v, (int, float)) and v is not None]
            if numeric_values and "min_value" in feature:
                min_val = min(numeric_values)
                result["passed"] = result["passed"] and min_val >= feature["min_value"]
                result["details"]["min_value"] = min_val
            
            if numeric_values and "max_value" in feature:
                max_val = max(numeric_values)
                result["passed"] = result["passed"] and max_val <= feature["max_value"]
                result["details"]["max_value"] = max_val
        
        elif rule["type"] == "uniqueness_check" and feature.get("unique", False):
            unique_ratio = len(set(values)) / len(values) if values else 1
            result["passed"] = unique_ratio == 1.0
            result["details"]["unique_ratio"] = unique_ratio
        
        return result
    
    async def _get_feature_sample(self, feature_set: Dict) -> Dict[str, List]:
        """Get sample data for a feature set"""
        # In production, sample from actual data source
        # For now, generate mock sample
        sample_size = 1000
        sample_data = {}
        
        for feature in feature_set["features"]:
            fname = feature["name"]
            if feature["dtype"] == "float":
                sample_data[fname] = list(np.random.randn(sample_size))
            elif feature["dtype"] == "int":
                sample_data[fname] = list(np.random.randint(0, 100, sample_size))
            elif feature["dtype"] == "string":
                sample_data[fname] = [f"val_{i}" for i in range(sample_size)]
            elif feature["dtype"] == "bool":
                sample_data[fname] = list(np.random.choice([True, False], sample_size))
            
            # Add some nulls
            null_indices = np.random.choice(sample_size, int(sample_size * 0.05))
            for idx in null_indices:
                sample_data[fname][idx] = None
        
        return sample_data
    
    async def _log_feature_serving(self, 
                                 serving_type: str,
                                 entity_ids: List[str],
                                 feature_names: List[str]):
        """Log feature serving for monitoring"""
        await self.event_publisher.publish("feature_store.features.served", {
            "serving_type": serving_type,
            "entity_count": len(entity_ids),
            "feature_count": len(feature_names),
            "features": feature_names,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def _register_in_catalog(self, feature_set: Dict):
        """Register feature set in data catalog"""
        # In production, integrate with data catalog service
        await self.event_publisher.publish("catalog.asset.register", {
            "asset_type": "feature_set",
            "asset_id": feature_set["feature_set_id"],
            "name": feature_set["name"],
            "metadata": {
                "features": len(feature_set["features"]),
                "entity": feature_set["entity"],
                "source": feature_set["source"]
            }
        })
    
    async def _get_feature_set(self, feature_set_id: str) -> Optional[Dict]:
        """Get feature set by ID"""
        if feature_set_id in self.feature_sets:
            return self.feature_sets[feature_set_id]
        
        cached = await self.cache.get(f"feature_store:set:{feature_set_id}")
        if cached:
            self.feature_sets[feature_set_id] = cached
            return cached
        
        return None
    
    async def _get_feature_set_by_name(self, name: str) -> Optional[Dict]:
        """Get feature set by name"""
        for fs in self.feature_sets.values():
            if fs["name"] == name:
                return fs
        
        # Search in cache
        # In production, query from database
        return None
    
    async def _get_feature_view(self, view_id: str) -> Optional[Dict]:
        """Get feature view by ID"""
        if view_id in self.feature_views:
            return self.feature_views[view_id]
        
        cached = await self.cache.get(f"feature_store:view:{view_id}")
        if cached:
            self.feature_views[view_id] = cached
            return cached
        
        return None
    
    async def _update_feature_view(self, view_id: str, view: Dict):
        """Update feature view"""
        view["updated_at"] = datetime.utcnow().isoformat()
        self.feature_views[view_id] = view
        await self.cache.put(f"feature_store:view:{view_id}", view) 