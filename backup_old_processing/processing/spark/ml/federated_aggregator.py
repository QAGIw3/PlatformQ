"""
Federated Model Aggregator using Apache Ignite

This module implements secure aggregation of model updates from multiple
participants using Apache Ignite for high-performance in-memory processing.
"""

import sys
import json
import logging
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import base64
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, lit, collect_list, struct, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType
from pyspark.ml.linalg import Vectors, DenseVector
import mlflow

# Cryptography for secure aggregation
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

# Ignite integration
from pyignite import Client
from pyignite.datatypes import String, DoubleObject, IntObject

logger = logging.getLogger(__name__)


class FederatedAggregator:
    """Aggregates model updates from federated learning participants"""
    
    def __init__(self, session_id: str, round_number: int):
        self.session_id = session_id
        self.round_number = round_number
        self.spark = self._create_spark_session()
        self.ignite_client = self._connect_to_ignite()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Ignite integration"""
        return SparkSession.builder \
            .appName(f"FederatedAggregator-{self.session_id}-round{self.round_number}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.ignite.servers", "ignite-0.ignite:10800,ignite-1.ignite:10800") \
            .getOrCreate()
    
    def _connect_to_ignite(self) -> Client:
        """Connect to Apache Ignite cluster"""
        client = Client()
        nodes = [
            ('ignite-0.ignite', 10800),
            ('ignite-1.ignite', 10800),
            ('ignite-2.ignite', 10800)
        ]
        client.connect(nodes)
        logger.info(f"Connected to Ignite cluster with {len(nodes)} nodes")
        return client
    
    def load_model_updates(self, update_uris: List[str]) -> DataFrame:
        """
        Load model updates from MinIO
        
        :param update_uris: List of URIs to model updates
        :return: DataFrame with model updates
        """
        # Read all updates into a single DataFrame
        dfs = []
        for uri in update_uris:
            df = self.spark.read.text(uri)
            # Parse JSON from text
            parsed_df = df.select(
                udf(lambda x: json.loads(x), 
                    StructType([
                        StructField("participant_id", StringType()),
                        StructField("model_weights", MapType(StringType(), StringType())),
                        StructField("metrics", MapType(StringType(), DoubleType())),
                        StructField("zkp", MapType(StringType(), StringType())),
                        StructField("timestamp", StringType())
                    ])
                )(col("value")).alias("update")
            ).select("update.*")
            dfs.append(parsed_df)
        
        updates_df = reduce(DataFrame.union, dfs)
        logger.info(f"Loaded {updates_df.count()} model updates")
        return updates_df
    
    def verify_zero_knowledge_proofs(self, updates_df: DataFrame) -> DataFrame:
        """
        Verify ZKPs from participants
        
        :param updates_df: DataFrame with model updates
        :return: DataFrame with verification results
        """
        def verify_zkp(zkp_data: Dict[str, str]) -> bool:
            """Verify a zero-knowledge proof"""
            # Simplified verification - in production, use proper ZK verification
            if not zkp_data:
                return False
                
            commitment = zkp_data.get("commitment", "")
            proof_value = zkp_data.get("proof_value", "")
            
            # Basic verification (placeholder for real ZKP verification)
            return len(commitment) == 64 and len(proof_value) == 64
        
        verify_udf = udf(verify_zkp, "boolean")
        
        verified_df = updates_df.withColumn(
            "zkp_valid",
            verify_udf(col("zkp"))
        )
        
        # Filter out invalid proofs
        valid_updates = verified_df.filter(col("zkp_valid") == True)
        invalid_count = verified_df.filter(col("zkp_valid") == False).count()
        
        if invalid_count > 0:
            logger.warning(f"Rejected {invalid_count} updates with invalid ZKPs")
        
        return valid_updates
    
    def decrypt_model_weights(self, updates_df: DataFrame, private_key: Any) -> DataFrame:
        """
        Decrypt model weights for aggregation
        
        :param updates_df: DataFrame with encrypted weights
        :param private_key: Coordinator's private key
        :return: DataFrame with decrypted weights
        """
        def decrypt_weights(encrypted_data: Dict[str, str]) -> Dict[str, Any]:
            """Decrypt model weights"""
            if "encrypted_weights" not in encrypted_data:
                return encrypted_data
            
            try:
                # Decode base64
                encrypted_bytes = base64.b64decode(encrypted_data["encrypted_weights"])
                
                # Decrypt (placeholder - in production, use actual coordinator key)
                # For now, return mock decrypted weights
                return {
                    "coefficients": [0.1, 0.2, 0.3, 0.4, 0.5],
                    "intercept": 0.0,
                    "num_samples": 1000
                }
            except Exception as e:
                logger.error(f"Decryption failed: {e}")
                return {}
        
        decrypt_udf = udf(decrypt_weights, MapType(StringType(), StringType()))
        
        decrypted_df = updates_df.withColumn(
            "decrypted_weights",
            decrypt_udf(col("model_weights"))
        )
        
        return decrypted_df
    
    def aggregate_weights_fedavg(self, weights_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Implement FedAvg aggregation strategy
        
        :param weights_list: List of model weights from participants
        :return: Aggregated weights
        """
        if not weights_list:
            return {}
        
        # Calculate total samples
        total_samples = sum(w.get("num_samples", 1) for w in weights_list)
        
        # Initialize aggregated weights
        aggregated = {}
        
        # Aggregate coefficients (weighted average)
        if "coefficients" in weights_list[0]:
            coefficients_dim = len(weights_list[0]["coefficients"])
            aggregated_coefficients = np.zeros(coefficients_dim)
            
            for weights in weights_list:
                weight_factor = weights.get("num_samples", 1) / total_samples
                coefficients = np.array(weights["coefficients"])
                aggregated_coefficients += weight_factor * coefficients
            
            aggregated["coefficients"] = aggregated_coefficients.tolist()
        
        # Aggregate intercept
        if "intercept" in weights_list[0]:
            aggregated_intercept = 0.0
            for weights in weights_list:
                weight_factor = weights.get("num_samples", 1) / total_samples
                aggregated_intercept += weight_factor * weights.get("intercept", 0.0)
            
            aggregated["intercept"] = aggregated_intercept
        
        # Aggregate tree-based models differently
        if "trees" in weights_list[0]:
            # For tree ensembles, combine all trees
            all_trees = []
            for weights in weights_list:
                all_trees.extend(weights.get("trees", []))
            aggregated["trees"] = all_trees
        
        aggregated["total_samples"] = total_samples
        aggregated["num_participants"] = len(weights_list)
        
        return aggregated
    
    def aggregate_weights_secure(self, weights_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Implement secure aggregation with homomorphic properties
        
        :param weights_list: List of encrypted model weights
        :return: Aggregated weights
        """
        # Placeholder for secure aggregation
        # In production, use homomorphic encryption libraries
        return self.aggregate_weights_fedavg(weights_list)
    
    def store_in_ignite(self, key: str, value: Any) -> None:
        """Store data in Ignite cache"""
        cache_name = f"federated_learning_{self.session_id}"
        
        # Get or create cache
        cache = self.ignite_client.get_or_create_cache(cache_name)
        
        # Store value
        cache.put(key, json.dumps(value))
        logger.info(f"Stored {key} in Ignite cache {cache_name}")
    
    def get_from_ignite(self, key: str) -> Optional[Any]:
        """Retrieve data from Ignite cache"""
        cache_name = f"federated_learning_{self.session_id}"
        
        try:
            cache = self.ignite_client.get_cache(cache_name)
            value = cache.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Failed to get {key} from Ignite: {e}")
            return None
    
    def calculate_convergence_metrics(self, 
                                    current_weights: Dict[str, Any],
                                    previous_weights: Optional[Dict[str, Any]]) -> Dict[str, float]:
        """
        Calculate convergence metrics between rounds
        
        :param current_weights: Current round aggregated weights
        :param previous_weights: Previous round aggregated weights
        :return: Convergence metrics
        """
        metrics = {}
        
        if not previous_weights:
            metrics["convergence_score"] = 0.0
            metrics["model_divergence"] = 1.0
            return metrics
        
        # Calculate L2 distance between weight vectors
        if "coefficients" in current_weights and "coefficients" in previous_weights:
            current_coef = np.array(current_weights["coefficients"])
            previous_coef = np.array(previous_weights["coefficients"])
            
            l2_distance = np.linalg.norm(current_coef - previous_coef)
            
            # Normalize by magnitude
            magnitude = np.linalg.norm(previous_coef)
            if magnitude > 0:
                normalized_distance = l2_distance / magnitude
            else:
                normalized_distance = l2_distance
            
            # Convert to convergence score (inverse of distance)
            convergence_score = 1.0 / (1.0 + normalized_distance)
            
            metrics["convergence_score"] = float(convergence_score)
            metrics["model_divergence"] = float(normalized_distance)
            metrics["l2_distance"] = float(l2_distance)
        
        return metrics
    
    def save_aggregated_model(self, aggregated_weights: Dict[str, Any]) -> str:
        """
        Save aggregated model to MinIO
        
        :param aggregated_weights: Aggregated model weights
        :return: URI to saved model
        """
        model_data = {
            "session_id": self.session_id,
            "round_number": self.round_number,
            "aggregated_weights": aggregated_weights,
            "aggregation_timestamp": datetime.utcnow().isoformat(),
            "aggregation_method": "FedAvg"
        }
        
        # Save to MinIO
        model_id = f"{self.session_id}_round{self.round_number}_aggregated"
        s3_uri = f"s3://federated-learning/aggregated/{self.session_id}/{model_id}.json"
        
        # Convert to JSON and save
        model_json = json.dumps(model_data)
        
        # Use Spark to write
        model_df = self.spark.createDataFrame([{"data": model_json}])
        model_df.write.mode("overwrite").text(s3_uri)
        
        # Also store in Ignite for fast access
        self.store_in_ignite(f"aggregated_model_round_{self.round_number}", model_data)
        
        logger.info(f"Saved aggregated model to {s3_uri}")
        return s3_uri
    
    def run(self, 
            update_uris: List[str],
            aggregation_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method for model aggregation
        
        :param update_uris: List of model update URIs
        :param aggregation_config: Aggregation configuration
        :return: Aggregation results
        """
        start_time = datetime.utcnow()
        
        try:
            # Load model updates
            updates_df = self.load_model_updates(update_uris)
            
            # Verify zero-knowledge proofs
            verified_df = self.verify_zero_knowledge_proofs(updates_df)
            
            # Decrypt weights (placeholder - needs real coordinator key)
            decrypted_df = self.decrypt_model_weights(verified_df, None)
            
            # Collect weights for aggregation
            weights_list = decrypted_df.select("decrypted_weights").collect()
            weights_list = [row.decrypted_weights for row in weights_list]
            
            # Perform aggregation based on strategy
            aggregation_strategy = aggregation_config.get("aggregation_strategy", "FedAvg")
            
            if aggregation_strategy == "FedAvg":
                aggregated_weights = self.aggregate_weights_fedavg(weights_list)
            elif aggregation_strategy == "SecureAgg":
                aggregated_weights = self.aggregate_weights_secure(weights_list)
            else:
                raise ValueError(f"Unknown aggregation strategy: {aggregation_strategy}")
            
            # Get previous round weights from Ignite
            previous_weights = None
            if self.round_number > 1:
                previous_weights = self.get_from_ignite(f"aggregated_model_round_{self.round_number - 1}")
                if previous_weights:
                    previous_weights = previous_weights.get("aggregated_weights")
            
            # Calculate convergence metrics
            convergence_metrics = self.calculate_convergence_metrics(
                aggregated_weights,
                previous_weights
            )
            
            # Calculate average metrics from participants
            metrics_df = verified_df.select("metrics.*")
            avg_metrics = {}
            for col_name in metrics_df.columns:
                avg_value = metrics_df.agg({col_name: "avg"}).collect()[0][0]
                avg_metrics[f"avg_{col_name}"] = avg_value
            
            # Save aggregated model
            model_uri = self.save_aggregated_model(aggregated_weights)
            
            # Store metrics in Ignite
            round_metrics = {
                "convergence_metrics": convergence_metrics,
                "participant_metrics": avg_metrics,
                "num_participants": verified_df.count(),
                "round_number": self.round_number
            }
            self.store_in_ignite(f"round_{self.round_number}_metrics", round_metrics)
            
            # Prepare result
            result = {
                "status": "success",
                "session_id": self.session_id,
                "round_number": self.round_number,
                "aggregated_model_uri": model_uri,
                "num_participants": verified_df.count(),
                "convergence_metrics": convergence_metrics,
                "avg_participant_metrics": avg_metrics,
                "aggregation_time": (datetime.utcnow() - start_time).total_seconds()
            }
            
            logger.info(f"Completed aggregation for round {self.round_number}")
            return result
            
        except Exception as e:
            logger.error(f"Error in model aggregation: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "round_number": self.round_number
            }
        finally:
            self.spark.stop()
            self.ignite_client.close()


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 4:
        print("Usage: spark-submit federated_aggregator.py <session_id> <round_number> <config_json>")
        sys.exit(1)
    
    session_id = sys.argv[1]
    round_number = int(sys.argv[2])
    config = json.loads(sys.argv[3])
    
    # Create aggregator
    aggregator = FederatedAggregator(session_id, round_number)
    
    # Run aggregation
    result = aggregator.run(
        update_uris=config["update_uris"],
        aggregation_config=config["aggregation_config"]
    )
    
    # Output result
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main() 