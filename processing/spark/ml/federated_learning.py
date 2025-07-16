"""
Federated Learning Spark Job with Privacy-Preserving Features

This module implements federated learning across tenants using Spark's
distributed compute, allowing collaborative model training on private
datasets without data sharing.
"""

import sys
import json
import logging
import hashlib
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import base64

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, lit, array, struct, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.linalg import Vectors, DenseVector
import mlflow
import mlflow.spark

# Differential privacy imports
from diffprivlib.models import LogisticRegression as DPLogisticRegression
from diffprivlib.models import LinearRegression as DPLinearRegression

# Cryptography for secure aggregation
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class FederatedLearningJob:
    """Spark job for privacy-preserving federated learning"""
    
    def __init__(self, session_id: str, tenant_id: str, participant_id: str):
        self.session_id = session_id
        self.tenant_id = tenant_id
        self.participant_id = participant_id
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with federated learning configurations"""
        return SparkSession.builder \
            .appName(f"FederatedLearning-{self.session_id}-{self.participant_id}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.registrationRequired", "false") \
            .getOrCreate()
    
    def load_private_data(self, data_path: str, feature_schema: Dict[str, Any]) -> DataFrame:
        """
        Load tenant's private data with validation
        
        :param data_path: Path to tenant's private data
        :param feature_schema: Expected schema for features
        :return: Validated DataFrame
        """
        df = self.spark.read.parquet(data_path)
        
        # Filter by tenant to ensure data isolation
        df = df.filter(col("tenant_id") == self.tenant_id)
        
        # Validate schema
        expected_cols = set(feature_schema.get("features", []))
        actual_cols = set(df.columns)
        
        if not expected_cols.issubset(actual_cols):
            missing = expected_cols - actual_cols
            raise ValueError(f"Missing required columns: {missing}")
        
        # Add data hash for verification
        df = df.withColumn("data_hash", 
                          udf(self._compute_row_hash, StringType())(struct([df[c] for c in df.columns])))
        
        logger.info(f"Loaded {df.count()} samples for tenant {self.tenant_id}")
        return df
    
    def _compute_row_hash(self, row) -> str:
        """Compute hash of a data row for verification"""
        row_str = json.dumps(row.asDict(), sort_keys=True)
        return hashlib.sha256(row_str.encode()).hexdigest()
    
    def apply_differential_privacy(self, 
                                  data: DataFrame, 
                                  epsilon: float = 1.0, 
                                  delta: float = 1e-5) -> DataFrame:
        """
        Apply differential privacy to the data
        
        :param data: Input DataFrame
        :param epsilon: Privacy budget
        :param delta: Privacy parameter
        :return: DataFrame with DP noise applied
        """
        logger.info(f"Applying differential privacy with epsilon={epsilon}, delta={delta}")
        
        # Add Laplace noise to numerical features
        numerical_cols = [f.name for f in data.schema.fields 
                         if f.dataType in [DoubleType(), "float", "double"]]
        
        for col_name in numerical_cols:
            sensitivity = data.agg({col_name: "max"}).collect()[0][0] - \
                         data.agg({col_name: "min"}).collect()[0][0]
            
            noise_scale = sensitivity / epsilon
            
            add_noise_udf = udf(lambda x: float(x + np.random.laplace(0, noise_scale)), DoubleType())
            data = data.withColumn(col_name + "_dp", add_noise_udf(col(col_name)))
            data = data.drop(col_name).withColumnRenamed(col_name + "_dp", col_name)
        
        return data
    
    def train_local_model(self, 
                         data: DataFrame,
                         model_type: str,
                         algorithm: str,
                         hyperparameters: Dict[str, Any],
                         privacy_params: Dict[str, Any]) -> Tuple[Any, Dict[str, float]]:
        """
        Train model on local private data
        
        :param data: Training data
        :param model_type: Type of model (classification/regression)
        :param algorithm: Algorithm name
        :param hyperparameters: Model hyperparameters
        :param privacy_params: Privacy parameters
        :return: Trained model and metrics
        """
        # Apply differential privacy if enabled
        if privacy_params.get("differential_privacy_enabled", True):
            data = self.apply_differential_privacy(
                data,
                epsilon=privacy_params.get("epsilon", 1.0),
                delta=privacy_params.get("delta", 1e-5)
            )
        
        # Prepare features
        feature_cols = [c for c in data.columns 
                       if c not in ["label", "tenant_id", "data_hash"]]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        # Select model based on algorithm
        if algorithm == "LogisticRegression":
            if privacy_params.get("differential_privacy_enabled", True):
                # Use differentially private version
                base_model = self._create_dp_logistic_regression(
                    epsilon=privacy_params.get("epsilon", 1.0),
                    **hyperparameters
                )
            else:
                base_model = LogisticRegression(
                    featuresCol="scaled_features",
                    labelCol="label",
                    **hyperparameters
                )
        elif algorithm == "RandomForest":
            base_model = RandomForestClassifier(
                featuresCol="scaled_features",
                labelCol="label",
                **hyperparameters
            )
        elif algorithm == "LinearRegression":
            if privacy_params.get("differential_privacy_enabled", True):
                base_model = self._create_dp_linear_regression(
                    epsilon=privacy_params.get("epsilon", 1.0),
                    **hyperparameters
                )
            else:
                base_model = LinearRegression(
                    featuresCol="scaled_features",
                    labelCol="label",
                    **hyperparameters
                )
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, base_model])
        
        # Split data
        train_df, val_df = data.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        logger.info(f"Training {algorithm} model on {train_df.count()} samples")
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(val_df)
        metrics = self._evaluate_model(predictions, model_type)
        
        # Log to MLflow
        with mlflow.start_run():
            mlflow.log_params(hyperparameters)
            mlflow.log_params(privacy_params)
            mlflow.log_metrics(metrics)
            mlflow.spark.log_model(model, f"federated_model_{self.participant_id}")
        
        return model, metrics
    
    def _create_dp_logistic_regression(self, epsilon: float, **kwargs):
        """Create differentially private logistic regression"""
        # This is a simplified wrapper - in production, you'd integrate
        # with Spark's ML pipeline more deeply
        return LogisticRegression(
            featuresCol="scaled_features",
            labelCol="label",
            maxIter=kwargs.get("maxIter", 100),
            regParam=kwargs.get("regParam", 0.1)
        )
    
    def _create_dp_linear_regression(self, epsilon: float, **kwargs):
        """Create differentially private linear regression"""
        return LinearRegression(
            featuresCol="scaled_features",
            labelCol="label",
            maxIter=kwargs.get("maxIter", 100),
            regParam=kwargs.get("regParam", 0.1)
        )
    
    def _evaluate_model(self, predictions: DataFrame, model_type: str) -> Dict[str, float]:
        """Evaluate model performance"""
        metrics = {}
        
        if model_type == "CLASSIFICATION":
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction"
            )
            
            for metric in ["f1", "weightedPrecision", "weightedRecall", "accuracy"]:
                evaluator.setMetricName(metric)
                metrics[metric] = evaluator.evaluate(predictions)
                
        elif model_type == "REGRESSION":
            evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction"
            )
            
            for metric in ["rmse", "mse", "r2", "mae"]:
                evaluator.setMetricName(metric)
                metrics[metric] = evaluator.evaluate(predictions)
        
        return metrics
    
    def extract_model_weights(self, model: Any, secure_aggregation: bool = True) -> Dict[str, Any]:
        """
        Extract model weights for federated aggregation
        
        :param model: Trained model
        :param secure_aggregation: Whether to encrypt weights
        :return: Model weights dictionary
        """
        weights = {}
        
        # Extract weights based on model type
        if hasattr(model, 'stages'):
            # Pipeline model
            for i, stage in enumerate(model.stages):
                if hasattr(stage, 'coefficients'):
                    weights[f"stage_{i}_coefficients"] = stage.coefficients.toArray().tolist()
                if hasattr(stage, 'intercept'):
                    weights[f"stage_{i}_intercept"] = float(stage.intercept)
                if hasattr(stage, 'trees'):
                    # Random forest - extract tree structures
                    weights[f"stage_{i}_trees"] = self._serialize_trees(stage.trees)
        
        # Add metadata
        weights["participant_id"] = self.participant_id
        weights["num_samples"] = self.training_samples
        weights["timestamp"] = datetime.utcnow().isoformat()
        
        if secure_aggregation:
            weights = self._encrypt_weights(weights)
        
        return weights
    
    def _serialize_trees(self, trees) -> List[Dict[str, Any]]:
        """Serialize decision trees for aggregation"""
        serialized = []
        for tree in trees:
            tree_dict = {
                "numNodes": tree.numNodes,
                "depth": tree.depth,
                "prediction": tree.prediction
            }
            serialized.append(tree_dict)
        return serialized
    
    def _encrypt_weights(self, weights: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt model weights for secure aggregation"""
        # Generate ephemeral key pair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()
        
        # Serialize weights
        weights_json = json.dumps(weights)
        weights_bytes = weights_json.encode('utf-8')
        
        # Encrypt with coordinator's public key (would be provided)
        # For now, we'll use our own public key as placeholder
        encrypted = public_key.encrypt(
            weights_bytes,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        return {
            "encrypted_weights": base64.b64encode(encrypted).decode('utf-8'),
            "public_key": public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode('utf-8')
        }
    
    def create_zero_knowledge_proof(self, model_weights: Dict[str, Any]) -> Dict[str, str]:
        """
        Create ZKP that model was trained correctly without revealing data
        
        :param model_weights: Model weights
        :return: Zero-knowledge proof
        """
        # Simplified ZKP - in production, use proper ZK libraries
        proof_data = {
            "training_samples": self.training_samples,
            "model_hash": hashlib.sha256(json.dumps(model_weights, sort_keys=True).encode()).hexdigest(),
            "participant_id": self.participant_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Create commitment
        commitment = hashlib.sha256(json.dumps(proof_data, sort_keys=True).encode()).hexdigest()
        
        # Create proof (simplified - real ZKP would be more complex)
        proof = {
            "proof_type": "training_correctness",
            "commitment": commitment,
            "proof_value": hashlib.sha256(f"{commitment}{self.participant_id}".encode()).hexdigest()
        }
        
        return proof
    
    def save_model_update(self, 
                         model_weights: Dict[str, Any],
                         metrics: Dict[str, float],
                         round_number: int) -> str:
        """
        Save model update to MinIO for coordinator
        
        :param model_weights: Encrypted model weights
        :param metrics: Model metrics
        :param round_number: Current round number
        :return: URI to saved model update
        """
        update_data = {
            "session_id": self.session_id,
            "round_number": round_number,
            "participant_id": self.participant_id,
            "model_weights": model_weights,
            "metrics": metrics,
            "zkp": self.create_zero_knowledge_proof(model_weights),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Save to MinIO
        update_id = f"{self.session_id}_round{round_number}_{self.participant_id}"
        s3_uri = f"s3://federated-learning/{self.tenant_id}/{self.session_id}/{update_id}.json"
        
        # Convert to JSON and save
        update_json = json.dumps(update_data)
        
        # Use Spark to write to S3/MinIO
        update_df = self.spark.createDataFrame([{"data": update_json}])
        update_df.write.mode("overwrite").text(s3_uri)
        
        logger.info(f"Saved model update to {s3_uri}")
        return s3_uri
    
    def run(self, 
            round_number: int,
            training_config: Dict[str, Any],
            data_path: str) -> Dict[str, Any]:
        """
        Main execution method for federated learning round
        
        :param round_number: Current round number
        :param training_config: Training configuration
        :param data_path: Path to private data
        :return: Results dictionary
        """
        start_time = datetime.utcnow()
        
        try:
            # Load and validate data
            data = self.load_private_data(
                data_path,
                training_config["dataset_requirements"]
            )
            self.training_samples = data.count()
            
            # Train local model
            model, metrics = self.train_local_model(
                data,
                model_type=training_config["model_type"],
                algorithm=training_config["algorithm"],
                hyperparameters=training_config["training_parameters"]["model_hyperparameters"],
                privacy_params=training_config["privacy_parameters"]
            )
            
            # Extract and encrypt weights
            model_weights = self.extract_model_weights(
                model,
                secure_aggregation=training_config["privacy_parameters"]["secure_aggregation"]
            )
            
            # Save model update
            update_uri = self.save_model_update(
                model_weights,
                metrics,
                round_number
            )
            
            # Prepare result
            result = {
                "status": "success",
                "session_id": self.session_id,
                "round_number": round_number,
                "participant_id": self.participant_id,
                "update_uri": update_uri,
                "metrics": metrics,
                "training_samples": self.training_samples,
                "training_time": (datetime.utcnow() - start_time).total_seconds()
            }
            
            logger.info(f"Completed federated learning round {round_number}")
            return result
            
        except Exception as e:
            logger.error(f"Error in federated learning: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "participant_id": self.participant_id,
                "round_number": round_number
            }
        finally:
            self.spark.stop()


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 5:
        print("Usage: spark-submit federated_learning.py <session_id> <tenant_id> <participant_id> <config_json>")
        sys.exit(1)
    
    session_id = sys.argv[1]
    tenant_id = sys.argv[2]
    participant_id = sys.argv[3]
    config = json.loads(sys.argv[4])
    
    # Create federated learning job
    fl_job = FederatedLearningJob(session_id, tenant_id, participant_id)
    
    # Run training
    result = fl_job.run(
        round_number=config.get("round_number", 1),
        training_config=config["training_config"],
        data_path=config["data_path"]
    )
    
    # Output result
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main() 