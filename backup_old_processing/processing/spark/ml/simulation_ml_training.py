"""
Spark ML Training Job for Simulation Optimization

Processes historical simulation data from MinIO/Data Lake for batch ML training
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType, TimestampType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import mlflow
import mlflow.spark
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationMLTrainer:
    """Spark-based ML trainer for simulation optimization"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.mlflow_tracking_uri = "http://mlflow:5000"
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        
    def load_simulation_data(self, 
                           data_path: str,
                           start_date: str,
                           end_date: str) -> DataFrame:
        """Load simulation data from data lake"""
        logger.info(f"Loading simulation data from {data_path}")
        
        # Read parquet files from MinIO/S3
        df = self.spark.read.parquet(f"{data_path}/silver/simulations/")
        
        # Filter by date range
        df = df.filter(
            (F.col("timestamp") >= start_date) & 
            (F.col("timestamp") <= end_date)
        )
        
        return df
    
    def prepare_features(self, df: DataFrame, model_type: str) -> DataFrame:
        """Prepare features for ML training"""
        logger.info(f"Preparing features for {model_type} model")
        
        if model_type == "agent_behavior":
            # Extract agent-level features
            features_df = df.select(
                "simulation_id",
                "timestamp",
                F.explode("agents").alias("agent")
            ).select(
                "simulation_id",
                "timestamp",
                F.col("agent.id").alias("agent_id"),
                F.col("agent.position.x").alias("pos_x"),
                F.col("agent.position.y").alias("pos_y"),
                F.col("agent.position.z").alias("pos_z"),
                F.col("agent.velocity.x").alias("vel_x"),
                F.col("agent.velocity.y").alias("vel_y"),
                F.col("agent.velocity.z").alias("vel_z"),
                F.col("agent.properties").alias("properties")
            )
            
            # Calculate aggregated features
            features_df = features_df.groupBy("simulation_id", "timestamp").agg(
                F.avg("pos_x").alias("avg_pos_x"),
                F.avg("pos_y").alias("avg_pos_y"),
                F.avg("pos_z").alias("avg_pos_z"),
                F.stddev("pos_x").alias("std_pos_x"),
                F.stddev("pos_y").alias("std_pos_y"),
                F.stddev("pos_z").alias("std_pos_z"),
                F.max(F.sqrt(F.pow("vel_x", 2) + F.pow("vel_y", 2) + F.pow("vel_z", 2))).alias("max_speed"),
                F.count("agent_id").alias("num_agents")
            )
            
        elif model_type == "parameter_tuning":
            # Extract parameter optimization features
            features_df = df.select(
                "simulation_id",
                "timestamp",
                F.col("parameters.time_step").alias("time_step"),
                F.col("parameters.gravity").alias("gravity"),
                F.col("parameters.friction").alias("friction"),
                F.col("parameters.damping").alias("damping"),
                F.col("parameters.spring_constant").alias("spring_constant"),
                F.col("metrics.convergence_rate").alias("convergence_rate"),
                F.col("metrics.total_energy").alias("total_energy"),
                F.col("metrics.objective_value").alias("objective_value")
            )
            
        elif model_type == "outcome_prediction":
            # Extract features for outcome prediction
            features_df = df.select(
                "simulation_id",
                "timestamp",
                F.col("parameters").alias("params"),
                F.col("initial_conditions").alias("init_cond"),
                F.col("metrics").alias("metrics")
            )
            
            # Flatten nested structures
            params_cols = ["time_step", "gravity", "friction", "damping"]
            for col_name in params_cols:
                features_df = features_df.withColumn(
                    f"param_{col_name}",
                    F.col(f"params.{col_name}")
                )
                
            metrics_cols = ["final_energy", "convergence_time", "stability_score"]
            for col_name in metrics_cols:
                features_df = features_df.withColumn(
                    f"metric_{col_name}",
                    F.col(f"metrics.{col_name}")
                )
                
            features_df = features_df.drop("params", "init_cond", "metrics")
            
        else:
            raise ValueError(f"Unknown model type: {model_type}")
            
        return features_df
    
    def create_ml_pipeline(self, 
                         feature_cols: List[str],
                         label_col: str,
                         model_type: str) -> Pipeline:
        """Create ML pipeline with feature engineering and model"""
        
        # Vector assembler
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        # Feature scaling
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features_scaled",
            withStd=True,
            withMean=True
        )
        
        # PCA for dimensionality reduction (optional)
        pca = PCA(
            k=min(len(feature_cols), 20),
            inputCol="features_scaled",
            outputCol="features"
        )
        
        # Select model based on type
        if model_type in ["agent_behavior", "parameter_tuning"]:
            # Regression models
            model = GBTRegressor(
                featuresCol="features",
                labelCol=label_col,
                maxDepth=10,
                maxBins=32,
                maxIter=20,
                seed=42
            )
        else:
            # Use random forest for outcome prediction
            model = RandomForestRegressor(
                featuresCol="features",
                labelCol=label_col,
                numTrees=100,
                maxDepth=10,
                seed=42
            )
            
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, pca, model])
        
        return pipeline
    
    def train_model(self,
                   train_df: DataFrame,
                   test_df: DataFrame,
                   feature_cols: List[str],
                   label_col: str,
                   model_type: str,
                   experiment_name: str) -> Dict[str, Any]:
        """Train ML model with MLflow tracking"""
        
        # Set up MLflow experiment
        mlflow.set_experiment(experiment_name)
        
        with mlflow.start_run():
            # Log parameters
            mlflow.log_param("model_type", model_type)
            mlflow.log_param("num_features", len(feature_cols))
            mlflow.log_param("train_size", train_df.count())
            mlflow.log_param("test_size", test_df.count())
            
            # Create and train pipeline
            pipeline = self.create_ml_pipeline(feature_cols, label_col, model_type)
            
            # Cross-validation for hyperparameter tuning
            if model_type in ["agent_behavior", "parameter_tuning"]:
                paramGrid = ParamGridBuilder() \
                    .addGrid(pipeline.getStages()[-1].maxDepth, [5, 10, 15]) \
                    .addGrid(pipeline.getStages()[-1].maxIter, [10, 20, 30]) \
                    .build()
                    
                evaluator = RegressionEvaluator(
                    labelCol=label_col,
                    predictionCol="prediction",
                    metricName="rmse"
                )
                
                cv = CrossValidator(
                    estimator=pipeline,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3,
                    seed=42
                )
                
                # Train model
                model = cv.fit(train_df)
                best_model = model.bestModel
                
            else:
                # Direct training without cross-validation
                model = pipeline.fit(train_df)
                best_model = model
                
            # Make predictions
            predictions = best_model.transform(test_df)
            
            # Evaluate model
            if model_type in ["agent_behavior", "parameter_tuning"]:
                evaluator = RegressionEvaluator(
                    labelCol=label_col,
                    predictionCol="prediction"
                )
                
                rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
                mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
                r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
                
                metrics = {
                    "rmse": rmse,
                    "mae": mae,
                    "r2": r2
                }
                
                # Log metrics
                mlflow.log_metrics(metrics)
                
            else:
                # For classification tasks
                evaluator = MulticlassClassificationEvaluator(
                    labelCol=label_col,
                    predictionCol="prediction"
                )
                
                accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
                f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
                
                metrics = {
                    "accuracy": accuracy,
                    "f1": f1
                }
                
                mlflow.log_metrics(metrics)
                
            # Log model
            mlflow.spark.log_model(
                best_model,
                "model",
                registered_model_name=f"simulation_{model_type}_model"
            )
            
            # Save feature importance if available
            if hasattr(best_model.stages[-1], "featureImportances"):
                importances = best_model.stages[-1].featureImportances
                feature_importance = {
                    feature_cols[i]: float(importances[i]) 
                    for i in range(len(feature_cols))
                }
                mlflow.log_dict(feature_importance, "feature_importance.json")
                
            return {
                "model": best_model,
                "metrics": metrics,
                "run_id": mlflow.active_run().info.run_id
            }
    
    def batch_predict(self,
                     model,
                     data_df: DataFrame,
                     output_path: str):
        """Perform batch predictions and save results"""
        logger.info("Performing batch predictions")
        
        predictions = model.transform(data_df)
        
        # Select relevant columns
        result_df = predictions.select(
            "simulation_id",
            "timestamp",
            "prediction",
            *[col for col in predictions.columns if col.startswith("param_")]
        )
        
        # Save predictions
        result_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Predictions saved to {output_path}")
        
        return result_df
    
    def federated_aggregation(self,
                            local_models: List[Dict[str, Any]],
                            aggregation_weights: Optional[List[float]] = None) -> Any:
        """Aggregate models from federated learning participants"""
        if not local_models:
            raise ValueError("No models to aggregate")
            
        if aggregation_weights is None:
            # Equal weights
            aggregation_weights = [1.0 / len(local_models)] * len(local_models)
            
        # For tree-based models, we can't directly average
        # Instead, create an ensemble
        logger.info(f"Creating ensemble from {len(local_models)} models")
        
        # This is a simplified approach - in practice, you'd implement
        # more sophisticated aggregation strategies
        return local_models  # Return as ensemble
    
    def export_for_deployment(self,
                            model,
                            model_type: str,
                            output_path: str):
        """Export model for deployment in simulation service"""
        logger.info(f"Exporting {model_type} model for deployment")
        
        # Save model in format compatible with simulation service
        model_metadata = {
            "model_type": model_type,
            "created_at": datetime.utcnow().isoformat(),
            "spark_version": self.spark.version,
            "feature_columns": model.stages[0].getInputCols() if hasattr(model.stages[0], 'getInputCols') else []
        }
        
        # Save metadata
        with open(f"{output_path}/metadata.json", "w") as f:
            json.dump(model_metadata, f)
            
        # Save model
        model.write().overwrite().save(f"{output_path}/spark_model")
        
        logger.info(f"Model exported to {output_path}")


def main():
    """Main entry point for Spark ML training job"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SimulationMLTraining") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
        
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Initialize trainer
    trainer = SimulationMLTrainer(spark)
    
    # Configuration (would come from job parameters)
    config = {
        "data_path": "s3a://platformq-datalake",
        "start_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "end_date": datetime.now().strftime("%Y-%m-%d"),
        "model_type": "parameter_tuning",
        "experiment_name": "simulation_optimization",
        "output_path": "s3a://platformq-models/simulation"
    }
    
    try:
        # Load data
        df = trainer.load_simulation_data(
            config["data_path"],
            config["start_date"],
            config["end_date"]
        )
        
        # Prepare features
        features_df = trainer.prepare_features(df, config["model_type"])
        
        # Define feature columns and label
        if config["model_type"] == "parameter_tuning":
            feature_cols = [
                "time_step", "gravity", "friction", "damping", "spring_constant"
            ]
            label_col = "convergence_rate"
        else:
            feature_cols = [col for col in features_df.columns 
                          if col not in ["simulation_id", "timestamp", "convergence_rate"]]
            label_col = "objective_value"
            
        # Split data
        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        result = trainer.train_model(
            train_df,
            test_df,
            feature_cols,
            label_col,
            config["model_type"],
            config["experiment_name"]
        )
        
        logger.info(f"Training completed. Metrics: {result['metrics']}")
        
        # Export model
        trainer.export_for_deployment(
            result["model"],
            config["model_type"],
            config["output_path"]
        )
        
    except Exception as e:
        logger.error(f"Error in ML training job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 