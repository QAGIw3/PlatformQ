"""
Digital Asset Classifier using Spark MLlib

This module implements a machine learning pipeline for classifying
digital assets based on their metadata and characteristics.
"""

import sys
import json
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, udf, lit, array, struct
from pyspark.sql.types import StringType, DoubleType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler,
    HashingTF, IDF, Word2Vec, OneHotEncoder
)
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark

logger = logging.getLogger(__name__)


class AssetClassifierPipeline:
    """ML Pipeline for digital asset classification"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with ML support"""
        return SparkSession.builder \
            .appName(f"AssetClassifier-{self.tenant_id}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def load_training_data(self, data_path: str) -> DataFrame:
        """
        Load training data from data lake
        
        Expected schema:
        - asset_id: string
        - file_size: long
        - file_extension: string
        - mime_type: string
        - creation_date: timestamp
        - metadata: map<string, string>
        - tags: array<string>
        - asset_type: string (label)
        """
        df = self.spark.read.parquet(data_path)
        
        # Filter by tenant
        df = df.filter(col("tenant_id") == self.tenant_id)
        
        # Extract features from metadata
        df = self._extract_metadata_features(df)
        
        return df
    
    def _extract_metadata_features(self, df: DataFrame) -> DataFrame:
        """Extract numerical features from metadata"""
        # Extract common metadata fields
        df = df.withColumn("has_description", 
                          when(col("metadata.description").isNotNull(), 1.0).otherwise(0.0))
        df = df.withColumn("has_author", 
                          when(col("metadata.author").isNotNull(), 1.0).otherwise(0.0))
        df = df.withColumn("metadata_count", 
                          udf(lambda m: len(m) if m else 0, DoubleType())(col("metadata")))
        
        # Extract file characteristics
        df = df.withColumn("file_size_mb", col("file_size") / (1024 * 1024))
        df = df.withColumn("is_large_file", when(col("file_size_mb") > 100, 1.0).otherwise(0.0))
        
        # Tag features
        df = df.withColumn("tag_count", 
                          udf(lambda t: len(t) if t else 0, DoubleType())(col("tags")))
        
        return df
    
    def create_pipeline(self, features_cols: List[str]) -> Pipeline:
        """
        Create ML pipeline for asset classification
        
        :param features_cols: List of feature column names
        :return: Spark ML Pipeline
        """
        stages = []
        
        # 1. String indexing for categorical features
        file_ext_indexer = StringIndexer(
            inputCol="file_extension",
            outputCol="file_ext_index",
            handleInvalid="keep"
        )
        stages.append(file_ext_indexer)
        
        mime_type_indexer = StringIndexer(
            inputCol="mime_type",
            outputCol="mime_type_index",
            handleInvalid="keep"
        )
        stages.append(mime_type_indexer)
        
        # 2. One-hot encoding
        ext_encoder = OneHotEncoder(
            inputCols=["file_ext_index"],
            outputCols=["file_ext_vec"]
        )
        stages.append(ext_encoder)
        
        mime_encoder = OneHotEncoder(
            inputCols=["mime_type_index"],
            outputCols=["mime_type_vec"]
        )
        stages.append(mime_encoder)
        
        # 3. Text features from tags
        tags_tf = HashingTF(
            inputCol="tags",
            outputCol="tags_raw_features",
            numFeatures=100
        )
        stages.append(tags_tf)
        
        tags_idf = IDF(
            inputCol="tags_raw_features",
            outputCol="tags_features"
        )
        stages.append(tags_idf)
        
        # 4. Assemble all features
        numerical_features = [
            "file_size_mb", "is_large_file", "has_description",
            "has_author", "metadata_count", "tag_count"
        ]
        
        vector_features = ["file_ext_vec", "mime_type_vec", "tags_features"]
        
        assembler = VectorAssembler(
            inputCols=numerical_features + vector_features,
            outputCol="raw_features"
        )
        stages.append(assembler)
        
        # 5. Scale features
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        stages.append(scaler)
        
        # 6. Label indexing
        label_indexer = StringIndexer(
            inputCol="asset_type",
            outputCol="label"
        )
        stages.append(label_indexer)
        
        # 7. Random Forest Classifier
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        stages.append(rf)
        
        # Create pipeline
        pipeline = Pipeline(stages=stages)
        
        return pipeline
    
    def train_model(self, 
                   training_data: DataFrame,
                   validation_split: float = 0.2) -> Tuple[Any, Dict[str, float]]:
        """
        Train the asset classifier model
        
        :param training_data: Training DataFrame
        :param validation_split: Validation data split ratio
        :return: Trained model and metrics
        """
        # Split data
        train_df, val_df = training_data.randomSplit(
            [1 - validation_split, validation_split],
            seed=42
        )
        
        # Log to MLflow
        mlflow.set_experiment(f"asset_classifier_{self.tenant_id}")
        
        with mlflow.start_run():
            # Log parameters
            mlflow.log_param("tenant_id", self.tenant_id)
            mlflow.log_param("training_samples", train_df.count())
            mlflow.log_param("validation_samples", val_df.count())
            
            # Create pipeline
            pipeline = self.create_pipeline([])
            
            # Hyperparameter tuning
            param_grid = ParamGridBuilder() \
                .addGrid(pipeline.getStages()[-1].numTrees, [50, 100, 200]) \
                .addGrid(pipeline.getStages()[-1].maxDepth, [5, 10, 15]) \
                .build()
            
            # Cross validation
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="f1"
            )
            
            cv = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=3,
                seed=42
            )
            
            # Train model
            logger.info("Training asset classifier model...")
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            
            # Evaluate on validation set
            predictions = best_model.transform(val_df)
            
            # Calculate metrics
            metrics = {}
            for metric in ["f1", "weightedPrecision", "weightedRecall", "accuracy"]:
                evaluator.setMetricName(metric)
                value = evaluator.evaluate(predictions)
                metrics[metric] = value
                mlflow.log_metric(metric, value)
            
            # Feature importance
            rf_model = best_model.stages[-1]
            feature_importance = rf_model.featureImportances.toArray()
            
            # Log model
            mlflow.spark.log_model(
                best_model,
                "asset_classifier",
                registered_model_name=f"asset_classifier_{self.tenant_id}"
            )
            
            logger.info(f"Model training completed. F1 Score: {metrics['f1']:.4f}")
            
            return best_model, metrics
    
    def predict(self, model: Any, data: DataFrame) -> DataFrame:
        """
        Make predictions on new data
        
        :param model: Trained model
        :param data: Data to predict
        :return: Predictions DataFrame
        """
        # Extract features
        data = self._extract_metadata_features(data)
        
        # Make predictions
        predictions = model.transform(data)
        
        # Add prediction confidence
        get_confidence = udf(lambda v: float(v.toArray().max()), DoubleType())
        predictions = predictions.withColumn(
            "confidence",
            get_confidence(col("probability"))
        )
        
        return predictions.select(
            "asset_id",
            "prediction",
            "confidence",
            "probability"
        )
    
    def analyze_feature_importance(self, model: Any) -> Dict[str, float]:
        """
        Analyze feature importance from the trained model
        
        :param model: Trained pipeline model
        :return: Feature importance dictionary
        """
        # Get Random Forest model from pipeline
        rf_model = model.stages[-1]
        
        # Get feature names from vector assembler
        assembler = None
        for stage in model.stages:
            if isinstance(stage, VectorAssembler):
                assembler = stage
                break
        
        if not assembler:
            return {}
        
        feature_names = assembler.getInputCols()
        importances = rf_model.featureImportances.toArray()
        
        # Create importance dictionary
        importance_dict = {}
        for name, importance in zip(feature_names, importances):
            if importance > 0:
                importance_dict[name] = float(importance)
        
        # Sort by importance
        sorted_importance = dict(
            sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        )
        
        return sorted_importance


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 5:
        print("Usage: spark-submit asset_classifier.py <tenant_id> <training_data> <model_name> <parameters>")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    training_data_path = sys.argv[2]
    model_name = sys.argv[3]
    parameters = json.loads(sys.argv[4])
    
    # Create classifier
    classifier = AssetClassifierPipeline(tenant_id)
    
    # Load data
    training_data = classifier.load_training_data(training_data_path)
    
    # Train model
    model, metrics = classifier.train_model(
        training_data,
        validation_split=parameters.get("validation_split", 0.2)
    )
    
    # Analyze feature importance
    importance = classifier.analyze_feature_importance(model)
    
    # Output results
    result = {
        "model_name": model_name,
        "tenant_id": tenant_id,
        "metrics": metrics,
        "feature_importance": importance,
        "training_completed": datetime.utcnow().isoformat()
    }
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main() 