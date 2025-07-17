"""
ML-based Anomaly Predictor Spark Job
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def anomaly_predictor_job():
    spark = SparkSession.builder.appName("AnomalyPredictor").getOrCreate()
    
    # Load historical data from MinIO
    data = spark.read.parquet("s3a://bucket/historical_resources/")
    
    # Feature engineering
    assembler = VectorAssembler(inputCols=["cpu", "memory", "events"], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    rf = RandomForestClassifier(featuresCol="scaled_features", labelCol="anomaly_label")
    
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    model = pipeline.fit(data)
    
    # Predict on new data
    new_data = spark.read.parquet("s3a://bucket/new_resources/")
    predictions = model.transform(new_data)
    
    # Evaluate
    evaluator = MulticlassClassificationEvaluator(labelCol="anomaly_label", metricName="f1")
    f1 = evaluator.evaluate(predictions)
    
    # Save predictions to Ignite or Pulsar
    predictions.write.mode("append").parquet("s3a://bucket/predictions/")
    
    spark.stop()

if __name__ == "__main__":
    anomaly_predictor_job() 