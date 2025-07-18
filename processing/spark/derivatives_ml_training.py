from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import mlflow
import mlflow.spark
from datetime import datetime, timedelta
import numpy as np

class DerivativesMLTrainingJob:
    """
    Spark-based ML training for derivatives platform
    Trains models for:
    1. Price prediction (next 1h, 4h, 24h)
    2. Volatility forecasting
    3. Liquidation risk assessment
    4. Funding rate prediction
    5. Market anomaly detection
    """
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DerivativesMLTraining") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.avro.compression.codec", "snappy") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        
        # MLflow configuration
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("derivatives-ml-models")
        
    def train_price_prediction_model(self):
        """
        Train models to predict future prices using historical data
        """
        with mlflow.start_run(run_name="price-prediction-training"):
            # Read historical trade data from Cassandra
            trades_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="trades", keyspace="platformq") \
                .load()
                
            # Read market metrics from Elasticsearch
            metrics_df = self.spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", "derivative-metrics/doc") \
                .option("es.query", '{"range": {"timestamp": {"gte": "now-90d"}}}') \
                .load()
            
            # Feature engineering
            features_df = self._create_price_features(trades_df, metrics_df)
            
            # Split data
            train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
            
            # Log dataset info
            mlflow.log_param("train_samples", train_df.count())
            mlflow.log_param("test_samples", test_df.count())
            mlflow.log_param("features", features_df.columns)
            
            # Train multiple models for different time horizons
            horizons = ["1h", "4h", "24h"]
            models = {}
            
            for horizon in horizons:
                print(f"Training model for {horizon} price prediction...")
                
                # Prepare features and labels
                feature_cols = [col for col in features_df.columns 
                               if col not in [f"price_{h}" for h in horizons] + ["market_id", "timestamp"]]
                
                assembler = VectorAssembler(
                    inputCols=feature_cols,
                    outputCol="features"
                )
                
                scaler = StandardScaler(
                    inputCol="features",
                    outputCol="scaled_features"
                )
                
                # Gradient Boosted Trees for better performance
                gbt = GBTRegressor(
                    labelCol=f"price_{horizon}",
                    featuresCol="scaled_features",
                    maxDepth=6,
                    maxBins=32,
                    maxIter=100,
                    subsamplingRate=0.8
                )
                
                # Create pipeline
                pipeline = Pipeline(stages=[assembler, scaler, gbt])
                
                # Hyperparameter tuning
                paramGrid = ParamGridBuilder() \
                    .addGrid(gbt.maxDepth, [4, 6, 8]) \
                    .addGrid(gbt.subsamplingRate, [0.7, 0.8, 0.9]) \
                    .build()
                
                evaluator = RegressionEvaluator(
                    labelCol=f"price_{horizon}",
                    predictionCol="prediction",
                    metricName="rmse"
                )
                
                cv = CrossValidator(
                    estimator=pipeline,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=5,
                    parallelism=4
                )
                
                # Train model
                model = cv.fit(train_df)
                models[horizon] = model
                
                # Evaluate
                predictions = model.transform(test_df)
                rmse = evaluator.evaluate(predictions)
                mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
                r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
                
                # Log metrics
                mlflow.log_metric(f"rmse_{horizon}", rmse)
                mlflow.log_metric(f"mae_{horizon}", mae)
                mlflow.log_metric(f"r2_{horizon}", r2)
                
                # Feature importance
                feature_importance = self._get_feature_importance(
                    model.bestModel.stages[-1],
                    feature_cols
                )
                mlflow.log_dict(feature_importance, f"feature_importance_{horizon}.json")
                
                # Save model
                model_path = f"models/price_prediction_{horizon}"
                mlflow.spark.save_model(model, model_path)
                
            return models
    
    def train_volatility_model(self):
        """
        Train GARCH-style model for volatility forecasting
        """
        with mlflow.start_run(run_name="volatility-forecasting"):
            # Read OHLCV data
            ohlcv_df = self._load_ohlcv_data()
            
            # Calculate returns and squared returns
            window_spec = Window.partitionBy("market_id").orderBy("timestamp")
            
            volatility_df = ohlcv_df \
                .withColumn("returns", (col("close") - lag("close", 1).over(window_spec)) / lag("close", 1).over(window_spec)) \
                .withColumn("squared_returns", col("returns") ** 2) \
                .withColumn("log_returns", log(col("close") / lag("close", 1).over(window_spec))) \
                .na.drop()
            
            # Calculate historical volatility features
            for window in [24, 48, 168]:  # 1 day, 2 days, 1 week
                volatility_df = volatility_df.withColumn(
                    f"volatility_{window}h",
                    stddev(col("returns")).over(
                        window_spec.rowsBetween(-window, -1)
                    ) * sqrt(lit(24))  # Annualized
                )
            
            # Additional features
            volatility_df = volatility_df \
                .withColumn("volume_ratio", col("volume") / avg("volume").over(
                    window_spec.rowsBetween(-168, -1)
                )) \
                .withColumn("price_range", (col("high") - col("low")) / col("close")) \
                .withColumn("hour_of_day", hour("timestamp")) \
                .withColumn("day_of_week", dayofweek("timestamp"))
            
            # Target: realized volatility over next 24 hours
            volatility_df = volatility_df.withColumn(
                "target_volatility",
                stddev(col("returns")).over(
                    window_spec.rowsBetween(1, 24)
                ) * sqrt(lit(24))
            ).na.drop()
            
            # Feature preparation
            feature_cols = [
                "volatility_24h", "volatility_48h", "volatility_168h",
                "volume_ratio", "price_range", "hour_of_day", "day_of_week"
            ]
            
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # Random Forest for volatility prediction
            rf = RandomForestRegressor(
                labelCol="target_volatility",
                featuresCol="features",
                numTrees=200,
                maxDepth=10,
                subsamplingRate=0.8
            )
            
            pipeline = Pipeline(stages=[assembler, rf])
            
            # Train/test split
            train_df, test_df = volatility_df.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            model = pipeline.fit(train_df)
            
            # Evaluate
            predictions = model.transform(test_df)
            evaluator = RegressionEvaluator(
                labelCol="target_volatility",
                predictionCol="prediction"
            )
            
            rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
            mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
            
            mlflow.log_metric("volatility_rmse", rmse)
            mlflow.log_metric("volatility_mae", mae)
            
            # Save model
            mlflow.spark.save_model(model, "models/volatility_forecasting")
            
            return model
    
    def train_liquidation_risk_model(self):
        """
        Train model to assess liquidation risk for positions
        """
        with mlflow.start_run(run_name="liquidation-risk-assessment"):
            # Load position and liquidation data
            positions_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="positions", keyspace="platformq") \
                .load()
                
            liquidations_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="liquidations", keyspace="platformq") \
                .load()
            
            # Create labels (1 if liquidated within 24h, 0 otherwise)
            labeled_df = self._create_liquidation_labels(positions_df, liquidations_df)
            
            # Feature engineering
            features_df = labeled_df \
                .withColumn("health_factor", 
                    when(col("side") == "LONG",
                        (col("mark_price") - col("liquidation_price")) / 
                        (col("entry_price") - col("liquidation_price"))
                    ).otherwise(
                        (col("liquidation_price") - col("mark_price")) / 
                        (col("liquidation_price") - col("entry_price"))
                    )
                ) \
                .withColumn("distance_to_liquidation",
                    abs(col("mark_price") - col("liquidation_price")) / col("mark_price")
                ) \
                .withColumn("position_size_ratio", 
                    col("position_size") / col("market_open_interest")
                ) \
                .withColumn("leverage_bucket", 
                    when(col("leverage") <= 5, "low")
                    .when(col("leverage") <= 20, "medium")
                    .otherwise("high")
                ) \
                .withColumn("volatility_adjusted_distance",
                    col("distance_to_liquidation") / col("market_volatility")
                )
            
            # Prepare features
            feature_cols = [
                "health_factor", "distance_to_liquidation", "leverage",
                "position_size_ratio", "market_volatility", 
                "volatility_adjusted_distance", "collateral_tier_encoded"
            ]
            
            # Handle categorical variables
            from pyspark.ml.feature import StringIndexer, OneHotEncoder
            
            indexer = StringIndexer(
                inputCol="leverage_bucket",
                outputCol="leverage_bucket_indexed"
            )
            
            encoder = OneHotEncoder(
                inputCols=["leverage_bucket_indexed"],
                outputCols=["leverage_bucket_encoded"]
            )
            
            assembler = VectorAssembler(
                inputCols=feature_cols + ["leverage_bucket_encoded"],
                outputCol="features"
            )
            
            # Random Forest Classifier
            rf = RandomForestClassifier(
                labelCol="is_liquidated",
                featuresCol="features",
                numTrees=300,
                maxDepth=15,
                featureSubsetStrategy="sqrt"
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])
            
            # Train/test split with stratification
            train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
            
            # Handle class imbalance
            positive_count = train_df.filter(col("is_liquidated") == 1).count()
            total_count = train_df.count()
            balance_ratio = positive_count / total_count
            
            # Train with class weights
            model = pipeline.fit(train_df, 
                {rf.weightCol: when(col("is_liquidated") == 1, 1.0/balance_ratio).otherwise(1.0)}
            )
            
            # Evaluate
            predictions = model.transform(test_df)
            
            # Binary classification metrics
            evaluator = BinaryClassificationEvaluator(
                labelCol="is_liquidated",
                rawPredictionCol="rawPrediction"
            )
            
            auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
            pr_auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderPR"})
            
            # Calculate precision/recall at different thresholds
            metrics_df = predictions.select("probability", "is_liquidated") \
                .rdd.flatMap(lambda row: [(float(row.probability[1]), float(row.is_liquidated))]) \
                .toDF(["probability", "label"])
            
            # Log metrics
            mlflow.log_metric("liquidation_auc", auc)
            mlflow.log_metric("liquidation_pr_auc", pr_auc)
            mlflow.log_param("class_balance_ratio", balance_ratio)
            
            # Feature importance
            feature_importance = self._get_feature_importance(
                model.stages[-1],
                feature_cols + ["leverage_bucket_encoded"]
            )
            mlflow.log_dict(feature_importance, "liquidation_feature_importance.json")
            
            # Save model
            mlflow.spark.save_model(model, "models/liquidation_risk_assessment")
            
            return model
    
    def train_funding_rate_model(self):
        """
        Train model to predict funding rates based on market conditions
        """
        with mlflow.start_run(run_name="funding-rate-prediction"):
            # Load funding rate history
            funding_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="funding_rates", keyspace="platformq") \
                .load()
            
            # Calculate features
            window_spec = Window.partitionBy("market_id").orderBy("timestamp")
            
            features_df = funding_df \
                .withColumn("premium_index", 
                    (col("mark_price") - col("index_price")) / col("index_price")
                ) \
                .withColumn("open_interest_ratio",
                    col("open_interest_long") / (col("open_interest_long") + col("open_interest_short"))
                ) \
                .withColumn("volume_imbalance",
                    (col("buy_volume") - col("sell_volume")) / (col("buy_volume") + col("sell_volume"))
                ) \
                .withColumn("funding_rate_ma_8h",
                    avg("funding_rate").over(window_spec.rowsBetween(-8, -1))
                ) \
                .withColumn("funding_rate_ma_24h",
                    avg("funding_rate").over(window_spec.rowsBetween(-24, -1))
                ) \
                .withColumn("volatility",
                    stddev("mark_price").over(window_spec.rowsBetween(-24, -1)) / avg("mark_price").over(window_spec.rowsBetween(-24, -1))
                ) \
                .na.drop()
            
            # Target: next funding rate
            features_df = features_df.withColumn(
                "next_funding_rate",
                lead("funding_rate", 1).over(window_spec)
            ).na.drop()
            
            # Feature columns
            feature_cols = [
                "premium_index", "open_interest_ratio", "volume_imbalance",
                "funding_rate_ma_8h", "funding_rate_ma_24h", "volatility"
            ]
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # GBT Regressor for funding rate prediction
            gbt = GBTRegressor(
                labelCol="next_funding_rate",
                featuresCol="features",
                maxDepth=5,
                maxIter=100
            )
            
            pipeline = Pipeline(stages=[assembler, gbt])
            
            # Train/test split
            train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            model = pipeline.fit(train_df)
            
            # Evaluate
            predictions = model.transform(test_df)
            evaluator = RegressionEvaluator(
                labelCol="next_funding_rate",
                predictionCol="prediction"
            )
            
            rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
            mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
            r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
            
            # Log metrics
            mlflow.log_metric("funding_rmse", rmse)
            mlflow.log_metric("funding_mae", mae)
            mlflow.log_metric("funding_r2", r2)
            
            # Save model
            mlflow.spark.save_model(model, "models/funding_rate_prediction")
            
            return model
    
    def train_anomaly_detection_model(self):
        """
        Train unsupervised model for market anomaly detection
        """
        with mlflow.start_run(run_name="anomaly-detection"):
            # Load market metrics
            metrics_df = self._load_market_metrics()
            
            # Feature engineering for anomaly detection
            features_df = metrics_df \
                .withColumn("volume_zscore",
                    (col("volume") - avg("volume").over(Window.partitionBy("market_id").rowsBetween(-168, -1))) /
                    stddev("volume").over(Window.partitionBy("market_id").rowsBetween(-168, -1))
                ) \
                .withColumn("price_change_zscore",
                    (col("price_change") - avg("price_change").over(Window.partitionBy("market_id").rowsBetween(-168, -1))) /
                    stddev("price_change").over(Window.partitionBy("market_id").rowsBetween(-168, -1))
                ) \
                .withColumn("spread_zscore",
                    (col("spread") - avg("spread").over(Window.partitionBy("market_id").rowsBetween(-168, -1))) /
                    stddev("spread").over(Window.partitionBy("market_id").rowsBetween(-168, -1))
                ) \
                .withColumn("trade_count_zscore",
                    (col("trade_count") - avg("trade_count").over(Window.partitionBy("market_id").rowsBetween(-168, -1))) /
                    stddev("trade_count").over(Window.partitionBy("market_id").rowsBetween(-168, -1))
                ) \
                .na.drop()
            
            # Feature columns for anomaly detection
            feature_cols = [
                "volume_zscore", "price_change_zscore", 
                "spread_zscore", "trade_count_zscore"
            ]
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # Use Isolation Forest for anomaly detection
            from pyspark.ml.feature import IsolationForest
            
            iso_forest = IsolationForest(
                featuresCol="features",
                predictionCol="anomaly",
                contamination=0.05,  # Expected 5% anomalies
                numTrees=100,
                maxSamples=256
            )
            
            pipeline = Pipeline(stages=[assembler, iso_forest])
            
            # Train model
            model = pipeline.fit(features_df)
            
            # Detect anomalies
            anomalies_df = model.transform(features_df)
            
            # Analyze anomaly patterns
            anomaly_stats = anomalies_df.groupBy("market_id", "anomaly") \
                .count() \
                .groupBy("market_id") \
                .pivot("anomaly") \
                .sum("count") \
                .fillna(0)
            
            # Log statistics
            total_records = features_df.count()
            anomaly_count = anomalies_df.filter(col("anomaly") == 1).count()
            anomaly_rate = anomaly_count / total_records
            
            mlflow.log_metric("total_records", total_records)
            mlflow.log_metric("anomaly_count", anomaly_count)
            mlflow.log_metric("anomaly_rate", anomaly_rate)
            
            # Save model
            mlflow.spark.save_model(model, "models/anomaly_detection")
            
            return model
    
    def _create_price_features(self, trades_df, metrics_df):
        """
        Create features for price prediction
        """
        # Aggregate trades to hourly OHLCV
        hourly_df = trades_df \
            .withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("market_id", "hour") \
            .agg(
                first("price").alias("open"),
                max("price").alias("high"),
                min("price").alias("low"),
                last("price").alias("close"),
                sum("size").alias("volume"),
                count("*").alias("trade_count")
            )
        
        # Technical indicators
        window_spec = Window.partitionBy("market_id").orderBy("hour")
        
        # Moving averages
        for period in [24, 48, 168]:  # 1 day, 2 days, 1 week
            hourly_df = hourly_df.withColumn(
                f"ma_{period}",
                avg("close").over(window_spec.rowsBetween(-period, -1))
            )
        
        # RSI
        hourly_df = hourly_df \
            .withColumn("price_change", col("close") - lag("close", 1).over(window_spec)) \
            .withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0)) \
            .withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0)) \
            .withColumn("avg_gain", avg("gain").over(window_spec.rowsBetween(-14, -1))) \
            .withColumn("avg_loss", avg("loss").over(window_spec.rowsBetween(-14, -1))) \
            .withColumn("rs", col("avg_gain") / col("avg_loss")) \
            .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
        
        # Bollinger Bands
        hourly_df = hourly_df \
            .withColumn("bb_middle", avg("close").over(window_spec.rowsBetween(-20, -1))) \
            .withColumn("bb_std", stddev("close").over(window_spec.rowsBetween(-20, -1))) \
            .withColumn("bb_upper", col("bb_middle") + 2 * col("bb_std")) \
            .withColumn("bb_lower", col("bb_middle") - 2 * col("bb_std"))
        
        # Volume features
        hourly_df = hourly_df \
            .withColumn("volume_ratio", col("volume") / avg("volume").over(window_spec.rowsBetween(-24, -1))) \
            .withColumn("trade_intensity", col("trade_count") / col("volume"))
        
        # Create target variables for different horizons
        for hours in [1, 4, 24]:
            hourly_df = hourly_df.withColumn(
                f"price_{hours}h",
                lead("close", hours).over(window_spec)
            )
        
        return hourly_df.na.drop()
    
    def _get_feature_importance(self, model, feature_cols):
        """
        Extract feature importance from tree-based models
        """
        if hasattr(model, 'featureImportances'):
            importances = model.featureImportances.toArray()
            feature_importance = {
                feature_cols[i]: float(importances[i]) 
                for i in range(len(feature_cols))
            }
            # Sort by importance
            feature_importance = dict(sorted(
                feature_importance.items(), 
                key=lambda x: x[1], 
                reverse=True
            ))
            return feature_importance
        return {}
    
    def _load_ohlcv_data(self):
        """Load OHLCV data from data lake"""
        return self.spark.read \
            .format("delta") \
            .load("s3a://platformq-data-lake/derivatives/ohlcv")
    
    def _load_market_metrics(self):
        """Load market metrics from data lake"""
        return self.spark.read \
            .format("parquet") \
            .load("s3a://platformq-data-lake/derivatives/market_metrics")
    
    def _create_liquidation_labels(self, positions_df, liquidations_df):
        """Create labels for liquidation prediction"""
        # Join positions with liquidations
        labeled_df = positions_df.alias("p") \
            .join(
                liquidations_df.alias("l"),
                (col("p.position_id") == col("l.position_id")) &
                (col("l.timestamp") <= col("p.timestamp") + expr("INTERVAL 24 HOURS")),
                "left"
            ) \
            .withColumn("is_liquidated", 
                when(col("l.position_id").isNotNull(), 1).otherwise(0)
            ) \
            .select("p.*", "is_liquidated")
        
        return labeled_df
    
    def run_all_training_jobs(self):
        """
        Run all ML training jobs
        """
        print("Starting ML training pipeline...")
        
        # Price prediction
        print("Training price prediction models...")
        price_models = self.train_price_prediction_model()
        
        # Volatility forecasting
        print("Training volatility model...")
        volatility_model = self.train_volatility_model()
        
        # Liquidation risk
        print("Training liquidation risk model...")
        liquidation_model = self.train_liquidation_risk_model()
        
        # Funding rate
        print("Training funding rate model...")
        funding_model = self.train_funding_rate_model()
        
        # Anomaly detection
        print("Training anomaly detection model...")
        anomaly_model = self.train_anomaly_detection_model()
        
        print("ML training pipeline completed!")
        
        # Register models in model registry
        for model_name, model in [
            ("price_prediction_1h", price_models.get("1h")),
            ("price_prediction_4h", price_models.get("4h")),
            ("price_prediction_24h", price_models.get("24h")),
            ("volatility_forecast", volatility_model),
            ("liquidation_risk", liquidation_model),
            ("funding_rate", funding_model),
            ("anomaly_detection", anomaly_model)
        ]:
            if model:
                mlflow.register_model(
                    f"models/{model_name}",
                    model_name
                )
        
        self.spark.stop()

if __name__ == "__main__":
    job = DerivativesMLTrainingJob()
    job.run_all_training_jobs() 