"""
Apache Spark client for distributed ML training
"""
import logging
from typing import Dict, List, Optional, Any
import asyncio
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator

logger = logging.getLogger(__name__)


class SparkClient:
    """
    Client for Apache Spark ML operations
    """
    
    def __init__(self,
                 master: str,
                 app_name: str,
                 executor_memory: str = "4g",
                 executor_cores: int = 4):
        self.master = master
        self.app_name = app_name
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.spark: Optional[SparkSession] = None
        
    async def initialize(self):
        """Initialize Spark session"""
        try:
            loop = asyncio.get_event_loop()
            
            def _create_session():
                return SparkSession.builder \
                    .appName(self.app_name) \
                    .master(self.master) \
                    .config("spark.executor.memory", self.executor_memory) \
                    .config("spark.executor.cores", str(self.executor_cores)) \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                    .getOrCreate()
                    
            self.spark = await loop.run_in_executor(None, _create_session)
            logger.info(f"Spark session initialized: {self.app_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise
    
    async def read_data(self,
                       path: str,
                       format: str = "parquet",
                       **options) -> DataFrame:
        """Read data from storage"""
        loop = asyncio.get_event_loop()
        
        def _read_data():
            reader = self.spark.read.format(format)
            for key, value in options.items():
                reader = reader.option(key, value)
            return reader.load(path)
            
        return await loop.run_in_executor(None, _read_data)
    
    async def train_model(self,
                         train_df: DataFrame,
                         features_col: str,
                         label_col: str,
                         model_type: str,
                         hyperparameters: Dict[str, Any]) -> PipelineModel:
        """Train a Spark ML model"""
        loop = asyncio.get_event_loop()
        
        def _train_model():
            # Create vector assembler if needed
            if isinstance(train_df.schema[features_col].dataType, type(None)):
                feature_cols = [col for col in train_df.columns if col != label_col]
                assembler = VectorAssembler(
                    inputCols=feature_cols,
                    outputCol="features"
                )
                stages = [assembler]
                features_col = "features"
            else:
                stages = []
            
            # Add scaler
            scaler = StandardScaler(
                inputCol=features_col,
                outputCol="scaled_features",
                withStd=True,
                withMean=False
            )
            stages.append(scaler)
            
            # Create model based on type
            if model_type == "random_forest_classifier":
                model = RandomForestClassifier(
                    featuresCol="scaled_features",
                    labelCol=label_col,
                    **hyperparameters
                )
            elif model_type == "logistic_regression":
                model = LogisticRegression(
                    featuresCol="scaled_features",
                    labelCol=label_col,
                    **hyperparameters
                )
            elif model_type == "random_forest_regressor":
                model = RandomForestRegressor(
                    featuresCol="scaled_features",
                    labelCol=label_col,
                    **hyperparameters
                )
            elif model_type == "linear_regression":
                model = LinearRegression(
                    featuresCol="scaled_features",
                    labelCol=label_col,
                    **hyperparameters
                )
            else:
                raise ValueError(f"Unknown model type: {model_type}")
                
            stages.append(model)
            
            # Create and fit pipeline
            pipeline = Pipeline(stages=stages)
            return pipeline.fit(train_df)
            
        return await loop.run_in_executor(None, _train_model)
    
    async def evaluate_model(self,
                           model: PipelineModel,
                           test_df: DataFrame,
                           label_col: str,
                           is_classification: bool = True) -> Dict[str, float]:
        """Evaluate a trained model"""
        loop = asyncio.get_event_loop()
        
        def _evaluate_model():
            predictions = model.transform(test_df)
            
            metrics = {}
            if is_classification:
                evaluator = MulticlassClassificationEvaluator(
                    labelCol=label_col,
                    predictionCol="prediction"
                )
                metrics["accuracy"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "accuracy"}
                )
                metrics["f1"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "f1"}
                )
                metrics["weightedPrecision"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "weightedPrecision"}
                )
                metrics["weightedRecall"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "weightedRecall"}
                )
            else:
                evaluator = RegressionEvaluator(
                    labelCol=label_col,
                    predictionCol="prediction"
                )
                metrics["rmse"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "rmse"}
                )
                metrics["mse"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "mse"}
                )
                metrics["r2"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "r2"}
                )
                metrics["mae"] = evaluator.evaluate(
                    predictions, {evaluator.metricName: "mae"}
                )
                
            return metrics
            
        return await loop.run_in_executor(None, _evaluate_model)
    
    async def save_model(self, model: PipelineModel, path: str):
        """Save a Spark ML model"""
        loop = asyncio.get_event_loop()
        
        def _save_model():
            model.write().overwrite().save(path)
            
        await loop.run_in_executor(None, _save_model)
    
    async def load_model(self, path: str) -> PipelineModel:
        """Load a Spark ML model"""
        loop = asyncio.get_event_loop()
        
        def _load_model():
            return PipelineModel.load(path)
            
        return await loop.run_in_executor(None, _load_model)
    
    async def create_dataframe(self, 
                             data: List[Dict[str, Any]],
                             schema: Optional[Any] = None) -> DataFrame:
        """Create a Spark DataFrame from Python data"""
        loop = asyncio.get_event_loop()
        
        def _create_df():
            return self.spark.createDataFrame(data, schema=schema)
            
        return await loop.run_in_executor(None, _create_df)
    
    async def sql_query(self, query: str) -> DataFrame:
        """Execute a SQL query"""
        loop = asyncio.get_event_loop()
        
        def _sql_query():
            return self.spark.sql(query)
            
        return await loop.run_in_executor(None, _sql_query)
    
    async def cache_dataframe(self, df: DataFrame) -> DataFrame:
        """Cache a DataFrame in memory"""
        loop = asyncio.get_event_loop()
        
        def _cache_df():
            return df.cache()
            
        return await loop.run_in_executor(None, _cache_df)
    
    async def get_spark_context_status(self) -> Dict[str, Any]:
        """Get Spark context status"""
        loop = asyncio.get_event_loop()
        
        def _get_status():
            sc = self.spark.sparkContext
            return {
                "app_id": sc.applicationId,
                "app_name": sc.appName,
                "master": sc.master,
                "default_parallelism": sc.defaultParallelism,
                "spark_user": sc.sparkUser(),
                "version": sc.version,
                "is_stopped": sc._jsc.sc().isStopped()
            }
            
        return await loop.run_in_executor(None, _get_status)
    
    async def close(self):
        """Close Spark session"""
        if self.spark:
            loop = asyncio.get_event_loop()
            
            def _stop_spark():
                self.spark.stop()
                
            await loop.run_in_executor(None, _stop_spark)
            logger.info("Spark session closed") 