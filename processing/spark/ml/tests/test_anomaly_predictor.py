import unittest
from pyspark.sql import SparkSession
from processing.spark.ml.anomaly_predictor import anomaly_predictor_job

from unittest.mock import MagicMock

from processing.spark.ml import anomaly_predictor

class TestAnomalyPredictor(unittest.TestCase):
    def test_job_runs(self):
        SparkSession = MagicMock()
        spark = SparkSession.builder.getOrCreate()
        spark.read.parquet.return_value = MagicMock()
        anomaly_predictor_job()
        self.assertTrue(spark.stop.called) 