"""
Apache Spark Client
"""

from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class SparkClient:
    """Client for Apache Spark operations"""
    
    def __init__(self, app_name: str = "DatasetMarketplace"):
        self.app_name = app_name
        self._spark: Optional[SparkSession] = None
    
    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session"""
        if not self._spark:
            self._spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
        return self._spark
    
    def read_dataset(self, path: str, format: str = "parquet") -> DataFrame:
        """Read dataset from storage"""
        return self.spark.read.format(format).load(path)
    
    def analyze_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze dataset quality"""
        total_rows = df.count()
        
        # Calculate completeness for each column
        completeness = {}
        for col in df.columns:
            non_null_count = df.filter(df[col].isNotNull()).count()
            completeness[col] = non_null_count / total_rows if total_rows > 0 else 0
        
        # Get basic statistics
        stats = df.describe().toPandas().to_dict('records')
        
        return {
            "total_rows": total_rows,
            "columns": df.columns,
            "completeness": completeness,
            "statistics": stats
        }
    
    def close(self):
        """Close Spark session"""
        if self._spark:
            self._spark.stop()
            self._spark = None 