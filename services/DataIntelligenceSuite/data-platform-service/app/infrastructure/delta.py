"""
Delta Lake Infrastructure Client

Manages Delta Lake table operations
"""

class DeltaLakeClient:
    """Delta Lake client for table management"""
    
    def __init__(self, spark_client, warehouse_location: str):
        self.spark_client = spark_client
        self.warehouse_location = warehouse_location 