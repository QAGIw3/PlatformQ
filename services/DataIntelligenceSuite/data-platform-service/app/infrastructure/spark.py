"""
Spark Infrastructure Client

Manages interactions with Apache Spark
"""

class SparkClient:
    """Spark client for batch processing"""
    
    def __init__(self, master_url: str, app_name: str):
        self.master_url = master_url
        self.app_name = app_name 