"""
Flink Infrastructure Client

Manages interactions with Apache Flink
"""

class FlinkClient:
    """Flink client for stream processing"""
    
    def __init__(self, job_manager_url: str):
        self.job_manager_url = job_manager_url 