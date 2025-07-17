"""Health check script for compute workers."""
import sys
import os
import psutil
import pulsar
import json
from minio import Minio

def check_health():
    """Check if worker is healthy."""
    try:
        # Check CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 90:
            print(f"High CPU usage: {cpu_percent}%")
            return False
        
        # Check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            print(f"High memory usage: {memory.percent}%")
            return False
        
        # Check Pulsar connection
        pulsar_url = os.getenv('PULSAR_URL', 'pulsar://localhost:6650')
        try:
            client = pulsar.Client(pulsar_url)
            client.close()
        except Exception as e:
            print(f"Pulsar connection failed: {e}")
            return False
        
        # Check MinIO connection
        minio_url = os.getenv('MINIO_URL', 'localhost:9000')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        
        try:
            client = Minio(
                minio_url,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=False
            )
            # Try to list buckets
            buckets = client.list_buckets()
        except Exception as e:
            print(f"MinIO connection failed: {e}")
            return False
        
        print("Health check passed")
        return True
        
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

if __name__ == "__main__":
    if check_health():
        sys.exit(0)
    else:
        sys.exit(1) 