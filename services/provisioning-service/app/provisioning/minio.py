from minio import Minio

def create_minio_bucket(minio_client: Minio, tenant_id: str):
    """
    Creates a new bucket for a tenant.
    """
    bucket_name = f"tenant-{tenant_id}"
    
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
    
    # Here you could also set a bucket policy to restrict access
    # to only the tenant's users. 