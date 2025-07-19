import asyncio
from minio import Minio
from minio.error import S3Error
from fastapi import UploadFile, HTTPException
from typing import AsyncGenerator

from .base import BaseStorage

class MinIOStorage(BaseStorage):
    """
    MinIO storage backend.
    """
    def __init__(self, host, access_key, secret_key):
        self._client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

    async def _ensure_bucket_exists(self, bucket_name: str):
        def _check_bucket():
            found = self._client.bucket_exists(bucket_name)
            if not found:
                self._client.make_bucket(bucket_name)
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _check_bucket)


    async def upload(self, file: UploadFile, tenant_id: str) -> str:
        """
        Uploads a file to a tenant-specific bucket in MinIO.
        Returns the object name (filename).
        """
        try:
            bucket_name = tenant_id
            object_name = file.filename
            await self._ensure_bucket_exists(bucket_name)

            def _upload():
                self._client.put_object(
                    bucket_name,
                    object_name,
                    file.file,
                    length=-1, 
                    part_size=10*1024*1024
                )

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _upload)
            
            return object_name
        except S3Error as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload to MinIO: {e}")

    async def download(self, identifier: str, tenant_id: str) -> AsyncGenerator[bytes, None]:
        """
        Downloads a file from a tenant-specific bucket in MinIO.
        Identifier is the object name (filename).
        """
        try:
            bucket_name = tenant_id
            
            def _download():
                return self._client.get_object(bucket_name, identifier)

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, _download)
            
            for data in response.stream(32*1024):
                yield data
        except S3Error as e:
            raise HTTPException(status_code=404, detail=f"Failed to download from MinIO: {e}")
        finally:
            if 'response' in locals() and response:
                response.close()
                response.release_conn() 